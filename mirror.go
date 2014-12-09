package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"code.google.com/p/go-uuid/uuid"
	"github.com/crowdmob/goamz/s3"
	"github.com/crowdmob/goamz/sqs"
)

type Mirror struct {
	Bucket    *s3.Bucket
	Queue     *sqs.Queue
	LocalPath string
	Workers   int
}

func (m *Mirror) Poll() error {
	params := map[string]string{
		"MaxNumberOfMessages": strconv.Itoa(m.Workers),
		"VisibilityTimeout":   strconv.Itoa(60),
		"WaitTimeSeconds":     strconv.Itoa(20),
	}

	smr, err := m.Queue.ReceiveMessageWithParameters(params)
	if err != nil {
		return err
	}

	for _, msg := range smr.Messages {
		if err := m.handleMessage(msg); err != nil {
			return err
		} else if _, err := m.Queue.DeleteMessage(&msg); err != nil {
			return err
		}
	}
	return nil
}

func (m *Mirror) handleMessage(msg sqs.Message) error {
	var snsMsg struct {
		Type    string
		Subject string
		Message string
	}

	// unpack the message body
	if err := json.Unmarshal([]byte(msg.Body), &snsMsg); err != nil {
		log.Printf("error unmarshalling SNS message: %+q\nmsgattrs: %+v\nattrs: %+v\n\n", msg.Body, msg.MessageAttribute, msg.Attribute)
		return err
	}

	switch snsMsg.Subject {
	case "Amazon S3 Notification":
		return m.handleS3Message(snsMsg.Message)
	default:
		log.Printf("unhandled subject %q:\nmsg: %+q\nmsgattrs: %+v\nattrs: %+v\n\n", snsMsg.Subject, msg.Body, msg.MessageAttribute, msg.Attribute)
	}

	return nil
}

func (m *Mirror) handleS3Message(msg string) error {
	var s3msg struct {
		Records []struct {
			EventName string `json:"eventName"`

			S3 struct {
				Object struct {
					Key  string `json:"key"`
					Size int64  `json:"size"`
					ETag string `json:"eTag"`
				} `json:"object"`
			} `json:"s3"`
		}
	}

	if err := json.Unmarshal([]byte(msg), &s3msg); err != nil {
		log.Printf("error unmarshalling S3 message: %+q", msg)
		return err
	}

	if len(s3msg.Records) == 0 {
		log.Printf("S3 message is empty: %+q", msg)
		return fmt.Errorf("SQS S3 message contains no records")
	}

	for _, record := range s3msg.Records {
		key := s3.Key{
			Key:  record.S3.Object.Key,
			Size: record.S3.Object.Size,
			ETag: record.S3.Object.ETag,
		}

		if strings.HasPrefix(record.EventName, "ObjectCreated:") {
			// magic!
			// download the new key
			if err := m.syncKey(key); err != nil {
				return err
			}
		} else {
			// do nothing
			log.Printf("unhandled S3 event type: %q", record.EventName)
		}
	}

	return nil
}

func (m *Mirror) Sync() error {
	// TODO: delete local files that no longer exist in S3

	// set up a worker pool
	keyChan := make(chan s3.Key)
	errorChan := make(chan error)
	wg := &sync.WaitGroup{}
	for i := 0; i < m.Workers; i++ {
		wg.Add(1)
		go m.syncKeyWorker(keyChan, errorChan, wg)
	}

	// start listing
	wg.Add(1)
	go m.syncListWorker(keyChan, errorChan, wg)

	// close the error chan when the wait group is complete
	go func() {
		wg.Wait()
		close(errorChan)
	}()

	// return the first error once the error chan is closed (i.e. once the job is done)
	var firstError error
	for {
		if err, ok := <-errorChan; err != nil {
			firstError = err
		} else if !ok {
			break
		}
	}

	return firstError
}

func (m *Mirror) syncListWorker(keyChan chan<- s3.Key, errorChan chan<- error, wg *sync.WaitGroup) {
	defer wg.Done()
	defer close(keyChan)

	// list the bucket, iterating on marker
	marker := ""
	for {
		resp, err := m.Bucket.List("", "", marker, 500)
		if err != nil {
			errorChan <- err
			return
		}

		for _, key := range resp.Contents {
			// attempt to sync this key
			keyChan <- key
		}

		if resp.IsTruncated && marker != resp.NextMarker {
			// we need to make a subsequent request
			marker = resp.NextMarker
		} else {
			// done
			break
		}
	}
}

func (m *Mirror) syncKeyWorker(keyChan <-chan s3.Key, errorChan chan<- error, wg *sync.WaitGroup) {
	defer wg.Done()

	for key := range keyChan {
		if err := m.syncKey(key); err != nil {
			errorChan <- err
		}
	}
}

func (m *Mirror) syncKey(key s3.Key) error {
	localFile := filepath.Join(m.LocalPath, key.Key)

	// equality check
	if fi, err := os.Stat(localFile); err != nil {
		// problem stat()ing
		if os.IsNotExist(err) {
			// doesn't exist
			// download
			return m.downloadObject(key)
		} else {
			// some other error
			// bubble outwards
			return err
		}
	} else {
		// we stat()ed
		// compare fi against the S3 key

		// sanity check
		if fi.IsDir() {
			// boo
			return fmt.Errorf("expected %+q to be a file, is a directory", localFile)
		}

		if fi.Size() != key.Size {
			// download
			return m.downloadObject(key)
		}

		// do we have an mtime?
		if key.LastModified != "" {
			// parse it
			if keyMtime, err := time.Parse("2006-01-02T15:04:05.000Z", key.LastModified); err != nil {
				return err
			} else if keyMtime.UTC().Truncate(time.Second) == fi.ModTime().UTC().Truncate(time.Second) {
				// close enough!
			} else {
				// time is too different
				return m.downloadObject(key)
			}
		}

		// TODO: validate etags

		// good enough
		return nil
	}
}

func (m *Mirror) downloadObject(key s3.Key) error {
	localFile := filepath.Join(m.LocalPath, key.Key)

	// make a local tempfile
	tempfile := fmt.Sprintf("%s.tmp.%s", localFile, uuid.NewRandom().String())
	file, err := os.OpenFile(tempfile, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	defer os.Remove(tempfile)
	defer file.Close()

	// read the key
	log.Printf("downloading %q (ETag: %q)", key.Key, key.ETag)
	params := map[string][]string{"If-Match": []string{key.ETag}}
	rc, err := m.Bucket.GetResponseWithHeaders(key.Key, params)
	if err != nil {
		return err
	}
	defer rc.Body.Close()

	// get the mtime off the HTTP response
	mtime, err := time.Parse(time.RFC1123, rc.Header.Get("Last-Modified"))
	if err != nil {
		return err
	}

	// copy all the bits
	if _, err := io.Copy(file, rc.Body); err != nil {
		return err
	}

	// close all the things
	if err := file.Close(); err != nil {
		return err
	}

	// set the file's mtime
	os.Chtimes(file.Name(), mtime, mtime)

	// rename
	if err := os.Rename(file.Name(), localFile); err != nil {
		return err
	}

	// great success!
	return nil
}
