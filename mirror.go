package main

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/sqs"
)

type Mirror struct {
	S3         *s3.S3
	BucketName string

	SQS      *sqs.SQS
	QueueURL string

	LocalPath string
	Workers   int
}

func (m *Mirror) Poll() error {
	req := sqs.ReceiveMessageInput{
		QueueUrl:            &m.QueueURL,
		MaxNumberOfMessages: aws.Int64(int64(m.Workers)),
		VisibilityTimeout:   aws.Int64(60),
		WaitTimeSeconds:     aws.Int64(20),
	}

	resp, err := m.SQS.ReceiveMessage(&req)
	if err != nil {
		return err
	}

	for _, msg := range resp.Messages {
		if err := m.handleMessage(msg); err != nil {
			return err
		}

		// handled, attempt to delete
		dreq := sqs.DeleteMessageInput{
			QueueUrl:      &m.QueueURL,
			ReceiptHandle: msg.ReceiptHandle,
		}
		if _, err := m.SQS.DeleteMessage(&dreq); err != nil {
			return err
		}
	}

	return nil
}

func (m *Mirror) handleMessage(msg *sqs.Message) error {
	var snsMsg struct {
		Type    string
		Subject string
		Message string
	}

	// unpack the message body
	if err := json.Unmarshal([]byte(*msg.Body), &snsMsg); err != nil {
		log.Printf("error unmarshalling SNS message: %+q\nmsgattrs: %+v\nattrs: %+v\n\n", msg.Body, msg.MessageAttributes, msg.Attributes)
		return err
	}

	switch snsMsg.Subject {
	case "Amazon S3 Notification":
		return m.handleS3Message(snsMsg.Message)
	default:
		log.Printf("unhandled subject %q:\nmsg: %+q\nmsgattrs: %+v\nattrs: %+v\n\n", snsMsg.Subject, msg.Body, msg.MessageAttributes, msg.Attributes)
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
		key := s3.Object{
			Key:  aws.String(record.S3.Object.Key),
			Size: aws.Int64(record.S3.Object.Size),
			ETag: aws.String(record.S3.Object.ETag),
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
	keyChan := make(chan s3.Object)
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

func (m *Mirror) syncListWorker(keyChan chan<- s3.Object, errorChan chan<- error, wg *sync.WaitGroup) {
	defer wg.Done()
	defer close(keyChan)

	// list the bucket, iterating on marker
	var marker *string
	for {
		req := s3.ListObjectsInput{
			Bucket:  aws.String(m.BucketName),
			Marker:  marker,
			MaxKeys: aws.Int64(500),
		}

		resp, err := m.S3.ListObjects(&req)
		if err != nil {
			errorChan <- err
			return
		}

		for _, key := range resp.Contents {
			// attempt to sync this key
			keyChan <- *key
		}

		if (resp.IsTruncated != nil && *resp.IsTruncated) && marker != resp.NextMarker {
			// we need to make a subsequent request
			marker = resp.NextMarker
		} else {
			// done
			break
		}
	}
}

func (m *Mirror) syncKeyWorker(keyChan <-chan s3.Object, errorChan chan<- error, wg *sync.WaitGroup) {
	defer wg.Done()

	for key := range keyChan {
		if err := m.syncKey(key); err != nil {
			errorChan <- err
		}
	}
}

func (m *Mirror) syncKey(key s3.Object) error {
	localFile := filepath.Join(m.LocalPath, *key.Key)

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

		if fi.Size() != *key.Size {
			// download
			return m.downloadObject(key)
		}

		// do we have an mtime?
		if key.LastModified != nil {
			if key.LastModified.UTC().Truncate(time.Second) == fi.ModTime().UTC().Truncate(time.Second) {
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

func (m *Mirror) downloadObject(key s3.Object) error {
	localFile := filepath.Join(m.LocalPath, *key.Key)

	// make a local tempfile
	tempfile, err := ioutil.TempFile(filepath.Dir(localFile), filepath.Base(localFile))
	if err != nil {
		return err
	}
	defer os.Remove(tempfile.Name())
	defer tempfile.Close()

	// read the key
	log.Printf("downloading %q (ETag: %q)", *key.Key, *key.ETag)
	getObject := s3.GetObjectInput{
		Bucket:  &m.BucketName,
		Key:     key.Key,
		IfMatch: key.ETag,
	}
	getObjectResp, err := m.S3.GetObject(&getObject)
	if err != nil {
		return err
	}
	defer getObjectResp.Body.Close()

	// get the mtime off the HTTP response
	if getObjectResp.LastModified == nil {
		log.Fatal("last-modified is nil")
	}
	mtime := *getObjectResp.LastModified

	// copy all the bits
	if _, err := io.Copy(tempfile, getObjectResp.Body); err != nil {
		return err
	}

	// close all the things
	if err := tempfile.Close(); err != nil {
		return err
	}

	// set the file's mtime
	os.Chtimes(tempfile.Name(), mtime, mtime)

	// set the permissions
	os.Chmod(tempfile.Name(), 0644)

	// rename into place
	if err := os.Rename(tempfile.Name(), localFile); err != nil {
		return err
	}

	// great success!
	return nil
}
