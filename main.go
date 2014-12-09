package main

import (
	"flag"
	"log"
	"os"
	"time"

	"github.com/crowdmob/goamz/aws"
	"github.com/crowdmob/goamz/s3"
	"github.com/crowdmob/goamz/sqs"
)

func main() {
	localPath := flag.String("local-path", "./mirror", "local path in which to mirror")

	awsAccessKeyId := flag.String("aws-access-key-id", "", "")
	awsSecretAccessKey := flag.String("aws-secret-access-key", "", "")
	regionName := flag.String("region", "us-east-1", "AWS region name")
	bucketName := flag.String("bucket", "", "S3 bucket name")
	queueName := flag.String("queue", "", "SQS queue name")

	flag.Parse()

	// look up region
	region, regionOk := aws.Regions[*regionName]
	if !regionOk {
		log.Fatalf("%q is not a valid region name", regionName)
	}

	// ensure the local path is a directory
	if fi, err := os.Stat(*localPath); err != nil {
		log.Fatal(err)
	} else if !fi.IsDir() {
		log.Fatalf("%q is not a directory", localPath)
	}

	// ensure we have AWS auth
	auth, err := aws.GetAuth(*awsAccessKeyId, *awsSecretAccessKey, "", time.Now())
	if err != nil {
		log.Fatal(err)
	}

	// set up clients
	s3c := s3.New(auth, region)
	sqc := sqs.New(auth, region)

	// set up the Mirror
	mirror := Mirror{
		Bucket:    s3c.Bucket(*bucketName),
		LocalPath: *localPath,
		Workers:   4,
	}

	// get the queue, which apparently can fail
	if queueName != nil && *queueName != "" {
		if queue, err := sqc.GetQueue(*queueName); err != nil {
			log.Fatal(err)
		} else {
			mirror.Queue = queue
		}
	}

	log.Printf("performing initial sync on S3 bucket %q", *bucketName)
	nextSync := time.Now().Add(6 * time.Hour)
	if err := mirror.Sync(); err != nil {
		log.Fatal(err)
	}

	if mirror.Queue != nil {
		log.Printf("long-polling SQS queue")
		for {
			if err := mirror.Poll(); err != nil {
				log.Fatal(err)
			}

			if time.Now().After(nextSync) {
				// sync again
				log.Printf("resyncing again")
				nextSync = time.Now().Add(6 * time.Hour)
				if err := mirror.Sync(); err != nil {
					log.Fatal(err)
				}
			}
		}
	}
}
