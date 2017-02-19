package main

import (
	"flag"
	"log"
	"os"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/sqs"
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
	sess, err := session.NewSession(&aws.Config{Region: aws.String(*regionName)})
	if err != nil {
		log.Fatalf("failed to create session for region %q", regionName, err)
	}

	// ensure the local path is a directory
	if fi, err := os.Stat(*localPath); err != nil {
		log.Fatal(err)
	} else if !fi.IsDir() {
		log.Fatalf("%q is not a directory", localPath)
	}

	// use any AWS auth we have
	auth := aws.NewConfig()
	if *awsAccessKeyId != "" && *awsSecretAccessKey != "" {
		auth = auth.WithCredentials(credentials.NewStaticCredentials(*awsAccessKeyId, *awsSecretAccessKey, ""))
	}

	// set up clients
	s3c := s3.New(sess, auth)
	sqc := sqs.New(sess, auth)

	// set up the Mirror
	mirror := Mirror{
		S3:         s3c,
		BucketName: *bucketName,
		SQS:        sqc,
		LocalPath:  *localPath,
		Workers:    4,
	}

	if *queueName != "" {
		// find the SQS queue
		getQueue := sqs.GetQueueUrlInput{
			QueueName: aws.String(*queueName),
		}
		if getQueueResp, err := sqc.GetQueueUrl(&getQueue); err != nil {
			log.Fatal(err)
		} else {
			mirror.QueueURL = *getQueueResp.QueueUrl
		}
	}

	log.Printf("performing initial sync on S3 bucket %q", mirror.BucketName)
	nextSync := time.Now().Add(6 * time.Hour)
	if err := mirror.Sync(); err != nil {
		log.Fatal(err)
	}

	if mirror.QueueURL != "" {
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
