s3mirror
========

Look, sometimes you gotta mirror an S3 bucket to a local path. And sometimes
you *really* want your local copy to be updated as soon as a new object
arrives in that bucket.

    $ s3mirror -bucket my-bucket -local-path=/path/to/my-bucket

`s3mirror` will LIST the contents of an S3 bucket and compare it against a
local path. For any objects that exist on S3 that don't exist locally with the
same size and modification time, `s3mirror` will retrieve those objects and put
them on the local filesystem.

From there, you can [set up S3 notifications](http://docs.aws.amazon.com/AmazonS3/latest/dev/NotificationHowTo.html)
and have it publish to an SNS topic. From *there*, you can set up an SQS queue
subscribed to that topic, and tell `s3mirror -queue queuename`. In that event,
`s3mirror` will perform a sync and then long-poll the SQS queue, immediately
retrieving any new objects.
