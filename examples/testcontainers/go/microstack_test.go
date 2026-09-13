package microstack_test

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

const microstackPort = "4566/tcp"

func TestMicroStack(t *testing.T) {
	image := os.Getenv("MICROSTACK_TEST_IMAGE")
	if image == "" {
		t.Skip("MICROSTACK_TEST_IMAGE is not set")
	}

	ctx := context.Background()
	container, err := testcontainers.Run(
		ctx,
		image,
		testcontainers.WithExposedPorts(microstackPort),
		testcontainers.WithWaitStrategy(
			wait.ForHTTP("/_microstack/health").
				WithPort(microstackPort).
				WithStartupTimeout(2*time.Minute),
		),
	)
	if err != nil {
		t.Fatalf("start MicroStack: %v", err)
	}
	t.Cleanup(func() {
		if err := container.Terminate(context.Background()); err != nil {
			t.Errorf("terminate MicroStack: %v", err)
		}
	})

	endpoint, err := container.PortEndpoint(ctx, microstackPort, "http")
	if err != nil {
		t.Fatalf("resolve mapped endpoint: %v", err)
	}

	cfg, err := config.LoadDefaultConfig(
		ctx,
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	if err != nil {
		t.Fatalf("load AWS configuration: %v", err)
	}

	sqsClient := sqs.NewFromConfig(cfg, func(options *sqs.Options) {
		options.BaseEndpoint = aws.String(endpoint)
	})
	s3Client := s3.NewFromConfig(cfg, func(options *s3.Options) {
		options.BaseEndpoint = aws.String(endpoint)
		options.UsePathStyle = true
	})

	testSQS(ctx, t, sqsClient, endpoint)
	testS3(ctx, t, s3Client)
}

func testSQS(ctx context.Context, t *testing.T, client *sqs.Client, endpoint string) {
	t.Helper()

	const (
		queueName = "testcontainers-queue"
		payload   = "hello from testcontainers-go"
	)

	created, err := client.CreateQueue(ctx, &sqs.CreateQueueInput{
		QueueName: aws.String(queueName),
	})
	if err != nil {
		t.Fatalf("create queue: %v", err)
	}
	queueURL := aws.ToString(created.QueueUrl)
	assertSameAuthority(t, queueURL, endpoint)

	resolved, err := client.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{
		QueueName: aws.String(queueName),
	})
	if err != nil {
		t.Fatalf("get queue URL: %v", err)
	}
	if got := aws.ToString(resolved.QueueUrl); got != queueURL {
		t.Fatalf("get queue URL = %q, want unchanged %q", got, queueURL)
	}

	listed, err := client.ListQueues(ctx, &sqs.ListQueuesInput{
		QueueNamePrefix: aws.String("testcontainers-"),
	})
	if err != nil {
		t.Fatalf("list queues: %v", err)
	}
	if !slices.Contains(listed.QueueUrls, queueURL) {
		t.Fatalf("listed queue URLs %q do not contain unchanged URL %q", listed.QueueUrls, queueURL)
	}

	if _, err := client.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl:    aws.String(queueURL),
		MessageBody: aws.String(payload),
	}); err != nil {
		t.Fatalf("send message: %v", err)
	}

	received, err := client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:            aws.String(queueURL),
		MaxNumberOfMessages: 1,
		WaitTimeSeconds:     1,
	})
	if err != nil {
		t.Fatalf("receive message: %v", err)
	}
	if len(received.Messages) != 1 || aws.ToString(received.Messages[0].Body) != payload {
		t.Fatalf("received messages = %#v, want one message with body %q", received.Messages, payload)
	}

	if _, err := client.DeleteQueue(ctx, &sqs.DeleteQueueInput{
		QueueUrl: aws.String(queueURL),
	}); err != nil {
		t.Fatalf("delete queue: %v", err)
	}
}

func assertSameAuthority(t *testing.T, queueURL, endpoint string) {
	t.Helper()

	queue, err := url.Parse(queueURL)
	if err != nil {
		t.Fatalf("parse queue URL %q: %v", queueURL, err)
	}
	base, err := url.Parse(endpoint)
	if err != nil {
		t.Fatalf("parse endpoint %q: %v", endpoint, err)
	}

	got := fmt.Sprintf("%s://%s", queue.Scheme, queue.Host)
	want := fmt.Sprintf("%s://%s", base.Scheme, base.Host)
	if got != want {
		t.Fatalf("queue URL authority = %q, want exactly %q", got, want)
	}
}

func testS3(ctx context.Context, t *testing.T, client *s3.Client) {
	t.Helper()

	const (
		bucket  = "testcontainers-bucket"
		key     = "payload.txt"
		payload = "hello from testcontainers-go"
	)

	if _, err := client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(bucket),
	}); err != nil {
		t.Fatalf("create bucket: %v", err)
	}
	if _, err := client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   strings.NewReader(payload),
	}); err != nil {
		t.Fatalf("put object: %v", err)
	}

	object, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		t.Fatalf("get object: %v", err)
	}
	defer object.Body.Close()

	body, err := io.ReadAll(object.Body)
	if err != nil {
		t.Fatalf("read object: %v", err)
	}
	if got := string(body); got != payload {
		t.Fatalf("object payload = %q, want exactly %q", got, payload)
	}
}
