import assert from "node:assert/strict";
import { test } from "node:test";
import {
  CreateBucketCommand,
  GetObjectCommand,
  PutObjectCommand,
  S3Client,
} from "@aws-sdk/client-s3";
import {
  CreateQueueCommand,
  DeleteQueueCommand,
  GetQueueUrlCommand,
  ListQueuesCommand,
  ReceiveMessageCommand,
  SendMessageCommand,
  SQSClient,
} from "@aws-sdk/client-sqs";
import { GenericContainer, Wait } from "testcontainers";

const microStackPort = 4566;
const image =
  process.env.MICROSTACK_TEST_IMAGE ??
  "ghcr.io/damianh/microstack:latest";

test("random mapped endpoint supports SQS and path-style S3", async () => {
  const container = await new GenericContainer(image)
    .withExposedPorts(microStackPort)
    .withWaitStrategy(
      Wait.forHttp("/_microstack/health", microStackPort).forStatusCode(200),
    )
    .start();
  let sqs: SQSClient | undefined;
  let s3: S3Client | undefined;

  try {
    const endpoint = `http://${container.getHost()}:${container.getMappedPort(microStackPort)}`;
    const clientConfig = {
      endpoint,
      region: "us-east-1",
      credentials: { accessKeyId: "test", secretAccessKey: "test" },
    };
    sqs = new SQSClient(clientConfig);
    s3 = new S3Client({ ...clientConfig, forcePathStyle: true });

    const created = await sqs.send(
      new CreateQueueCommand({ QueueName: "testcontainers-queue" }),
    );
    const queueUrl = assertQueueUrl(created.QueueUrl);
    assert.equal(new URL(queueUrl).origin, endpoint);

    const resolved = await sqs.send(
      new GetQueueUrlCommand({ QueueName: "testcontainers-queue" }),
    );
    assert.equal(resolved.QueueUrl, queueUrl);

    const listed = await sqs.send(
      new ListQueuesCommand({ QueueNamePrefix: "testcontainers-" }),
    );
    assert.ok(listed.QueueUrls?.includes(queueUrl));

    await sqs.send(
      new SendMessageCommand({
        QueueUrl: queueUrl,
        MessageBody: "hello from testcontainers",
      }),
    );
    const received = await sqs.send(
      new ReceiveMessageCommand({
        QueueUrl: queueUrl,
        MaxNumberOfMessages: 1,
      }),
    );
    assert.equal(received.Messages?.length, 1);
    assert.equal(received.Messages[0]?.Body, "hello from testcontainers");
    await sqs.send(new DeleteQueueCommand({ QueueUrl: queueUrl }));

    const bucket = "testcontainers-bucket";
    const key = "payload.txt";
    const payload = "hello from testcontainers";
    await s3.send(new CreateBucketCommand({ Bucket: bucket }));
    await s3.send(
      new PutObjectCommand({ Bucket: bucket, Key: key, Body: payload }),
    );
    const object = await s3.send(
      new GetObjectCommand({ Bucket: bucket, Key: key }),
    );
    assert.equal(await object.Body?.transformToString(), payload);
  } finally {
    sqs?.destroy();
    s3?.destroy();
    await container.stop();
  }
});

function assertQueueUrl(queueUrl: string | undefined): string {
  assert.ok(queueUrl, "CreateQueue must return a queue URL");
  return queueUrl;
}
