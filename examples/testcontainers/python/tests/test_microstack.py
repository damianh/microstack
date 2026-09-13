import os
from collections.abc import Iterator
from urllib.parse import urlsplit

import boto3
import pytest
from botocore.config import Config
from testcontainers.core.container import DockerContainer
from testcontainers.core.wait_strategies import HttpWaitStrategy

PORT = 4566
REGION = "us-east-1"
PAYLOAD = b"hello from testcontainers-python"


@pytest.fixture(scope="module")
def endpoint() -> Iterator[str]:
    image = os.environ["MICROSTACK_TEST_IMAGE"]
    with (
        DockerContainer(image)
        .with_exposed_ports(PORT)
        .waiting_for(
            HttpWaitStrategy(PORT, "/_microstack/health").for_status_code(200)
        )
    ) as container:
        yield f"http://{container.get_container_host_ip()}:{container.get_exposed_port(PORT)}"


def test_sqs_and_path_style_s3(endpoint: str) -> None:
    client_options = {
        "endpoint_url": endpoint,
        "region_name": REGION,
        "aws_access_key_id": "test",
        "aws_secret_access_key": "test",
    }
    sqs = boto3.client("sqs", **client_options)
    s3 = boto3.client(
        "s3",
        config=Config(s3={"addressing_style": "path"}),
        **client_options,
    )

    queue_url = sqs.create_queue(QueueName="testcontainers-python")["QueueUrl"]
    assert urlsplit(queue_url).netloc == urlsplit(endpoint).netloc
    assert sqs.get_queue_url(QueueName="testcontainers-python")["QueueUrl"] == queue_url
    assert queue_url in sqs.list_queues(QueueNamePrefix="testcontainers-")["QueueUrls"]

    sqs.send_message(QueueUrl=queue_url, MessageBody=PAYLOAD.decode())
    messages = sqs.receive_message(
        QueueUrl=queue_url,
        MaxNumberOfMessages=1,
        WaitTimeSeconds=1,
    )["Messages"]
    assert len(messages) == 1
    assert messages[0]["Body"] == PAYLOAD.decode()
    sqs.delete_queue(QueueUrl=queue_url)

    bucket = "testcontainers-python"
    key = "payload.bin"
    s3.create_bucket(Bucket=bucket)
    s3.put_object(Bucket=bucket, Key=key, Body=PAYLOAD)
    assert s3.get_object(Bucket=bucket, Key=key)["Body"].read() == PAYLOAD

