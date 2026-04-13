---
title: AWS CLI
description: Using MicroStack with the AWS CLI.
order: 6
section: Guides
---

# AWS CLI

MicroStack works with the standard AWS CLI. Point it at `http://localhost:4566` using `--endpoint-url` or environment variables.

## Environment Variables

The simplest approach — no profile configuration needed:

```bash
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test
export AWS_DEFAULT_REGION=us-east-1
```

Then use `--endpoint-url` on every command:

```bash
aws --endpoint-url=http://localhost:4566 s3 mb s3://my-bucket
aws --endpoint-url=http://localhost:4566 s3 cp ./file.txt s3://my-bucket/
aws --endpoint-url=http://localhost:4566 sqs create-queue --queue-name my-queue
aws --endpoint-url=http://localhost:4566 dynamodb list-tables
aws --endpoint-url=http://localhost:4566 sts get-caller-identity
```

## Named Profile

Create a profile for local development:

```bash
aws configure --profile local
# AWS Access Key ID: test
# AWS Secret Access Key: test
# Default region: us-east-1
# Default output format: json
```

Then pass `--profile local` on each command:

```bash
aws --profile local --endpoint-url=http://localhost:4566 s3 ls
aws --profile local --endpoint-url=http://localhost:4566 sqs list-queues
```

## Common Examples

### S3

```bash
# Create bucket and upload file
aws --endpoint-url=http://localhost:4566 s3 mb s3://my-bucket
aws --endpoint-url=http://localhost:4566 s3 cp ./file.txt s3://my-bucket/

# List buckets and objects
aws --endpoint-url=http://localhost:4566 s3 ls
aws --endpoint-url=http://localhost:4566 s3 ls s3://my-bucket/
```

### SQS

```bash
# Create queue and send message
aws --endpoint-url=http://localhost:4566 sqs create-queue --queue-name my-queue
aws --endpoint-url=http://localhost:4566 sqs send-message \
  --queue-url http://localhost:4566/000000000000/my-queue \
  --message-body "hello world"

# Receive messages
aws --endpoint-url=http://localhost:4566 sqs receive-message \
  --queue-url http://localhost:4566/000000000000/my-queue
```

### DynamoDB

```bash
# Create table
aws --endpoint-url=http://localhost:4566 dynamodb create-table \
  --table-name Users \
  --key-schema AttributeName=userId,KeyType=HASH \
  --attribute-definitions AttributeName=userId,AttributeType=S \
  --billing-mode PAY_PER_REQUEST

# Put and get items
aws --endpoint-url=http://localhost:4566 dynamodb put-item \
  --table-name Users \
  --item '{"userId": {"S": "u1"}, "name": {"S": "Alice"}}'

aws --endpoint-url=http://localhost:4566 dynamodb get-item \
  --table-name Users \
  --key '{"userId": {"S": "u1"}}'
```

### SSM Parameter Store

```bash
aws --endpoint-url=http://localhost:4566 ssm put-parameter \
  --name /app/db/host --value localhost --type String

aws --endpoint-url=http://localhost:4566 ssm get-parameter \
  --name /app/db/host
```

### Secrets Manager

```bash
aws --endpoint-url=http://localhost:4566 secretsmanager create-secret \
  --name db-password --secret-string '{"password":"s3cr3t"}'

aws --endpoint-url=http://localhost:4566 secretsmanager get-secret-value \
  --secret-id db-password
```

## Multi-Tenancy with CLI

Use a 12-digit access key to target a specific account:

```bash
export AWS_ACCESS_KEY_ID=111111111111
aws --endpoint-url=http://localhost:4566 sts get-caller-identity
# → { "Account": "111111111111", ... }
```

See [Multi-Tenancy](/docs/architecture/multi-tenancy) for details.
