---
title: Services Overview
description: All 39 AWS services supported by MicroStack.
order: 1
section: Services
---

# Services Overview

MicroStack emulates 39 AWS services on a single port. Each service handler implements the same `IServiceHandler` interface with `HandleAsync`, `Reset`, `GetState`, and `RestoreState` methods.

## Supported Services

| Service | Protocol | Key Operations |
|---------|----------|----------------|
| **S3** | REST/XML | Buckets, objects, multipart upload, versioning, lifecycle, CORS, tagging |
| **SQS** | JSON + Query/XML | Queues, messages, FIFO, dead-letter queues, tags |
| **DynamoDB** | JSON | Tables, items, queries, scans, transactions, streams, GSIs |
| **SNS** | Query/XML | Topics, subscriptions, publish, fanout to SQS |
| **Lambda** | REST/JSON | Functions, invocations, versions, aliases, layers, ESM (SQS/DynamoDB/Kinesis) |
| **API Gateway v2** | REST/JSON | HTTP APIs, routes, integrations, stages, deployments |
| **API Gateway v1** | REST/JSON | REST APIs, resources, methods, MOCK + AWS_PROXY integrations |
| **Step Functions** | JSON | State machines, executions, full ASL engine (Pass, Task, Choice, Wait, Parallel, Map, Succeed, Fail) |
| **IAM** | Query/XML | Users, roles, policies, access keys, groups |
| **STS** | JSON + Query/XML | AssumeRole, GetCallerIdentity, AssumeRoleWithWebIdentity |
| **Secrets Manager** | JSON | Secrets, versions, rotation staging, batch get |
| **SSM Parameter Store** | JSON | Parameters, history, tags, SecureString |
| **KMS** | JSON | Keys (symmetric + RSA), encrypt, decrypt, sign, verify, aliases, grants |
| **EC2** | Query/XML | Instances, VPCs, subnets, security groups, EBS volumes, AMIs, key pairs, route tables, internet gateways, NAT gateways, elastic IPs |
| **ELBv2 (ALB)** | Query/XML | Load balancers, target groups, listeners, rules |
| **Route 53** | REST/XML | Hosted zones, record sets |
| **ACM** | JSON | Certificates, tags |
| **CloudWatch Logs** | JSON | Log groups, log streams, log events, metric filters |
| **CloudWatch Metrics** | Query/XML + CBOR | Metrics, alarms, dashboards |
| **ECS** | JSON | Clusters, task definitions, tasks, services, container instances |
| **RDS** | Query/XML | DB instances, clusters, global clusters, DB proxies, parameter groups, subnet groups, snapshots |
| **ElastiCache** | Query/XML | Cache clusters, replication groups, subnet groups, parameter groups, snapshots |
| **ECR** | JSON | Repositories, images, lifecycle policies |
| **RDS Data API** | REST/JSON | Execute statement, batch execute |
| **EventBridge** | JSON | Event buses, rules, targets, archives, replays |
| **Kinesis** | JSON | Streams, shards, records, consumers |
| **Firehose** | JSON | Delivery streams, records |
| **Glue** | JSON | Databases, tables, crawlers, jobs, triggers, partitions, connections |
| **Athena** | JSON | Work groups, named queries, query executions |
| **CloudFormation** | Query/XML | Stacks, change sets, resource provisioning (S3, SQS, SNS, DynamoDB, Lambda, IAM, SSM, Logs, Events) |
| **Cognito User Pools** | JSON | User pools, users, groups, app clients, JWT tokens |
| **Cognito Identity** | JSON | Identity pools, identities, credentials |
| **SES** | Query/XML + REST/JSON | Email identities, templates (v1 + v2) |
| **WAF v2** | JSON | Web ACLs, IP sets, rule groups, regex pattern sets |
| **EFS** | REST/JSON | File systems, mount targets, access points |
| **EMR** | JSON | Clusters, instance groups, steps |
| **AppSync** | REST/JSON | GraphQL APIs, types, resolvers, data sources |
| **CloudFront** | REST/XML | Distributions, invalidations |
| **Service Discovery** | JSON | Namespaces, services, instances (with Route 53 integration) |

## Protocol Support

MicroStack handles four protocol families:

- **AWS Query/XML** — Form-encoded `Action` parameter, XML responses (EC2, RDS, SQS legacy, etc.)
- **AWS JSON** — `X-Amz-Target` header, JSON request/response (DynamoDB, ECS, Step Functions, etc.)
- **REST/JSON** — Path-based routing, JSON bodies (Lambda, API Gateway, EFS, etc.)
- **REST/XML** — Path-based routing, XML bodies (S3, Route 53, CloudFront)
- **Smithy RPC v2 CBOR** — Binary CBOR encoding (CloudWatch Metrics alternate protocol)
