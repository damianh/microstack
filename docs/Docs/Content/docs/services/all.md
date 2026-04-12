---
title: All Services
description: Complete list of all 39 AWS services emulated by MicroStack.
order: 100
section: Services
---

# All Services

MicroStack emulates **39 AWS services** on a single port. Every service runs in-memory with account-scoped state isolation.

## Compute & Serverless
- Lambda (functions, invocations, ESM, layers, aliases)
- Step Functions (full ASL engine)
- ECS (clusters, task definitions, tasks, services)

## Storage
- S3 (buckets, objects, multipart, versioning)
- EFS (file systems, mount targets, access points)

## Database
- DynamoDB (tables, items, queries, scans, transactions, GSIs, streams)
- RDS (DB instances, clusters, parameter groups, snapshots)
- RDS Data API (execute statement)
- ElastiCache (clusters, replication groups)

## Messaging & Events
- SQS (standard + FIFO queues, DLQ)
- SNS (topics, subscriptions, fanout to SQS)
- EventBridge (event buses, rules, targets, archives)
- Kinesis (streams, shards, records, consumers)
- Firehose (delivery streams)

## Networking & Content Delivery
- API Gateway v1 + v2 (HTTP/REST APIs, Lambda proxy, mock)
- ELBv2 / ALB (load balancers, target groups, listeners)
- Route 53 (hosted zones, record sets)
- CloudFront (distributions, invalidations)
- Service Discovery (namespaces, services, Route 53 integration)

## Security & Identity
- IAM (users, roles, policies, access keys)
- STS (assume role, caller identity)
- KMS (symmetric + RSA keys, encrypt/decrypt/sign/verify)
- ACM (certificates)
- Cognito User Pools (pools, users, groups, JWT tokens)
- Cognito Identity (identity pools, credentials)
- Secrets Manager (secrets, versions, rotation staging)
- WAF v2 (web ACLs, IP sets, rule groups)

## Management & Monitoring
- CloudWatch Logs (log groups, streams, events, metric filters)
- CloudWatch Metrics (metrics, alarms, dashboards)
- CloudFormation (stacks, change sets, resource provisioning)
- SSM Parameter Store (parameters, SecureString, history)

## Analytics & Data
- Glue (databases, tables, crawlers, jobs, partitions)
- Athena (work groups, named queries, executions)

## Containers & Registry
- ECR (repositories, images, lifecycle policies)

## Other
- SES v1 + v2 (email identities, templates)
- EMR (clusters, instance groups, steps)
- AppSync (GraphQL APIs, types, resolvers)
