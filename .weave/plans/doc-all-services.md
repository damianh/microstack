# Document All 39 MicroStack Services

## TL;DR
> **Summary**: Create dedicated documentation pages for the 33 undocumented MicroStack services, following the established template from the 6 existing service docs (S3, SQS, DynamoDB, Lambda, Step Functions, API Gateway). Each page lists supported operations, provides 2-3 SDK usage examples adapted from integration tests, and notes any quirks or limitations.
> **Estimated Effort**: XL

## Context

### Original Request
Document all 39 AWS services emulated by MicroStack. Currently 6 services have dedicated doc pages; the remaining 33 need documentation pages with usage examples.

### Key Findings

**Existing doc template** (from `docs/Docs/Content/docs/services/s3.md`, `sqs.md`, etc.):
- Frontmatter: `title`, `description`, `order`, `section: Services`
- `order` values: overview=1, s3=2, sqs=3, dynamodb=4, lambda=5, api-gateway=6, step-functions=7, all=100
- H1 heading matching service name
- One-paragraph intro describing what the handler supports
- `## Supported Operations` section — bullet list or flat list of API operations
- `## Usage` section — primary C# code example with SDK client setup, create resource, basic CRUD
- Optional additional sections for advanced features (e.g., `## FIFO Queues`, `## Transactions`, `## Mock Configuration`)
- Optional `:::aside{type="note" title="..."}` callout for important notes/limitations
- Examples use `new BasicAWSCredentials("test", "test")` and `ServiceURL = "http://localhost:4566"`

**Source material for each service**:
- **Operations list**: Already fully enumerated in `docs/Docs/Content/docs/services/overview.md` (lines 14-332) — copy from there
- **Handler code**: `src/MicroStack/Services/{ServiceName}/{ServiceName}ServiceHandler.cs` — read the XML doc summary for feature details
- **Test code**: `tests/MicroStack.Tests/{ServiceName}Tests.cs` — adapt the simplest test methods as doc examples

**Services inventory** (33 undocumented):
1. SNS, 2. IAM, 3. STS, 4. KMS, 5. Secrets Manager, 6. Cognito (User Pools + Identity), 7. SSM, 8. CloudWatch Logs, 9. EventBridge, 10. EC2, 11. RDS, 12. ALB (ELBv2), 13. Route 53, 14. CloudFront, 15. ACM, 16. WAF v2, 17. ECS, 18. CloudFormation, 19. CloudWatch (Metrics), 20. Kinesis, 21. Firehose, 22. AppSync, 23. Glue, 24. Athena, 25. EMR, 26. EFS, 27. ECR, 28. ElastiCache, 29. SES, 30. Service Discovery, 31. RDS Data, 32. S3Files, 33. DynamoDB Streams (part of DynamoDB — skip as separate page, note in DynamoDB page)

**Note**: DynamoDB Streams is not a separate handler — it's part of DynamoDB functionality. S3Files is an internal/EFS-like handler. The `all.md` page already exists and lists all services categorically.

## Objectives

### Core Objective
Create documentation pages for all undocumented MicroStack services so users can quickly understand supported operations and see working code examples.

### Deliverables
- [ ] 33 new markdown files in `docs/Docs/Content/docs/services/`
- [ ] Updated `overview.md` with links to each service's dedicated page
- [ ] Consistent template across all pages

### Definition of Done
- [ ] Every service listed in `overview.md` has a corresponding dedicated doc page in `docs/Docs/Content/docs/services/`
- [ ] Each page has correct frontmatter, operations list, and at least 2 C# SDK examples
- [ ] All markdown files are valid and render without errors

### Guardrails (Must NOT)
- Do NOT modify existing 6 service doc pages (s3.md, sqs.md, dynamodb.md, lambda.md, step-functions.md, api-gateway.md)
- Do NOT modify source code or test files
- Do NOT invent operations — only document operations listed in `overview.md`
- Do NOT add examples that aren't grounded in the test files

## Documentation Template

Every new service page MUST follow this exact structure:

```markdown
---
title: {Service Display Name}
description: {Service Display Name} emulation — {brief feature summary}.
order: {N}
section: Services
---

# {Service Display Name}

{One-paragraph intro: what the handler supports, key features, protocol.}

## Supported Operations

{Operations list — use bullet groups by category OR flat comma-separated list depending on
how many operations there are. For services with <20 ops, use a flat list. For services
with 20+ ops, group by category with bold sub-headings.}

## Usage

```csharp
// SDK client setup + basic CRUD example
// Adapted from the simplest test in {Service}Tests.cs
// Always use: new BasicAWSCredentials("test", "test")
// Always use: ServiceURL = "http://localhost:4566"
```

## {Advanced Feature Section} (optional, 1-2 additional sections)

```csharp
// Second example showing a more advanced feature
```

:::aside{type="note" title="{Title}"} (optional)
{Important quirk or limitation worth noting.}
:::
```

**Order numbering**: Continue from 7 (step-functions). Tier 2 services get orders 8-16, Tier 3 get 17-26, Tier 4 get 27-59. The `all.md` page keeps order=100.

## TODOs

### Phase 1 — Tier 2: High-Value Core Services (orders 8-16)

- [x] 1. **SNS documentation page**
  **What**: Create SNS service doc with topic/subscription CRUD, publish, SNS-to-SQS fanout, and FIFO topic examples.
  **Files**: `docs/Docs/Content/docs/services/sns.md`
  **Acceptance**: Page has frontmatter (title: SNS, order: 8), operations from overview.md line 44, examples adapted from `tests/MicroStack.Tests/SnsTests.cs` (CreateTopic, Publish, SnsSqsFanout, FifoDeduplicationPassthrough). Note about SNS-to-Lambda fanout being supported.

- [x] 2. **IAM documentation page**
  **What**: Create IAM service doc with users, roles, policies, access keys, instance profiles, groups, service-linked roles, OIDC providers.
  **Files**: `docs/Docs/Content/docs/services/iam.md`
  **Acceptance**: Page has frontmatter (title: IAM, order: 9), operations from overview.md line 70, examples adapted from `tests/MicroStack.Tests/IamTests.cs` (CreateRole + AttachRolePolicy, CreateUser + CreateAccessKey, InstanceProfile). Group operations by category since there are 60+ operations.

- [x] 3. **STS documentation page**
  **What**: Create STS service doc with GetCallerIdentity, AssumeRole, AssumeRoleWithWebIdentity, GetSessionToken.
  **Files**: `docs/Docs/Content/docs/services/sts.md`
  **Acceptance**: Page has frontmatter (title: STS, order: 10), operations from overview.md line 76, examples adapted from `tests/MicroStack.Tests/StsTests.cs` (GetCallerIdentity, AssumeRole). Note that STS is lightweight — all calls return synthetic credentials.

- [x] 4. **Cognito documentation page**
  **What**: Create combined Cognito doc covering both User Pools (IdP) and Identity (Pools). Sections: User Pool CRUD, User Management, Auth Flows (JWT tokens), Groups, MFA, Identity Pools.
  **Files**: `docs/Docs/Content/docs/services/cognito.md`
  **Acceptance**: Page has frontmatter (title: Cognito, order: 11), operations from overview.md lines 100 and 108, examples adapted from `tests/MicroStack.Tests/CognitoTests.cs` (CreateUserPool + CreateUserPoolClient, AdminCreateUser + AdminInitiateAuth with JWT, SignUp + ConfirmSignUp, Identity Pool creation). Note about JWT tokens being valid but self-signed.

- [x] 5. **KMS documentation page**
  **What**: Create KMS service doc with key management, encrypt/decrypt, sign/verify, aliases, data keys.
  **Files**: `docs/Docs/Content/docs/services/kms.md`
  **Acceptance**: Page has frontmatter (title: KMS, order: 12), operations from overview.md line 84, examples adapted from `tests/MicroStack.Tests/KmsTests.cs` (CreateKey + EncryptDecryptRoundtrip, CreateRsa + SignAndVerify, CreateAlias + UseAliasForEncrypt). Note about key types supported (SYMMETRIC_DEFAULT, RSA_2048, RSA_4096).

- [x] 6. **Secrets Manager documentation page**
  **What**: Create Secrets Manager doc with secret CRUD, versioning, rotation staging, batch retrieval, resource policies.
  **Files**: `docs/Docs/Content/docs/services/secrets-manager.md`
  **Acceptance**: Page has frontmatter (title: Secrets Manager, order: 13), operations from overview.md line 92, examples adapted from `tests/MicroStack.Tests/SecretsManagerTests.cs` (CreateSecret + GetSecretValue, PutSecretValue version rotation, BatchGetSecretValue). Note about rotation being stub-only (no Lambda invocation).

- [x] 7. **SSM Parameter Store documentation page**
  **What**: Create SSM doc with parameter CRUD, GetParametersByPath, SecureString, history, labels, tags.
  **Files**: `docs/Docs/Content/docs/services/ssm.md`
  **Acceptance**: Page has frontmatter (title: SSM Parameter Store, order: 14), operations from overview.md line 232, examples adapted from `tests/MicroStack.Tests/SsmTests.cs` (PutParameter + GetParameter, GetParametersByPath, SecureString handling). Note about SecureString values being stored in plain text (no actual encryption).

- [x] 8. **CloudWatch Logs documentation page**
  **What**: Create CloudWatch Logs doc with log groups, streams, events, metric filters, subscription filters.
  **Files**: `docs/Docs/Content/docs/services/cloudwatch-logs.md`
  **Acceptance**: Page has frontmatter (title: CloudWatch Logs, order: 15), operations from overview.md line 216, examples adapted from `tests/MicroStack.Tests/CloudWatchLogsTests.cs` (CreateLogGroup + CreateLogStream + PutLogEvents + GetLogEvents, FilterLogEvents).

- [x] 9. **EventBridge documentation page**
  **What**: Create EventBridge doc with event buses, rules, targets, PutEvents, archives, replays, connections, API destinations.
  **Files**: `docs/Docs/Content/docs/services/eventbridge.md`
  **Acceptance**: Page has frontmatter (title: EventBridge, order: 16), operations from overview.md line 182, examples adapted from `tests/MicroStack.Tests/EventBridgeTests.cs` (CreateEventBus + PutRule + PutTargets, PutEvents, Archive + Replay). Group operations by category since there are 50+ operations.

### Phase 2 — Tier 3: Infrastructure Services (orders 17-26)

- [x] 10. **EC2 documentation page**
  **What**: Create EC2 doc with instances, VPCs, subnets, security groups, EBS, key pairs, route tables, and more. This is the largest service — group operations by resource type.
  **Files**: `docs/Docs/Content/docs/services/ec2.md`
  **Acceptance**: Page has frontmatter (title: EC2, order: 17), operations from overview.md line 134 (truncated — read full from handler), examples adapted from `tests/MicroStack.Tests/Ec2Tests.cs` (RunInstances + DescribeInstances, CreateVpc + CreateSubnet + CreateSecurityGroup, CreateKeyPair). Note that EC2 is metadata-only (no actual VMs launched).

- [x] 11. **RDS documentation page**
  **What**: Create RDS doc with DB instances, clusters, parameter groups, subnet groups, snapshots, proxies, global clusters.
  **Files**: `docs/Docs/Content/docs/services/rds.md`
  **Acceptance**: Page has frontmatter (title: RDS, order: 18), operations from overview.md line 268, examples adapted from `tests/MicroStack.Tests/RdsTests.cs` (CreateDBInstance + DescribeDBInstances, CreateDBCluster, CreateDBSubnetGroup). Note that RDS is metadata-only (no actual database engine).

- [x] 12. **ALB (ELBv2) documentation page**
  **What**: Create ALB doc with load balancers, target groups, listeners, rules, ALB-to-Lambda routing.
  **Files**: `docs/Docs/Content/docs/services/alb.md`
  **Acceptance**: Page has frontmatter (title: ALB (ELBv2), order: 19), operations from overview.md line 158, examples adapted from `tests/MicroStack.Tests/AlbTests.cs` (CreateLoadBalancer + CreateTargetGroup + CreateListener, CreateRule). Note about ALB-to-Lambda live traffic routing.

- [x] 13. **Route 53 documentation page**
  **What**: Create Route 53 doc with hosted zones, record sets, health checks.
  **Files**: `docs/Docs/Content/docs/services/route53.md`
  **Acceptance**: Page has frontmatter (title: Route 53, order: 20), operations from overview.md line 142, examples adapted from `tests/MicroStack.Tests/Route53Tests.cs` (CreateHostedZone + ChangeResourceRecordSets + ListResourceRecordSets).

- [x] 14. **CloudFront documentation page**
  **What**: Create CloudFront doc with distributions, invalidations, origin access controls. Note ETag-based concurrency.
  **Files**: `docs/Docs/Content/docs/services/cloudfront.md`
  **Acceptance**: Page has frontmatter (title: CloudFront, order: 21), operations from overview.md line 150, examples adapted from `tests/MicroStack.Tests/CloudFrontTests.cs` (CreateDistribution + GetDistribution, CreateInvalidation). Note about ETag-based optimistic concurrency for updates.

- [x] 15. **ACM documentation page**
  **What**: Create ACM doc with certificate request, describe, import, DNS validation.
  **Files**: `docs/Docs/Content/docs/services/acm.md`
  **Acceptance**: Page has frontmatter (title: ACM, order: 22), operations from overview.md line 116, examples adapted from `tests/MicroStack.Tests/AcmTests.cs` (RequestCertificate + DescribeCertificate, ImportCertificate). Note about auto-issuance of self-signed certs.

- [x] 16. **WAF v2 documentation page**
  **What**: Create WAF v2 doc with web ACLs, IP sets, rule groups, resource associations.
  **Files**: `docs/Docs/Content/docs/services/waf.md`
  **Acceptance**: Page has frontmatter (title: WAF v2, order: 23), operations from overview.md line 124, examples adapted from `tests/MicroStack.Tests/WafTests.cs` (CreateWebACL + GetWebACL, CreateIPSet, AssociateWebACL). Note about LockToken enforcement for updates.

- [x] 17. **ECS documentation page**
  **What**: Create ECS doc with clusters, task definitions, tasks, services, capacity providers.
  **Files**: `docs/Docs/Content/docs/services/ecs.md`
  **Acceptance**: Page has frontmatter (title: ECS, order: 24), operations from overview.md line 242, examples adapted from `tests/MicroStack.Tests/EcsTests.cs` (CreateCluster + RegisterTaskDefinition + RunTask, CreateService). Note about metadata-only emulation (no actual containers). Group operations by category.

- [x] 18. **CloudFormation documentation page**
  **What**: Create CloudFormation doc with stacks, change sets, exports, cross-stack references, intrinsic functions.
  **Files**: `docs/Docs/Content/docs/services/cloudformation.md`
  **Acceptance**: Page has frontmatter (title: CloudFormation, order: 25), operations from overview.md line 208, examples adapted from `tests/MicroStack.Tests/CloudFormationTests.cs` (CreateStack with JSON template, DescribeStacks, CreateChangeSet + ExecuteChangeSet). Note about supported intrinsic functions and resource types.

- [x] 19. **CloudWatch Metrics documentation page**
  **What**: Create CloudWatch Metrics doc with metrics, alarms (standard + composite), dashboards.
  **Files**: `docs/Docs/Content/docs/services/cloudwatch.md`
  **Acceptance**: Page has frontmatter (title: CloudWatch Metrics, order: 26), operations from overview.md line 224, examples adapted from `tests/MicroStack.Tests/CloudWatchTests.cs` (PutMetricData + GetMetricData, PutMetricAlarm + DescribeAlarms). Note about CBOR protocol support.

### Phase 3 — Tier 4: Niche/Internal Services (orders 27-59)

- [x] 20. **Kinesis documentation page**
  **What**: Create Kinesis doc with streams, shards, records, consumers. Note partition key routing.
  **Files**: `docs/Docs/Content/docs/services/kinesis.md`
  **Acceptance**: Page has frontmatter (title: Kinesis, order: 27), operations from overview.md line 190, examples adapted from `tests/MicroStack.Tests/KinesisTests.cs` (CreateStream + PutRecord + GetShardIterator + GetRecords). Note about AWS limits enforcement.

- [x] 21. **Firehose documentation page**
  **What**: Create Firehose doc with delivery streams, records, S3 destination integration.
  **Files**: `docs/Docs/Content/docs/services/firehose.md`
  **Acceptance**: Page has frontmatter (title: Firehose, order: 28), operations from overview.md line 198, examples adapted from `tests/MicroStack.Tests/FirehoseTests.cs` (CreateDeliveryStream + PutRecord + PutRecordBatch). Note about S3 destination writing to local S3 emulator.

- [x] 22. **AppSync documentation page**
  **What**: Create AppSync doc with GraphQL APIs, types, resolvers, data sources, API keys.
  **Files**: `docs/Docs/Content/docs/services/appsync.md`
  **Acceptance**: Page has frontmatter (title: AppSync, order: 29), operations from overview.md line 320 (brief), examples adapted from `tests/MicroStack.Tests/AppSyncTests.cs` (CreateGraphqlApi + CreateApiKey, CreateType + CreateResolver).

- [x] 23. **Glue documentation page**
  **What**: Create Glue doc with databases, tables, crawlers, jobs, partitions, schema registry.
  **Files**: `docs/Docs/Content/docs/services/glue.md`
  **Acceptance**: Page has frontmatter (title: Glue, order: 30), operations from overview.md line 292, examples adapted from `tests/MicroStack.Tests/GlueTests.cs` (CreateDatabase + CreateTable, CreateCrawler, CreateJob). Group operations by category.

- [x] 24. **Athena documentation page**
  **What**: Create Athena doc with work groups, named queries, query executions, data catalogs.
  **Files**: `docs/Docs/Content/docs/services/athena.md`
  **Acceptance**: Page has frontmatter (title: Athena, order: 31), operations from overview.md line 300, examples adapted from `tests/MicroStack.Tests/AthenaTests.cs` (CreateWorkGroup, StartQueryExecution + GetQueryResults, CreateNamedQuery).

- [x] 25. **EMR documentation page**
  **What**: Create EMR doc with clusters, instance groups, steps, bootstrap actions.
  **Files**: `docs/Docs/Content/docs/services/emr.md`
  **Acceptance**: Page has frontmatter (title: EMR, order: 32), operations from overview.md line 250, examples adapted from `tests/MicroStack.Tests/EmrTests.cs` (RunJobFlow + DescribeCluster, AddJobFlowSteps). Note metadata-only emulation.

- [x] 26. **EFS documentation page**
  **What**: Create EFS doc with file systems, mount targets, access points.
  **Files**: `docs/Docs/Content/docs/services/efs.md`
  **Acceptance**: Page has frontmatter (title: EFS, order: 33), operations from overview.md line 316 (brief), examples adapted from `tests/MicroStack.Tests/EfsTests.cs` (CreateFileSystem + CreateMountTarget + CreateAccessPoint). Reference handler at `src/MicroStack/Services/Efs/EfsServiceHandler.cs` for full operation list.

- [x] 27. **ECR documentation page**
  **What**: Create ECR doc with repositories, images, lifecycle policies, authorization.
  **Files**: `docs/Docs/Content/docs/services/ecr.md`
  **Acceptance**: Page has frontmatter (title: ECR, order: 34), operations from overview.md line 258, examples adapted from `tests/MicroStack.Tests/EcrTests.cs` (CreateRepository + PutImage + BatchGetImage, GetAuthorizationToken). Note metadata-only for images (no actual container registry).

- [x] 28. **ElastiCache documentation page**
  **What**: Create ElastiCache doc with cache clusters, replication groups, subnet groups, users.
  **Files**: `docs/Docs/Content/docs/services/elasticache.md`
  **Acceptance**: Page has frontmatter (title: ElastiCache, order: 35), operations from overview.md line 282, examples adapted from `tests/MicroStack.Tests/ElastiCacheTests.cs` (CreateCacheCluster + DescribeCacheClusters, CreateReplicationGroup). Note metadata-only emulation.

- [x] 29. **SES documentation page**
  **What**: Create SES doc with email identities, templates, send operations, configuration sets. Cover both v1 and v2 APIs.
  **Files**: `docs/Docs/Content/docs/services/ses.md`
  **Acceptance**: Page has frontmatter (title: SES, order: 36), operations from overview.md line 310, examples adapted from `tests/MicroStack.Tests/SesTests.cs` (VerifyEmailIdentity + SendEmail, CreateTemplate + SendTemplatedEmail). Note that emails are stored in-memory, not actually sent.

- [x] 30. **Service Discovery (Cloud Map) documentation page**
  **What**: Create Service Discovery doc with namespaces, services, instances, Route 53 integration.
  **Files**: `docs/Docs/Content/docs/services/service-discovery.md`
  **Acceptance**: Page has frontmatter (title: Service Discovery, order: 37), operations from overview.md line 172, examples adapted from `tests/MicroStack.Tests/ServiceDiscoveryTests.cs` (CreateHttpNamespace + CreateService + RegisterInstance, DiscoverInstances).

- [x] 31. **RDS Data API documentation page**
  **What**: Create RDS Data API doc with ExecuteStatement, BatchExecuteStatement, transactions.
  **Files**: `docs/Docs/Content/docs/services/rds-data.md`
  **Acceptance**: Page has frontmatter (title: RDS Data API, order: 38), operations from overview.md line 274, examples adapted from `tests/MicroStack.Tests/RdsDataTests.cs` (ExecuteStatement, BeginTransaction + CommitTransaction). Note this is a separate service from RDS.

- [x] 32. **S3Files documentation page**
  **What**: Create S3Files doc based on handler at `src/MicroStack/Services/S3Files/S3FilesServiceHandler.cs`. Covers file systems, mount targets, access points, policies.
  **Files**: `docs/Docs/Content/docs/services/s3files.md`
  **Acceptance**: Page has frontmatter (title: S3Files, order: 39), operations extracted from handler docstring (CreateFileSystem, GetFileSystem, ListFileSystems, DeleteFileSystem, CreateMountTarget, GetMountTarget, ListMountTargets, DeleteMountTarget, UpdateMountTarget, CreateAccessPoint, GetAccessPoint, ListAccessPoints, DeleteAccessPoint, GetFileSystemPolicy, PutFileSystemPolicy, DeleteFileSystemPolicy, GetSynchronizationConfiguration, PutSynchronizationConfiguration, TagResource, UntagResource, ListTagsForResource). No test file exists — provide example from handler patterns.

### Phase 4 — Overview Update and Verification

- [x] 33. **Update overview.md with links to all new service pages**
  **What**: Add markdown links from each service heading in `overview.md` to the corresponding dedicated doc page. E.g., change `### SNS` to `### [SNS](sns)` or add a "See [detailed SNS documentation](sns)" line under each section.
  **Files**: `docs/Docs/Content/docs/services/overview.md`
  **Acceptance**: Every service in overview.md links to its dedicated page. Links use relative paths. No broken links.

- [x] 34. **Verify all pages render correctly**
  **What**: Check all 33 new markdown files have valid frontmatter (YAML parses), valid markdown (no broken code blocks), and correct `order` values (no duplicates). Grep for common issues: unclosed code blocks, missing frontmatter delimiters, duplicate order numbers.
  **Acceptance**: All files pass validation. No duplicate `order` values. All service titles match between overview.md and dedicated pages.

## Verification

- [ ] All 33 new `.md` files exist in `docs/Docs/Content/docs/services/`
- [ ] No duplicate `order` values across all service pages
- [ ] Every service page has: frontmatter with title/description/order/section, H1 heading, Supported Operations section, Usage section with at least one C# code example
- [ ] `overview.md` links to all dedicated service pages
- [ ] No regressions to existing 6 service doc pages (files unchanged)
- [ ] `all.md` unchanged (it already lists all services)
