---
title: Services Overview
description: All 39 AWS services supported by MicroStack with detailed operation lists.
order: 1
section: Services
---

# Services Overview

MicroStack emulates 39 AWS services on a single port. Each service handler implements the `IServiceHandler` interface with `HandleAsync`, `Reset`, `GetState`, and `RestoreState` methods.

## Core Services

### S3

Buckets, objects, multipart upload, versioning, lifecycle, CORS, tagging, object lock, encryption, ACLs, policies, website hosting. Optional disk persistence via `S3_PERSIST=1`.

**Protocol:** REST/XML (path-based routing by HTTP method and URL path)

**Operations:** CreateBucket, DeleteBucket, ListBuckets, HeadBucket, PutObject, GetObject, DeleteObject, HeadObject, CopyObject, ListObjects (v1+v2), DeleteObjects, CreateMultipartUpload, UploadPart, CompleteMultipartUpload, AbortMultipartUpload, ListMultipartUploads, ListParts, GetBucketVersioning, PutBucketVersioning, ListObjectVersions, GetBucketEncryption, PutBucketEncryption, DeleteBucketEncryption, GetBucketLifecycleConfiguration, PutBucketLifecycleConfiguration, DeleteBucketLifecycle, GetBucketCors, PutBucketCors, DeleteBucketCors, GetBucketAcl, PutBucketAcl, GetBucketTagging, PutBucketTagging, DeleteBucketTagging, GetBucketPolicy, PutBucketPolicy, DeleteBucketPolicy, GetBucketLogging, PutBucketLogging, GetBucketWebsite, PutBucketWebsite, DeleteBucketWebsite, PutObjectLockConfiguration, GetObjectLockConfiguration, PutObjectRetention, GetObjectRetention, PutObjectLegalHold, GetObjectLegalHold, GetObjectTagging, PutObjectTagging, DeleteObjectTagging, GetPublicAccessBlock, PutPublicAccessBlock, DeletePublicAccessBlock, PutBucketOwnershipControls, GetBucketOwnershipControls, DeleteBucketOwnershipControls

### SQS

Standard and FIFO queues, message batching, dead-letter queues, visibility timeout, tags.

**Protocol:** JSON + Query/XML

**Operations:** CreateQueue, DeleteQueue, ListQueues, GetQueueUrl, SendMessage, ReceiveMessage, DeleteMessage, ChangeMessageVisibility, ChangeMessageVisibilityBatch, GetQueueAttributes, SetQueueAttributes, PurgeQueue, SendMessageBatch, DeleteMessageBatch, ListQueueTags, TagQueue, UntagQueue

### DynamoDB

Tables, items, queries, scans, transactions, batch operations, TTL, continuous backups, streams, GSIs.

**Protocol:** JSON (`X-Amz-Target` header)

**Operations:** CreateTable, DeleteTable, DescribeTable, ListTables, UpdateTable, PutItem, GetItem, DeleteItem, UpdateItem, Query, Scan, BatchWriteItem, BatchGetItem, TransactWriteItems, TransactGetItems, DescribeTimeToLive, UpdateTimeToLive, DescribeContinuousBackups, UpdateContinuousBackups, DescribeEndpoints, TagResource, UntagResource, ListTagsOfResource

### SNS

Topics, subscriptions, publish/fanout to SQS and Lambda. FIFO topics with deduplication.

**Protocol:** Query/XML

**Operations:** CreateTopic, DeleteTopic, ListTopics, GetTopicAttributes, SetTopicAttributes, Subscribe, ConfirmSubscription, Unsubscribe, ListSubscriptions, ListSubscriptionsByTopic, GetSubscriptionAttributes, SetSubscriptionAttributes, Publish, PublishBatch, ListTagsForResource, TagResource, UntagResource, CreatePlatformApplication, CreatePlatformEndpoint

### Lambda

Functions, invocations, versions, aliases, layers, event source mappings (SQS/DynamoDB/Kinesis). Python and Node.js runtimes via warm worker pool.

**Protocol:** REST/JSON (path-based)

**Operations:** CreateFunction, DeleteFunction, GetFunction, GetFunctionConfiguration, ListFunctions, Invoke, UpdateFunctionCode, UpdateFunctionConfiguration, AddPermission, RemovePermission, GetPolicy, ListVersionsByFunction, PublishVersion, CreateAlias, GetAlias, UpdateAlias, DeleteAlias, ListAliases, TagResource, UntagResource, ListTags, CreateEventSourceMapping, DeleteEventSourceMapping, GetEventSourceMapping, ListEventSourceMappings, UpdateEventSourceMapping, CreateFunctionUrlConfig, GetFunctionUrlConfig, UpdateFunctionUrlConfig, DeleteFunctionUrlConfig, ListFunctionUrlConfigs, PutFunctionConcurrency, GetFunctionConcurrency, DeleteFunctionConcurrency, PutFunctionEventInvokeConfig, GetFunctionEventInvokeConfig, DeleteFunctionEventInvokeConfig, PublishLayerVersion, GetLayerVersion, GetLayerVersionByArn, ListLayerVersions, DeleteLayerVersion, ListLayers, AddLayerVersionPermission, RemoveLayerVersionPermission, GetLayerVersionPolicy

### Step Functions

Full ASL interpreter with Retry/Catch, waitForTaskToken, Activities, TestState API. All state types: Pass, Task, Choice, Wait, Succeed, Fail, Map, Parallel. SFN mock config compatible.

**Protocol:** JSON (`X-Amz-Target` header)

**Operations:** CreateStateMachine, DeleteStateMachine, DescribeStateMachine, UpdateStateMachine, ListStateMachines, StartExecution, StartSyncExecution, StopExecution, DescribeExecution, DescribeStateMachineForExecution, ListExecutions, GetExecutionHistory, SendTaskSuccess, SendTaskFailure, SendTaskHeartbeat, CreateActivity, DeleteActivity, DescribeActivity, ListActivities, GetActivityTask, TestState, TagResource, UntagResource, ListTagsForResource

## Security & Identity

### IAM

Users, roles, policies, access keys, groups, instance profiles, service-linked roles, OIDC providers.

**Protocol:** Query/XML

**Operations:** CreateUser, GetUser, ListUsers, DeleteUser, CreateRole, GetRole, ListRoles, DeleteRole, UpdateRole, UpdateAssumeRolePolicy, CreatePolicy, GetPolicy, GetPolicyVersion, ListPolicyVersions, CreatePolicyVersion, DeletePolicyVersion, ListPolicies, DeletePolicy, ListEntitiesForPolicy, AttachRolePolicy, DetachRolePolicy, ListAttachedRolePolicies, PutRolePolicy, GetRolePolicy, DeleteRolePolicy, ListRolePolicies, AttachUserPolicy, DetachUserPolicy, ListAttachedUserPolicies, CreateAccessKey, ListAccessKeys, DeleteAccessKey, CreateInstanceProfile, DeleteInstanceProfile, GetInstanceProfile, AddRoleToInstanceProfile, RemoveRoleFromInstanceProfile, ListInstanceProfiles, ListInstanceProfilesForRole, TagRole, UntagRole, ListRoleTags, TagUser, UntagUser, ListUserTags, TagPolicy, UntagPolicy, ListPolicyTags, SimulatePrincipalPolicy, SimulateCustomPolicy, CreateGroup, GetGroup, DeleteGroup, ListGroups, AddUserToGroup, RemoveUserFromGroup, ListGroupsForUser, PutUserPolicy, GetUserPolicy, DeleteUserPolicy, ListUserPolicies, CreateServiceLinkedRole, DeleteServiceLinkedRole, GetServiceLinkedRoleDeletionStatus, CreateOpenIDConnectProvider, GetOpenIDConnectProvider, DeleteOpenIDConnectProvider

### STS

**Protocol:** JSON + Query/XML

**Operations:** GetCallerIdentity, AssumeRole, AssumeRoleWithWebIdentity, GetSessionToken, GetAccessKeyInfo

### KMS

Symmetric and RSA keys, encryption, signing, aliases, key rotation, key policies.

**Protocol:** JSON

**Operations:** CreateKey, ListKeys, DescribeKey, GetPublicKey, Sign, Verify, Encrypt, Decrypt, GenerateDataKey, GenerateDataKeyWithoutPlaintext, CreateAlias, DeleteAlias, ListAliases, UpdateAlias, EnableKeyRotation, DisableKeyRotation, GetKeyRotationStatus, GetKeyPolicy, PutKeyPolicy, ListKeyPolicies, EnableKey, DisableKey, ScheduleKeyDeletion, CancelKeyDeletion, TagResource, UntagResource, ListResourceTags

### Secrets Manager

Secrets with versioning, rotation staging, batch retrieval, resource policies.

**Protocol:** JSON

**Operations:** CreateSecret, GetSecretValue, BatchGetSecretValue, ListSecrets, DeleteSecret, RestoreSecret, UpdateSecret, DescribeSecret, PutSecretValue, UpdateSecretVersionStage, TagResource, UntagResource, ListSecretVersionIds, RotateSecret, GetRandomPassword, ReplicateSecretToRegions, PutResourcePolicy, GetResourcePolicy, DeleteResourcePolicy, ValidateResourcePolicy

### Cognito User Pools

User pools, users, groups, app clients, JWT tokens, MFA, auth flows.

**Protocol:** JSON

**Operations:** CreateUserPool, DeleteUserPool, DescribeUserPool, ListUserPools, UpdateUserPool, CreateUserPoolClient, DeleteUserPoolClient, DescribeUserPoolClient, ListUserPoolClients, UpdateUserPoolClient, AdminCreateUser, AdminDeleteUser, AdminGetUser, ListUsers, AdminSetUserPassword, AdminUpdateUserAttributes, AdminConfirmSignUp, AdminDisableUser, AdminEnableUser, AdminResetUserPassword, AdminUserGlobalSignOut, AdminListGroupsForUser, AdminListUserAuthEvents, AdminAddUserToGroup, AdminRemoveUserFromGroup, AdminInitiateAuth, AdminRespondToAuthChallenge, InitiateAuth, RespondToAuthChallenge, GlobalSignOut, RevokeToken, SignUp, ConfirmSignUp, ForgotPassword, ConfirmForgotPassword, ChangePassword, GetUser, UpdateUserAttributes, DeleteUser, CreateGroup, DeleteGroup, GetGroup, ListGroups, ListUsersInGroup, CreateUserPoolDomain, DeleteUserPoolDomain, DescribeUserPoolDomain, GetUserPoolMfaConfig, SetUserPoolMfaConfig, AssociateSoftwareToken, VerifySoftwareToken, AdminSetUserMFAPreference, SetUserMFAPreference, TagResource, UntagResource, ListTagsForResource

### Cognito Identity

Identity pools, federated identities, credentials.

**Protocol:** JSON

**Operations:** CreateIdentityPool, DeleteIdentityPool, DescribeIdentityPool, ListIdentityPools, UpdateIdentityPool, GetId, GetCredentialsForIdentity, GetOpenIdToken, SetIdentityPoolRoles, GetIdentityPoolRoles, ListIdentities, DescribeIdentity, MergeDeveloperIdentities, UnlinkDeveloperIdentity, UnlinkIdentity, TagResource, UntagResource, ListTagsForResource

### ACM

Certificates with auto-issuance, DNS validation, SANs.

**Protocol:** JSON

**Operations:** RequestCertificate, DescribeCertificate, ListCertificates, DeleteCertificate, GetCertificate, ImportCertificate, AddTagsToCertificate, RemoveTagsFromCertificate, ListTagsForCertificate, UpdateCertificateOptions, RenewCertificate, ResendValidationEmail

### WAF v2

Web ACLs, IP sets, rule groups, resource associations. LockToken enforced.

**Protocol:** JSON

**Operations:** CreateWebACL, GetWebACL, UpdateWebACL, DeleteWebACL, ListWebACLs, AssociateWebACL, DisassociateWebACL, GetWebACLForResource, ListResourcesForWebACL, CreateIPSet, GetIPSet, UpdateIPSet, DeleteIPSet, ListIPSets, CreateRuleGroup, GetRuleGroup, UpdateRuleGroup, DeleteRuleGroup, ListRuleGroups, TagResource, UntagResource, ListTagsForResource, CheckCapacity, DescribeManagedRuleGroup

## Networking & Content Delivery

### EC2

Instances, VPCs, subnets, security groups, EBS volumes, snapshots, key pairs, route tables, internet gateways, NAT gateways, elastic IPs, network ACLs, VPC endpoints, launch templates, and more.

**Protocol:** Query/XML

**Operations:** RunInstances, DescribeInstances, TerminateInstances, StopInstances, StartInstances, RebootInstances, DescribeInstanceAttribute, DescribeInstanceTypes, DescribeImages, CreateSecurityGroup, DeleteSecurityGroup, DescribeSecurityGroups, AuthorizeSecurityGroupIngress, RevokeSecurityGroupIngress, AuthorizeSecurityGroupEgress, RevokeSecurityGroupEgress, DescribeSecurityGroupRules, CreateKeyPair, DeleteKeyPair, DescribeKeyPairs, ImportKeyPair, DescribeVpcs, CreateVpc, DeleteVpc, DescribeVpcAttribute, ModifyVpcAttribute, DescribeSubnets, CreateSubnet, DeleteSubnet, ModifySubnetAttribute, CreateInternetGateway, DeleteInternetGateway, DescribeInternetGateways, AttachInternetGateway, DetachInternetGateway, DescribeAvailabilityZones, AllocateAddress, ReleaseAddress, AssociateAddress, DisassociateAddress, DescribeAddresses, CreateTags, DeleteTags, DescribeTags, CreateRouteTable, DeleteRouteTable, DescribeRouteTables, AssociateRouteTable, DisassociateRouteTable, ReplaceRouteTableAssociation, CreateRoute, ReplaceRoute, DeleteRoute, CreateNetworkInterface, DeleteNetworkInterface, DescribeNetworkInterfaces, AttachNetworkInterface, DetachNetworkInterface, CreateVpcEndpoint, DeleteVpcEndpoints, DescribeVpcEndpoints, ModifyVpcEndpoint, DescribePrefixLists, CreateVolume, DeleteVolume, DescribeVolumes, DescribeVolumeStatus, AttachVolume, DetachVolume, ModifyVolume, CreateSnapshot, DeleteSnapshot, DescribeSnapshots, CopySnapshot, CreateNatGateway, DescribeNatGateways, DeleteNatGateway, CreateNetworkAcl, DescribeNetworkAcls, DeleteNetworkAcl, CreateNetworkAclEntry, DeleteNetworkAclEntry, ReplaceNetworkAclEntry, CreateFlowLogs, DescribeFlowLogs, DeleteFlowLogs, CreateVpcPeeringConnection, AcceptVpcPeeringConnection, DescribeVpcPeeringConnections, DeleteVpcPeeringConnection, CreateDhcpOptions, AssociateDhcpOptions, DescribeDhcpOptions, DeleteDhcpOptions, CreateManagedPrefixList, DescribeManagedPrefixLists, GetManagedPrefixListEntries, ModifyManagedPrefixList, DeleteManagedPrefixList, CreateLaunchTemplate, CreateLaunchTemplateVersion, DescribeLaunchTemplates, DescribeLaunchTemplateVersions, ModifyLaunchTemplate, DeleteLaunchTemplate, DescribeAccountAttributes

### Route 53

Hosted zones, record sets, health checks, tags.

**Protocol:** REST/XML (path-based)

**Operations:** CreateHostedZone, GetHostedZone, ListHostedZones, DeleteHostedZone, ChangeResourceRecordSets, ListResourceRecordSets, GetChange, ChangeTagsForResource, ListTagsForResource, CreateHealthCheck, GetHealthCheck, ListHealthChecks, UpdateHealthCheck, DeleteHealthCheck

### CloudFront

Distributions, invalidations, origin access controls. ETag-based optimistic concurrency.

**Protocol:** REST/XML (path-based)

**Operations:** CreateDistribution, GetDistribution, GetDistributionConfig, ListDistributions, UpdateDistribution, DeleteDistribution, CreateInvalidation, ListInvalidations, GetInvalidation, CreateOriginAccessControl, GetOriginAccessControl, GetOriginAccessControlConfig, ListOriginAccessControls, UpdateOriginAccessControl, DeleteOriginAccessControl, TagResource, UntagResource, ListTagsForResource

### ALB (ELBv2)

Load balancers, target groups, listeners, rules. ALB→Lambda live traffic routing.

**Protocol:** Query/XML

**Operations:** CreateLoadBalancer, DescribeLoadBalancers, DeleteLoadBalancer, DescribeLoadBalancerAttributes, ModifyLoadBalancerAttributes, SetSecurityGroups, SetSubnets, CreateTargetGroup, DescribeTargetGroups, ModifyTargetGroup, DeleteTargetGroup, DescribeTargetGroupAttributes, ModifyTargetGroupAttributes, CreateListener, DescribeListeners, ModifyListener, DeleteListener, CreateRule, DescribeRules, ModifyRule, DeleteRule, SetRulePriorities, RegisterTargets, DeregisterTargets, DescribeTargetHealth, AddTags, RemoveTags, DescribeTags

### API Gateway v1 + v2

REST APIs (v1) with MOCK and AWS_PROXY integrations. HTTP APIs (v2) with Lambda proxy. Data plane via `{apiId}.execute-api.localhost`.

**Protocol:** REST/JSON (path-based)

### Service Discovery (Cloud Map)

Namespaces, services, instances with Route 53 integration.

**Protocol:** JSON

**Operations:** CreateHttpNamespace, CreatePrivateDnsNamespace, CreatePublicDnsNamespace, CreateService, DeleteNamespace, DeleteService, DeregisterInstance, DiscoverInstances, DiscoverInstancesRevision, GetInstance, GetInstancesHealthStatus, GetNamespace, GetOperation, GetService, ListInstances, ListNamespaces, ListOperations, ListServices, ListTagsForResource, RegisterInstance, TagResource, UntagResource, UpdateHttpNamespace, UpdateInstanceCustomHealthStatus, UpdatePrivateDnsNamespace, UpdatePublicDnsNamespace, UpdateService

## Messaging & Streaming

### EventBridge

Event buses, rules, targets, archives, replays, connections, API destinations, endpoints, partner events.

**Protocol:** JSON

**Operations:** CreateEventBus, UpdateEventBus, DeleteEventBus, ListEventBuses, DescribeEventBus, PutRule, DeleteRule, ListRules, DescribeRule, EnableRule, DisableRule, PutTargets, RemoveTargets, ListTargetsByRule, ListRuleNamesByTarget, TestEventPattern, PutEvents, TagResource, UntagResource, ListTagsForResource, CreateArchive, DeleteArchive, DescribeArchive, UpdateArchive, ListArchives, StartReplay, DescribeReplay, ListReplays, CancelReplay, PutPermission, RemovePermission, CreateConnection, DescribeConnection, DeleteConnection, ListConnections, UpdateConnection, DeauthorizeConnection, CreateApiDestination, DescribeApiDestination, DeleteApiDestination, ListApiDestinations, UpdateApiDestination, CreateEndpoint, DeleteEndpoint, DescribeEndpoint, ListEndpoints, UpdateEndpoint, ActivateEventSource, DeactivateEventSource, DescribeEventSource, CreatePartnerEventSource, DeletePartnerEventSource, DescribePartnerEventSource, ListPartnerEventSources, ListPartnerEventSourceAccounts, ListEventSources, PutPartnerEvents

### Kinesis

Streams, shards, records, consumers. Partition key routing, AWS limits enforced.

**Protocol:** JSON

**Operations:** CreateStream, DeleteStream, DescribeStream, DescribeStreamSummary, ListStreams, ListShards, PutRecord, PutRecords, GetShardIterator, GetRecords, IncreaseStreamRetentionPeriod, DecreaseStreamRetentionPeriod, AddTagsToStream, RemoveTagsFromStream, ListTagsForStream, MergeShards, SplitShard, UpdateShardCount, RegisterStreamConsumer, DeregisterStreamConsumer, ListStreamConsumers, DescribeStreamConsumer, StartStreamEncryption, StopStreamEncryption, EnableEnhancedMonitoring, DisableEnhancedMonitoring

### Firehose

Delivery streams, records. S3 destinations write to the local S3 emulator.

**Protocol:** JSON

**Operations:** CreateDeliveryStream, DeleteDeliveryStream, DescribeDeliveryStream, ListDeliveryStreams, PutRecord, PutRecordBatch, UpdateDestination, TagDeliveryStream, UntagDeliveryStream, ListTagsForDeliveryStream, StartDeliveryStreamEncryption, StopDeliveryStreamEncryption

## Management & Monitoring

### CloudFormation

Stacks, change sets, exports, cross-stack references. JSON and YAML templates with intrinsic functions and pseudo-parameters.

**Protocol:** Query/XML

**Operations:** CreateStack, DescribeStacks, ListStacks, DeleteStack, UpdateStack, DescribeStackEvents, DescribeStackResource, DescribeStackResources, ListStackResources, GetTemplate, ValidateTemplate, ListExports, CreateChangeSet, DescribeChangeSet, ExecuteChangeSet, DeleteChangeSet, ListChangeSets, GetTemplateSummary, UpdateTerminationProtection, SetStackPolicy, GetStackPolicy, ListImports

### CloudWatch Logs

Log groups, streams, events, metric filters, subscription filters, destinations, queries.

**Protocol:** JSON

**Operations:** CreateLogGroup, DeleteLogGroup, DescribeLogGroups, CreateLogStream, DeleteLogStream, DescribeLogStreams, PutLogEvents, GetLogEvents, FilterLogEvents, PutRetentionPolicy, DeleteRetentionPolicy, PutSubscriptionFilter, DeleteSubscriptionFilter, DescribeSubscriptionFilters, TagLogGroup, UntagLogGroup, ListTagsLogGroup, TagResource, UntagResource, ListTagsForResource, PutDestination, DeleteDestination, DescribeDestinations, PutDestinationPolicy, PutMetricFilter, DeleteMetricFilter, DescribeMetricFilters, StartQuery, GetQueryResults, StopQuery

### CloudWatch Metrics

Metrics, alarms (standard + composite), dashboards. CBOR and JSON protocol.

**Protocol:** Query/XML + Smithy RPC v2 CBOR

**Operations:** PutMetricData, GetMetricStatistics, GetMetricData, ListMetrics, PutMetricAlarm, PutCompositeAlarm, DescribeAlarms, DescribeAlarmsForMetric, DescribeAlarmHistory, DeleteAlarms, EnableAlarmActions, DisableAlarmActions, SetAlarmState, TagResource, UntagResource, ListTagsForResource, PutDashboard, GetDashboard, DeleteDashboards, ListDashboards

### SSM Parameter Store

Parameters (String, SecureString, StringList), history, labels, tags.

**Protocol:** JSON

**Operations:** PutParameter, GetParameter, GetParameters, GetParametersByPath, DeleteParameter, DeleteParameters, DescribeParameters, GetParameterHistory, LabelParameterVersion, AddTagsToResource, RemoveTagsFromResource, ListTagsForResource

## Compute & Containers

### ECS

Clusters, task definitions, tasks, services, capacity providers. Full Terraform ECS coverage.

**Protocol:** JSON

**Operations:** CreateCluster, DeleteCluster, DescribeClusters, ListClusters, UpdateCluster, UpdateClusterSettings, PutClusterCapacityProviders, RegisterTaskDefinition, DeregisterTaskDefinition, DescribeTaskDefinition, ListTaskDefinitions, ListTaskDefinitionFamilies, DeleteTaskDefinitions, CreateService, DeleteService, DescribeServices, UpdateService, ListServices, ListServicesByNamespace, RunTask, StopTask, DescribeTasks, ListTasks, ExecuteCommand, UpdateTaskProtection, GetTaskProtection, TagResource, UntagResource, ListTagsForResource, ListAccountSettings, PutAccountSetting, PutAccountSettingDefault, DeleteAccountSetting, CreateCapacityProvider, DeleteCapacityProvider, UpdateCapacityProvider, DescribeCapacityProviders, PutAttributes, DeleteAttributes, ListAttributes, DescribeServiceDeployments, ListServiceDeployments, DescribeServiceRevisions, SubmitTaskStateChange, SubmitContainerStateChange, SubmitAttachmentStateChanges, DiscoverPollEndpoint

### EMR

Clusters, instance groups, steps, bootstrap actions.

**Protocol:** JSON

**Operations:** RunJobFlow, DescribeCluster, ListClusters, TerminateJobFlows, ModifyCluster, SetTerminationProtection, SetVisibleToAllUsers, AddJobFlowSteps, DescribeStep, ListSteps, CancelSteps, AddInstanceFleet, ListInstanceFleets, ModifyInstanceFleet, AddInstanceGroups, ListInstanceGroups, ModifyInstanceGroups, ListBootstrapActions, AddTags, RemoveTags, GetBlockPublicAccessConfiguration, PutBlockPublicAccessConfiguration

### ECR

Repositories, images, lifecycle policies, layer upload.

**Protocol:** JSON

**Operations:** CreateRepository, DescribeRepositories, DeleteRepository, ListImages, PutImage, BatchGetImage, BatchDeleteImage, DescribeImages, GetAuthorizationToken, GetRepositoryPolicy, SetRepositoryPolicy, DeleteRepositoryPolicy, PutLifecyclePolicy, GetLifecyclePolicy, DeleteLifecyclePolicy, ListTagsForResource, TagResource, UntagResource, PutImageTagMutability, PutImageScanningConfiguration, DescribeRegistry, GetDownloadUrlForLayer, BatchCheckLayerAvailability, InitiateLayerUpload, UploadLayerPart, CompleteLayerUpload

## Database

### RDS

DB instances, clusters, global clusters, proxies, parameter groups, subnet groups, snapshots, event subscriptions.

**Protocol:** Query/XML

**Operations:** CreateDBInstance, DescribeDBInstances, ModifyDBInstance, DeleteDBInstance, RebootDBInstance, StopDBInstance, StartDBInstance, CreateDBCluster, DescribeDBClusters, ModifyDBCluster, DeleteDBCluster, StopDBCluster, StartDBCluster, CreateDBSubnetGroup, DescribeDBSubnetGroups, ModifyDBSubnetGroup, DeleteDBSubnetGroup, CreateDBParameterGroup, DescribeDBParameterGroups, DeleteDBParameterGroup, DescribeDBParameters, ModifyDBParameterGroup, CreateDBClusterParameterGroup, DescribeDBClusterParameterGroups, DeleteDBClusterParameterGroup, CreateDBSnapshot, DescribeDBSnapshots, DeleteDBSnapshot, CopyDBSnapshot, CreateDBClusterSnapshot, DescribeDBClusterSnapshots, DeleteDBClusterSnapshot, AddTagsToResource, ListTagsForResource, RemoveTagsFromResource, CreateEventSubscription, DescribeEventSubscriptions, DeleteEventSubscription, CreateDBProxy, DescribeDBProxies, DeleteDBProxy, CreateOptionGroup, DescribeOptionGroups, DeleteOptionGroup, CreateGlobalCluster, DescribeGlobalClusters, ModifyGlobalCluster, DeleteGlobalCluster, RemoveFromGlobalCluster, DescribeOrderableDBInstanceOptions, DescribeEngineDefaultClusterParameters, DescribeDBEngineVersions

### RDS Data API

**Protocol:** REST/JSON (path-based)

**Operations:** ExecuteStatement, BatchExecuteStatement, BeginTransaction, CommitTransaction, RollbackTransaction

### ElastiCache

Cache clusters, replication groups, subnet groups, parameter groups, users, user groups, snapshots.

**Protocol:** Query/XML

**Operations:** CreateCacheCluster, DeleteCacheCluster, DescribeCacheClusters, ModifyCacheCluster, RebootCacheCluster, CreateReplicationGroup, DeleteReplicationGroup, DescribeReplicationGroups, ModifyReplicationGroup, IncreaseReplicaCount, DecreaseReplicaCount, CreateCacheSubnetGroup, DescribeCacheSubnetGroups, DeleteCacheSubnetGroup, ModifyCacheSubnetGroup, CreateCacheParameterGroup, DescribeCacheParameterGroups, DeleteCacheParameterGroup, DescribeCacheParameters, ModifyCacheParameterGroup, ResetCacheParameterGroup, DescribeCacheEngineVersions, CreateUser, DescribeUsers, DeleteUser, ModifyUser, CreateUserGroup, DescribeUserGroups, DeleteUserGroup, ModifyUserGroup, ListTagsForResource, AddTagsToResource, RemoveTagsFromResource, CreateSnapshot, DeleteSnapshot, DescribeSnapshots, DescribeEvents

## Analytics

### Glue

Databases, tables, crawlers, jobs, triggers, partitions, connections, schema registry.

**Protocol:** JSON

**Operations:** CreateDatabase, GetDatabase, GetDatabases, DeleteDatabase, UpdateDatabase, CreateTable, GetTable, GetTables, DeleteTable, UpdateTable, BatchDeleteTable, BatchGetTable, CreatePartition, GetPartition, GetPartitions, DeletePartition, BatchCreatePartition, BatchDeletePartition, UpdatePartition, CreateCrawler, GetCrawler, GetCrawlers, DeleteCrawler, UpdateCrawler, StartCrawler, StopCrawler, GetCrawlerMetrics, CreateJob, GetJob, GetJobs, DeleteJob, UpdateJob, StartJobRun, GetJobRun, GetJobRuns, BatchStopJobRun, CreateRegistry, GetRegistry, ListRegistries, DeleteRegistry, CreateSchema, GetSchema, ListSchemas, DeleteSchema, RegisterSchemaVersion, GetSchemaVersion, ListSchemaVersions, TagResource, UntagResource, GetTags

### Athena

Work groups, named queries, query executions, data catalogs, prepared statements.

**Protocol:** JSON

**Operations:** StartQueryExecution, GetQueryExecution, GetQueryResults, StopQueryExecution, ListQueryExecutions, BatchGetQueryExecution, CreateWorkGroup, GetWorkGroup, ListWorkGroups, DeleteWorkGroup, UpdateWorkGroup, CreateNamedQuery, GetNamedQuery, DeleteNamedQuery, ListNamedQueries, BatchGetNamedQuery, CreateDataCatalog, GetDataCatalog, ListDataCatalogs, DeleteDataCatalog, UpdateDataCatalog, CreatePreparedStatement, GetPreparedStatement, DeletePreparedStatement, ListPreparedStatements, GetTableMetadata, ListTableMetadata, GetDatabase, ListDatabases, TagResource, UntagResource, ListTagsForResource

## Other

### SES

Email identities, templates, configuration sets (v1 + v2). Emails stored in-memory, not sent.

**Protocol:** Query/XML + REST/JSON (v2)

**Operations:** SendEmail, SendRawEmail, SendTemplatedEmail, SendBulkTemplatedEmail, VerifyEmailIdentity, VerifyEmailAddress, VerifyDomainIdentity, VerifyDomainDkim, ListIdentities, GetIdentityVerificationAttributes, DeleteIdentity, GetSendQuota, GetSendStatistics, ListVerifiedEmailAddresses, CreateConfigurationSet, DeleteConfigurationSet, DescribeConfigurationSet, ListConfigurationSets, CreateTemplate, GetTemplate, DeleteTemplate, ListTemplates, UpdateTemplate, GetIdentityDkimAttributes, SetIdentityNotificationTopic, SetIdentityFeedbackForwardingEnabled

### EFS

File systems, mount targets, access points.

**Protocol:** REST/JSON (path-based)

### AppSync

GraphQL APIs, types, resolvers, data sources, API keys.

**Protocol:** REST/JSON (path-based)

## Protocol Support

MicroStack handles five protocol families:

- **AWS Query/XML** — Form-encoded `Action` parameter, XML responses (EC2, RDS, SQS legacy, etc.)
- **AWS JSON** — `X-Amz-Target` header, JSON request/response (DynamoDB, ECS, Step Functions, etc.)
- **REST/JSON** — Path-based routing, JSON bodies (Lambda, API Gateway, EFS, etc.)
- **REST/XML** — Path-based routing, XML bodies (S3, Route 53, CloudFront)
- **Smithy RPC v2 CBOR** — Binary CBOR encoding (CloudWatch Metrics alternate protocol)
