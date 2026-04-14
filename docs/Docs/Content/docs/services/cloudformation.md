---
title: CloudFormation
description: CloudFormation emulation — stacks, change sets, exports, rollback, and cross-stack references with intrinsic function support.
order: 25
section: Services
---

# CloudFormation

MicroStack emulates AWS CloudFormation, provisioning actual MicroStack resources (S3 buckets, SQS queues, DynamoDB tables, Lambda functions, IAM roles, etc.) when a stack is created or updated. Stacks complete synchronously — `CreateStack` returns only after all resources are provisioned. Failed resources trigger automatic rollback, leaving the stack in `ROLLBACK_COMPLETE`. Change sets are also supported for staged updates.

## Supported Operations

CreateStack, DescribeStacks, ListStacks, DeleteStack, UpdateStack, DescribeStackEvents, DescribeStackResource, DescribeStackResources, ListStackResources, GetTemplate, ValidateTemplate, ListExports, CreateChangeSet, DescribeChangeSet, ExecuteChangeSet, DeleteChangeSet, ListChangeSets, GetTemplateSummary, UpdateTerminationProtection, SetStackPolicy, GetStackPolicy, ListImports

## Usage

```csharp
var client = new AmazonCloudFormationClient(
    new BasicAWSCredentials("test", "test"),
    new AmazonCloudFormationConfig
    {
        RegionEndpoint = RegionEndpoint.USEast1,
        ServiceURL = "http://localhost:4566",
    });

// Define a simple template as JSON
var template = """
    {
      "AWSTemplateFormatVersion": "2010-09-09",
      "Parameters": {
        "QueueName": { "Type": "String", "Default": "my-queue" }
      },
      "Resources": {
        "MyQueue": {
          "Type": "AWS::SQS::Queue",
          "Properties": {
            "QueueName": { "Ref": "QueueName" }
          }
        },
        "MyBucket": {
          "Type": "AWS::S3::Bucket",
          "Properties": {
            "BucketName": "my-cfn-bucket"
          }
        }
      },
      "Outputs": {
        "QueueArn": {
          "Value": { "Fn::GetAtt": ["MyQueue", "Arn"] },
          "Export": { "Name": "my-queue-arn" }
        }
      }
    }
    """;

// Create the stack — resources are provisioned synchronously
await client.CreateStackAsync(new CreateStackRequest
{
    StackName = "my-stack",
    TemplateBody = template,
    Parameters =
    [
        new Parameter { ParameterKey = "QueueName", ParameterValue = "custom-queue" },
    ],
});

// Describe the stack
var descResp = await client.DescribeStacksAsync(new DescribeStacksRequest
{
    StackName = "my-stack",
});

Console.WriteLine(descResp.Stacks[0].StackStatus); // CREATE_COMPLETE

// Read an exported output
var exports = await client.ListExportsAsync(new ListExportsRequest());
var queueArn = exports.Exports.First(e => e.Name == "my-queue-arn").Value;
Console.WriteLine(queueArn); // arn:aws:sqs:us-east-1:...

// Delete the stack (resources are deleted too)
await client.DeleteStackAsync(new DeleteStackRequest { StackName = "my-stack" });
```

## Change Sets

Use change sets to preview and stage stack updates before applying them.

```csharp
// Create stack via change set
await client.CreateChangeSetAsync(new CreateChangeSetRequest
{
    StackName = "my-stack",
    ChangeSetName = "initial-create",
    TemplateBody = template,
    ChangeSetType = ChangeSetType.CREATE,
});

var csDesc = await client.DescribeChangeSetAsync(new DescribeChangeSetRequest
{
    StackName = "my-stack",
    ChangeSetName = "initial-create",
});
Console.WriteLine(csDesc.ChangeSetName); // initial-create

// Execute the change set to actually provision resources
await client.ExecuteChangeSetAsync(new ExecuteChangeSetRequest
{
    StackName = "my-stack",
    ChangeSetName = "initial-create",
});

var desc = await client.DescribeStacksAsync(new DescribeStacksRequest { StackName = "my-stack" });
Console.WriteLine(desc.Stacks[0].StackStatus); // CREATE_COMPLETE
```

## Supported Resource Types

CloudFormation in MicroStack provisions real emulated resources for the following types:

- `AWS::S3::Bucket`, `AWS::S3::BucketPolicy`
- `AWS::SQS::Queue`
- `AWS::SNS::Topic`, `AWS::SNS::Subscription`
- `AWS::DynamoDB::Table`
- `AWS::Lambda::Function`, `AWS::Lambda::EventSourceMapping`
- `AWS::IAM::Role`, `AWS::IAM::Policy`, `AWS::IAM::ManagedPolicy`, `AWS::IAM::InstanceProfile`
- `AWS::KMS::Key`, `AWS::KMS::Alias`
- `AWS::SSM::Parameter`
- `AWS::SecretsManager::Secret`
- `AWS::ECR::Repository`
- `AWS::Kinesis::Stream`
- `AWS::ElasticLoadBalancingV2::LoadBalancer`, `AWS::ElasticLoadBalancingV2::Listener`, `AWS::ElasticLoadBalancingV2::TargetGroup`
- `AWS::CloudFormation::WaitConditionHandle`, `AWS::CloudFormation::WaitCondition` (no-op)

## Supported Intrinsic Functions

`Ref`, `Fn::GetAtt`, `Fn::Sub`, `Fn::Join`, `Fn::Select`, `Fn::If`, `Fn::Equals`, `Fn::And`, `Fn::Or`, `Fn::Not`, `Fn::Base64`, `Fn::Split`, `Fn::FindInMap`, `Condition`

:::aside{type="note" title="Synchronous provisioning"}
Unlike real AWS CloudFormation, MicroStack processes stacks synchronously — `CreateStack` and `UpdateStack` block until provisioning completes. Stacks with unsupported resource types fail and roll back automatically, leaving the stack in `ROLLBACK_COMPLETE` or `UPDATE_ROLLBACK_COMPLETE`.
:::
