---
title: EC2
description: EC2 emulation — instances, VPCs, subnets, security groups, EBS, key pairs, route tables, and more.
order: 17
section: Services
---

# EC2

MicroStack's EC2 handler provides metadata-only emulation of the full EC2 API surface — instances, VPCs, subnets, security groups, EBS volumes, snapshots, key pairs, route tables, internet gateways, NAT gateways, elastic IPs, and more. No actual virtual machines are launched.

## Supported Operations

**Instances**
- RunInstances, DescribeInstances, TerminateInstances, StopInstances, StartInstances, RebootInstances
- DescribeInstanceAttribute, DescribeInstanceTypes, DescribeImages

**VPC and Networking**
- CreateVpc, DeleteVpc, DescribeVpcs, DescribeVpcAttribute, ModifyVpcAttribute
- CreateSubnet, DeleteSubnet, DescribeSubnets, ModifySubnetAttribute
- CreateInternetGateway, DeleteInternetGateway, DescribeInternetGateways, AttachInternetGateway, DetachInternetGateway
- CreateNatGateway, DescribeNatGateways, DeleteNatGateway
- CreateRouteTable, DeleteRouteTable, DescribeRouteTables, AssociateRouteTable, DisassociateRouteTable, CreateRoute, DeleteRoute
- AllocateAddress, ReleaseAddress, AssociateAddress, DisassociateAddress, DescribeAddresses
- CreateNetworkInterface, DeleteNetworkInterface, DescribeNetworkInterfaces, AttachNetworkInterface, DetachNetworkInterface
- CreateVpcEndpoint, DeleteVpcEndpoints, DescribeVpcEndpoints, ModifyVpcEndpoint
- CreateNetworkAcl, DescribeNetworkAcls, DeleteNetworkAcl, CreateNetworkAclEntry, DeleteNetworkAclEntry
- CreateVpcPeeringConnection, AcceptVpcPeeringConnection, DescribeVpcPeeringConnections, DeleteVpcPeeringConnection
- CreateFlowLogs, DescribeFlowLogs, DeleteFlowLogs
- CreateDhcpOptions, AssociateDhcpOptions, DescribeDhcpOptions, DeleteDhcpOptions
- CreateManagedPrefixList, DescribeManagedPrefixLists, GetManagedPrefixListEntries, ModifyManagedPrefixList, DeleteManagedPrefixList

**Security Groups**
- CreateSecurityGroup, DeleteSecurityGroup, DescribeSecurityGroups, DescribeSecurityGroupRules
- AuthorizeSecurityGroupIngress, RevokeSecurityGroupIngress
- AuthorizeSecurityGroupEgress, RevokeSecurityGroupEgress

**Key Pairs**
- CreateKeyPair, DeleteKeyPair, DescribeKeyPairs, ImportKeyPair

**EBS Volumes and Snapshots**
- CreateVolume, DeleteVolume, DescribeVolumes, DescribeVolumeStatus, AttachVolume, DetachVolume, ModifyVolume
- CreateSnapshot, DeleteSnapshot, DescribeSnapshots, CopySnapshot

**Launch Templates**
- CreateLaunchTemplate, DescribeLaunchTemplates, GetLaunchTemplateData, DeleteLaunchTemplate, CreateLaunchTemplateVersion

**Tags**
- CreateTags, DeleteTags, DescribeTags

**Other**
- DescribeAvailabilityZones, DescribePrefixLists

## Usage

```csharp
var ec2 = new AmazonEC2Client(
    new BasicAWSCredentials("test", "test"),
    new AmazonEC2Config { ServiceURL = "http://localhost:4566" });

// Run an instance
var reservation = await ec2.RunInstancesAsync(new RunInstancesRequest
{
    ImageId = "ami-00000000",
    MinCount = 1,
    MaxCount = 1,
    InstanceType = InstanceType.T2Micro,
});

var instanceId = reservation.Reservation.Instances[0].InstanceId;
Console.WriteLine(instanceId);  // i-...

// Describe the instance
var instances = await ec2.DescribeInstancesAsync(new DescribeInstancesRequest
{
    InstanceIds = [instanceId],
});

var state = instances.Reservations[0].Instances[0].State.Name;
Console.WriteLine(state);  // running

// Terminate it
await ec2.TerminateInstancesAsync(new TerminateInstancesRequest
{
    InstanceIds = [instanceId],
});
```

## VPC and Subnet Setup

```csharp
// Create a VPC
var vpc = await ec2.CreateVpcAsync(new CreateVpcRequest
{
    CidrBlock = "10.0.0.0/16",
});
var vpcId = vpc.Vpc.VpcId;

// Create a subnet inside the VPC
var subnet = await ec2.CreateSubnetAsync(new CreateSubnetRequest
{
    VpcId = vpcId,
    CidrBlock = "10.0.1.0/24",
    AvailabilityZone = "us-east-1a",
});

// Create a security group
var sg = await ec2.CreateSecurityGroupAsync(new CreateSecurityGroupRequest
{
    GroupName = "web-sg",
    Description = "Web server security group",
    VpcId = vpcId,
});

// Allow inbound HTTP
await ec2.AuthorizeSecurityGroupIngressAsync(new AuthorizeSecurityGroupIngressRequest
{
    GroupId = sg.GroupId,
    IpPermissions =
    [
        new IpPermission
        {
            IpProtocol = "tcp",
            FromPort = 80,
            ToPort = 80,
            Ipv4Ranges = [new IpRange { CidrIp = "0.0.0.0/0" }],
        },
    ],
});
```

## Key Pairs

```csharp
// Create a key pair
var keyPair = await ec2.CreateKeyPairAsync(new CreateKeyPairRequest
{
    KeyName = "my-key",
});

Console.WriteLine(keyPair.KeyPair.KeyName);
Console.WriteLine(keyPair.KeyPair.KeyFingerprint);
// Private key material is returned only at creation time
```

:::aside{type="note" title="Metadata only"}
EC2 is metadata-only — no actual virtual machines are started. Instance state transitions (stop/start/terminate) are tracked in memory but no compute resources are provisioned.
:::
