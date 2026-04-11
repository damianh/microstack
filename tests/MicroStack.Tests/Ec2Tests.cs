using Amazon;
using Amazon.EC2;
using Amazon.EC2.Model;
using Amazon.Runtime;

namespace MicroStack.Tests;

public sealed class Ec2Tests : IClassFixture<MicroStackFixture>, IAsyncLifetime
{
    private readonly MicroStackFixture _fixture;
    private readonly AmazonEC2Client _ec2;

    public Ec2Tests(MicroStackFixture fixture)
    {
        _fixture = fixture;
        _ec2 = CreateEc2Client(fixture);
    }

    private static AmazonEC2Client CreateEc2Client(MicroStackFixture fixture)
    {
        var innerHandler = fixture.Factory.Server.CreateHandler();
        var httpClient = new HttpClient(new CanonicalizeUriHandler(innerHandler))
        {
            BaseAddress = new Uri("http://localhost/"),
        };
        var config = new AmazonEC2Config
        {
            RegionEndpoint = RegionEndpoint.USEast1,
            ServiceURL = "http://localhost/",
            HttpClientFactory = new FixedHttpClientFactory(httpClient),
        };
        return new AmazonEC2Client(new BasicAWSCredentials("test", "test"), config);
    }

    public async Task InitializeAsync()
    {
        await _fixture.HttpClient.PostAsync("/_ministack/reset", null);
    }

    public Task DisposeAsync()
    {
        _ec2.Dispose();
        return Task.CompletedTask;
    }

    // ── VPC Tests ───────────────────────────────────────────────────────────────

    [Fact]
    public async Task DescribeVpcsDefault()
    {
        var resp = await _ec2.DescribeVpcsAsync(new DescribeVpcsRequest());
        Assert.Contains(resp.Vpcs, v => v.IsDefault == true);
    }

    [Fact]
    public async Task CreateAndDeleteVpc()
    {
        var createResp = await _ec2.CreateVpcAsync(new CreateVpcRequest { CidrBlock = "10.1.0.0/16" });
        var vpcId = createResp.Vpc.VpcId;
        Assert.StartsWith("vpc-", vpcId);

        var descResp = await _ec2.DescribeVpcsAsync(new DescribeVpcsRequest
        {
            VpcIds = [vpcId],
        });
        Assert.Single(descResp.Vpcs);
        Assert.Equal("10.1.0.0/16", descResp.Vpcs[0].CidrBlock);
        Assert.False(descResp.Vpcs[0].IsDefault);

        await _ec2.DeleteVpcAsync(new DeleteVpcRequest { VpcId = vpcId });

        var descAfter = await _ec2.DescribeVpcsAsync(new DescribeVpcsRequest());
        Assert.DoesNotContain(descAfter.Vpcs, v => v.VpcId == vpcId);
    }

    [Fact]
    public async Task DescribeVpcAttribute()
    {
        var vpc = await _ec2.CreateVpcAsync(new CreateVpcRequest { CidrBlock = "10.99.0.0/16" });
        var vpcId = vpc.Vpc.VpcId;

        var dnsSupport = await _ec2.DescribeVpcAttributeAsync(new DescribeVpcAttributeRequest
        {
            VpcId = vpcId,
            Attribute = VpcAttributeName.EnableDnsSupport,
        });
        Assert.NotNull(dnsSupport.EnableDnsSupport);

        var dnsHostnames = await _ec2.DescribeVpcAttributeAsync(new DescribeVpcAttributeRequest
        {
            VpcId = vpcId,
            Attribute = VpcAttributeName.EnableDnsHostnames,
        });
        Assert.NotNull(dnsHostnames.EnableDnsHostnames);

        await _ec2.DeleteVpcAsync(new DeleteVpcRequest { VpcId = vpcId });
    }

    [Fact]
    public async Task ModifyVpcAttribute()
    {
        var vpc = await _ec2.CreateVpcAsync(new CreateVpcRequest { CidrBlock = "10.10.0.0/16" });
        var vpcId = vpc.Vpc.VpcId;

        await _ec2.ModifyVpcAttributeAsync(new ModifyVpcAttributeRequest
        {
            VpcId = vpcId,
            EnableDnsSupport = true,
        });
        await _ec2.ModifyVpcAttributeAsync(new ModifyVpcAttributeRequest
        {
            VpcId = vpcId,
            EnableDnsHostnames = true,
        });

        await _ec2.DeleteVpcAsync(new DeleteVpcRequest { VpcId = vpcId });
    }

    [Fact]
    public async Task CreateVpcDefaultResources()
    {
        var vpc = await _ec2.CreateVpcAsync(new CreateVpcRequest { CidrBlock = "10.99.0.0/16" });
        var vpcId = vpc.Vpc.VpcId;

        try
        {
            // Default ACL
            var acls = await _ec2.DescribeNetworkAclsAsync(new DescribeNetworkAclsRequest
            {
                Filters =
                [
                    new Filter("vpc-id", [vpcId]),
                    new Filter("default", ["true"]),
                ],
            });
            Assert.Single(acls.NetworkAcls);
            Assert.True(acls.NetworkAcls[0].IsDefault);
            Assert.Equal(vpcId, acls.NetworkAcls[0].VpcId);

            // Default SG
            var sgs = await _ec2.DescribeSecurityGroupsAsync(new DescribeSecurityGroupsRequest
            {
                Filters =
                [
                    new Filter("vpc-id", [vpcId]),
                    new Filter("group-name", ["default"]),
                ],
            });
            Assert.Single(sgs.SecurityGroups);
            Assert.Equal(vpcId, sgs.SecurityGroups[0].VpcId);

            // Main route table
            var rtbs = await _ec2.DescribeRouteTablesAsync(new DescribeRouteTablesRequest
            {
                Filters =
                [
                    new Filter("vpc-id", [vpcId]),
                    new Filter("association.main", ["true"]),
                ],
            });
            Assert.Single(rtbs.RouteTables);
            Assert.Equal(vpcId, rtbs.RouteTables[0].VpcId);
        }
        finally
        {
            await _ec2.DeleteVpcAsync(new DeleteVpcRequest { VpcId = vpcId });
        }
    }

    // ── Subnet Tests ────────────────────────────────────────────────────────────

    [Fact]
    public async Task DescribeSubnetsDefault()
    {
        var resp = await _ec2.DescribeSubnetsAsync(new DescribeSubnetsRequest());
        Assert.True(resp.Subnets.Count >= 1);
    }

    [Fact]
    public async Task CreateAndDeleteSubnet()
    {
        var vpc = await _ec2.CreateVpcAsync(new CreateVpcRequest { CidrBlock = "10.2.0.0/16" });
        var vpcId = vpc.Vpc.VpcId;

        var subnet = await _ec2.CreateSubnetAsync(new CreateSubnetRequest
        {
            VpcId = vpcId,
            CidrBlock = "10.2.1.0/24",
        });
        var subnetId = subnet.Subnet.SubnetId;
        Assert.StartsWith("subnet-", subnetId);

        var desc = await _ec2.DescribeSubnetsAsync(new DescribeSubnetsRequest
        {
            SubnetIds = [subnetId],
        });
        Assert.Single(desc.Subnets);
        Assert.Equal("10.2.1.0/24", desc.Subnets[0].CidrBlock);

        await _ec2.DeleteSubnetAsync(new DeleteSubnetRequest { SubnetId = subnetId });
        await _ec2.DeleteVpcAsync(new DeleteVpcRequest { VpcId = vpcId });
    }

    [Fact]
    public async Task ModifySubnetAttribute()
    {
        var vpc = await _ec2.CreateVpcAsync(new CreateVpcRequest { CidrBlock = "10.11.0.0/16" });
        var vpcId = vpc.Vpc.VpcId;
        var subnet = await _ec2.CreateSubnetAsync(new CreateSubnetRequest
        {
            VpcId = vpcId,
            CidrBlock = "10.11.1.0/24",
        });
        var subnetId = subnet.Subnet.SubnetId;

        await _ec2.ModifySubnetAttributeAsync(new ModifySubnetAttributeRequest
        {
            SubnetId = subnetId,
            MapPublicIpOnLaunch = true,
        });

        var desc = await _ec2.DescribeSubnetsAsync(new DescribeSubnetsRequest
        {
            SubnetIds = [subnetId],
        });
        Assert.True(desc.Subnets[0].MapPublicIpOnLaunch);

        await _ec2.DeleteSubnetAsync(new DeleteSubnetRequest { SubnetId = subnetId });
        await _ec2.DeleteVpcAsync(new DeleteVpcRequest { VpcId = vpcId });
    }

    [Fact]
    public async Task DefaultSubnetsThreeAzs()
    {
        var resp = await _ec2.DescribeSubnetsAsync(new DescribeSubnetsRequest
        {
            Filters = [new Filter("vpc-id", ["vpc-00000001"])],
        });
        Assert.True(resp.Subnets.Count >= 3);

        var byAz = resp.Subnets.ToDictionary(s => s.AvailabilityZone);
        Assert.Contains("us-east-1a", byAz.Keys);
        Assert.Contains("us-east-1b", byAz.Keys);
        Assert.Contains("us-east-1c", byAz.Keys);

        Assert.Equal("172.31.0.0/20", byAz["us-east-1a"].CidrBlock);
        Assert.Equal("172.31.16.0/20", byAz["us-east-1b"].CidrBlock);
        Assert.Equal("172.31.32.0/20", byAz["us-east-1c"].CidrBlock);

        foreach (var s in resp.Subnets)
        {
            Assert.True(s.DefaultForAz);
            Assert.True(s.MapPublicIpOnLaunch);
        }
    }

    // ── Security Group Tests ────────────────────────────────────────────────────

    [Fact]
    public async Task SecurityGroupCrud()
    {
        var create = await _ec2.CreateSecurityGroupAsync(new CreateSecurityGroupRequest
        {
            GroupName = "qa-ec2-sg",
            Description = "test sg",
        });
        var sgId = create.GroupId;
        Assert.StartsWith("sg-", sgId);

        var desc = await _ec2.DescribeSecurityGroupsAsync(new DescribeSecurityGroupsRequest
        {
            GroupIds = [sgId],
        });
        Assert.Equal("qa-ec2-sg", desc.SecurityGroups[0].GroupName);

        await _ec2.DeleteSecurityGroupAsync(new DeleteSecurityGroupRequest { GroupId = sgId });

        var descAfter = await _ec2.DescribeSecurityGroupsAsync(new DescribeSecurityGroupsRequest());
        Assert.DoesNotContain(descAfter.SecurityGroups, sg => sg.GroupId == sgId);
    }

    [Fact]
    public async Task SecurityGroupDuplicate()
    {
        await _ec2.CreateSecurityGroupAsync(new CreateSecurityGroupRequest
        {
            GroupName = "qa-ec2-sg-dup",
            Description = "d",
        });

        var ex = await Assert.ThrowsAsync<AmazonEC2Exception>(() =>
            _ec2.CreateSecurityGroupAsync(new CreateSecurityGroupRequest
            {
                GroupName = "qa-ec2-sg-dup",
                Description = "d",
            }));
        Assert.Equal("InvalidGroup.Duplicate", ex.ErrorCode);
    }

    [Fact]
    public async Task SecurityGroupAuthorizeRevokeIngress()
    {
        var create = await _ec2.CreateSecurityGroupAsync(new CreateSecurityGroupRequest
        {
            GroupName = "qa-ec2-sg-rules",
            Description = "rules test",
        });
        var sgId = create.GroupId;

        await _ec2.AuthorizeSecurityGroupIngressAsync(new AuthorizeSecurityGroupIngressRequest
        {
            GroupId = sgId,
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

        var desc = await _ec2.DescribeSecurityGroupsAsync(new DescribeSecurityGroupsRequest
        {
            GroupIds = [sgId],
        });
        Assert.Contains(desc.SecurityGroups[0].IpPermissions, p => p.FromPort == 80);

        await _ec2.RevokeSecurityGroupIngressAsync(new RevokeSecurityGroupIngressRequest
        {
            GroupId = sgId,
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

        var descAfter = await _ec2.DescribeSecurityGroupsAsync(new DescribeSecurityGroupsRequest
        {
            GroupIds = [sgId],
        });
        Assert.DoesNotContain(descAfter.SecurityGroups[0].IpPermissions ?? [], p => p.FromPort == 80);

        await _ec2.DeleteSecurityGroupAsync(new DeleteSecurityGroupRequest { GroupId = sgId });
    }

    // ── Key Pair Tests ──────────────────────────────────────────────────────────

    [Fact]
    public async Task KeyPairCrud()
    {
        var create = await _ec2.CreateKeyPairAsync(new CreateKeyPairRequest
        {
            KeyName = "qa-ec2-key",
        });
        Assert.Equal("qa-ec2-key", create.KeyPair.KeyName);
        Assert.NotEmpty(create.KeyPair.KeyMaterial);

        var desc = await _ec2.DescribeKeyPairsAsync(new DescribeKeyPairsRequest
        {
            KeyNames = ["qa-ec2-key"],
        });
        Assert.Single(desc.KeyPairs);

        await _ec2.DeleteKeyPairAsync(new DeleteKeyPairRequest { KeyName = "qa-ec2-key" });

        var descAfter = await _ec2.DescribeKeyPairsAsync(new DescribeKeyPairsRequest());
        Assert.DoesNotContain(descAfter.KeyPairs ?? [], kp => kp.KeyName == "qa-ec2-key");
    }

    [Fact]
    public async Task KeyPairDuplicate()
    {
        await _ec2.CreateKeyPairAsync(new CreateKeyPairRequest { KeyName = "qa-ec2-key-dup" });

        var ex = await Assert.ThrowsAsync<AmazonEC2Exception>(() =>
            _ec2.CreateKeyPairAsync(new CreateKeyPairRequest { KeyName = "qa-ec2-key-dup" }));
        Assert.Equal("InvalidKeyPair.Duplicate", ex.ErrorCode);
    }

    [Fact]
    public async Task ImportKeyPair()
    {
        var publicKeyMaterial = Convert.ToBase64String("ssh-rsa AAAAB3... test@test"u8.ToArray());
        var resp = await _ec2.ImportKeyPairAsync(new ImportKeyPairRequest
        {
            KeyName = "qa-ec2-import-key",
            PublicKeyMaterial = publicKeyMaterial,
        });
        Assert.Equal("qa-ec2-import-key", resp.KeyName);
        Assert.NotEmpty(resp.KeyFingerprint);

        var desc = await _ec2.DescribeKeyPairsAsync(new DescribeKeyPairsRequest
        {
            KeyNames = ["qa-ec2-import-key"],
        });
        Assert.Single(desc.KeyPairs);

        await _ec2.DeleteKeyPairAsync(new DeleteKeyPairRequest { KeyName = "qa-ec2-import-key" });
    }

    // ── Internet Gateway Tests ──────────────────────────────────────────────────

    [Fact]
    public async Task InternetGatewayCrud()
    {
        var igw = await _ec2.CreateInternetGatewayAsync(new CreateInternetGatewayRequest());
        var igwId = igw.InternetGateway.InternetGatewayId;
        Assert.StartsWith("igw-", igwId);

        var vpc = await _ec2.CreateVpcAsync(new CreateVpcRequest { CidrBlock = "10.3.0.0/16" });
        var vpcId = vpc.Vpc.VpcId;

        await _ec2.AttachInternetGatewayAsync(new AttachInternetGatewayRequest
        {
            InternetGatewayId = igwId,
            VpcId = vpcId,
        });

        var desc = await _ec2.DescribeInternetGatewaysAsync(new DescribeInternetGatewaysRequest
        {
            InternetGatewayIds = [igwId],
        });
        Assert.Single(desc.InternetGateways[0].Attachments);

        await _ec2.DetachInternetGatewayAsync(new DetachInternetGatewayRequest
        {
            InternetGatewayId = igwId,
            VpcId = vpcId,
        });

        await _ec2.DeleteInternetGatewayAsync(new DeleteInternetGatewayRequest
        {
            InternetGatewayId = igwId,
        });
        await _ec2.DeleteVpcAsync(new DeleteVpcRequest { VpcId = vpcId });
    }

    // ── Route Table Tests ───────────────────────────────────────────────────────

    [Fact]
    public async Task RouteTableCrud()
    {
        var vpc = await _ec2.CreateVpcAsync(new CreateVpcRequest { CidrBlock = "10.20.0.0/16" });
        var vpcId = vpc.Vpc.VpcId;

        var rtb = await _ec2.CreateRouteTableAsync(new CreateRouteTableRequest { VpcId = vpcId });
        var rtbId = rtb.RouteTable.RouteTableId;
        Assert.StartsWith("rtb-", rtbId);

        var desc = await _ec2.DescribeRouteTablesAsync(new DescribeRouteTablesRequest
        {
            RouteTableIds = [rtbId],
        });
        Assert.Single(desc.RouteTables);
        Assert.Equal(rtbId, desc.RouteTables[0].RouteTableId);

        await _ec2.DeleteRouteTableAsync(new DeleteRouteTableRequest { RouteTableId = rtbId });
        await _ec2.DeleteVpcAsync(new DeleteVpcRequest { VpcId = vpcId });
    }

    [Fact]
    public async Task RouteTableAssociateDisassociate()
    {
        var vpc = await _ec2.CreateVpcAsync(new CreateVpcRequest { CidrBlock = "10.21.0.0/16" });
        var vpcId = vpc.Vpc.VpcId;
        var subnet = await _ec2.CreateSubnetAsync(new CreateSubnetRequest
        {
            VpcId = vpcId,
            CidrBlock = "10.21.1.0/24",
        });
        var subnetId = subnet.Subnet.SubnetId;
        var rtb = await _ec2.CreateRouteTableAsync(new CreateRouteTableRequest { VpcId = vpcId });
        var rtbId = rtb.RouteTable.RouteTableId;

        var assoc = await _ec2.AssociateRouteTableAsync(new AssociateRouteTableRequest
        {
            RouteTableId = rtbId,
            SubnetId = subnetId,
        });
        var assocId = assoc.AssociationId;
        Assert.StartsWith("rtbassoc-", assocId);

        var desc = await _ec2.DescribeRouteTablesAsync(new DescribeRouteTablesRequest
        {
            RouteTableIds = [rtbId],
        });
        Assert.Contains(desc.RouteTables[0].Associations, a => a.RouteTableAssociationId == assocId);

        await _ec2.DisassociateRouteTableAsync(new DisassociateRouteTableRequest
        {
            AssociationId = assocId,
        });

        var descAfter = await _ec2.DescribeRouteTablesAsync(new DescribeRouteTablesRequest
        {
            RouteTableIds = [rtbId],
        });
        Assert.DoesNotContain(descAfter.RouteTables[0].Associations ?? [],
            a => a.RouteTableAssociationId == assocId);

        await _ec2.DeleteRouteTableAsync(new DeleteRouteTableRequest { RouteTableId = rtbId });
        await _ec2.DeleteSubnetAsync(new DeleteSubnetRequest { SubnetId = subnetId });
        await _ec2.DeleteVpcAsync(new DeleteVpcRequest { VpcId = vpcId });
    }

    [Fact]
    public async Task RouteCreateReplaceDelete()
    {
        var vpc = await _ec2.CreateVpcAsync(new CreateVpcRequest { CidrBlock = "10.22.0.0/16" });
        var vpcId = vpc.Vpc.VpcId;
        var rtb = await _ec2.CreateRouteTableAsync(new CreateRouteTableRequest { VpcId = vpcId });
        var rtbId = rtb.RouteTable.RouteTableId;
        var igw = await _ec2.CreateInternetGatewayAsync(new CreateInternetGatewayRequest());
        var igwId = igw.InternetGateway.InternetGatewayId;

        await _ec2.CreateRouteAsync(new CreateRouteRequest
        {
            RouteTableId = rtbId,
            DestinationCidrBlock = "0.0.0.0/0",
            GatewayId = igwId,
        });

        var desc = await _ec2.DescribeRouteTablesAsync(new DescribeRouteTablesRequest
        {
            RouteTableIds = [rtbId],
        });
        Assert.Contains(desc.RouteTables[0].Routes, r => r.DestinationCidrBlock == "0.0.0.0/0");

        await _ec2.ReplaceRouteAsync(new ReplaceRouteRequest
        {
            RouteTableId = rtbId,
            DestinationCidrBlock = "0.0.0.0/0",
            GatewayId = "local",
        });

        await _ec2.DeleteRouteAsync(new DeleteRouteRequest
        {
            RouteTableId = rtbId,
            DestinationCidrBlock = "0.0.0.0/0",
        });

        var descAfter = await _ec2.DescribeRouteTablesAsync(new DescribeRouteTablesRequest
        {
            RouteTableIds = [rtbId],
        });
        Assert.DoesNotContain(descAfter.RouteTables[0].Routes, r => r.DestinationCidrBlock == "0.0.0.0/0");

        await _ec2.DeleteInternetGatewayAsync(new DeleteInternetGatewayRequest { InternetGatewayId = igwId });
        await _ec2.DeleteRouteTableAsync(new DeleteRouteTableRequest { RouteTableId = rtbId });
        await _ec2.DeleteVpcAsync(new DeleteVpcRequest { VpcId = vpcId });
    }

    [Fact]
    public async Task DescribeRouteTablesDefault()
    {
        var desc = await _ec2.DescribeRouteTablesAsync(new DescribeRouteTablesRequest());
        Assert.Contains(desc.RouteTables, rt => rt.VpcId == "vpc-00000001");
    }

    [Fact]
    public async Task RouteTableAssociationFilter()
    {
        var vpc = await _ec2.CreateVpcAsync(new CreateVpcRequest { CidrBlock = "10.98.0.0/16" });
        var vpcId = vpc.Vpc.VpcId;

        try
        {
            var subnet = await _ec2.CreateSubnetAsync(new CreateSubnetRequest
            {
                VpcId = vpcId,
                CidrBlock = "10.98.1.0/24",
            });
            var subnetId = subnet.Subnet.SubnetId;
            var rtb = await _ec2.CreateRouteTableAsync(new CreateRouteTableRequest { VpcId = vpcId });
            var rtbId = rtb.RouteTable.RouteTableId;
            var assoc = await _ec2.AssociateRouteTableAsync(new AssociateRouteTableRequest
            {
                RouteTableId = rtbId,
                SubnetId = subnetId,
            });
            var assocId = assoc.AssociationId;

            // Filter by association ID
            var result = await _ec2.DescribeRouteTablesAsync(new DescribeRouteTablesRequest
            {
                Filters =
                [
                    new Filter("association.route-table-association-id", [assocId]),
                ],
            });
            Assert.Single(result.RouteTables);
            Assert.Equal(rtbId, result.RouteTables[0].RouteTableId);

            // Filter by subnet ID
            var result2 = await _ec2.DescribeRouteTablesAsync(new DescribeRouteTablesRequest
            {
                Filters = [new Filter("association.subnet-id", [subnetId])],
            });
            Assert.Single(result2.RouteTables);

            await _ec2.DisassociateRouteTableAsync(new DisassociateRouteTableRequest { AssociationId = assocId });
            await _ec2.DeleteRouteTableAsync(new DeleteRouteTableRequest { RouteTableId = rtbId });
            await _ec2.DeleteSubnetAsync(new DeleteSubnetRequest { SubnetId = subnetId });
        }
        finally
        {
            await _ec2.DeleteVpcAsync(new DeleteVpcRequest { VpcId = vpcId });
        }
    }

    [Fact]
    public async Task ReplaceRouteTableAssociation()
    {
        var vpc = await _ec2.CreateVpcAsync(new CreateVpcRequest { CidrBlock = "10.97.0.0/16" });
        var vpcId = vpc.Vpc.VpcId;

        try
        {
            var subnet = await _ec2.CreateSubnetAsync(new CreateSubnetRequest
            {
                VpcId = vpcId,
                CidrBlock = "10.97.1.0/24",
            });
            var subnetId = subnet.Subnet.SubnetId;
            var rtb1 = await _ec2.CreateRouteTableAsync(new CreateRouteTableRequest { VpcId = vpcId });
            var rtb1Id = rtb1.RouteTable.RouteTableId;
            var rtb2 = await _ec2.CreateRouteTableAsync(new CreateRouteTableRequest { VpcId = vpcId });
            var rtb2Id = rtb2.RouteTable.RouteTableId;

            var assoc = await _ec2.AssociateRouteTableAsync(new AssociateRouteTableRequest
            {
                RouteTableId = rtb1Id,
                SubnetId = subnetId,
            });
            var oldAssocId = assoc.AssociationId;

            var newResp = await _ec2.ReplaceRouteTableAssociationAsync(new ReplaceRouteTableAssociationRequest
            {
                AssociationId = oldAssocId,
                RouteTableId = rtb2Id,
            });
            Assert.NotEqual(oldAssocId, newResp.NewAssociationId);

            var result = await _ec2.DescribeRouteTablesAsync(new DescribeRouteTablesRequest
            {
                Filters = [new Filter("association.subnet-id", [subnetId])],
            });
            Assert.Equal(rtb2Id, result.RouteTables[0].RouteTableId);

            await _ec2.DisassociateRouteTableAsync(new DisassociateRouteTableRequest
            {
                AssociationId = newResp.NewAssociationId,
            });
            await _ec2.DeleteRouteTableAsync(new DeleteRouteTableRequest { RouteTableId = rtb1Id });
            await _ec2.DeleteRouteTableAsync(new DeleteRouteTableRequest { RouteTableId = rtb2Id });
            await _ec2.DeleteSubnetAsync(new DeleteSubnetRequest { SubnetId = subnetId });
        }
        finally
        {
            await _ec2.DeleteVpcAsync(new DeleteVpcRequest { VpcId = vpcId });
        }
    }

    // ── Instance Tests ──────────────────────────────────────────────────────────

    [Fact]
    public async Task RunDescribeTerminateInstances()
    {
        var run = await _ec2.RunInstancesAsync(new RunInstancesRequest
        {
            ImageId = "ami-00000000",
            MinCount = 1,
            MaxCount = 1,
            InstanceType = InstanceType.T2Micro,
        });
        Assert.Single(run.Reservation.Instances);
        var instanceId = run.Reservation.Instances[0].InstanceId;
        Assert.StartsWith("i-", instanceId);
        Assert.Equal(InstanceStateName.Running, run.Reservation.Instances[0].State.Name);

        var desc = await _ec2.DescribeInstancesAsync(new DescribeInstancesRequest
        {
            InstanceIds = [instanceId],
        });
        Assert.Single(desc.Reservations);
        Assert.Equal(instanceId, desc.Reservations[0].Instances[0].InstanceId);

        var term = await _ec2.TerminateInstancesAsync(new TerminateInstancesRequest
        {
            InstanceIds = [instanceId],
        });
        Assert.Equal(InstanceStateName.Terminated,
            term.TerminatingInstances[0].CurrentState.Name);
    }

    [Fact]
    public async Task StopStartInstances()
    {
        var run = await _ec2.RunInstancesAsync(new RunInstancesRequest
        {
            ImageId = "ami-00000000",
            MinCount = 1,
            MaxCount = 1,
        });
        var iid = run.Reservation.Instances[0].InstanceId;

        var stop = await _ec2.StopInstancesAsync(new StopInstancesRequest
        {
            InstanceIds = [iid],
        });
        Assert.Equal(InstanceStateName.Stopped, stop.StoppingInstances[0].CurrentState.Name);

        var start = await _ec2.StartInstancesAsync(new StartInstancesRequest
        {
            InstanceIds = [iid],
        });
        Assert.Equal(InstanceStateName.Running, start.StartingInstances[0].CurrentState.Name);

        await _ec2.TerminateInstancesAsync(new TerminateInstancesRequest { InstanceIds = [iid] });
    }

    [Fact]
    public async Task RunMultipleInstances()
    {
        var run = await _ec2.RunInstancesAsync(new RunInstancesRequest
        {
            ImageId = "ami-00000000",
            MinCount = 3,
            MaxCount = 3,
        });
        Assert.Equal(3, run.Reservation.Instances.Count);
        var ids = run.Reservation.Instances.Select(i => i.InstanceId).ToList();
        Assert.Equal(3, ids.Distinct().Count());

        await _ec2.TerminateInstancesAsync(new TerminateInstancesRequest { InstanceIds = ids });
    }

    [Fact]
    public async Task RebootInstances()
    {
        var run = await _ec2.RunInstancesAsync(new RunInstancesRequest
        {
            ImageId = "ami-00000000",
            MinCount = 1,
            MaxCount = 1,
        });
        var iid = run.Reservation.Instances[0].InstanceId;

        // Reboot should not throw
        await _ec2.RebootInstancesAsync(new RebootInstancesRequest { InstanceIds = [iid] });

        await _ec2.TerminateInstancesAsync(new TerminateInstancesRequest { InstanceIds = [iid] });
    }

    [Fact]
    public async Task DescribeInstanceAttributeInstanceType()
    {
        var run = await _ec2.RunInstancesAsync(new RunInstancesRequest
        {
            ImageId = "ami-00000000",
            MinCount = 1,
            MaxCount = 1,
            InstanceType = InstanceType.T3Micro,
        });
        var iid = run.Reservation.Instances[0].InstanceId;

        var attr = await _ec2.DescribeInstanceAttributeAsync(new DescribeInstanceAttributeRequest
        {
            InstanceId = iid,
            Attribute = InstanceAttributeName.InstanceType,
        });
        Assert.Equal(iid, attr.InstanceAttribute.InstanceId);
        Assert.Equal("t3.micro", attr.InstanceAttribute.InstanceType);

        await _ec2.TerminateInstancesAsync(new TerminateInstancesRequest { InstanceIds = [iid] });
    }

    [Fact]
    public async Task DescribeInstanceAttributeShutdownBehavior()
    {
        var run = await _ec2.RunInstancesAsync(new RunInstancesRequest
        {
            ImageId = "ami-00000000",
            MinCount = 1,
            MaxCount = 1,
        });
        var iid = run.Reservation.Instances[0].InstanceId;

        var attr = await _ec2.DescribeInstanceAttributeAsync(new DescribeInstanceAttributeRequest
        {
            InstanceId = iid,
            Attribute = InstanceAttributeName.InstanceInitiatedShutdownBehavior,
        });
        Assert.Equal(iid, attr.InstanceAttribute.InstanceId);
        Assert.Equal("stop", attr.InstanceAttribute.InstanceInitiatedShutdownBehavior);

        await _ec2.TerminateInstancesAsync(new TerminateInstancesRequest { InstanceIds = [iid] });
    }

    [Fact]
    public async Task DescribeInstanceAttributeNotFound()
    {
        var ex = await Assert.ThrowsAsync<AmazonEC2Exception>(() =>
            _ec2.DescribeInstanceAttributeAsync(new DescribeInstanceAttributeRequest
            {
                InstanceId = "i-000000000000nonex",
                Attribute = InstanceAttributeName.InstanceType,
            }));
        Assert.Equal("InvalidInstanceID.NotFound", ex.ErrorCode);
    }

    // ── Images ──────────────────────────────────────────────────────────────────

    [Fact]
    public async Task DescribeImages()
    {
        var resp = await _ec2.DescribeImagesAsync(new DescribeImagesRequest
        {
            Owners = ["self"],
        });
        Assert.True(resp.Images.Count >= 1);
        Assert.All(resp.Images, img => Assert.NotEmpty(img.ImageId));
    }

    // ── Availability Zones ──────────────────────────────────────────────────────

    [Fact]
    public async Task DescribeAvailabilityZones()
    {
        var resp = await _ec2.DescribeAvailabilityZonesAsync(new DescribeAvailabilityZonesRequest());
        var azNames = resp.AvailabilityZones.Select(az => az.ZoneName).ToList();
        Assert.Contains(azNames, az => az.Contains("us-east-1"));
    }

    // ── Elastic IP Tests ────────────────────────────────────────────────────────

    [Fact]
    public async Task ElasticIpCrud()
    {
        var alloc = await _ec2.AllocateAddressAsync(new AllocateAddressRequest { Domain = DomainType.Vpc });
        var allocId = alloc.AllocationId;
        Assert.StartsWith("eipalloc-", allocId);
        Assert.NotEmpty(alloc.PublicIp);

        var run = await _ec2.RunInstancesAsync(new RunInstancesRequest
        {
            ImageId = "ami-00000000",
            MinCount = 1,
            MaxCount = 1,
        });
        var iid = run.Reservation.Instances[0].InstanceId;

        var assocResp = await _ec2.AssociateAddressAsync(new AssociateAddressRequest
        {
            AllocationId = allocId,
            InstanceId = iid,
        });
        Assert.NotEmpty(assocResp.AssociationId);

        var desc = await _ec2.DescribeAddressesAsync(new DescribeAddressesRequest
        {
            AllocationIds = [allocId],
        });
        Assert.Equal(iid, desc.Addresses[0].InstanceId);

        await _ec2.DisassociateAddressAsync(new DisassociateAddressRequest
        {
            AssociationId = assocResp.AssociationId,
        });

        await _ec2.ReleaseAddressAsync(new ReleaseAddressRequest { AllocationId = allocId });
        await _ec2.TerminateInstancesAsync(new TerminateInstancesRequest { InstanceIds = [iid] });
    }

    // ── Tags Tests ──────────────────────────────────────────────────────────────

    [Fact]
    public async Task TagsCrud()
    {
        var run = await _ec2.RunInstancesAsync(new RunInstancesRequest
        {
            ImageId = "ami-00000000",
            MinCount = 1,
            MaxCount = 1,
        });
        var iid = run.Reservation.Instances[0].InstanceId;

        await _ec2.CreateTagsAsync(new CreateTagsRequest
        {
            Resources = [iid],
            Tags = [new Tag("Name", "qa-box")],
        });

        var desc = await _ec2.DescribeInstancesAsync(new DescribeInstancesRequest
        {
            InstanceIds = [iid],
        });
        var tags = desc.Reservations[0].Instances[0].Tags;
        Assert.Contains(tags, t => t.Key == "Name" && t.Value == "qa-box");

        await _ec2.DeleteTagsAsync(new DeleteTagsRequest
        {
            Resources = [iid],
            Tags = [new Tag { Key = "Name" }],
        });

        var desc2 = await _ec2.DescribeInstancesAsync(new DescribeInstancesRequest
        {
            InstanceIds = [iid],
        });
        var tags2 = desc2.Reservations[0].Instances[0].Tags;
        Assert.DoesNotContain(tags2 ?? [], t => t.Key == "Name");

        await _ec2.TerminateInstancesAsync(new TerminateInstancesRequest { InstanceIds = [iid] });
    }

    [Fact]
    public async Task DescribeTagsFilters()
    {
        var r1 = await _ec2.RunInstancesAsync(new RunInstancesRequest
        {
            ImageId = "ami-test1",
            InstanceType = InstanceType.T2Micro,
            MinCount = 1,
            MaxCount = 1,
        });
        var r2 = await _ec2.RunInstancesAsync(new RunInstancesRequest
        {
            ImageId = "ami-test2",
            InstanceType = InstanceType.T2Micro,
            MinCount = 1,
            MaxCount = 1,
        });
        var id1 = r1.Reservation.Instances[0].InstanceId;
        var id2 = r2.Reservation.Instances[0].InstanceId;

        await _ec2.CreateTagsAsync(new CreateTagsRequest
        {
            Resources = [id1],
            Tags = [new Tag("Name", "first"), new Tag("Env", "prod")],
        });
        await _ec2.CreateTagsAsync(new CreateTagsRequest
        {
            Resources = [id2],
            Tags = [new Tag("Name", "second")],
        });

        // Filter by resource-id
        var resp = await _ec2.DescribeTagsAsync(new DescribeTagsRequest
        {
            Filters = [new Filter("resource-id", [id1])],
        });
        Assert.All(resp.Tags, t => Assert.Equal(id1, t.ResourceId));
        Assert.Equal(2, resp.Tags.Count);

        // Filter by key
        var resp2 = await _ec2.DescribeTagsAsync(new DescribeTagsRequest
        {
            Filters = [new Filter("key", ["Env"])],
        });
        Assert.All(resp2.Tags, t => Assert.Equal("Env", t.Key));
        Assert.Contains(resp2.Tags, t => t.ResourceId == id1);

        // Filter by resource-id + key
        var resp3 = await _ec2.DescribeTagsAsync(new DescribeTagsRequest
        {
            Filters =
            [
                new Filter("resource-id", [id1]),
                new Filter("key", ["Name"]),
            ],
        });
        Assert.Single(resp3.Tags);
        Assert.Equal(id1, resp3.Tags[0].ResourceId);
        Assert.Equal("Name", resp3.Tags[0].Key);
        Assert.Equal("first", resp3.Tags[0].Value);

        // Filter for nonexistent resource
        var resp4 = await _ec2.DescribeTagsAsync(new DescribeTagsRequest
        {
            Filters = [new Filter("resource-id", ["i-doesnotexist"])],
        });
        Assert.Empty(resp4.Tags ?? []);

        // All tags have correct resource type
        var resp5 = await _ec2.DescribeTagsAsync(new DescribeTagsRequest
        {
            Filters = [new Filter("resource-id", [id1, id2])],
        });
        Assert.All(resp5.Tags, t => Assert.Equal("instance", t.ResourceType));
    }

    // ── Network Interface Tests ─────────────────────────────────────────────────

    [Fact]
    public async Task NetworkInterfaceCrud()
    {
        var vpc = await _ec2.CreateVpcAsync(new CreateVpcRequest { CidrBlock = "10.30.0.0/16" });
        var vpcId = vpc.Vpc.VpcId;
        var subnet = await _ec2.CreateSubnetAsync(new CreateSubnetRequest
        {
            VpcId = vpcId,
            CidrBlock = "10.30.1.0/24",
        });
        var subnetId = subnet.Subnet.SubnetId;

        var eni = await _ec2.CreateNetworkInterfaceAsync(new CreateNetworkInterfaceRequest
        {
            SubnetId = subnetId,
            Description = "qa-eni",
        });
        var eniId = eni.NetworkInterface.NetworkInterfaceId;
        Assert.StartsWith("eni-", eniId);

        var desc = await _ec2.DescribeNetworkInterfacesAsync(new DescribeNetworkInterfacesRequest
        {
            NetworkInterfaceIds = [eniId],
        });
        Assert.Equal("qa-eni", desc.NetworkInterfaces[0].Description);
        Assert.Equal(NetworkInterfaceStatus.Available, desc.NetworkInterfaces[0].Status);

        await _ec2.DeleteNetworkInterfaceAsync(new DeleteNetworkInterfaceRequest
        {
            NetworkInterfaceId = eniId,
        });

        var descAfter = await _ec2.DescribeNetworkInterfacesAsync(new DescribeNetworkInterfacesRequest());
        Assert.DoesNotContain(descAfter.NetworkInterfaces ?? [], e => e.NetworkInterfaceId == eniId);

        await _ec2.DeleteSubnetAsync(new DeleteSubnetRequest { SubnetId = subnetId });
        await _ec2.DeleteVpcAsync(new DeleteVpcRequest { VpcId = vpcId });
    }

    [Fact]
    public async Task NetworkInterfaceAttachDetach()
    {
        var vpc = await _ec2.CreateVpcAsync(new CreateVpcRequest { CidrBlock = "10.31.0.0/16" });
        var vpcId = vpc.Vpc.VpcId;
        var subnet = await _ec2.CreateSubnetAsync(new CreateSubnetRequest
        {
            VpcId = vpcId,
            CidrBlock = "10.31.1.0/24",
        });
        var subnetId = subnet.Subnet.SubnetId;
        var eni = await _ec2.CreateNetworkInterfaceAsync(new CreateNetworkInterfaceRequest
        {
            SubnetId = subnetId,
        });
        var eniId = eni.NetworkInterface.NetworkInterfaceId;

        var run = await _ec2.RunInstancesAsync(new RunInstancesRequest
        {
            ImageId = "ami-00000000",
            MinCount = 1,
            MaxCount = 1,
        });
        var iid = run.Reservation.Instances[0].InstanceId;

        var attach = await _ec2.AttachNetworkInterfaceAsync(new AttachNetworkInterfaceRequest
        {
            NetworkInterfaceId = eniId,
            InstanceId = iid,
            DeviceIndex = 1,
        });
        Assert.StartsWith("eni-attach-", attach.AttachmentId);

        var desc = await _ec2.DescribeNetworkInterfacesAsync(new DescribeNetworkInterfacesRequest
        {
            NetworkInterfaceIds = [eniId],
        });
        Assert.Equal(NetworkInterfaceStatus.InUse, desc.NetworkInterfaces[0].Status);

        await _ec2.DetachNetworkInterfaceAsync(new DetachNetworkInterfaceRequest
        {
            AttachmentId = attach.AttachmentId,
        });

        var descAfter = await _ec2.DescribeNetworkInterfacesAsync(new DescribeNetworkInterfacesRequest
        {
            NetworkInterfaceIds = [eniId],
        });
        Assert.Equal(NetworkInterfaceStatus.Available, descAfter.NetworkInterfaces[0].Status);

        await _ec2.TerminateInstancesAsync(new TerminateInstancesRequest { InstanceIds = [iid] });
        await _ec2.DeleteNetworkInterfaceAsync(new DeleteNetworkInterfaceRequest { NetworkInterfaceId = eniId });
        await _ec2.DeleteSubnetAsync(new DeleteSubnetRequest { SubnetId = subnetId });
        await _ec2.DeleteVpcAsync(new DeleteVpcRequest { VpcId = vpcId });
    }

    // ── VPC Endpoint Tests ──────────────────────────────────────────────────────

    [Fact]
    public async Task VpcEndpointCrud()
    {
        var vpc = await _ec2.CreateVpcAsync(new CreateVpcRequest { CidrBlock = "10.40.0.0/16" });
        var vpcId = vpc.Vpc.VpcId;

        var vpce = await _ec2.CreateVpcEndpointAsync(new CreateVpcEndpointRequest
        {
            VpcId = vpcId,
            ServiceName = "com.amazonaws.us-east-1.s3",
            VpcEndpointType = VpcEndpointType.Gateway,
        });
        var vpceId = vpce.VpcEndpoint.VpcEndpointId;
        Assert.StartsWith("vpce-", vpceId);

        var desc = await _ec2.DescribeVpcEndpointsAsync(new DescribeVpcEndpointsRequest
        {
            VpcEndpointIds = [vpceId],
        });
        Assert.Equal("com.amazonaws.us-east-1.s3", desc.VpcEndpoints[0].ServiceName);
        Assert.Equal("Available", desc.VpcEndpoints[0].State);

        await _ec2.DeleteVpcEndpointsAsync(new DeleteVpcEndpointsRequest
        {
            VpcEndpointIds = [vpceId],
        });

        var descAfter = await _ec2.DescribeVpcEndpointsAsync(new DescribeVpcEndpointsRequest());
        Assert.DoesNotContain(descAfter.VpcEndpoints ?? [], e => e.VpcEndpointId == vpceId);

        await _ec2.DeleteVpcAsync(new DeleteVpcRequest { VpcId = vpcId });
    }

    [Fact]
    public async Task ModifyVpcEndpoint()
    {
        var vpc = await _ec2.CreateVpcAsync(new CreateVpcRequest { CidrBlock = "10.96.0.0/16" });
        var vpcId = vpc.Vpc.VpcId;

        try
        {
            var rtb = await _ec2.CreateRouteTableAsync(new CreateRouteTableRequest { VpcId = vpcId });
            var rtbId = rtb.RouteTable.RouteTableId;
            var ep = await _ec2.CreateVpcEndpointAsync(new CreateVpcEndpointRequest
            {
                VpcId = vpcId,
                ServiceName = "com.amazonaws.us-east-1.s3",
                VpcEndpointType = VpcEndpointType.Gateway,
            });
            var vpceId = ep.VpcEndpoint.VpcEndpointId;

            await _ec2.ModifyVpcEndpointAsync(new ModifyVpcEndpointRequest
            {
                VpcEndpointId = vpceId,
                AddRouteTableIds = [rtbId],
            });
            var desc = await _ec2.DescribeVpcEndpointsAsync(new DescribeVpcEndpointsRequest
            {
                VpcEndpointIds = [vpceId],
            });
            Assert.Contains(rtbId, desc.VpcEndpoints[0].RouteTableIds);

            await _ec2.ModifyVpcEndpointAsync(new ModifyVpcEndpointRequest
            {
                VpcEndpointId = vpceId,
                RemoveRouteTableIds = [rtbId],
            });
            desc = await _ec2.DescribeVpcEndpointsAsync(new DescribeVpcEndpointsRequest
            {
                VpcEndpointIds = [vpceId],
            });
            Assert.DoesNotContain(rtbId, desc.VpcEndpoints[0].RouteTableIds ?? []);

            await _ec2.DeleteVpcEndpointsAsync(new DeleteVpcEndpointsRequest { VpcEndpointIds = [vpceId] });
            await _ec2.DeleteRouteTableAsync(new DeleteRouteTableRequest { RouteTableId = rtbId });
        }
        finally
        {
            await _ec2.DeleteVpcAsync(new DeleteVpcRequest { VpcId = vpcId });
        }
    }

    // ── Describe Account Attributes ─────────────────────────────────────────────

    [Fact]
    public async Task DescribeAccountAttributes()
    {
        var resp = await _ec2.DescribeAccountAttributesAsync(new DescribeAccountAttributesRequest());
        Assert.NotNull(resp);
    }

    // ── EBS Volume Tests ────────────────────────────────────────────────────────

    [Fact]
    public async Task CreateAndDescribeVolume()
    {
        var vol = await _ec2.CreateVolumeAsync(new CreateVolumeRequest
        {
            AvailabilityZone = "us-east-1a",
            Size = 20,
            VolumeType = VolumeType.Gp3,
        });
        var volId = vol.Volume.VolumeId;
        Assert.StartsWith("vol-", volId);
        Assert.Equal(VolumeState.Available, vol.Volume.State);
        Assert.Equal(20, vol.Volume.Size);
        Assert.Equal(VolumeType.Gp3, vol.Volume.VolumeType);

        var desc = await _ec2.DescribeVolumesAsync(new DescribeVolumesRequest
        {
            VolumeIds = [volId],
        });
        Assert.Single(desc.Volumes);
        Assert.Equal(volId, desc.Volumes[0].VolumeId);
    }

    [Fact]
    public async Task AttachDetachVolume()
    {
        var inst = await _ec2.RunInstancesAsync(new RunInstancesRequest
        {
            ImageId = "ami-00000001",
            MinCount = 1,
            MaxCount = 1,
        });
        var instanceId = inst.Reservation.Instances[0].InstanceId;

        var vol = await _ec2.CreateVolumeAsync(new CreateVolumeRequest
        {
            AvailabilityZone = "us-east-1a",
            Size = 10,
            VolumeType = VolumeType.Gp2,
        });
        var volId = vol.Volume.VolumeId;

        await _ec2.AttachVolumeAsync(new AttachVolumeRequest
        {
            VolumeId = volId,
            InstanceId = instanceId,
            Device = "/dev/xvdf",
        });

        var desc = await _ec2.DescribeVolumesAsync(new DescribeVolumesRequest
        {
            VolumeIds = [volId],
        });
        Assert.Equal(VolumeState.InUse, desc.Volumes[0].State);
        Assert.Equal(instanceId, desc.Volumes[0].Attachments[0].InstanceId);

        await _ec2.DetachVolumeAsync(new DetachVolumeRequest { VolumeId = volId });

        var desc2 = await _ec2.DescribeVolumesAsync(new DescribeVolumesRequest
        {
            VolumeIds = [volId],
        });
        Assert.Equal(VolumeState.Available, desc2.Volumes[0].State);
        Assert.Empty(desc2.Volumes[0].Attachments ?? []);
    }

    [Fact]
    public async Task DeleteVolume()
    {
        var vol = await _ec2.CreateVolumeAsync(new CreateVolumeRequest
        {
            AvailabilityZone = "us-east-1a",
            Size = 5,
            VolumeType = VolumeType.Gp2,
        });
        var volId = vol.Volume.VolumeId;

        await _ec2.DeleteVolumeAsync(new DeleteVolumeRequest { VolumeId = volId });

        var desc = await _ec2.DescribeVolumesAsync(new DescribeVolumesRequest
        {
            VolumeIds = [volId],
        });
        Assert.Empty(desc.Volumes ?? []);
    }

    [Fact]
    public async Task ModifyVolume()
    {
        var vol = await _ec2.CreateVolumeAsync(new CreateVolumeRequest
        {
            AvailabilityZone = "us-east-1a",
            Size = 10,
            VolumeType = VolumeType.Gp2,
        });
        var volId = vol.Volume.VolumeId;

        var resp = await _ec2.ModifyVolumeAsync(new ModifyVolumeRequest
        {
            VolumeId = volId,
            Size = 50,
            VolumeType = VolumeType.Gp3,
        });
        Assert.Equal(50, resp.VolumeModification.TargetSize);
        Assert.Equal(VolumeType.Gp3, resp.VolumeModification.TargetVolumeType);
    }

    [Fact]
    public async Task DescribeVolumeStatus()
    {
        var vol = await _ec2.CreateVolumeAsync(new CreateVolumeRequest
        {
            AvailabilityZone = "us-east-1a",
            Size = 8,
            VolumeType = VolumeType.Gp2,
        });
        var volId = vol.Volume.VolumeId;

        var resp = await _ec2.DescribeVolumeStatusAsync(new DescribeVolumeStatusRequest
        {
            VolumeIds = [volId],
        });
        Assert.Single(resp.VolumeStatuses);
        Assert.Equal("ok", resp.VolumeStatuses[0].VolumeStatus.Status);
    }

    [Fact]
    public async Task DescribeVolumeAttribute()
    {
        var vol = await _ec2.CreateVolumeAsync(new CreateVolumeRequest
        {
            AvailabilityZone = "us-east-1a",
            Size = 10,
            VolumeType = VolumeType.Gp2,
        });
        var volId = vol.Volume.VolumeId;

        var resp = await _ec2.DescribeVolumeAttributeAsync(new DescribeVolumeAttributeRequest
        {
            VolumeId = volId,
            Attribute = VolumeAttributeName.AutoEnableIO,
        });
        Assert.Equal(volId, resp.VolumeId);
        Assert.NotNull(resp.AutoEnableIO);
    }

    [Fact]
    public async Task DescribeVolumesModifications()
    {
        var vol = await _ec2.CreateVolumeAsync(new CreateVolumeRequest
        {
            AvailabilityZone = "us-east-1a",
            Size = 10,
            VolumeType = VolumeType.Gp2,
        });
        var volId = vol.Volume.VolumeId;

        await _ec2.ModifyVolumeAsync(new ModifyVolumeRequest
        {
            VolumeId = volId,
            Size = 50,
            VolumeType = VolumeType.Gp3,
        });

        var resp = await _ec2.DescribeVolumesModificationsAsync(new DescribeVolumesModificationsRequest
        {
            VolumeIds = [volId],
        });
        Assert.True(resp.VolumesModifications.Count >= 1);
        Assert.Equal(volId, resp.VolumesModifications[0].VolumeId);
        Assert.Equal(50, resp.VolumesModifications[0].TargetSize);
        Assert.Equal(VolumeType.Gp3, resp.VolumesModifications[0].TargetVolumeType);
    }

    // ── EBS Snapshot Tests ──────────────────────────────────────────────────────

    [Fact]
    public async Task CreateAndDescribeSnapshot()
    {
        var vol = await _ec2.CreateVolumeAsync(new CreateVolumeRequest
        {
            AvailabilityZone = "us-east-1a",
            Size = 10,
            VolumeType = VolumeType.Gp2,
        });
        var volId = vol.Volume.VolumeId;

        var snap = await _ec2.CreateSnapshotAsync(new CreateSnapshotRequest
        {
            VolumeId = volId,
            Description = "test snapshot",
        });
        Assert.StartsWith("snap-", snap.Snapshot.SnapshotId);
        Assert.Equal(SnapshotState.Completed, snap.Snapshot.State);

        var desc = await _ec2.DescribeSnapshotsAsync(new DescribeSnapshotsRequest
        {
            SnapshotIds = [snap.Snapshot.SnapshotId],
        });
        Assert.Single(desc.Snapshots);
        Assert.Equal(volId, desc.Snapshots[0].VolumeId);
        Assert.Equal("test snapshot", desc.Snapshots[0].Description);
    }

    [Fact]
    public async Task DeleteSnapshot()
    {
        var vol = await _ec2.CreateVolumeAsync(new CreateVolumeRequest
        {
            AvailabilityZone = "us-east-1a",
            Size = 10,
            VolumeType = VolumeType.Gp2,
        });
        var snap = await _ec2.CreateSnapshotAsync(new CreateSnapshotRequest
        {
            VolumeId = vol.Volume.VolumeId,
        });

        await _ec2.DeleteSnapshotAsync(new DeleteSnapshotRequest
        {
            SnapshotId = snap.Snapshot.SnapshotId,
        });

        var desc = await _ec2.DescribeSnapshotsAsync(new DescribeSnapshotsRequest
        {
            SnapshotIds = [snap.Snapshot.SnapshotId],
        });
        Assert.Empty(desc.Snapshots ?? []);
    }

    [Fact]
    public async Task CopySnapshot()
    {
        var vol = await _ec2.CreateVolumeAsync(new CreateVolumeRequest
        {
            AvailabilityZone = "us-east-1a",
            Size = 10,
            VolumeType = VolumeType.Gp2,
        });
        var snap = await _ec2.CreateSnapshotAsync(new CreateSnapshotRequest
        {
            VolumeId = vol.Volume.VolumeId,
            Description = "original",
        });

        var copy = await _ec2.CopySnapshotAsync(new CopySnapshotRequest
        {
            SourceRegion = "us-east-1",
            SourceSnapshotId = snap.Snapshot.SnapshotId,
            Description = "copy",
        });
        Assert.NotEqual(snap.Snapshot.SnapshotId, copy.SnapshotId);
        Assert.StartsWith("snap-", copy.SnapshotId);
    }

    [Fact]
    public async Task SnapshotAttribute()
    {
        var vol = await _ec2.CreateVolumeAsync(new CreateVolumeRequest
        {
            AvailabilityZone = "us-east-1a",
            Size = 10,
            VolumeType = VolumeType.Gp2,
        });
        var snap = await _ec2.CreateSnapshotAsync(new CreateSnapshotRequest
        {
            VolumeId = vol.Volume.VolumeId,
            Description = "attr test",
        });

        await _ec2.ModifySnapshotAttributeAsync(new ModifySnapshotAttributeRequest
        {
            SnapshotId = snap.Snapshot.SnapshotId,
            Attribute = SnapshotAttributeName.CreateVolumePermission,
            OperationType = OperationType.Add,
            UserIds = ["123456789012"],
        });

        var resp = await _ec2.DescribeSnapshotAttributeAsync(new DescribeSnapshotAttributeRequest
        {
            SnapshotId = snap.Snapshot.SnapshotId,
            Attribute = SnapshotAttributeName.CreateVolumePermission,
        });
        Assert.Equal(snap.Snapshot.SnapshotId, resp.SnapshotId);
        Assert.Contains(resp.CreateVolumePermissions, p => p.UserId == "123456789012");
    }

    // ── Describe Instance Types ─────────────────────────────────────────────────

    [Fact]
    public async Task DescribeInstanceTypesDefaults()
    {
        var resp = await _ec2.DescribeInstanceTypesAsync(new DescribeInstanceTypesRequest());
        var types = resp.InstanceTypes.Select(t => t.InstanceType.Value).ToList();
        Assert.Contains("t2.micro", types);
        Assert.Contains("t3.micro", types);
        Assert.True(resp.InstanceTypes.Count >= 4);

        var sample = resp.InstanceTypes[0];
        Assert.NotNull(sample.VCpuInfo);
        Assert.NotNull(sample.MemoryInfo);
        Assert.True(sample.VCpuInfo.DefaultVCpus >= 1);
        Assert.True(sample.MemoryInfo.SizeInMiB >= 512);
    }

    [Fact]
    public async Task DescribeInstanceTypesFilter()
    {
        var resp = await _ec2.DescribeInstanceTypesAsync(new DescribeInstanceTypesRequest
        {
            InstanceTypes = [InstanceType.T2Micro, InstanceType.M5Large],
        });
        var types = resp.InstanceTypes.Select(t => t.InstanceType.Value).ToHashSet();
        Assert.Contains("t2.micro", types);
        Assert.Contains("m5.large", types);
        Assert.Equal(2, types.Count);
    }

    // ── Describe Instance Credit Specifications ─────────────────────────────────

    [Fact]
    public async Task DescribeInstanceCreditSpecifications()
    {
        var run = await _ec2.RunInstancesAsync(new RunInstancesRequest
        {
            ImageId = "ami-test",
            MinCount = 1,
            MaxCount = 1,
        });
        var iid = run.Reservation.Instances[0].InstanceId;

        var resp = await _ec2.DescribeInstanceCreditSpecificationsAsync(
            new DescribeInstanceCreditSpecificationsRequest
            {
                InstanceIds = [iid],
            });
        Assert.Single(resp.InstanceCreditSpecifications);
        Assert.Equal(iid, resp.InstanceCreditSpecifications[0].InstanceId);
        Assert.Equal("standard", resp.InstanceCreditSpecifications[0].CpuCredits);
    }

    // ── Describe Spot Instance Requests / Capacity Reservations (stubs) ─────────

    [Fact]
    public async Task DescribeSpotInstanceRequests()
    {
        var resp = await _ec2.DescribeSpotInstanceRequestsAsync(new DescribeSpotInstanceRequestsRequest());
        Assert.Empty(resp.SpotInstanceRequests ?? []);
    }

    [Fact]
    public async Task DescribeCapacityReservations()
    {
        var resp = await _ec2.DescribeCapacityReservationsAsync(new DescribeCapacityReservationsRequest());
        Assert.Empty(resp.CapacityReservations ?? []);
    }

    // ── Describe Prefix Lists ───────────────────────────────────────────────────

    [Fact]
    public async Task DescribePrefixLists()
    {
        var resp = await _ec2.DescribePrefixListsAsync(new DescribePrefixListsRequest());
        var names = resp.PrefixLists.Select(p => p.PrefixListName).ToList();
        Assert.Contains(names, n => n.Contains("s3"));
        Assert.Contains(names, n => n.Contains("dynamodb"));
    }

    // ── Full Terraform VPC Flow ─────────────────────────────────────────────────

    [Fact]
    public async Task FullTerraformVpcFlow()
    {
        // 1. Create VPC
        var vpc = await _ec2.CreateVpcAsync(new CreateVpcRequest { CidrBlock = "10.50.0.0/16" });
        var vpcId = vpc.Vpc.VpcId;

        try
        {
            // 2. Verify default resources
            var acls = await _ec2.DescribeNetworkAclsAsync(new DescribeNetworkAclsRequest
            {
                Filters =
                [
                    new Filter("vpc-id", [vpcId]),
                    new Filter("default", ["true"]),
                ],
            });
            Assert.Single(acls.NetworkAcls);

            var sgs = await _ec2.DescribeSecurityGroupsAsync(new DescribeSecurityGroupsRequest
            {
                Filters =
                [
                    new Filter("vpc-id", [vpcId]),
                    new Filter("group-name", ["default"]),
                ],
            });
            Assert.Single(sgs.SecurityGroups);

            var mainRtbs = await _ec2.DescribeRouteTablesAsync(new DescribeRouteTablesRequest
            {
                Filters =
                [
                    new Filter("vpc-id", [vpcId]),
                    new Filter("association.main", ["true"]),
                ],
            });
            Assert.Single(mainRtbs.RouteTables);

            // 3. Create 6 subnets
            var subnetCidrs = new[]
            {
                ("10.50.0.0/20", "us-east-1a"), ("10.50.16.0/20", "us-east-1b"),
                ("10.50.32.0/20", "us-east-1c"), ("10.50.64.0/20", "us-east-1a"),
                ("10.50.80.0/20", "us-east-1b"), ("10.50.96.0/20", "us-east-1c"),
            };
            var subnets = new List<string>();
            foreach (var (cidr, az) in subnetCidrs)
            {
                var s = await _ec2.CreateSubnetAsync(new CreateSubnetRequest
                {
                    VpcId = vpcId,
                    CidrBlock = cidr,
                    AvailabilityZone = az,
                });
                subnets.Add(s.Subnet.SubnetId);
            }

            // 4. IGW
            var igw = await _ec2.CreateInternetGatewayAsync(new CreateInternetGatewayRequest());
            var igwId = igw.InternetGateway.InternetGatewayId;
            await _ec2.AttachInternetGatewayAsync(new AttachInternetGatewayRequest
            {
                InternetGatewayId = igwId,
                VpcId = vpcId,
            });

            // 5. EIP + NAT
            var eip = await _ec2.AllocateAddressAsync(new AllocateAddressRequest { Domain = DomainType.Vpc });
            var nat = await _ec2.CreateNatGatewayAsync(new CreateNatGatewayRequest
            {
                SubnetId = subnets[3],
                AllocationId = eip.AllocationId,
            });
            var natId = nat.NatGateway.NatGatewayId;

            // 6. Public + private route tables
            var pubRtb = (await _ec2.CreateRouteTableAsync(new CreateRouteTableRequest { VpcId = vpcId })).RouteTable.RouteTableId;
            var privRtb = (await _ec2.CreateRouteTableAsync(new CreateRouteTableRequest { VpcId = vpcId })).RouteTable.RouteTableId;

            // 7. Associate subnets
            var assocIds = new List<string>();
            for (var i = 0; i < 3; i++)
            {
                var a = await _ec2.AssociateRouteTableAsync(new AssociateRouteTableRequest
                {
                    RouteTableId = pubRtb,
                    SubnetId = subnets[i + 3],
                });
                assocIds.Add(a.AssociationId);
            }
            for (var i = 0; i < 3; i++)
            {
                var a = await _ec2.AssociateRouteTableAsync(new AssociateRouteTableRequest
                {
                    RouteTableId = privRtb,
                    SubnetId = subnets[i],
                });
                assocIds.Add(a.AssociationId);
            }

            // 8. Routes
            await _ec2.CreateRouteAsync(new CreateRouteRequest
            {
                RouteTableId = pubRtb,
                DestinationCidrBlock = "0.0.0.0/0",
                GatewayId = igwId,
            });
            await _ec2.CreateRouteAsync(new CreateRouteRequest
            {
                RouteTableId = privRtb,
                DestinationCidrBlock = "0.0.0.0/0",
                NatGatewayId = natId,
            });

            // Verify NAT route
            var descPriv = await _ec2.DescribeRouteTablesAsync(new DescribeRouteTablesRequest
            {
                RouteTableIds = [privRtb],
            });
            var natRoute = descPriv.RouteTables[0].Routes.First(r => r.DestinationCidrBlock == "0.0.0.0/0");
            Assert.Equal(natId, natRoute.NatGatewayId);

            // 9. Cleanup
            await _ec2.DeleteRouteAsync(new DeleteRouteRequest { RouteTableId = pubRtb, DestinationCidrBlock = "0.0.0.0/0" });
            await _ec2.DeleteRouteAsync(new DeleteRouteRequest { RouteTableId = privRtb, DestinationCidrBlock = "0.0.0.0/0" });
            foreach (var aid in assocIds)
                await _ec2.DisassociateRouteTableAsync(new DisassociateRouteTableRequest { AssociationId = aid });
            await _ec2.DeleteRouteTableAsync(new DeleteRouteTableRequest { RouteTableId = pubRtb });
            await _ec2.DeleteRouteTableAsync(new DeleteRouteTableRequest { RouteTableId = privRtb });
            await _ec2.DeleteNatGatewayAsync(new DeleteNatGatewayRequest { NatGatewayId = natId });
            await _ec2.ReleaseAddressAsync(new ReleaseAddressRequest { AllocationId = eip.AllocationId });
            await _ec2.DetachInternetGatewayAsync(new DetachInternetGatewayRequest
            {
                InternetGatewayId = igwId,
                VpcId = vpcId,
            });
            await _ec2.DeleteInternetGatewayAsync(new DeleteInternetGatewayRequest { InternetGatewayId = igwId });
            foreach (var sid in subnets)
                await _ec2.DeleteSubnetAsync(new DeleteSubnetRequest { SubnetId = sid });
        }
        finally
        {
            await _ec2.DeleteVpcAsync(new DeleteVpcRequest { VpcId = vpcId });
        }
    }

    // ── NAT Gateway Tests ───────────────────────────────────────────────────────

    [Fact]
    public async Task NatGatewayCrud()
    {
        var vpc = await _ec2.CreateVpcAsync(new CreateVpcRequest { CidrBlock = "10.100.0.0/16" });
        var vpcId = vpc.Vpc.VpcId;
        var subnet = await _ec2.CreateSubnetAsync(new CreateSubnetRequest
        {
            VpcId = vpcId,
            CidrBlock = "10.100.1.0/24",
        });
        var subnetId = subnet.Subnet.SubnetId;

        var resp = await _ec2.CreateNatGatewayAsync(new CreateNatGatewayRequest
        {
            SubnetId = subnetId,
            ConnectivityType = ConnectivityType.Private,
        });
        var natId = resp.NatGateway.NatGatewayId;
        Assert.StartsWith("nat-", natId);
        Assert.Equal(NatGatewayState.Available, resp.NatGateway.State);

        var desc = await _ec2.DescribeNatGatewaysAsync(new DescribeNatGatewaysRequest
        {
            NatGatewayIds = [natId],
        });
        Assert.Single(desc.NatGateways);
        Assert.Equal(natId, desc.NatGateways[0].NatGatewayId);
        Assert.Equal(subnetId, desc.NatGateways[0].SubnetId);

        await _ec2.DeleteNatGatewayAsync(new DeleteNatGatewayRequest { NatGatewayId = natId });
        var desc2 = await _ec2.DescribeNatGatewaysAsync(new DescribeNatGatewaysRequest
        {
            NatGatewayIds = [natId],
        });
        Assert.Equal(NatGatewayState.Deleted, desc2.NatGateways[0].State);
    }

    [Fact]
    public async Task NatGatewayFilterByVpc()
    {
        var vpc = await _ec2.CreateVpcAsync(new CreateVpcRequest { CidrBlock = "10.101.0.0/16" });
        var vpcId = vpc.Vpc.VpcId;
        var subnet = await _ec2.CreateSubnetAsync(new CreateSubnetRequest
        {
            VpcId = vpcId,
            CidrBlock = "10.101.1.0/24",
        });
        var subnetId = subnet.Subnet.SubnetId;
        await _ec2.CreateNatGatewayAsync(new CreateNatGatewayRequest
        {
            SubnetId = subnetId,
            ConnectivityType = ConnectivityType.Private,
        });

        var desc = await _ec2.DescribeNatGatewaysAsync(new DescribeNatGatewaysRequest
        {
            Filter = [new Filter("vpc-id", [vpcId])],
        });
        Assert.All(desc.NatGateways, n => Assert.Equal(vpcId, n.VpcId));
    }

    // ── Network ACL Tests ───────────────────────────────────────────────────────

    [Fact]
    public async Task NetworkAclCrud()
    {
        var vpc = await _ec2.CreateVpcAsync(new CreateVpcRequest { CidrBlock = "10.102.0.0/16" });
        var vpcId = vpc.Vpc.VpcId;

        var resp = await _ec2.CreateNetworkAclAsync(new CreateNetworkAclRequest { VpcId = vpcId });
        var aclId = resp.NetworkAcl.NetworkAclId;
        Assert.StartsWith("acl-", aclId);
        Assert.Equal(vpcId, resp.NetworkAcl.VpcId);
        Assert.False(resp.NetworkAcl.IsDefault);

        var desc = await _ec2.DescribeNetworkAclsAsync(new DescribeNetworkAclsRequest
        {
            NetworkAclIds = [aclId],
        });
        Assert.Single(desc.NetworkAcls);
        Assert.Equal(aclId, desc.NetworkAcls[0].NetworkAclId);

        await _ec2.CreateNetworkAclEntryAsync(new CreateNetworkAclEntryRequest
        {
            NetworkAclId = aclId,
            RuleNumber = 100,
            Protocol = "-1",
            RuleAction = RuleAction.Allow,
            Egress = false,
            CidrBlock = "0.0.0.0/0",
        });

        var desc2 = await _ec2.DescribeNetworkAclsAsync(new DescribeNetworkAclsRequest
        {
            NetworkAclIds = [aclId],
        });
        Assert.Single(desc2.NetworkAcls[0].Entries);

        await _ec2.DeleteNetworkAclEntryAsync(new DeleteNetworkAclEntryRequest
        {
            NetworkAclId = aclId,
            RuleNumber = 100,
            Egress = false,
        });

        var desc3 = await _ec2.DescribeNetworkAclsAsync(new DescribeNetworkAclsRequest
        {
            NetworkAclIds = [aclId],
        });
        Assert.Empty(desc3.NetworkAcls[0].Entries ?? []);

        await _ec2.DeleteNetworkAclAsync(new DeleteNetworkAclRequest { NetworkAclId = aclId });
        var desc4 = await _ec2.DescribeNetworkAclsAsync(new DescribeNetworkAclsRequest
        {
            NetworkAclIds = [aclId],
        });
        Assert.Empty(desc4.NetworkAcls ?? []);
    }

    [Fact]
    public async Task NetworkAclReplaceEntry()
    {
        var vpc = await _ec2.CreateVpcAsync(new CreateVpcRequest { CidrBlock = "10.103.0.0/16" });
        var vpcId = vpc.Vpc.VpcId;
        var resp = await _ec2.CreateNetworkAclAsync(new CreateNetworkAclRequest { VpcId = vpcId });
        var aclId = resp.NetworkAcl.NetworkAclId;

        await _ec2.CreateNetworkAclEntryAsync(new CreateNetworkAclEntryRequest
        {
            NetworkAclId = aclId,
            RuleNumber = 200,
            Protocol = "-1",
            RuleAction = RuleAction.Deny,
            Egress = false,
            CidrBlock = "10.0.0.0/8",
        });

        await _ec2.ReplaceNetworkAclEntryAsync(new ReplaceNetworkAclEntryRequest
        {
            NetworkAclId = aclId,
            RuleNumber = 200,
            Protocol = "-1",
            RuleAction = RuleAction.Allow,
            Egress = false,
            CidrBlock = "10.0.0.0/8",
        });

        var desc = await _ec2.DescribeNetworkAclsAsync(new DescribeNetworkAclsRequest
        {
            NetworkAclIds = [aclId],
        });
        var entries = desc.NetworkAcls[0].Entries;
        Assert.Single(entries);
        Assert.Equal(RuleAction.Allow, entries[0].RuleAction);
    }

    // ── Flow Logs Tests ─────────────────────────────────────────────────────────

    [Fact]
    public async Task FlowLogsCrud()
    {
        var vpc = await _ec2.CreateVpcAsync(new CreateVpcRequest { CidrBlock = "10.104.0.0/16" });
        var vpcId = vpc.Vpc.VpcId;

        var resp = await _ec2.CreateFlowLogsAsync(new CreateFlowLogsRequest
        {
            ResourceIds = [vpcId],
            ResourceType = FlowLogsResourceType.VPC,
            TrafficType = TrafficType.ALL,
            LogDestinationType = LogDestinationType.CloudWatchLogs,
            LogGroupName = "/aws/vpc/flowlogs",
        });
        Assert.Empty(resp.Unsuccessful ?? []);
        var flIds = resp.FlowLogIds;
        Assert.Single(flIds);
        Assert.StartsWith("fl-", flIds[0]);

        var desc = await _ec2.DescribeFlowLogsAsync(new DescribeFlowLogsRequest
        {
            FlowLogIds = flIds,
        });
        Assert.Single(desc.FlowLogs);
        Assert.Equal(flIds[0], desc.FlowLogs[0].FlowLogId);
        Assert.Equal("ACTIVE", desc.FlowLogs[0].FlowLogStatus);

        await _ec2.DeleteFlowLogsAsync(new DeleteFlowLogsRequest { FlowLogIds = flIds });
        var desc2 = await _ec2.DescribeFlowLogsAsync(new DescribeFlowLogsRequest
        {
            FlowLogIds = flIds,
        });
        Assert.Empty(desc2.FlowLogs ?? []);
    }

    // ── VPC Peering Tests ───────────────────────────────────────────────────────

    [Fact]
    public async Task VpcPeeringCrud()
    {
        var vpc1 = await _ec2.CreateVpcAsync(new CreateVpcRequest { CidrBlock = "10.105.0.0/16" });
        var vpc2 = await _ec2.CreateVpcAsync(new CreateVpcRequest { CidrBlock = "10.106.0.0/16" });
        var vpcId1 = vpc1.Vpc.VpcId;
        var vpcId2 = vpc2.Vpc.VpcId;

        var resp = await _ec2.CreateVpcPeeringConnectionAsync(new CreateVpcPeeringConnectionRequest
        {
            VpcId = vpcId1,
            PeerVpcId = vpcId2,
        });
        var pcxId = resp.VpcPeeringConnection.VpcPeeringConnectionId;
        Assert.StartsWith("pcx-", pcxId);
        Assert.Equal("pending-acceptance", resp.VpcPeeringConnection.Status.Code);

        var accepted = await _ec2.AcceptVpcPeeringConnectionAsync(new AcceptVpcPeeringConnectionRequest
        {
            VpcPeeringConnectionId = pcxId,
        });
        Assert.Equal("active", accepted.VpcPeeringConnection.Status.Code);

        var desc = await _ec2.DescribeVpcPeeringConnectionsAsync(new DescribeVpcPeeringConnectionsRequest
        {
            VpcPeeringConnectionIds = [pcxId],
        });
        Assert.Single(desc.VpcPeeringConnections);
        Assert.Equal("active", desc.VpcPeeringConnections[0].Status.Code);

        await _ec2.DeleteVpcPeeringConnectionAsync(new DeleteVpcPeeringConnectionRequest
        {
            VpcPeeringConnectionId = pcxId,
        });
        var desc2 = await _ec2.DescribeVpcPeeringConnectionsAsync(new DescribeVpcPeeringConnectionsRequest
        {
            VpcPeeringConnectionIds = [pcxId],
        });
        Assert.Equal("deleted", desc2.VpcPeeringConnections[0].Status.Code);
    }

    [Fact]
    public async Task VpcPeeringNotFound()
    {
        var ex = await Assert.ThrowsAsync<AmazonEC2Exception>(() =>
            _ec2.AcceptVpcPeeringConnectionAsync(new AcceptVpcPeeringConnectionRequest
            {
                VpcPeeringConnectionId = "pcx-nonexistent",
            }));
        Assert.Contains("NotFound", ex.ErrorCode);
    }

    // ── DHCP Options Tests ──────────────────────────────────────────────────────

    [Fact]
    public async Task DhcpOptionsCrud()
    {
        var resp = await _ec2.CreateDhcpOptionsAsync(new CreateDhcpOptionsRequest
        {
            DhcpConfigurations =
            [
                new DhcpConfiguration
                {
                    Key = "domain-name",
                    Values = ["example.internal"],
                },
                new DhcpConfiguration
                {
                    Key = "domain-name-servers",
                    Values = ["10.0.0.1", "10.0.0.2"],
                },
            ],
        });
        var doptId = resp.DhcpOptions.DhcpOptionsId;
        Assert.StartsWith("dopt-", doptId);

        var desc = await _ec2.DescribeDhcpOptionsAsync(new DescribeDhcpOptionsRequest
        {
            DhcpOptionsIds = [doptId],
        });
        Assert.Single(desc.DhcpOptions);
        var configs = desc.DhcpOptions[0].DhcpConfigurations.ToDictionary(
            c => c.Key,
            c => c.Values.ToList());
        Assert.Equal(["example.internal"], configs["domain-name"]);
        Assert.Contains("10.0.0.1", configs["domain-name-servers"]);

        var vpc = await _ec2.CreateVpcAsync(new CreateVpcRequest { CidrBlock = "10.107.0.0/16" });
        await _ec2.AssociateDhcpOptionsAsync(new AssociateDhcpOptionsRequest
        {
            DhcpOptionsId = doptId,
            VpcId = vpc.Vpc.VpcId,
        });

        await _ec2.DeleteDhcpOptionsAsync(new DeleteDhcpOptionsRequest { DhcpOptionsId = doptId });
        var desc2 = await _ec2.DescribeDhcpOptionsAsync(new DescribeDhcpOptionsRequest
        {
            DhcpOptionsIds = [doptId],
        });
        Assert.Empty(desc2.DhcpOptions ?? []);
    }

    [Fact]
    public async Task DhcpOptionsNotFound()
    {
        var ex = await Assert.ThrowsAsync<AmazonEC2Exception>(() =>
            _ec2.DeleteDhcpOptionsAsync(new DeleteDhcpOptionsRequest
            {
                DhcpOptionsId = "dopt-nonexistent",
            }));
        Assert.Contains("NotFound", ex.ErrorCode);
    }

    // ── Egress-Only Internet Gateway Tests ──────────────────────────────────────

    [Fact]
    public async Task EgressOnlyIgwCrud()
    {
        var vpc = await _ec2.CreateVpcAsync(new CreateVpcRequest { CidrBlock = "10.108.0.0/16" });
        var vpcId = vpc.Vpc.VpcId;

        var resp = await _ec2.CreateEgressOnlyInternetGatewayAsync(new CreateEgressOnlyInternetGatewayRequest
        {
            VpcId = vpcId,
        });
        var eigwId = resp.EgressOnlyInternetGateway.EgressOnlyInternetGatewayId;
        Assert.StartsWith("eigw-", eigwId);
        Assert.Equal("attached", resp.EgressOnlyInternetGateway.Attachments[0].State);
        Assert.Equal(vpcId, resp.EgressOnlyInternetGateway.Attachments[0].VpcId);

        var desc = await _ec2.DescribeEgressOnlyInternetGatewaysAsync(
            new DescribeEgressOnlyInternetGatewaysRequest
            {
                EgressOnlyInternetGatewayIds = [eigwId],
            });
        Assert.Single(desc.EgressOnlyInternetGateways);
        Assert.Equal(eigwId, desc.EgressOnlyInternetGateways[0].EgressOnlyInternetGatewayId);

        await _ec2.DeleteEgressOnlyInternetGatewayAsync(new DeleteEgressOnlyInternetGatewayRequest
        {
            EgressOnlyInternetGatewayId = eigwId,
        });
        var desc2 = await _ec2.DescribeEgressOnlyInternetGatewaysAsync(
            new DescribeEgressOnlyInternetGatewaysRequest
            {
                EgressOnlyInternetGatewayIds = [eigwId],
            });
        Assert.Empty(desc2.EgressOnlyInternetGateways ?? []);
    }

    [Fact]
    public async Task EgressOnlyIgwNotFound()
    {
        var ex = await Assert.ThrowsAsync<AmazonEC2Exception>(() =>
            _ec2.DeleteEgressOnlyInternetGatewayAsync(new DeleteEgressOnlyInternetGatewayRequest
            {
                EgressOnlyInternetGatewayId = "eigw-nonexistent",
            }));
        Assert.Contains("NotFound", ex.ErrorCode);
    }

    // ── VPN Gateway Tests ───────────────────────────────────────────────────────

    [Fact]
    public async Task VpnGatewayCrud()
    {
        var vpc = await _ec2.CreateVpcAsync(new CreateVpcRequest { CidrBlock = "10.95.0.0/16" });
        var vpcId = vpc.Vpc.VpcId;

        try
        {
            var vgw = await _ec2.CreateVpnGatewayAsync(new CreateVpnGatewayRequest
            {
                Type = GatewayType.Ipsec1,
            });
            var vgwId = vgw.VpnGateway.VpnGatewayId;
            Assert.Equal("available", vgw.VpnGateway.State);

            await _ec2.AttachVpnGatewayAsync(new AttachVpnGatewayRequest
            {
                VpnGatewayId = vgwId,
                VpcId = vpcId,
            });

            var desc = await _ec2.DescribeVpnGatewaysAsync(new DescribeVpnGatewaysRequest
            {
                VpnGatewayIds = [vgwId],
            });
            Assert.Single(desc.VpnGateways[0].VpcAttachments);
            Assert.Equal(vpcId, desc.VpnGateways[0].VpcAttachments[0].VpcId);
            Assert.Equal(AttachmentStatus.Attached, desc.VpnGateways[0].VpcAttachments[0].State);

            // Filter by attachment.vpc-id
            var filtered = await _ec2.DescribeVpnGatewaysAsync(new DescribeVpnGatewaysRequest
            {
                Filters = [new Filter("attachment.vpc-id", [vpcId])],
            });
            Assert.Single(filtered.VpnGateways);

            await _ec2.DetachVpnGatewayAsync(new DetachVpnGatewayRequest
            {
                VpnGatewayId = vgwId,
                VpcId = vpcId,
            });

            var desc2 = await _ec2.DescribeVpnGatewaysAsync(new DescribeVpnGatewaysRequest
            {
                VpnGatewayIds = [vgwId],
            });
            Assert.Empty(desc2.VpnGateways[0].VpcAttachments ?? []);

            await _ec2.DeleteVpnGatewayAsync(new DeleteVpnGatewayRequest { VpnGatewayId = vgwId });
            var desc3 = await _ec2.DescribeVpnGatewaysAsync(new DescribeVpnGatewaysRequest
            {
                VpnGatewayIds = [vgwId],
            });
            Assert.Empty(desc3.VpnGateways ?? []);
        }
        finally
        {
            await _ec2.DeleteVpcAsync(new DeleteVpcRequest { VpcId = vpcId });
        }
    }

    [Fact]
    public async Task VgwRoutePropagation()
    {
        var vpc = await _ec2.CreateVpcAsync(new CreateVpcRequest { CidrBlock = "10.94.0.0/16" });
        var vpcId = vpc.Vpc.VpcId;

        try
        {
            var rtb = await _ec2.CreateRouteTableAsync(new CreateRouteTableRequest { VpcId = vpcId });
            var rtbId = rtb.RouteTable.RouteTableId;
            var vgw = await _ec2.CreateVpnGatewayAsync(new CreateVpnGatewayRequest
            {
                Type = GatewayType.Ipsec1,
            });
            var vgwId = vgw.VpnGateway.VpnGatewayId;

            await _ec2.EnableVgwRoutePropagationAsync(new EnableVgwRoutePropagationRequest
            {
                RouteTableId = rtbId,
                GatewayId = vgwId,
            });

            await _ec2.DisableVgwRoutePropagationAsync(new DisableVgwRoutePropagationRequest
            {
                RouteTableId = rtbId,
                GatewayId = vgwId,
            });

            await _ec2.DeleteVpnGatewayAsync(new DeleteVpnGatewayRequest { VpnGatewayId = vgwId });
            await _ec2.DeleteRouteTableAsync(new DeleteRouteTableRequest { RouteTableId = rtbId });
        }
        finally
        {
            await _ec2.DeleteVpcAsync(new DeleteVpcRequest { VpcId = vpcId });
        }
    }

    // ── Customer Gateway Tests ──────────────────────────────────────────────────

    [Fact]
    public async Task CustomerGatewayCrud()
    {
        var cgw = await _ec2.CreateCustomerGatewayAsync(new CreateCustomerGatewayRequest
        {
            BgpAsn = 65000,
            IpAddress = "203.0.113.1",
            Type = GatewayType.Ipsec1,
        });
        var cgwId = cgw.CustomerGateway.CustomerGatewayId;
        Assert.Equal("available", cgw.CustomerGateway.State);
        Assert.Equal("203.0.113.1", cgw.CustomerGateway.IpAddress);

        var desc = await _ec2.DescribeCustomerGatewaysAsync(new DescribeCustomerGatewaysRequest
        {
            CustomerGatewayIds = [cgwId],
        });
        Assert.Single(desc.CustomerGateways);
        Assert.Equal("65000", desc.CustomerGateways[0].BgpAsn);

        await _ec2.DeleteCustomerGatewayAsync(new DeleteCustomerGatewayRequest
        {
            CustomerGatewayId = cgwId,
        });
        var desc2 = await _ec2.DescribeCustomerGatewaysAsync(new DescribeCustomerGatewaysRequest
        {
            CustomerGatewayIds = [cgwId],
        });
        Assert.Empty(desc2.CustomerGateways ?? []);
    }

    // ── Create Route with NAT Gateway ───────────────────────────────────────────

    [Fact]
    public async Task CreateRouteNatGateway()
    {
        var vpc = await _ec2.CreateVpcAsync(new CreateVpcRequest { CidrBlock = "10.93.0.0/16" });
        var vpcId = vpc.Vpc.VpcId;

        try
        {
            var subnet = await _ec2.CreateSubnetAsync(new CreateSubnetRequest
            {
                VpcId = vpcId,
                CidrBlock = "10.93.1.0/24",
            });
            var subnetId = subnet.Subnet.SubnetId;
            var eip = await _ec2.AllocateAddressAsync(new AllocateAddressRequest { Domain = DomainType.Vpc });
            var nat = await _ec2.CreateNatGatewayAsync(new CreateNatGatewayRequest
            {
                SubnetId = subnetId,
                AllocationId = eip.AllocationId,
            });
            var natId = nat.NatGateway.NatGatewayId;
            var rtb = await _ec2.CreateRouteTableAsync(new CreateRouteTableRequest { VpcId = vpcId });
            var rtbId = rtb.RouteTable.RouteTableId;

            await _ec2.CreateRouteAsync(new CreateRouteRequest
            {
                RouteTableId = rtbId,
                DestinationCidrBlock = "0.0.0.0/0",
                NatGatewayId = natId,
            });

            var desc = await _ec2.DescribeRouteTablesAsync(new DescribeRouteTablesRequest
            {
                RouteTableIds = [rtbId],
            });
            var natRoute = desc.RouteTables[0].Routes.First(r => r.DestinationCidrBlock == "0.0.0.0/0");
            Assert.Equal(natId, natRoute.NatGatewayId);
            Assert.True(string.IsNullOrEmpty(natRoute.GatewayId));

            await _ec2.DeleteRouteAsync(new DeleteRouteRequest
            {
                RouteTableId = rtbId,
                DestinationCidrBlock = "0.0.0.0/0",
            });
            await _ec2.DeleteRouteTableAsync(new DeleteRouteTableRequest { RouteTableId = rtbId });
            await _ec2.DeleteNatGatewayAsync(new DeleteNatGatewayRequest { NatGatewayId = natId });
            await _ec2.ReleaseAddressAsync(new ReleaseAddressRequest { AllocationId = eip.AllocationId });
            await _ec2.DeleteSubnetAsync(new DeleteSubnetRequest { SubnetId = subnetId });
        }
        finally
        {
            await _ec2.DeleteVpcAsync(new DeleteVpcRequest { VpcId = vpcId });
        }
    }

    // ── Managed Prefix List Tests ───────────────────────────────────────────────

    [Fact]
    public async Task ManagedPrefixListCrud()
    {
        var pl = await _ec2.CreateManagedPrefixListAsync(new CreateManagedPrefixListRequest
        {
            PrefixListName = "test-pl",
            MaxEntries = 5,
            AddressFamily = "IPv4",
            Entries =
            [
                new AddPrefixListEntry { Cidr = "10.0.0.0/8", Description = "RFC1918-10" },
            ],
        });
        var plId = pl.PrefixList.PrefixListId;
        Assert.Equal("test-pl", pl.PrefixList.PrefixListName);

        // Describe
        var desc = await _ec2.DescribeManagedPrefixListsAsync(new DescribeManagedPrefixListsRequest
        {
            PrefixListIds = [plId],
        });
        Assert.Single(desc.PrefixLists);
        Assert.Equal("test-pl", desc.PrefixLists[0].PrefixListName);

        // Get entries
        var entries = await _ec2.GetManagedPrefixListEntriesAsync(new GetManagedPrefixListEntriesRequest
        {
            PrefixListId = plId,
        });
        Assert.Single(entries.Entries);
        Assert.Equal("10.0.0.0/8", entries.Entries[0].Cidr);

        // Modify — add entry
        await _ec2.ModifyManagedPrefixListAsync(new ModifyManagedPrefixListRequest
        {
            PrefixListId = plId,
            CurrentVersion = 1,
            AddEntries =
            [
                new AddPrefixListEntry { Cidr = "172.16.0.0/12", Description = "RFC1918-172" },
            ],
        });
        entries = await _ec2.GetManagedPrefixListEntriesAsync(new GetManagedPrefixListEntriesRequest
        {
            PrefixListId = plId,
        });
        var cidrs = entries.Entries.Select(e => e.Cidr).ToList();
        Assert.Contains("10.0.0.0/8", cidrs);
        Assert.Contains("172.16.0.0/12", cidrs);

        // Modify — remove entry
        await _ec2.ModifyManagedPrefixListAsync(new ModifyManagedPrefixListRequest
        {
            PrefixListId = plId,
            CurrentVersion = 2,
            RemoveEntries =
            [
                new RemovePrefixListEntry { Cidr = "10.0.0.0/8" },
            ],
        });
        entries = await _ec2.GetManagedPrefixListEntriesAsync(new GetManagedPrefixListEntriesRequest
        {
            PrefixListId = plId,
        });
        cidrs = entries.Entries.Select(e => e.Cidr).ToList();
        Assert.DoesNotContain("10.0.0.0/8", cidrs);
        Assert.Contains("172.16.0.0/12", cidrs);

        // Delete
        await _ec2.DeleteManagedPrefixListAsync(new DeleteManagedPrefixListRequest
        {
            PrefixListId = plId,
        });
        desc = await _ec2.DescribeManagedPrefixListsAsync(new DescribeManagedPrefixListsRequest
        {
            PrefixListIds = [plId],
        });
        Assert.Empty(desc.PrefixLists ?? []);
    }

    // ── Launch Template Tests ───────────────────────────────────────────────────

    [Fact]
    public async Task LaunchTemplateCrud()
    {
        var resp = await _ec2.CreateLaunchTemplateAsync(new CreateLaunchTemplateRequest
        {
            LaunchTemplateName = "qa-lt-basic",
            LaunchTemplateData = new RequestLaunchTemplateData
            {
                InstanceType = InstanceType.T3Micro,
                ImageId = "ami-12345678",
                KeyName = "my-key",
            },
        });
        var lt = resp.LaunchTemplate;
        var ltId = lt.LaunchTemplateId;
        Assert.StartsWith("lt-", ltId);
        Assert.Equal("qa-lt-basic", lt.LaunchTemplateName);
        Assert.Equal(1, lt.DefaultVersionNumber);
        Assert.Equal(1, lt.LatestVersionNumber);

        // Describe
        var desc = await _ec2.DescribeLaunchTemplatesAsync(new DescribeLaunchTemplatesRequest
        {
            LaunchTemplateIds = [ltId],
        });
        Assert.Single(desc.LaunchTemplates);
        Assert.Equal("qa-lt-basic", desc.LaunchTemplates[0].LaunchTemplateName);

        // Describe by name
        var desc2 = await _ec2.DescribeLaunchTemplatesAsync(new DescribeLaunchTemplatesRequest
        {
            LaunchTemplateNames = ["qa-lt-basic"],
        });
        Assert.Single(desc2.LaunchTemplates);

        // Describe versions
        var versions = await _ec2.DescribeLaunchTemplateVersionsAsync(
            new DescribeLaunchTemplateVersionsRequest
            {
                LaunchTemplateId = ltId,
            });
        Assert.Single(versions.LaunchTemplateVersions);
        var ver = versions.LaunchTemplateVersions[0];
        Assert.Equal(1, ver.VersionNumber);
        Assert.Equal("t3.micro", ver.LaunchTemplateData.InstanceType);
        Assert.Equal("ami-12345678", ver.LaunchTemplateData.ImageId);

        // Delete
        await _ec2.DeleteLaunchTemplateAsync(new DeleteLaunchTemplateRequest
        {
            LaunchTemplateId = ltId,
        });
        var desc3 = await _ec2.DescribeLaunchTemplatesAsync(new DescribeLaunchTemplatesRequest
        {
            LaunchTemplateIds = [ltId],
        });
        Assert.Empty(desc3.LaunchTemplates ?? []);
    }

    [Fact]
    public async Task LaunchTemplateDuplicateName()
    {
        await _ec2.CreateLaunchTemplateAsync(new CreateLaunchTemplateRequest
        {
            LaunchTemplateName = "qa-lt-dup",
            LaunchTemplateData = new RequestLaunchTemplateData { InstanceType = InstanceType.T3Micro },
        });

        var ex = await Assert.ThrowsAsync<AmazonEC2Exception>(() =>
            _ec2.CreateLaunchTemplateAsync(new CreateLaunchTemplateRequest
            {
                LaunchTemplateName = "qa-lt-dup",
                LaunchTemplateData = new RequestLaunchTemplateData { InstanceType = InstanceType.T3Small },
            }));
        Assert.Contains("AlreadyExists", ex.ErrorCode);
    }

    [Fact]
    public async Task LaunchTemplateVersions()
    {
        var resp = await _ec2.CreateLaunchTemplateAsync(new CreateLaunchTemplateRequest
        {
            LaunchTemplateName = "qa-lt-ver",
            LaunchTemplateData = new RequestLaunchTemplateData
            {
                InstanceType = InstanceType.T3Micro,
                ImageId = "ami-v1",
            },
        });
        var ltId = resp.LaunchTemplate.LaunchTemplateId;

        // Create version 2
        await _ec2.CreateLaunchTemplateVersionAsync(new CreateLaunchTemplateVersionRequest
        {
            LaunchTemplateId = ltId,
            LaunchTemplateData = new RequestLaunchTemplateData
            {
                InstanceType = InstanceType.T3Small,
                ImageId = "ami-v2",
            },
            VersionDescription = "version two",
        });

        // Create version 3
        await _ec2.CreateLaunchTemplateVersionAsync(new CreateLaunchTemplateVersionRequest
        {
            LaunchTemplateId = ltId,
            LaunchTemplateData = new RequestLaunchTemplateData
            {
                InstanceType = InstanceType.T3Large,
                ImageId = "ami-v3",
            },
        });

        // Latest should be version 3
        var latest = await _ec2.DescribeLaunchTemplateVersionsAsync(
            new DescribeLaunchTemplateVersionsRequest
            {
                LaunchTemplateId = ltId,
                Versions = ["$Latest"],
            });
        Assert.Single(latest.LaunchTemplateVersions);
        Assert.Equal(3, latest.LaunchTemplateVersions[0].VersionNumber);
        Assert.Equal("t3.large", latest.LaunchTemplateVersions[0].LaunchTemplateData.InstanceType);

        // Default should still be version 1
        var defaultVer = await _ec2.DescribeLaunchTemplateVersionsAsync(
            new DescribeLaunchTemplateVersionsRequest
            {
                LaunchTemplateId = ltId,
                Versions = ["$Default"],
            });
        Assert.Equal(1, defaultVer.LaunchTemplateVersions[0].VersionNumber);

        // All versions
        var allVer = await _ec2.DescribeLaunchTemplateVersionsAsync(
            new DescribeLaunchTemplateVersionsRequest
            {
                LaunchTemplateId = ltId,
            });
        Assert.Equal(3, allVer.LaunchTemplateVersions.Count);

        // Modify default to version 2
        await _ec2.ModifyLaunchTemplateAsync(new ModifyLaunchTemplateRequest
        {
            LaunchTemplateId = ltId,
            DefaultVersion = "2",
        });
        var desc = await _ec2.DescribeLaunchTemplatesAsync(new DescribeLaunchTemplatesRequest
        {
            LaunchTemplateIds = [ltId],
        });
        Assert.Equal(2, desc.LaunchTemplates[0].DefaultVersionNumber);

        var default2 = await _ec2.DescribeLaunchTemplateVersionsAsync(
            new DescribeLaunchTemplateVersionsRequest
            {
                LaunchTemplateId = ltId,
                Versions = ["$Default"],
            });
        Assert.Equal(2, default2.LaunchTemplateVersions[0].VersionNumber);

        await _ec2.DeleteLaunchTemplateAsync(new DeleteLaunchTemplateRequest
        {
            LaunchTemplateId = ltId,
        });
    }

    [Fact]
    public async Task LaunchTemplateWithBlockDevices()
    {
        var resp = await _ec2.CreateLaunchTemplateAsync(new CreateLaunchTemplateRequest
        {
            LaunchTemplateName = "qa-lt-bdm",
            LaunchTemplateData = new RequestLaunchTemplateData
            {
                InstanceType = InstanceType.T3Micro,
                BlockDeviceMappings =
                [
                    new LaunchTemplateBlockDeviceMappingRequest
                    {
                        DeviceName = "/dev/xvda",
                        Ebs = new LaunchTemplateEbsBlockDeviceRequest
                        {
                            VolumeSize = 50,
                            VolumeType = VolumeType.Gp3,
                            Encrypted = true,
                            DeleteOnTermination = true,
                        },
                    },
                ],
            },
        });
        var ltId = resp.LaunchTemplate.LaunchTemplateId;

        var versions = await _ec2.DescribeLaunchTemplateVersionsAsync(
            new DescribeLaunchTemplateVersionsRequest
            {
                LaunchTemplateId = ltId,
            });
        var data = versions.LaunchTemplateVersions[0].LaunchTemplateData;
        Assert.Single(data.BlockDeviceMappings);
        var bdm = data.BlockDeviceMappings[0];
        Assert.Equal("/dev/xvda", bdm.DeviceName);
        Assert.Equal(50, bdm.Ebs.VolumeSize);
        Assert.Equal(VolumeType.Gp3, bdm.Ebs.VolumeType);

        await _ec2.DeleteLaunchTemplateAsync(new DeleteLaunchTemplateRequest { LaunchTemplateId = ltId });
    }

    [Fact]
    public async Task LaunchTemplateNotFound()
    {
        var ex = await Assert.ThrowsAsync<AmazonEC2Exception>(() =>
            _ec2.DescribeLaunchTemplateVersionsAsync(new DescribeLaunchTemplateVersionsRequest
            {
                LaunchTemplateId = "lt-nonexistent",
            }));
        Assert.Contains("NotFound", ex.ErrorCode);
    }
}
