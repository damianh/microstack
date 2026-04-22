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

    public async ValueTask InitializeAsync()
    {
        await _fixture.HttpClient.PostAsync("/_ministack/reset", null);
    }

    public ValueTask DisposeAsync()
    {
        _ec2.Dispose();
        return ValueTask.CompletedTask;
    }

    // ── VPC Tests ───────────────────────────────────────────────────────────────

    [Fact]
    public async Task DescribeVpcsDefault()
    {
        var resp = await _ec2.DescribeVpcsAsync(new DescribeVpcsRequest());
        resp.Vpcs.ShouldContain(v => v.IsDefault == true);
    }

    [Fact]
    public async Task CreateAndDeleteVpc()
    {
        var createResp = await _ec2.CreateVpcAsync(new CreateVpcRequest { CidrBlock = "10.1.0.0/16" });
        var vpcId = createResp.Vpc.VpcId;
        vpcId.ShouldStartWith("vpc-");

        var descResp = await _ec2.DescribeVpcsAsync(new DescribeVpcsRequest
        {
            VpcIds = [vpcId],
        });
        descResp.Vpcs.ShouldHaveSingleItem();
        descResp.Vpcs[0].CidrBlock.ShouldBe("10.1.0.0/16");
        descResp.Vpcs[0].IsDefault.ShouldBe(false);

        await _ec2.DeleteVpcAsync(new DeleteVpcRequest { VpcId = vpcId });

        var descAfter = await _ec2.DescribeVpcsAsync(new DescribeVpcsRequest());
        (descAfter.Vpcs ?? []).ShouldNotContain(v => v.VpcId == vpcId);
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
        dnsSupport.EnableDnsSupport.ShouldNotBeNull();

        var dnsHostnames = await _ec2.DescribeVpcAttributeAsync(new DescribeVpcAttributeRequest
        {
            VpcId = vpcId,
            Attribute = VpcAttributeName.EnableDnsHostnames,
        });
        dnsHostnames.EnableDnsHostnames.ShouldNotBeNull();

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
            acls.NetworkAcls.ShouldHaveSingleItem();
            acls.NetworkAcls[0].IsDefault.ShouldBe(true);
            acls.NetworkAcls[0].VpcId.ShouldBe(vpcId);

            // Default SG
            var sgs = await _ec2.DescribeSecurityGroupsAsync(new DescribeSecurityGroupsRequest
            {
                Filters =
                [
                    new Filter("vpc-id", [vpcId]),
                    new Filter("group-name", ["default"]),
                ],
            });
            sgs.SecurityGroups.ShouldHaveSingleItem();
            sgs.SecurityGroups[0].VpcId.ShouldBe(vpcId);

            // Main route table
            var rtbs = await _ec2.DescribeRouteTablesAsync(new DescribeRouteTablesRequest
            {
                Filters =
                [
                    new Filter("vpc-id", [vpcId]),
                    new Filter("association.main", ["true"]),
                ],
            });
            rtbs.RouteTables.ShouldHaveSingleItem();
            rtbs.RouteTables[0].VpcId.ShouldBe(vpcId);
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
        (resp.Subnets.Count >= 1).ShouldBe(true);
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
        subnetId.ShouldStartWith("subnet-");

        var desc = await _ec2.DescribeSubnetsAsync(new DescribeSubnetsRequest
        {
            SubnetIds = [subnetId],
        });
        desc.Subnets.ShouldHaveSingleItem();
        desc.Subnets[0].CidrBlock.ShouldBe("10.2.1.0/24");

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
        desc.Subnets[0].MapPublicIpOnLaunch.ShouldBe(true);

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
        (resp.Subnets.Count >= 3).ShouldBe(true);

        var byAz = resp.Subnets.ToDictionary(s => s.AvailabilityZone);
        byAz.Keys.ShouldContain("us-east-1a");
        byAz.Keys.ShouldContain("us-east-1b");
        byAz.Keys.ShouldContain("us-east-1c");

        byAz["us-east-1a"].CidrBlock.ShouldBe("172.31.0.0/20");
        byAz["us-east-1b"].CidrBlock.ShouldBe("172.31.16.0/20");
        byAz["us-east-1c"].CidrBlock.ShouldBe("172.31.32.0/20");

        foreach (var s in resp.Subnets)
        {
            s.DefaultForAz.ShouldBe(true);
            s.MapPublicIpOnLaunch.ShouldBe(true);
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
        sgId.ShouldStartWith("sg-");

        var desc = await _ec2.DescribeSecurityGroupsAsync(new DescribeSecurityGroupsRequest
        {
            GroupIds = [sgId],
        });
        desc.SecurityGroups[0].GroupName.ShouldBe("qa-ec2-sg");

        await _ec2.DeleteSecurityGroupAsync(new DeleteSecurityGroupRequest { GroupId = sgId });

        var descAfter = await _ec2.DescribeSecurityGroupsAsync(new DescribeSecurityGroupsRequest());
        (descAfter.SecurityGroups ?? []).ShouldNotContain(sg => sg.GroupId == sgId);
    }

    [Fact]
    public async Task SecurityGroupDuplicate()
    {
        await _ec2.CreateSecurityGroupAsync(new CreateSecurityGroupRequest
        {
            GroupName = "qa-ec2-sg-dup",
            Description = "d",
        });

        var ex = await Should.ThrowAsync<AmazonEC2Exception>(() =>
            _ec2.CreateSecurityGroupAsync(new CreateSecurityGroupRequest
            {
                GroupName = "qa-ec2-sg-dup",
                Description = "d",
            }));
        ex.ErrorCode.ShouldBe("InvalidGroup.Duplicate");
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
        desc.SecurityGroups[0].IpPermissions.ShouldContain(p => p.FromPort == 80);

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
        (descAfter.SecurityGroups[0].IpPermissions ?? []).ShouldNotContain(p => p.FromPort == 80);

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
        create.KeyPair.KeyName.ShouldBe("qa-ec2-key");
        create.KeyPair.KeyMaterial.ShouldNotBeEmpty();

        var desc = await _ec2.DescribeKeyPairsAsync(new DescribeKeyPairsRequest
        {
            KeyNames = ["qa-ec2-key"],
        });
        desc.KeyPairs.ShouldHaveSingleItem();

        await _ec2.DeleteKeyPairAsync(new DeleteKeyPairRequest { KeyName = "qa-ec2-key" });

        var descAfter = await _ec2.DescribeKeyPairsAsync(new DescribeKeyPairsRequest());
        (descAfter.KeyPairs ?? []).ShouldNotContain(kp => kp.KeyName == "qa-ec2-key");
    }

    [Fact]
    public async Task KeyPairDuplicate()
    {
        await _ec2.CreateKeyPairAsync(new CreateKeyPairRequest { KeyName = "qa-ec2-key-dup" });

        var ex = await Should.ThrowAsync<AmazonEC2Exception>(() =>
            _ec2.CreateKeyPairAsync(new CreateKeyPairRequest { KeyName = "qa-ec2-key-dup" }));
        ex.ErrorCode.ShouldBe("InvalidKeyPair.Duplicate");
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
        resp.KeyName.ShouldBe("qa-ec2-import-key");
        resp.KeyFingerprint.ShouldNotBeEmpty();

        var desc = await _ec2.DescribeKeyPairsAsync(new DescribeKeyPairsRequest
        {
            KeyNames = ["qa-ec2-import-key"],
        });
        desc.KeyPairs.ShouldHaveSingleItem();

        await _ec2.DeleteKeyPairAsync(new DeleteKeyPairRequest { KeyName = "qa-ec2-import-key" });
    }

    // ── Internet Gateway Tests ──────────────────────────────────────────────────

    [Fact]
    public async Task InternetGatewayCrud()
    {
        var igw = await _ec2.CreateInternetGatewayAsync(new CreateInternetGatewayRequest());
        var igwId = igw.InternetGateway.InternetGatewayId;
        igwId.ShouldStartWith("igw-");

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
        desc.InternetGateways[0].Attachments.ShouldHaveSingleItem();

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
        rtbId.ShouldStartWith("rtb-");

        var desc = await _ec2.DescribeRouteTablesAsync(new DescribeRouteTablesRequest
        {
            RouteTableIds = [rtbId],
        });
        desc.RouteTables.ShouldHaveSingleItem();
        desc.RouteTables[0].RouteTableId.ShouldBe(rtbId);

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
        assocId.ShouldStartWith("rtbassoc-");

        var desc = await _ec2.DescribeRouteTablesAsync(new DescribeRouteTablesRequest
        {
            RouteTableIds = [rtbId],
        });
        desc.RouteTables[0].Associations.ShouldContain(a => a.RouteTableAssociationId == assocId);

        await _ec2.DisassociateRouteTableAsync(new DisassociateRouteTableRequest
        {
            AssociationId = assocId,
        });

        var descAfter = await _ec2.DescribeRouteTablesAsync(new DescribeRouteTablesRequest
        {
            RouteTableIds = [rtbId],
        });
        (descAfter.RouteTables[0].Associations ?? []).ShouldNotContain(a => a.RouteTableAssociationId == assocId);

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
        desc.RouteTables[0].Routes.ShouldContain(r => r.DestinationCidrBlock == "0.0.0.0/0");

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
        (descAfter.RouteTables[0].Routes ?? []).ShouldNotContain(r => r.DestinationCidrBlock == "0.0.0.0/0");

        await _ec2.DeleteInternetGatewayAsync(new DeleteInternetGatewayRequest { InternetGatewayId = igwId });
        await _ec2.DeleteRouteTableAsync(new DeleteRouteTableRequest { RouteTableId = rtbId });
        await _ec2.DeleteVpcAsync(new DeleteVpcRequest { VpcId = vpcId });
    }

    [Fact]
    public async Task DescribeRouteTablesDefault()
    {
        var desc = await _ec2.DescribeRouteTablesAsync(new DescribeRouteTablesRequest());
        desc.RouteTables.ShouldContain(rt => rt.VpcId == "vpc-00000001");
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
            result.RouteTables.ShouldHaveSingleItem();
            result.RouteTables[0].RouteTableId.ShouldBe(rtbId);

            // Filter by subnet ID
            var result2 = await _ec2.DescribeRouteTablesAsync(new DescribeRouteTablesRequest
            {
                Filters = [new Filter("association.subnet-id", [subnetId])],
            });
            result2.RouteTables.ShouldHaveSingleItem();

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
            newResp.NewAssociationId.ShouldNotBe(oldAssocId);

            var result = await _ec2.DescribeRouteTablesAsync(new DescribeRouteTablesRequest
            {
                Filters = [new Filter("association.subnet-id", [subnetId])],
            });
            result.RouteTables[0].RouteTableId.ShouldBe(rtb2Id);

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
        run.Reservation.Instances.ShouldHaveSingleItem();
        var instanceId = run.Reservation.Instances[0].InstanceId;
        instanceId.ShouldStartWith("i-");
        run.Reservation.Instances[0].State.Name.ShouldBe(InstanceStateName.Running);

        var desc = await _ec2.DescribeInstancesAsync(new DescribeInstancesRequest
        {
            InstanceIds = [instanceId],
        });
        desc.Reservations.ShouldHaveSingleItem();
        desc.Reservations[0].Instances[0].InstanceId.ShouldBe(instanceId);

        var term = await _ec2.TerminateInstancesAsync(new TerminateInstancesRequest
        {
            InstanceIds = [instanceId],
        });
        term.TerminatingInstances[0].CurrentState.Name.ShouldBe(InstanceStateName.Terminated);
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
        stop.StoppingInstances[0].CurrentState.Name.ShouldBe(InstanceStateName.Stopped);

        var start = await _ec2.StartInstancesAsync(new StartInstancesRequest
        {
            InstanceIds = [iid],
        });
        start.StartingInstances[0].CurrentState.Name.ShouldBe(InstanceStateName.Running);

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
        run.Reservation.Instances.Count.ShouldBe(3);
        var ids = run.Reservation.Instances.Select(i => i.InstanceId).ToList();
        ids.Distinct().Count().ShouldBe(3);

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
        attr.InstanceAttribute.InstanceId.ShouldBe(iid);
        attr.InstanceAttribute.InstanceType.ShouldBe("t3.micro");

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
        attr.InstanceAttribute.InstanceId.ShouldBe(iid);
        attr.InstanceAttribute.InstanceInitiatedShutdownBehavior.ShouldBe("stop");

        await _ec2.TerminateInstancesAsync(new TerminateInstancesRequest { InstanceIds = [iid] });
    }

    [Fact]
    public async Task DescribeInstanceAttributeNotFound()
    {
        var ex = await Should.ThrowAsync<AmazonEC2Exception>(() =>
            _ec2.DescribeInstanceAttributeAsync(new DescribeInstanceAttributeRequest
            {
                InstanceId = "i-000000000000nonex",
                Attribute = InstanceAttributeName.InstanceType,
            }));
        ex.ErrorCode.ShouldBe("InvalidInstanceID.NotFound");
    }

    // ── Images ──────────────────────────────────────────────────────────────────

    [Fact]
    public async Task DescribeImages()
    {
        var resp = await _ec2.DescribeImagesAsync(new DescribeImagesRequest
        {
            Owners = ["self"],
        });
        (resp.Images.Count >= 1).ShouldBe(true);
        resp.Images.ShouldAllBe(img => !string.IsNullOrEmpty(img.ImageId));
    }

    // ── Availability Zones ──────────────────────────────────────────────────────

    [Fact]
    public async Task DescribeAvailabilityZones()
    {
        var resp = await _ec2.DescribeAvailabilityZonesAsync(new DescribeAvailabilityZonesRequest());
        var azNames = resp.AvailabilityZones.Select(az => az.ZoneName).ToList();
        azNames.ShouldContain(az => az.Contains("us-east-1"));
    }

    // ── Elastic IP Tests ────────────────────────────────────────────────────────

    [Fact]
    public async Task ElasticIpCrud()
    {
        var alloc = await _ec2.AllocateAddressAsync(new AllocateAddressRequest { Domain = DomainType.Vpc });
        var allocId = alloc.AllocationId;
        allocId.ShouldStartWith("eipalloc-");
        alloc.PublicIp.ShouldNotBeEmpty();

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
        assocResp.AssociationId.ShouldNotBeEmpty();

        var desc = await _ec2.DescribeAddressesAsync(new DescribeAddressesRequest
        {
            AllocationIds = [allocId],
        });
        desc.Addresses[0].InstanceId.ShouldBe(iid);

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
        tags.ShouldContain(t => t.Key == "Name" && t.Value == "qa-box");

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
        (tags2 ?? []).ShouldNotContain(t => t.Key == "Name");

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
        resp.Tags.ShouldAllBe(t => t.ResourceId == id1);
        resp.Tags.Count.ShouldBe(2);

        // Filter by key
        var resp2 = await _ec2.DescribeTagsAsync(new DescribeTagsRequest
        {
            Filters = [new Filter("key", ["Env"])],
        });
        resp2.Tags.ShouldAllBe(t => t.Key == "Env");
        resp2.Tags.ShouldContain(t => t.ResourceId == id1);

        // Filter by resource-id + key
        var resp3 = await _ec2.DescribeTagsAsync(new DescribeTagsRequest
        {
            Filters =
            [
                new Filter("resource-id", [id1]),
                new Filter("key", ["Name"]),
            ],
        });
        resp3.Tags.ShouldHaveSingleItem();
        resp3.Tags[0].ResourceId.ShouldBe(id1);
        resp3.Tags[0].Key.ShouldBe("Name");
        resp3.Tags[0].Value.ShouldBe("first");

        // Filter for nonexistent resource
        var resp4 = await _ec2.DescribeTagsAsync(new DescribeTagsRequest
        {
            Filters = [new Filter("resource-id", ["i-doesnotexist"])],
        });
        (resp4.Tags ?? []).ShouldBeEmpty();

        // All tags have correct resource type
        var resp5 = await _ec2.DescribeTagsAsync(new DescribeTagsRequest
        {
            Filters = [new Filter("resource-id", [id1, id2])],
        });
        resp5.Tags.ShouldAllBe(t => t.ResourceType == "instance");
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
        eniId.ShouldStartWith("eni-");

        var desc = await _ec2.DescribeNetworkInterfacesAsync(new DescribeNetworkInterfacesRequest
        {
            NetworkInterfaceIds = [eniId],
        });
        desc.NetworkInterfaces[0].Description.ShouldBe("qa-eni");
        desc.NetworkInterfaces[0].Status.ShouldBe(NetworkInterfaceStatus.Available);

        await _ec2.DeleteNetworkInterfaceAsync(new DeleteNetworkInterfaceRequest
        {
            NetworkInterfaceId = eniId,
        });

        var descAfter = await _ec2.DescribeNetworkInterfacesAsync(new DescribeNetworkInterfacesRequest());
        (descAfter.NetworkInterfaces ?? []).ShouldNotContain(e => e.NetworkInterfaceId == eniId);

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
        attach.AttachmentId.ShouldStartWith("eni-attach-");

        var desc = await _ec2.DescribeNetworkInterfacesAsync(new DescribeNetworkInterfacesRequest
        {
            NetworkInterfaceIds = [eniId],
        });
        desc.NetworkInterfaces[0].Status.ShouldBe(NetworkInterfaceStatus.InUse);

        await _ec2.DetachNetworkInterfaceAsync(new DetachNetworkInterfaceRequest
        {
            AttachmentId = attach.AttachmentId,
        });

        var descAfter = await _ec2.DescribeNetworkInterfacesAsync(new DescribeNetworkInterfacesRequest
        {
            NetworkInterfaceIds = [eniId],
        });
        descAfter.NetworkInterfaces[0].Status.ShouldBe(NetworkInterfaceStatus.Available);

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
        vpceId.ShouldStartWith("vpce-");

        var desc = await _ec2.DescribeVpcEndpointsAsync(new DescribeVpcEndpointsRequest
        {
            VpcEndpointIds = [vpceId],
        });
        desc.VpcEndpoints[0].ServiceName.ShouldBe("com.amazonaws.us-east-1.s3");
        desc.VpcEndpoints[0].State.Value.ShouldBe("Available");

        await _ec2.DeleteVpcEndpointsAsync(new DeleteVpcEndpointsRequest
        {
            VpcEndpointIds = [vpceId],
        });

        var descAfter = await _ec2.DescribeVpcEndpointsAsync(new DescribeVpcEndpointsRequest());
        (descAfter.VpcEndpoints ?? []).ShouldNotContain(e => e.VpcEndpointId == vpceId);

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
            desc.VpcEndpoints[0].RouteTableIds.ShouldContain(rtbId);

            await _ec2.ModifyVpcEndpointAsync(new ModifyVpcEndpointRequest
            {
                VpcEndpointId = vpceId,
                RemoveRouteTableIds = [rtbId],
            });
            desc = await _ec2.DescribeVpcEndpointsAsync(new DescribeVpcEndpointsRequest
            {
                VpcEndpointIds = [vpceId],
            });
            (desc.VpcEndpoints[0].RouteTableIds ?? []).ShouldNotContain(rtbId);

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
        resp.ShouldNotBeNull();
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
        volId.ShouldStartWith("vol-");
        vol.Volume.State.ShouldBe(VolumeState.Available);
        vol.Volume.Size.ShouldBe(20);
        vol.Volume.VolumeType.ShouldBe(VolumeType.Gp3);

        var desc = await _ec2.DescribeVolumesAsync(new DescribeVolumesRequest
        {
            VolumeIds = [volId],
        });
        desc.Volumes.ShouldHaveSingleItem();
        desc.Volumes[0].VolumeId.ShouldBe(volId);
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
        desc.Volumes[0].State.ShouldBe(VolumeState.InUse);
        desc.Volumes[0].Attachments[0].InstanceId.ShouldBe(instanceId);

        await _ec2.DetachVolumeAsync(new DetachVolumeRequest { VolumeId = volId });

        var desc2 = await _ec2.DescribeVolumesAsync(new DescribeVolumesRequest
        {
            VolumeIds = [volId],
        });
        desc2.Volumes[0].State.ShouldBe(VolumeState.Available);
        (desc2.Volumes[0].Attachments ?? []).ShouldBeEmpty();
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
        (desc.Volumes ?? []).ShouldBeEmpty();
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
        resp.VolumeModification.TargetSize.ShouldBe(50);
        resp.VolumeModification.TargetVolumeType.ShouldBe(VolumeType.Gp3);
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
        resp.VolumeStatuses.ShouldHaveSingleItem();
        resp.VolumeStatuses[0].VolumeStatus.Status.Value.ShouldBe("ok");
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
        resp.VolumeId.ShouldBe(volId);
        resp.AutoEnableIO.ShouldNotBeNull();
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
        (resp.VolumesModifications.Count >= 1).ShouldBe(true);
        resp.VolumesModifications[0].VolumeId.ShouldBe(volId);
        resp.VolumesModifications[0].TargetSize.ShouldBe(50);
        resp.VolumesModifications[0].TargetVolumeType.ShouldBe(VolumeType.Gp3);
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
        snap.Snapshot.SnapshotId.ShouldStartWith("snap-");
        snap.Snapshot.State.ShouldBe(SnapshotState.Completed);

        var desc = await _ec2.DescribeSnapshotsAsync(new DescribeSnapshotsRequest
        {
            SnapshotIds = [snap.Snapshot.SnapshotId],
        });
        desc.Snapshots.ShouldHaveSingleItem();
        desc.Snapshots[0].VolumeId.ShouldBe(volId);
        desc.Snapshots[0].Description.ShouldBe("test snapshot");
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
        (desc.Snapshots ?? []).ShouldBeEmpty();
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
        copy.SnapshotId.ShouldNotBe(snap.Snapshot.SnapshotId);
        copy.SnapshotId.ShouldStartWith("snap-");
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
        resp.SnapshotId.ShouldBe(snap.Snapshot.SnapshotId);
        resp.CreateVolumePermissions.ShouldContain(p => p.UserId == "123456789012");
    }

    // ── Describe Instance Types ─────────────────────────────────────────────────

    [Fact]
    public async Task DescribeInstanceTypesDefaults()
    {
        var resp = await _ec2.DescribeInstanceTypesAsync(new DescribeInstanceTypesRequest());
        var types = resp.InstanceTypes.Select(t => t.InstanceType.Value).ToList();
        types.ShouldContain("t2.micro");
        types.ShouldContain("t3.micro");
        (resp.InstanceTypes.Count >= 4).ShouldBe(true);

        var sample = resp.InstanceTypes[0];
        sample.VCpuInfo.ShouldNotBeNull();
        sample.MemoryInfo.ShouldNotBeNull();
        (sample.VCpuInfo.DefaultVCpus >= 1).ShouldBe(true);
        (sample.MemoryInfo.SizeInMiB >= 512).ShouldBe(true);
    }

    [Fact]
    public async Task DescribeInstanceTypesFilter()
    {
        var resp = await _ec2.DescribeInstanceTypesAsync(new DescribeInstanceTypesRequest
        {
            InstanceTypes = [InstanceType.T2Micro, InstanceType.M5Large],
        });
        var types = resp.InstanceTypes.Select(t => t.InstanceType.Value).ToHashSet();
        types.ShouldContain("t2.micro");
        types.ShouldContain("m5.large");
        types.Count.ShouldBe(2);
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
        resp.InstanceCreditSpecifications.ShouldHaveSingleItem();
        resp.InstanceCreditSpecifications[0].InstanceId.ShouldBe(iid);
        resp.InstanceCreditSpecifications[0].CpuCredits.ShouldBe("standard");
    }

    // ── Describe Spot Instance Requests / Capacity Reservations (stubs) ─────────

    [Fact]
    public async Task DescribeSpotInstanceRequests()
    {
        var resp = await _ec2.DescribeSpotInstanceRequestsAsync(new DescribeSpotInstanceRequestsRequest());
        (resp.SpotInstanceRequests ?? []).ShouldBeEmpty();
    }

    [Fact]
    public async Task DescribeCapacityReservations()
    {
        var resp = await _ec2.DescribeCapacityReservationsAsync(new DescribeCapacityReservationsRequest());
        (resp.CapacityReservations ?? []).ShouldBeEmpty();
    }

    // ── Describe Prefix Lists ───────────────────────────────────────────────────

    [Fact]
    public async Task DescribePrefixLists()
    {
        var resp = await _ec2.DescribePrefixListsAsync(new DescribePrefixListsRequest());
        var names = resp.PrefixLists.Select(p => p.PrefixListName).ToList();
        names.ShouldContain(n => n.Contains("s3"));
        names.ShouldContain(n => n.Contains("dynamodb"));
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
            acls.NetworkAcls.ShouldHaveSingleItem();

            var sgs = await _ec2.DescribeSecurityGroupsAsync(new DescribeSecurityGroupsRequest
            {
                Filters =
                [
                    new Filter("vpc-id", [vpcId]),
                    new Filter("group-name", ["default"]),
                ],
            });
            sgs.SecurityGroups.ShouldHaveSingleItem();

            var mainRtbs = await _ec2.DescribeRouteTablesAsync(new DescribeRouteTablesRequest
            {
                Filters =
                [
                    new Filter("vpc-id", [vpcId]),
                    new Filter("association.main", ["true"]),
                ],
            });
            mainRtbs.RouteTables.ShouldHaveSingleItem();

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
            natRoute.NatGatewayId.ShouldBe(natId);

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
        natId.ShouldStartWith("nat-");
        resp.NatGateway.State.ShouldBe(NatGatewayState.Available);

        var desc = await _ec2.DescribeNatGatewaysAsync(new DescribeNatGatewaysRequest
        {
            NatGatewayIds = [natId],
        });
        desc.NatGateways.ShouldHaveSingleItem();
        desc.NatGateways[0].NatGatewayId.ShouldBe(natId);
        desc.NatGateways[0].SubnetId.ShouldBe(subnetId);

        await _ec2.DeleteNatGatewayAsync(new DeleteNatGatewayRequest { NatGatewayId = natId });
        var desc2 = await _ec2.DescribeNatGatewaysAsync(new DescribeNatGatewaysRequest
        {
            NatGatewayIds = [natId],
        });
        desc2.NatGateways[0].State.ShouldBe(NatGatewayState.Deleted);
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
        desc.NatGateways.ShouldAllBe(n => n.VpcId == vpcId);
    }

    // ── Network ACL Tests ───────────────────────────────────────────────────────

    [Fact]
    public async Task NetworkAclCrud()
    {
        var vpc = await _ec2.CreateVpcAsync(new CreateVpcRequest { CidrBlock = "10.102.0.0/16" });
        var vpcId = vpc.Vpc.VpcId;

        var resp = await _ec2.CreateNetworkAclAsync(new CreateNetworkAclRequest { VpcId = vpcId });
        var aclId = resp.NetworkAcl.NetworkAclId;
        aclId.ShouldStartWith("acl-");
        resp.NetworkAcl.VpcId.ShouldBe(vpcId);
        resp.NetworkAcl.IsDefault.ShouldBe(false);

        var desc = await _ec2.DescribeNetworkAclsAsync(new DescribeNetworkAclsRequest
        {
            NetworkAclIds = [aclId],
        });
        desc.NetworkAcls.ShouldHaveSingleItem();
        desc.NetworkAcls[0].NetworkAclId.ShouldBe(aclId);

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
        desc2.NetworkAcls[0].Entries.ShouldHaveSingleItem();

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
        (desc3.NetworkAcls[0].Entries ?? []).ShouldBeEmpty();

        await _ec2.DeleteNetworkAclAsync(new DeleteNetworkAclRequest { NetworkAclId = aclId });
        var desc4 = await _ec2.DescribeNetworkAclsAsync(new DescribeNetworkAclsRequest
        {
            NetworkAclIds = [aclId],
        });
        (desc4.NetworkAcls ?? []).ShouldBeEmpty();
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
        entries.ShouldHaveSingleItem();
        entries[0].RuleAction.ShouldBe(RuleAction.Allow);
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
        (resp.Unsuccessful ?? []).ShouldBeEmpty();
        var flIds = resp.FlowLogIds;
        flIds.ShouldHaveSingleItem();
        flIds[0].ShouldStartWith("fl-");

        var desc = await _ec2.DescribeFlowLogsAsync(new DescribeFlowLogsRequest
        {
            FlowLogIds = flIds,
        });
        desc.FlowLogs.ShouldHaveSingleItem();
        desc.FlowLogs[0].FlowLogId.ShouldBe(flIds[0]);
        desc.FlowLogs[0].FlowLogStatus.ShouldBe("ACTIVE");

        await _ec2.DeleteFlowLogsAsync(new DeleteFlowLogsRequest { FlowLogIds = flIds });
        var desc2 = await _ec2.DescribeFlowLogsAsync(new DescribeFlowLogsRequest
        {
            FlowLogIds = flIds,
        });
        (desc2.FlowLogs ?? []).ShouldBeEmpty();
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
        pcxId.ShouldStartWith("pcx-");
        resp.VpcPeeringConnection.Status.Code.Value.ShouldBe("pending-acceptance");

        var accepted = await _ec2.AcceptVpcPeeringConnectionAsync(new AcceptVpcPeeringConnectionRequest
        {
            VpcPeeringConnectionId = pcxId,
        });
        accepted.VpcPeeringConnection.Status.Code.Value.ShouldBe("active");

        var desc = await _ec2.DescribeVpcPeeringConnectionsAsync(new DescribeVpcPeeringConnectionsRequest
        {
            VpcPeeringConnectionIds = [pcxId],
        });
        desc.VpcPeeringConnections.ShouldHaveSingleItem();
        desc.VpcPeeringConnections[0].Status.Code.Value.ShouldBe("active");

        await _ec2.DeleteVpcPeeringConnectionAsync(new DeleteVpcPeeringConnectionRequest
        {
            VpcPeeringConnectionId = pcxId,
        });
        var desc2 = await _ec2.DescribeVpcPeeringConnectionsAsync(new DescribeVpcPeeringConnectionsRequest
        {
            VpcPeeringConnectionIds = [pcxId],
        });
        desc2.VpcPeeringConnections[0].Status.Code.Value.ShouldBe("deleted");
    }

    [Fact]
    public async Task VpcPeeringNotFound()
    {
        var ex = await Should.ThrowAsync<AmazonEC2Exception>(() =>
            _ec2.AcceptVpcPeeringConnectionAsync(new AcceptVpcPeeringConnectionRequest
            {
                VpcPeeringConnectionId = "pcx-nonexistent",
            }));
        ex.ErrorCode.ShouldContain("NotFound");
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
        doptId.ShouldStartWith("dopt-");

        var desc = await _ec2.DescribeDhcpOptionsAsync(new DescribeDhcpOptionsRequest
        {
            DhcpOptionsIds = [doptId],
        });
        desc.DhcpOptions.ShouldHaveSingleItem();
        var configs = desc.DhcpOptions[0].DhcpConfigurations.ToDictionary(
            c => c.Key,
            c => c.Values.ToList());
        configs["domain-name"].ShouldBe(["example.internal"]);
        configs["domain-name-servers"].ShouldContain("10.0.0.1");

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
        (desc2.DhcpOptions ?? []).ShouldBeEmpty();
    }

    [Fact]
    public async Task DhcpOptionsNotFound()
    {
        var ex = await Should.ThrowAsync<AmazonEC2Exception>(() =>
            _ec2.DeleteDhcpOptionsAsync(new DeleteDhcpOptionsRequest
            {
                DhcpOptionsId = "dopt-nonexistent",
            }));
        ex.ErrorCode.ShouldContain("NotFound");
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
        eigwId.ShouldStartWith("eigw-");
        resp.EgressOnlyInternetGateway.Attachments[0].State.Value.ShouldBe("attached");
        resp.EgressOnlyInternetGateway.Attachments[0].VpcId.ShouldBe(vpcId);

        var desc = await _ec2.DescribeEgressOnlyInternetGatewaysAsync(
            new DescribeEgressOnlyInternetGatewaysRequest
            {
                EgressOnlyInternetGatewayIds = [eigwId],
            });
        desc.EgressOnlyInternetGateways.ShouldHaveSingleItem();
        desc.EgressOnlyInternetGateways[0].EgressOnlyInternetGatewayId.ShouldBe(eigwId);

        await _ec2.DeleteEgressOnlyInternetGatewayAsync(new DeleteEgressOnlyInternetGatewayRequest
        {
            EgressOnlyInternetGatewayId = eigwId,
        });
        var desc2 = await _ec2.DescribeEgressOnlyInternetGatewaysAsync(
            new DescribeEgressOnlyInternetGatewaysRequest
            {
                EgressOnlyInternetGatewayIds = [eigwId],
            });
        (desc2.EgressOnlyInternetGateways ?? []).ShouldBeEmpty();
    }

    [Fact]
    public async Task EgressOnlyIgwNotFound()
    {
        var ex = await Should.ThrowAsync<AmazonEC2Exception>(() =>
            _ec2.DeleteEgressOnlyInternetGatewayAsync(new DeleteEgressOnlyInternetGatewayRequest
            {
                EgressOnlyInternetGatewayId = "eigw-nonexistent",
            }));
        ex.ErrorCode.ShouldContain("NotFound");
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
            vgw.VpnGateway.State.Value.ShouldBe("available");

            await _ec2.AttachVpnGatewayAsync(new AttachVpnGatewayRequest
            {
                VpnGatewayId = vgwId,
                VpcId = vpcId,
            });

            var desc = await _ec2.DescribeVpnGatewaysAsync(new DescribeVpnGatewaysRequest
            {
                VpnGatewayIds = [vgwId],
            });
            desc.VpnGateways[0].VpcAttachments.ShouldHaveSingleItem();
            desc.VpnGateways[0].VpcAttachments[0].VpcId.ShouldBe(vpcId);
            desc.VpnGateways[0].VpcAttachments[0].State.ShouldBe(AttachmentStatus.Attached);

            // Filter by attachment.vpc-id
            var filtered = await _ec2.DescribeVpnGatewaysAsync(new DescribeVpnGatewaysRequest
            {
                Filters = [new Filter("attachment.vpc-id", [vpcId])],
            });
            filtered.VpnGateways.ShouldHaveSingleItem();

            await _ec2.DetachVpnGatewayAsync(new DetachVpnGatewayRequest
            {
                VpnGatewayId = vgwId,
                VpcId = vpcId,
            });

            var desc2 = await _ec2.DescribeVpnGatewaysAsync(new DescribeVpnGatewaysRequest
            {
                VpnGatewayIds = [vgwId],
            });
            (desc2.VpnGateways[0].VpcAttachments ?? []).ShouldBeEmpty();

            await _ec2.DeleteVpnGatewayAsync(new DeleteVpnGatewayRequest { VpnGatewayId = vgwId });
            var desc3 = await _ec2.DescribeVpnGatewaysAsync(new DescribeVpnGatewaysRequest
            {
                VpnGatewayIds = [vgwId],
            });
            (desc3.VpnGateways ?? []).ShouldBeEmpty();
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
        cgw.CustomerGateway.State.ShouldBe("available");
        cgw.CustomerGateway.IpAddress.ShouldBe("203.0.113.1");

        var desc = await _ec2.DescribeCustomerGatewaysAsync(new DescribeCustomerGatewaysRequest
        {
            CustomerGatewayIds = [cgwId],
        });
        desc.CustomerGateways.ShouldHaveSingleItem();
        desc.CustomerGateways[0].BgpAsn.ShouldBe("65000");

        await _ec2.DeleteCustomerGatewayAsync(new DeleteCustomerGatewayRequest
        {
            CustomerGatewayId = cgwId,
        });
        var desc2 = await _ec2.DescribeCustomerGatewaysAsync(new DescribeCustomerGatewaysRequest
        {
            CustomerGatewayIds = [cgwId],
        });
        (desc2.CustomerGateways ?? []).ShouldBeEmpty();
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
            natRoute.NatGatewayId.ShouldBe(natId);
            string.IsNullOrEmpty(natRoute.GatewayId).ShouldBe(true);

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
        pl.PrefixList.PrefixListName.ShouldBe("test-pl");

        // Describe
        var desc = await _ec2.DescribeManagedPrefixListsAsync(new DescribeManagedPrefixListsRequest
        {
            PrefixListIds = [plId],
        });
        desc.PrefixLists.ShouldHaveSingleItem();
        desc.PrefixLists[0].PrefixListName.ShouldBe("test-pl");

        // Get entries
        var entries = await _ec2.GetManagedPrefixListEntriesAsync(new GetManagedPrefixListEntriesRequest
        {
            PrefixListId = plId,
        });
        entries.Entries.ShouldHaveSingleItem();
        entries.Entries[0].Cidr.ShouldBe("10.0.0.0/8");

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
        cidrs.ShouldContain("10.0.0.0/8");
        cidrs.ShouldContain("172.16.0.0/12");

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
        (cidrs ?? []).ShouldNotContain("10.0.0.0/8");
        (cidrs ?? []).ShouldContain("172.16.0.0/12");

        // Delete
        await _ec2.DeleteManagedPrefixListAsync(new DeleteManagedPrefixListRequest
        {
            PrefixListId = plId,
        });
        desc = await _ec2.DescribeManagedPrefixListsAsync(new DescribeManagedPrefixListsRequest
        {
            PrefixListIds = [plId],
        });
        (desc.PrefixLists ?? []).ShouldBeEmpty();
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
        ltId.ShouldStartWith("lt-");
        lt.LaunchTemplateName.ShouldBe("qa-lt-basic");
        lt.DefaultVersionNumber.ShouldBe(1);
        lt.LatestVersionNumber.ShouldBe(1);

        // Describe
        var desc = await _ec2.DescribeLaunchTemplatesAsync(new DescribeLaunchTemplatesRequest
        {
            LaunchTemplateIds = [ltId],
        });
        desc.LaunchTemplates.ShouldHaveSingleItem();
        desc.LaunchTemplates[0].LaunchTemplateName.ShouldBe("qa-lt-basic");

        // Describe by name
        var desc2 = await _ec2.DescribeLaunchTemplatesAsync(new DescribeLaunchTemplatesRequest
        {
            LaunchTemplateNames = ["qa-lt-basic"],
        });
        desc2.LaunchTemplates.ShouldHaveSingleItem();

        // Describe versions
        var versions = await _ec2.DescribeLaunchTemplateVersionsAsync(
            new DescribeLaunchTemplateVersionsRequest
            {
                LaunchTemplateId = ltId,
            });
        versions.LaunchTemplateVersions.ShouldHaveSingleItem();
        var ver = versions.LaunchTemplateVersions[0];
        ver.VersionNumber.ShouldBe(1);
        ver.LaunchTemplateData.InstanceType.Value.ShouldBe("t3.micro");
        ver.LaunchTemplateData.ImageId.ShouldBe("ami-12345678");

        // Delete
        await _ec2.DeleteLaunchTemplateAsync(new DeleteLaunchTemplateRequest
        {
            LaunchTemplateId = ltId,
        });
        var desc3 = await _ec2.DescribeLaunchTemplatesAsync(new DescribeLaunchTemplatesRequest
        {
            LaunchTemplateIds = [ltId],
        });
        (desc3.LaunchTemplates ?? []).ShouldBeEmpty();
    }

    [Fact]
    public async Task LaunchTemplateDuplicateName()
    {
        await _ec2.CreateLaunchTemplateAsync(new CreateLaunchTemplateRequest
        {
            LaunchTemplateName = "qa-lt-dup",
            LaunchTemplateData = new RequestLaunchTemplateData { InstanceType = InstanceType.T3Micro },
        });

        var ex = await Should.ThrowAsync<AmazonEC2Exception>(() =>
            _ec2.CreateLaunchTemplateAsync(new CreateLaunchTemplateRequest
            {
                LaunchTemplateName = "qa-lt-dup",
                LaunchTemplateData = new RequestLaunchTemplateData { InstanceType = InstanceType.T3Small },
            }));
        ex.ErrorCode.ShouldContain("AlreadyExists");
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
        latest.LaunchTemplateVersions.ShouldHaveSingleItem();
        latest.LaunchTemplateVersions[0].VersionNumber.ShouldBe(3);
        latest.LaunchTemplateVersions[0].LaunchTemplateData.InstanceType.Value.ShouldBe("t3.large");

        // Default should still be version 1
        var defaultVer = await _ec2.DescribeLaunchTemplateVersionsAsync(
            new DescribeLaunchTemplateVersionsRequest
            {
                LaunchTemplateId = ltId,
                Versions = ["$Default"],
            });
        defaultVer.LaunchTemplateVersions[0].VersionNumber.ShouldBe(1);

        // All versions
        var allVer = await _ec2.DescribeLaunchTemplateVersionsAsync(
            new DescribeLaunchTemplateVersionsRequest
            {
                LaunchTemplateId = ltId,
            });
        allVer.LaunchTemplateVersions.Count.ShouldBe(3);

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
        desc.LaunchTemplates[0].DefaultVersionNumber.ShouldBe(2);

        var default2 = await _ec2.DescribeLaunchTemplateVersionsAsync(
            new DescribeLaunchTemplateVersionsRequest
            {
                LaunchTemplateId = ltId,
                Versions = ["$Default"],
            });
        default2.LaunchTemplateVersions[0].VersionNumber.ShouldBe(2);

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
        data.BlockDeviceMappings.ShouldHaveSingleItem();
        var bdm = data.BlockDeviceMappings[0];
        bdm.DeviceName.ShouldBe("/dev/xvda");
        bdm.Ebs.VolumeSize.ShouldBe(50);
        bdm.Ebs.VolumeType.ShouldBe(VolumeType.Gp3);

        await _ec2.DeleteLaunchTemplateAsync(new DeleteLaunchTemplateRequest { LaunchTemplateId = ltId });
    }

    [Fact]
    public async Task LaunchTemplateNotFound()
    {
        var ex = await Should.ThrowAsync<AmazonEC2Exception>(() =>
            _ec2.DescribeLaunchTemplateVersionsAsync(new DescribeLaunchTemplateVersionsRequest
            {
                LaunchTemplateId = "lt-nonexistent",
            }));
        ex.ErrorCode.ShouldContain("NotFound");
    }
}
