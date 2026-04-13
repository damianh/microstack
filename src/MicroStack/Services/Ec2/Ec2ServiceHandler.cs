using System.Text.Json;
using System.Text;
using System.Web;
using System.Xml.Linq;
using MicroStack.Internal;

namespace MicroStack.Services.Ec2;

internal sealed class Ec2ServiceHandler : IServiceHandler
{
    public string ServiceName => "ec2";

    private const string Ec2Ns = "http://ec2.amazonaws.com/doc/2016-11-15/";
    private const string DefaultVpcId = "vpc-00000001";
    private const string DefaultSubnetId = "subnet-00000001";
    private const string DefaultSubnetIdB = "subnet-00000002";
    private const string DefaultSubnetIdC = "subnet-00000003";
    private const string DefaultSgId = "sg-00000001";
    private const string DefaultRtbId = "rtb-00000001";
    private const string DefaultAclId = "acl-00000001";
    private const string DefaultIgwId = "igw-00000001";

    private static string Region =>
        MicroStackOptions.Instance.Region;

    private readonly AccountScopedDictionary<string, Dictionary<string, object>> _instances = new();
    private readonly AccountScopedDictionary<string, Dictionary<string, object>> _securityGroups = new();
    private readonly AccountScopedDictionary<string, Dictionary<string, object>> _keyPairs = new();
    private readonly AccountScopedDictionary<string, Dictionary<string, object>> _vpcs = new();
    private readonly AccountScopedDictionary<string, Dictionary<string, object>> _subnets = new();
    private readonly AccountScopedDictionary<string, Dictionary<string, object>> _internetGateways = new();
    private readonly AccountScopedDictionary<string, Dictionary<string, object>> _addresses = new();
    private readonly AccountScopedDictionary<string, List<Dictionary<string, string>>> _tags = new();
    private readonly AccountScopedDictionary<string, Dictionary<string, object>> _routeTables = new();
    private readonly AccountScopedDictionary<string, Dictionary<string, object>> _networkInterfaces = new();
    private readonly AccountScopedDictionary<string, Dictionary<string, object>> _vpcEndpoints = new();
    private readonly AccountScopedDictionary<string, Dictionary<string, object>> _volumes = new();
    private readonly AccountScopedDictionary<string, Dictionary<string, object>> _snapshots = new();
    private readonly AccountScopedDictionary<string, Dictionary<string, object>> _natGateways = new();
    private readonly AccountScopedDictionary<string, Dictionary<string, object>> _networkAcls = new();
    private readonly AccountScopedDictionary<string, Dictionary<string, object>> _flowLogs = new();
    private readonly AccountScopedDictionary<string, Dictionary<string, object>> _vpcPeering = new();
    private readonly AccountScopedDictionary<string, Dictionary<string, object>> _dhcpOptions = new();
    private readonly AccountScopedDictionary<string, Dictionary<string, object>> _egressIgws = new();
    private readonly AccountScopedDictionary<string, Dictionary<string, object>> _prefixLists = new();
    private readonly AccountScopedDictionary<string, Dictionary<string, object>> _vpnGateways = new();
    private readonly AccountScopedDictionary<string, Dictionary<string, object>> _customerGateways = new();
    private readonly AccountScopedDictionary<string, Dictionary<string, object>> _launchTemplates = new();

    private readonly Lock _lock = new();

    internal Ec2ServiceHandler()
    {
        InitDefaults();
    }

    private void InitDefaults()
    {
        var accountId = AccountContext.GetAccountId();

        if (!_vpcs.ContainsKey(DefaultVpcId))
        {
            _vpcs[DefaultVpcId] = new Dictionary<string, object>
            {
                ["VpcId"] = DefaultVpcId, ["CidrBlock"] = "172.31.0.0/16",
                ["State"] = "available", ["IsDefault"] = true,
                ["DhcpOptionsId"] = "dopt-00000001", ["InstanceTenancy"] = "default",
                ["OwnerId"] = accountId, ["DefaultNetworkAclId"] = DefaultAclId,
                ["DefaultSecurityGroupId"] = DefaultSgId, ["MainRouteTableId"] = DefaultRtbId,
            };
        }

        var defaultSubnets = new[]
        {
            (DefaultSubnetId, "172.31.0.0/20", Region + "a"),
            (DefaultSubnetIdB, "172.31.16.0/20", Region + "b"),
            (DefaultSubnetIdC, "172.31.32.0/20", Region + "c"),
        };

        foreach (var (id, cidr, az) in defaultSubnets)
        {
            if (!_subnets.ContainsKey(id))
            {
                _subnets[id] = new Dictionary<string, object>
                {
                    ["SubnetId"] = id, ["VpcId"] = DefaultVpcId, ["CidrBlock"] = cidr,
                    ["AvailabilityZone"] = az, ["AvailableIpAddressCount"] = 4091,
                    ["State"] = "available", ["DefaultForAz"] = true,
                    ["MapPublicIpOnLaunch"] = true, ["OwnerId"] = accountId,
                };
            }
        }

        if (!_securityGroups.ContainsKey(DefaultSgId))
        {
            _securityGroups[DefaultSgId] = new Dictionary<string, object>
            {
                ["GroupId"] = DefaultSgId, ["GroupName"] = "default",
                ["Description"] = "default VPC security group",
                ["VpcId"] = DefaultVpcId, ["OwnerId"] = accountId,
                ["IpPermissions"] = new List<Dictionary<string, object>>(),
                ["IpPermissionsEgress"] = new List<Dictionary<string, object>>
                {
                    new() { ["IpProtocol"] = "-1",
                        ["IpRanges"] = new List<Dictionary<string, string>> { new() { ["CidrIp"] = "0.0.0.0/0" } },
                        ["Ipv6Ranges"] = new List<object>(), ["PrefixListIds"] = new List<object>(),
                        ["UserIdGroupPairs"] = new List<object>() },
                },
            };
        }

        if (!_internetGateways.ContainsKey(DefaultIgwId))
        {
            _internetGateways[DefaultIgwId] = new Dictionary<string, object>
            {
                ["InternetGatewayId"] = DefaultIgwId, ["OwnerId"] = accountId,
                ["Attachments"] = new List<Dictionary<string, string>>
                    { new() { ["VpcId"] = DefaultVpcId, ["State"] = "available" } },
            };
        }

        if (!_routeTables.ContainsKey(DefaultRtbId))
        {
            _routeTables[DefaultRtbId] = new Dictionary<string, object>
            {
                ["RouteTableId"] = DefaultRtbId, ["VpcId"] = DefaultVpcId, ["OwnerId"] = accountId,
                ["Routes"] = new List<Dictionary<string, string>>
                {
                    new() { ["DestinationCidrBlock"] = "172.31.0.0/16", ["GatewayId"] = "local",
                        ["State"] = "active", ["Origin"] = "CreateRouteTable" },
                },
                ["Associations"] = new List<Dictionary<string, object>>
                {
                    new() { ["RouteTableAssociationId"] = "rtbassoc-00000001",
                        ["RouteTableId"] = DefaultRtbId, ["Main"] = true },
                },
            };
        }
    }

    public Task<ServiceResponse> HandleAsync(ServiceRequest request)
    {
        var p = ParseParams(request);
        var action = P(p, "Action");

        lock (_lock) { InitDefaults(); }

        ServiceResponse response;
        lock (_lock) { response = DispatchAction(action, p); }

        return Task.FromResult(response);
    }

    public void Reset()
    {
        lock (_lock)
        {
            _instances.Clear(); _securityGroups.Clear(); _keyPairs.Clear();
            _vpcs.Clear(); _subnets.Clear(); _internetGateways.Clear();
            _addresses.Clear(); _tags.Clear(); _routeTables.Clear();
            _networkInterfaces.Clear(); _vpcEndpoints.Clear(); _volumes.Clear();
            _snapshots.Clear(); _natGateways.Clear(); _networkAcls.Clear();
            _flowLogs.Clear(); _vpcPeering.Clear(); _dhcpOptions.Clear();
            _egressIgws.Clear(); _prefixLists.Clear(); _vpnGateways.Clear();
            _customerGateways.Clear(); _launchTemplates.Clear();
            InitDefaults();
        }
    }

    public JsonElement? GetState() => null;
    public void RestoreState(JsonElement state) { }

    // ── Action dispatch ────────────────────────────────────────────────────────

    private ServiceResponse DispatchAction(string action, Dictionary<string, string[]> p)
    {
        return action switch
        {
            "RunInstances" => RunInstances(p),
            "DescribeInstances" => DescribeInstances(p),
            "TerminateInstances" => TerminateInstances(p),
            "StopInstances" => StopInstances(p),
            "StartInstances" => StartInstances(p),
            "RebootInstances" => RebootInstances(p),
            "DescribeInstanceAttribute" => DescribeInstanceAttribute(p),
            "DescribeInstanceTypes" => DescribeInstanceTypes(p),
            "DescribeInstanceCreditSpecifications" => DescribeInstanceCreditSpecifications(p),
            "DescribeSpotInstanceRequests" => Xml(200, "DescribeSpotInstanceRequestsResponse", "<spotInstanceRequestSet/>"),
            "DescribeCapacityReservations" => Xml(200, "DescribeCapacityReservationsResponse", "<capacityReservationSet/>"),
            "DescribeImages" => DescribeImages(p),
            "CreateSecurityGroup" => CreateSecurityGroup(p),
            "DeleteSecurityGroup" => DeleteSecurityGroup(p),
            "DescribeSecurityGroups" => DescribeSecurityGroups(p),
            "AuthorizeSecurityGroupIngress" => AuthorizeSgIngress(p),
            "RevokeSecurityGroupIngress" => RevokeSgIngress(p),
            "AuthorizeSecurityGroupEgress" => AuthorizeSgEgress(p),
            "RevokeSecurityGroupEgress" => RevokeSgEgress(p),
            "DescribeSecurityGroupRules" => DescribeSecurityGroupRules(p),
            "CreateKeyPair" => CreateKeyPair(p),
            "DeleteKeyPair" => DeleteKeyPair(p),
            "DescribeKeyPairs" => DescribeKeyPairs(p),
            "ImportKeyPair" => ImportKeyPair(p),
            "DescribeVpcs" => DescribeVpcs(p),
            "CreateVpc" => CreateVpc(p),
            "DeleteVpc" => DeleteVpc(p),
            "DescribeVpcAttribute" => DescribeVpcAttribute(p),
            "ModifyVpcAttribute" => ModifyVpcAttribute(p),
            "DescribeSubnets" => DescribeSubnets(p),
            "CreateSubnet" => CreateSubnet(p),
            "DeleteSubnet" => DeleteSubnet(p),
            "ModifySubnetAttribute" => ModifySubnetAttribute(p),
            "CreateInternetGateway" => CreateInternetGateway(p),
            "DeleteInternetGateway" => DeleteInternetGateway(p),
            "DescribeInternetGateways" => DescribeInternetGateways(p),
            "AttachInternetGateway" => AttachInternetGateway(p),
            "DetachInternetGateway" => DetachInternetGateway(p),
            "DescribeAvailabilityZones" => DescribeAvailabilityZones(p),
            "AllocateAddress" => AllocateAddress(p),
            "ReleaseAddress" => ReleaseAddress(p),
            "AssociateAddress" => AssociateAddress(p),
            "DisassociateAddress" => DisassociateAddress(p),
            "DescribeAddresses" => DescribeAddresses(p),
            "DescribeAddressesAttribute" => DescribeAddressesAttribute(p),
            "CreateTags" => CreateTags(p),
            "DeleteTags" => DeleteTags(p),
            "DescribeTags" => DescribeTags(p),
            "CreateRouteTable" => CreateRouteTable(p),
            "DeleteRouteTable" => DeleteRouteTable(p),
            "DescribeRouteTables" => DescribeRouteTables(p),
            "AssociateRouteTable" => AssociateRouteTable(p),
            "DisassociateRouteTable" => DisassociateRouteTable(p),
            "ReplaceRouteTableAssociation" => ReplaceRouteTableAssociation(p),
            "CreateRoute" => CreateRoute(p),
            "ReplaceRoute" => ReplaceRoute(p),
            "DeleteRoute" => DeleteRoute(p),
            "CreateNetworkInterface" => CreateNetworkInterface(p),
            "DeleteNetworkInterface" => DeleteNetworkInterface(p),
            "DescribeNetworkInterfaces" => DescribeNetworkInterfaces(p),
            "AttachNetworkInterface" => AttachNetworkInterface(p),
            "DetachNetworkInterface" => DetachNetworkInterface(p),
            "CreateVpcEndpoint" => CreateVpcEndpoint(p),
            "DeleteVpcEndpoints" => DeleteVpcEndpoints(p),
            "DescribeVpcEndpoints" => DescribeVpcEndpoints(p),
            "ModifyVpcEndpoint" => ModifyVpcEndpoint(p),
            "DescribePrefixLists" => DescribePrefixLists(p),
            "CreateVolume" => CreateVolume(p),
            "DeleteVolume" => DeleteVolume(p),
            "DescribeVolumes" => DescribeVolumes(p),
            "DescribeVolumeStatus" => DescribeVolumeStatus(p),
            "AttachVolume" => AttachVolume(p),
            "DetachVolume" => DetachVolume(p),
            "ModifyVolume" => ModifyVolume(p),
            "DescribeVolumesModifications" => DescribeVolumesModifications(p),
            "EnableVolumeIO" => Xml(200, "EnableVolumeIOResponse", "<return>true</return>"),
            "ModifyVolumeAttribute" => Xml(200, "ModifyVolumeAttributeResponse", "<return>true</return>"),
            "DescribeVolumeAttribute" => DescribeVolumeAttribute(p),
            "CreateSnapshot" => CreateSnapshot(p),
            "DeleteSnapshot" => DeleteSnapshot(p),
            "DescribeSnapshots" => DescribeSnapshots(p),
            "CopySnapshot" => CopySnapshot(p),
            "ModifySnapshotAttribute" => ModifySnapshotAttribute(p),
            "DescribeSnapshotAttribute" => DescribeSnapshotAttribute(p),
            "CreateNatGateway" => CreateNatGateway(p),
            "DescribeNatGateways" => DescribeNatGateways(p),
            "DeleteNatGateway" => DeleteNatGateway(p),
            "CreateNetworkAcl" => CreateNetworkAcl(p),
            "DescribeNetworkAcls" => DescribeNetworkAcls(p),
            "DeleteNetworkAcl" => DeleteNetworkAcl(p),
            "CreateNetworkAclEntry" => CreateNetworkAclEntry(p),
            "DeleteNetworkAclEntry" => DeleteNetworkAclEntry(p),
            "ReplaceNetworkAclEntry" => ReplaceNetworkAclEntry(p),
            "ReplaceNetworkAclAssociation" => ReplaceNetworkAclAssociation(p),
            "CreateFlowLogs" => CreateFlowLogs(p),
            "DescribeFlowLogs" => DescribeFlowLogs(p),
            "DeleteFlowLogs" => DeleteFlowLogs(p),
            "CreateVpcPeeringConnection" => CreateVpcPeeringConnection(p),
            "AcceptVpcPeeringConnection" => AcceptVpcPeeringConnection(p),
            "DescribeVpcPeeringConnections" => DescribeVpcPeeringConnections(p),
            "DeleteVpcPeeringConnection" => DeleteVpcPeeringConnection(p),
            "CreateDhcpOptions" => CreateDhcpOptions(p),
            "AssociateDhcpOptions" => AssociateDhcpOptions(p),
            "DescribeDhcpOptions" => DescribeDhcpOptions(p),
            "DeleteDhcpOptions" => DeleteDhcpOptions(p),
            "CreateEgressOnlyInternetGateway" => CreateEgressOnlyInternetGateway(p),
            "DescribeEgressOnlyInternetGateways" => DescribeEgressOnlyInternetGateways(p),
            "DeleteEgressOnlyInternetGateway" => DeleteEgressOnlyInternetGateway(p),
            "CreateManagedPrefixList" => CreateManagedPrefixList(p),
            "DescribeManagedPrefixLists" => DescribeManagedPrefixLists(p),
            "GetManagedPrefixListEntries" => GetManagedPrefixListEntries(p),
            "ModifyManagedPrefixList" => ModifyManagedPrefixList(p),
            "DeleteManagedPrefixList" => DeleteManagedPrefixList(p),
            "CreateVpnGateway" => CreateVpnGateway(p),
            "DescribeVpnGateways" => DescribeVpnGateways(p),
            "AttachVpnGateway" => AttachVpnGateway(p),
            "DetachVpnGateway" => DetachVpnGateway(p),
            "DeleteVpnGateway" => DeleteVpnGateway(p),
            "EnableVgwRoutePropagation" => EnableVgwRoutePropagation(p),
            "DisableVgwRoutePropagation" => DisableVgwRoutePropagation(p),
            "CreateCustomerGateway" => CreateCustomerGateway(p),
            "DescribeCustomerGateways" => DescribeCustomerGateways(p),
            "DeleteCustomerGateway" => DeleteCustomerGateway(p),
            "CreateLaunchTemplate" => CreateLaunchTemplate(p),
            "CreateLaunchTemplateVersion" => CreateLaunchTemplateVersion(p),
            "DescribeLaunchTemplates" => DescribeLaunchTemplates(p),
            "DescribeLaunchTemplateVersions" => DescribeLaunchTemplateVersions(p),
            "ModifyLaunchTemplate" => ModifyLaunchTemplate(p),
            "DeleteLaunchTemplate" => DeleteLaunchTemplate(p),
            "DescribeAccountAttributes" => Xml(200, "DescribeAccountAttributesResponse", "<accountAttributeSet/>"),
            "DescribeInstanceMaintenanceOptions" => Xml(200, "DescribeInstanceMaintenanceOptionsResponse", "<instanceMaintenanceOptionSet/>"),
            "DescribeInstanceTopology" => Xml(200, "DescribeInstanceTopologyResponse", "<instanceSet/>"),
            _ => Error("InvalidAction", $"Unknown EC2 action: {action}", 400),
        };
    }
    // ── Instances ──────────────────────────────────────────────────────────────

    private ServiceResponse RunInstances(Dictionary<string, string[]> p)
    {
        var imageId = P(p, "ImageId", "ami-00000000");
        var instanceType = P(p, "InstanceType", "t2.micro");
        var minCount = int.Parse(P(p, "MinCount", "1"));
        var maxCount = int.Parse(P(p, "MaxCount", "1"));
        var keyName = P(p, "KeyName");
        var subnetId = P(p, "SubnetId", DefaultSubnetId);

        var sgIds = ParseMemberList(p, "SecurityGroupId");
        if (sgIds.Count == 0) sgIds.Add(DefaultSgId);

        var now = NowTs();
        var sb = new StringBuilder();
        var count = Math.Max(1, Math.Min(minCount, maxCount));

        for (var idx = 0; idx < count; idx++)
        {
            var instanceId = NewId("i-");
            var privateIp = RandomIp("10.0");
            var vpcId = DefaultVpcId;
            if (_subnets.TryGetValue(subnetId, out var subnetRec))
                vpcId = (string)subnetRec["VpcId"];

            var az = _subnets.TryGetValue(subnetId, out var subnetAzRec)
                ? (string)subnetAzRec["AvailabilityZone"]
                : Region + "a";

            _instances[instanceId] = new Dictionary<string, object>
            {
                ["InstanceId"] = instanceId, ["ImageId"] = imageId,
                ["InstanceType"] = instanceType, ["KeyName"] = keyName,
                ["State"] = new Dictionary<string, object> { ["Code"] = 16, ["Name"] = "running" },
                ["SubnetId"] = subnetId, ["VpcId"] = vpcId,
                ["PrivateIpAddress"] = privateIp,
                ["PublicIpAddress"] = RandomIp("54."),
                ["PrivateDnsName"] = $"ip-{privateIp.Replace('.', '-')}.ec2.internal",
                ["PublicDnsName"] = $"ec2-{privateIp.Replace('.', '-')}.compute-1.amazonaws.com",
                ["SecurityGroups"] = sgIds.Select(sg => new Dictionary<string, string>
                {
                    ["GroupId"] = sg,
                    ["GroupName"] = _securityGroups.TryGetValue(sg, out var sgRec) ? (string)sgRec["GroupName"] : sg,
                }).ToList(),
                ["LaunchTime"] = now,
                ["Placement"] = new Dictionary<string, object> { ["AvailabilityZone"] = az, ["Tenancy"] = "default" },
                ["Monitoring"] = new Dictionary<string, object> { ["State"] = "disabled" },
                ["Architecture"] = "x86_64",
                ["RootDeviceType"] = "ebs",
                ["RootDeviceName"] = "/dev/xvda",
                ["Virtualization"] = "hvm",
                ["Hypervisor"] = "xen",
                ["AmiLaunchIndex"] = idx,
            };

            sb.Append(InstanceXml(_instances[instanceId]));
        }

        var reservationId = "r-" + Guid.NewGuid().ToString("N")[..17];
        var inner = $"<instancesSet>{sb}</instancesSet><reservationId>{reservationId}</reservationId><ownerId>{AccountContext.GetAccountId()}</ownerId><groupSet/>";
        return Xml(200, "RunInstancesResponse", inner);
    }

    private ServiceResponse DescribeInstances(Dictionary<string, string[]> p)
    {
        var filterIds = ParseMemberList(p, "InstanceId");
        var filters = ParseFilters(p);
        var sb = new StringBuilder();

        foreach (var inst in _instances.Values.ToList())
        {
            if (filterIds.Count > 0 && !filterIds.Contains((string)inst["InstanceId"])) continue;
            if (!MatchesInstanceFilters(inst, filters)) continue;

            var iid = (string)inst["InstanceId"];
            sb.Append($"<item><reservationId>r-{iid[2..]}</reservationId><ownerId>{AccountContext.GetAccountId()}</ownerId><groupSet/><instancesSet>{InstanceXml(inst)}</instancesSet></item>");
        }

        return Xml(200, "DescribeInstancesResponse", $"<reservationSet>{sb}</reservationSet>");
    }

    private ServiceResponse TerminateInstances(Dictionary<string, string[]> p)
    {
        var ids = ParseMemberList(p, "InstanceId");
        var sb = new StringBuilder();
        foreach (var iid in ids)
        {
            if (_instances.TryGetValue(iid, out var inst))
            {
                var state = (Dictionary<string, object>)inst["State"];
                var prevCode = state["Code"];
                var prevName = state["Name"];
                state["Code"] = 48;
                state["Name"] = "terminated";
                sb.Append($"<item><instanceId>{iid}</instanceId><previousState><code>{prevCode}</code><name>{prevName}</name></previousState><currentState><code>48</code><name>terminated</name></currentState></item>");
            }
        }
        return Xml(200, "TerminateInstancesResponse", $"<instancesSet>{sb}</instancesSet>");
    }

    private ServiceResponse StopInstances(Dictionary<string, string[]> p)
    {
        var ids = ParseMemberList(p, "InstanceId");
        var sb = new StringBuilder();
        foreach (var iid in ids)
        {
            if (_instances.TryGetValue(iid, out var inst))
            {
                var state = (Dictionary<string, object>)inst["State"];
                var prevCode = state["Code"];
                var prevName = state["Name"];
                state["Code"] = 80;
                state["Name"] = "stopped";
                sb.Append($"<item><instanceId>{iid}</instanceId><previousState><code>{prevCode}</code><name>{prevName}</name></previousState><currentState><code>80</code><name>stopped</name></currentState></item>");
            }
        }
        return Xml(200, "StopInstancesResponse", $"<instancesSet>{sb}</instancesSet>");
    }

    private ServiceResponse StartInstances(Dictionary<string, string[]> p)
    {
        var ids = ParseMemberList(p, "InstanceId");
        var sb = new StringBuilder();
        foreach (var iid in ids)
        {
            if (_instances.TryGetValue(iid, out var inst))
            {
                var state = (Dictionary<string, object>)inst["State"];
                var prevCode = state["Code"];
                var prevName = state["Name"];
                state["Code"] = 16;
                state["Name"] = "running";
                sb.Append($"<item><instanceId>{iid}</instanceId><previousState><code>{prevCode}</code><name>{prevName}</name></previousState><currentState><code>16</code><name>running</name></currentState></item>");
            }
        }
        return Xml(200, "StartInstancesResponse", $"<instancesSet>{sb}</instancesSet>");
    }

    private ServiceResponse RebootInstances(Dictionary<string, string[]> p) =>
        Xml(200, "RebootInstancesResponse", "<return>true</return>");

    private ServiceResponse DescribeInstanceAttribute(Dictionary<string, string[]> p)
    {
        var instanceId = P(p, "InstanceId");
        var attribute = P(p, "Attribute");
        if (!_instances.TryGetValue(instanceId, out var inst))
            return Error("InvalidInstanceID.NotFound", $"The instance ID '{instanceId}' does not exist", 400);

        var valueXml = attribute switch
        {
            "instanceType" => $"<instanceType><value>{inst.GetValueOrDefault("InstanceType", "t2.micro")}</value></instanceType>",
            "instanceInitiatedShutdownBehavior" => "<instanceInitiatedShutdownBehavior><value>stop</value></instanceInitiatedShutdownBehavior>",
            "disableApiTermination" => "<disableApiTermination><value>false</value></disableApiTermination>",
            "userData" => "<userData/>",
            "rootDeviceName" => "<rootDeviceName><value>/dev/xvda</value></rootDeviceName>",
            "sourceDestCheck" => "<sourceDestCheck><value>true</value></sourceDestCheck>",
            "ebsOptimized" => "<ebsOptimized><value>false</value></ebsOptimized>",
            _ => $"<{attribute}/>",
        };

        return Xml(200, "DescribeInstanceAttributeResponse", $"<instanceId>{instanceId}</instanceId>{valueXml}");
    }

    private ServiceResponse DescribeInstanceTypes(Dictionary<string, string[]> p)
    {
        var requested = ParseMemberList(p, "InstanceType");
        var allTypes = requested.Count > 0 ? requested : new List<string>
        {
            "t2.micro", "t2.small", "t2.medium", "t2.large",
            "t3.micro", "t3.small", "t3.medium", "t3.large",
            "m5.large", "m5.xlarge", "c5.large", "c5.xlarge",
        };

        var sb = new StringBuilder();
        foreach (var itype in allTypes)
        {
            var family = itype.Split('.')[0];
            var vcpus = itype.Contains("micro") ? 2 : itype.Contains("small") ? 4 : 8;
            var memMib = itype.Contains("micro") ? 1024 : itype.Contains("small") ? 2048 : 4096;
            var burstable = family is "t2" or "t3" or "t4g" ? "true" : "false";
            sb.Append($"<item><instanceType>{itype}</instanceType><currentGeneration>true</currentGeneration><freeTierEligible>{(itype == "t2.micro" ? "true" : "false")}</freeTierEligible><supportedUsageClasses><item>on-demand</item><item>spot</item></supportedUsageClasses><supportedRootDeviceTypes><item>ebs</item></supportedRootDeviceTypes><supportedVirtualizationTypes><item>hvm</item></supportedVirtualizationTypes><bareMetal>false</bareMetal><hypervisor>xen</hypervisor><processorInfo><supportedArchitectures><item>x86_64</item></supportedArchitectures><sustainedClockSpeedInGhz>2.5</sustainedClockSpeedInGhz></processorInfo><vCpuInfo><defaultVCpus>{vcpus}</defaultVCpus><defaultCores>{vcpus}</defaultCores><defaultThreadsPerCore>1</defaultThreadsPerCore></vCpuInfo><memoryInfo><sizeInMiB>{memMib}</sizeInMiB></memoryInfo><instanceStorageSupported>false</instanceStorageSupported><ebsInfo><ebsOptimizedSupport>unsupported</ebsOptimizedSupport><encryptionSupport>supported</encryptionSupport><ebsOptimizedInfo><baselineBandwidthInMbps>256</baselineBandwidthInMbps><baselineThroughputInMBps>32.0</baselineThroughputInMBps><baselineIops>2000</baselineIops><maximumBandwidthInMbps>256</maximumBandwidthInMbps><maximumThroughputInMBps>32.0</maximumThroughputInMBps><maximumIops>2000</maximumIops></ebsOptimizedInfo><nvmeSupport>unsupported</nvmeSupport></ebsInfo><networkInfo><networkPerformance>Low to Moderate</networkPerformance><maximumNetworkInterfaces>2</maximumNetworkInterfaces><maximumNetworkCards>1</maximumNetworkCards><defaultNetworkCardIndex>0</defaultNetworkCardIndex><networkCards><item><networkCardIndex>0</networkCardIndex><networkPerformance>Low to Moderate</networkPerformance><maximumNetworkInterfaces>2</maximumNetworkInterfaces><baselineBandwidthInGbps>0.1</baselineBandwidthInGbps><peakBandwidthInGbps>0.5</peakBandwidthInGbps></item></networkCards><ipv4AddressesPerInterface>2</ipv4AddressesPerInterface><ipv6AddressesPerInterface>2</ipv6AddressesPerInterface><ipv6Supported>true</ipv6Supported><enaSupport>required</enaSupport><efaSupported>false</efaSupported></networkInfo><placementGroupInfo><supportedStrategies><item>partition</item><item>spread</item></supportedStrategies></placementGroupInfo><hibernationSupported>false</hibernationSupported><burstablePerformanceSupported>{burstable}</burstablePerformanceSupported><dedicatedHostsSupported>false</dedicatedHostsSupported><autoRecoverySupported>true</autoRecoverySupported></item>");
        }

        return Xml(200, "DescribeInstanceTypesResponse", $"<instanceTypeSet>{sb}</instanceTypeSet>");
    }

    private ServiceResponse DescribeInstanceCreditSpecifications(Dictionary<string, string[]> p)
    {
        var ids = ParseMemberList(p, "InstanceId");
        var sb = new StringBuilder();
        foreach (var iid in ids.Count > 0 ? ids : _instances.Keys.ToList())
            sb.Append($"<item><instanceId>{iid}</instanceId><cpuCredits>standard</cpuCredits></item>");
        return Xml(200, "DescribeInstanceCreditSpecificationsResponse", $"<instanceCreditSpecificationSet>{sb}</instanceCreditSpecificationSet>");
    }

    // ── Images ─────────────────────────────────────────────────────────────────

    private static readonly (string Id, string Name, string Desc)[] StubAmis =
    [
        ("ami-0abcdef1234567890", "amzn2-ami-hvm-2.0.20231116.0-x86_64-gp2", "Amazon Linux 2"),
        ("ami-0123456789abcdef0", "ubuntu/images/hvm-ssd/ubuntu-22.04-amd64-server", "Ubuntu 22.04"),
        ("ami-0fedcba9876543210", "Windows_Server-2022-English-Full-Base", "Windows Server 2022"),
    ];

    private ServiceResponse DescribeImages(Dictionary<string, string[]> p)
    {
        var filterIds = ParseMemberList(p, "ImageId");
        var sb = new StringBuilder();
        foreach (var (amiId, name, desc) in StubAmis)
        {
            if (filterIds.Count > 0 && !filterIds.Contains(amiId)) continue;
            sb.Append($"<item><imageId>{amiId}</imageId><imageLocation>{name}</imageLocation><imageState>available</imageState><imageOwnerId>{AccountContext.GetAccountId()}</imageOwnerId><isPublic>true</isPublic><architecture>x86_64</architecture><imageType>machine</imageType><name>{name}</name><description>{desc}</description><rootDeviceType>ebs</rootDeviceType><virtualizationType>hvm</virtualizationType><hypervisor>xen</hypervisor></item>");
        }
        return Xml(200, "DescribeImagesResponse", $"<imagesSet>{sb}</imagesSet>");
    }

    // ── Security Groups ────────────────────────────────────────────────────────

    private ServiceResponse CreateSecurityGroup(Dictionary<string, string[]> p)
    {
        var name = P(p, "GroupName");
        var desc = P(p, "Description", name);
        var vpcId = P(p, "VpcId", DefaultVpcId);
        if (string.IsNullOrEmpty(name))
            return Error("MissingParameter", "GroupName is required", 400);

        foreach (var sg in _securityGroups.Values)
        {
            if ((string)sg["GroupName"] == name && (string)sg["VpcId"] == vpcId)
                return Error("InvalidGroup.Duplicate", $"The security group '{name}' already exists", 400);
        }

        var sgId = NewId("sg-");
        _securityGroups[sgId] = new Dictionary<string, object>
        {
            ["GroupId"] = sgId, ["GroupName"] = name, ["Description"] = desc,
            ["VpcId"] = vpcId, ["OwnerId"] = AccountContext.GetAccountId(),
            ["IpPermissions"] = new List<Dictionary<string, object>>(),
            ["IpPermissionsEgress"] = new List<Dictionary<string, object>>
            {
                new() { ["IpProtocol"] = "-1",
                    ["IpRanges"] = new List<Dictionary<string, string>> { new() { ["CidrIp"] = "0.0.0.0/0" } },
                    ["Ipv6Ranges"] = new List<object>(), ["PrefixListIds"] = new List<object>(),
                    ["UserIdGroupPairs"] = new List<object>() },
            },
        };
        return Xml(200, "CreateSecurityGroupResponse", $"<return>true</return><groupId>{sgId}</groupId>");
    }

    private ServiceResponse DeleteSecurityGroup(Dictionary<string, string[]> p)
    {
        var sgId = P(p, "GroupId");
        if (!string.IsNullOrEmpty(sgId) && _securityGroups.ContainsKey(sgId))
            _securityGroups.TryRemove(sgId, out _);
        else if (!string.IsNullOrEmpty(sgId))
            return Error("InvalidGroup.NotFound", $"The security group '{sgId}' does not exist", 400);
        return Xml(200, "DeleteSecurityGroupResponse", "<return>true</return>");
    }

    private ServiceResponse DescribeSecurityGroups(Dictionary<string, string[]> p)
    {
        var filterIds = ParseMemberList(p, "GroupId");
        var filters = ParseFilters(p);
        var sb = new StringBuilder();
        foreach (var sg in _securityGroups.Values.ToList())
        {
            if (filterIds.Count > 0 && !filterIds.Contains((string)sg["GroupId"])) continue;
            var vpcFilter = filters.GetValueOrDefault("vpc-id");
            if (vpcFilter != null && !vpcFilter.Contains((string)sg["VpcId"])) continue;
            var nameFilter = filters.GetValueOrDefault("group-name");
            if (nameFilter != null && !nameFilter.Contains((string)sg["GroupName"])) continue;
            sb.Append(SgXml(sg));
        }
        return Xml(200, "DescribeSecurityGroupsResponse", $"<securityGroupInfo>{sb}</securityGroupInfo>");
    }

    private ServiceResponse AuthorizeSgIngress(Dictionary<string, string[]> p)
    {
        var sgId = P(p, "GroupId");
        if (!_securityGroups.TryGetValue(sgId, out var sg))
            return Error("InvalidGroup.NotFound", $"Security group {sgId} not found", 400);
        var rules = ParseIpPermissions(p, "IpPermissions");
        var existing = (List<Dictionary<string, object>>)sg["IpPermissions"];
        foreach (var r in rules) existing.Add(r);
        return Xml(200, "AuthorizeSecurityGroupIngressResponse", "<return>true</return>");
    }

    private ServiceResponse RevokeSgIngress(Dictionary<string, string[]> p)
    {
        var sgId = P(p, "GroupId");
        if (!_securityGroups.TryGetValue(sgId, out var sg))
            return Error("InvalidGroup.NotFound", $"Security group {sgId} not found", 400);
        var rules = ParseIpPermissions(p, "IpPermissions");
        var existing = (List<Dictionary<string, object>>)sg["IpPermissions"];
        foreach (var r in rules)
            existing.RemoveAll(e => PermEquals(e, r));
        return Xml(200, "RevokeSecurityGroupIngressResponse", "<return>true</return>");
    }

    private ServiceResponse AuthorizeSgEgress(Dictionary<string, string[]> p)
    {
        var sgId = P(p, "GroupId");
        if (!_securityGroups.TryGetValue(sgId, out var sg))
            return Error("InvalidGroup.NotFound", $"Security group {sgId} not found", 400);
        var rules = ParseIpPermissions(p, "IpPermissions");
        var existing = (List<Dictionary<string, object>>)sg["IpPermissionsEgress"];
        foreach (var r in rules) existing.Add(r);
        return Xml(200, "AuthorizeSecurityGroupEgressResponse", "<return>true</return>");
    }

    private ServiceResponse RevokeSgEgress(Dictionary<string, string[]> p)
    {
        var sgId = P(p, "GroupId");
        if (!_securityGroups.TryGetValue(sgId, out var sg))
            return Error("InvalidGroup.NotFound", $"Security group {sgId} not found", 400);
        var rules = ParseIpPermissions(p, "IpPermissions");
        var existing = (List<Dictionary<string, object>>)sg["IpPermissionsEgress"];
        foreach (var r in rules)
            existing.RemoveAll(e => PermEquals(e, r));
        return Xml(200, "RevokeSecurityGroupEgressResponse", "<return>true</return>");
    }

    private ServiceResponse DescribeSecurityGroupRules(Dictionary<string, string[]> p)
    {
        var sgIds = ParseMemberList(p, "SecurityGroupId");
        var filters = ParseFilters(p);
        if (filters.TryGetValue("group-id", out var gidFilter)) sgIds = gidFilter;
        var sb = new StringBuilder();
        foreach (var sgId in sgIds)
        {
            if (!_securityGroups.TryGetValue(sgId, out var sg)) continue;
            var ingress = (List<Dictionary<string, object>>)sg["IpPermissions"];
            for (var i = 0; i < ingress.Count; i++)
            {
                var rule = ingress[i];
                foreach (var cidr in (List<Dictionary<string, string>>)rule.GetValueOrDefault("IpRanges", new List<Dictionary<string, string>>()))
                    sb.Append($"<item><securityGroupRuleId>sgr-{sgId[3..]}-ingress-{i}</securityGroupRuleId><groupId>{sgId}</groupId><groupOwnerId>{AccountContext.GetAccountId()}</groupOwnerId><isEgress>false</isEgress><ipProtocol>{rule.GetValueOrDefault("IpProtocol", "-1")}</ipProtocol><fromPort>{rule.GetValueOrDefault("FromPort", -1)}</fromPort><toPort>{rule.GetValueOrDefault("ToPort", -1)}</toPort><cidrIpv4>{cidr.GetValueOrDefault("CidrIp", "")}</cidrIpv4></item>");
            }
            var egress = (List<Dictionary<string, object>>)sg["IpPermissionsEgress"];
            for (var i = 0; i < egress.Count; i++)
            {
                var rule = egress[i];
                foreach (var cidr in (List<Dictionary<string, string>>)rule.GetValueOrDefault("IpRanges", new List<Dictionary<string, string>>()))
                    sb.Append($"<item><securityGroupRuleId>sgr-{sgId[3..]}-egress-{i}</securityGroupRuleId><groupId>{sgId}</groupId><groupOwnerId>{AccountContext.GetAccountId()}</groupOwnerId><isEgress>true</isEgress><ipProtocol>{rule.GetValueOrDefault("IpProtocol", "-1")}</ipProtocol><fromPort>{rule.GetValueOrDefault("FromPort", -1)}</fromPort><toPort>{rule.GetValueOrDefault("ToPort", -1)}</toPort><cidrIpv4>{cidr.GetValueOrDefault("CidrIp", "")}</cidrIpv4></item>");
            }
        }
        return Xml(200, "DescribeSecurityGroupRulesResponse", $"<securityGroupRuleSet>{sb}</securityGroupRuleSet>");
    }

    // ── Key Pairs ──────────────────────────────────────────────────────────────

    private ServiceResponse CreateKeyPair(Dictionary<string, string[]> p)
    {
        var name = P(p, "KeyName");
        if (string.IsNullOrEmpty(name)) return Error("MissingParameter", "KeyName is required", 400);
        if (_keyPairs.ContainsKey(name)) return Error("InvalidKeyPair.Duplicate", $"The key pair '{name}' already exists", 400);
        var fingerprint = RandomFingerprint();
        var keyPairId = NewId("key-");
        _keyPairs[name] = new Dictionary<string, object>
        {
            ["KeyName"] = name, ["KeyFingerprint"] = fingerprint, ["KeyPairId"] = keyPairId,
        };
        var material = "-----BEGIN RSA PRIVATE KEY-----\nMIIEpAIBAAKCAQEA(stub)\n-----END RSA PRIVATE KEY-----";
        return Xml(200, "CreateKeyPairResponse", $"<keyName>{name}</keyName><keyFingerprint>{fingerprint}</keyFingerprint><keyMaterial>{material}</keyMaterial><keyPairId>{keyPairId}</keyPairId>");
    }

    private ServiceResponse DeleteKeyPair(Dictionary<string, string[]> p)
    {
        var name = P(p, "KeyName");
        _keyPairs.TryRemove(name, out _);
        return Xml(200, "DeleteKeyPairResponse", "<return>true</return>");
    }

    private ServiceResponse DescribeKeyPairs(Dictionary<string, string[]> p)
    {
        var filterNames = ParseMemberList(p, "KeyName");
        var sb = new StringBuilder();
        foreach (var kp in _keyPairs.Values.ToList())
        {
            if (filterNames.Count > 0 && !filterNames.Contains((string)kp["KeyName"])) continue;
            sb.Append($"<item><keyName>{kp["KeyName"]}</keyName><keyFingerprint>{kp["KeyFingerprint"]}</keyFingerprint><keyPairId>{kp["KeyPairId"]}</keyPairId></item>");
        }
        return Xml(200, "DescribeKeyPairsResponse", $"<keySet>{sb}</keySet>");
    }

    private ServiceResponse ImportKeyPair(Dictionary<string, string[]> p)
    {
        var name = P(p, "KeyName");
        if (string.IsNullOrEmpty(name)) return Error("MissingParameter", "KeyName is required", 400);
        var fingerprint = RandomFingerprint();
        var keyPairId = NewId("key-");
        _keyPairs[name] = new Dictionary<string, object>
        {
            ["KeyName"] = name, ["KeyFingerprint"] = fingerprint, ["KeyPairId"] = keyPairId,
        };
        return Xml(200, "ImportKeyPairResponse", $"<keyName>{name}</keyName><keyFingerprint>{fingerprint}</keyFingerprint><keyPairId>{keyPairId}</keyPairId>");
    }
    // ── VPCs ───────────────────────────────────────────────────────────────────

    private ServiceResponse DescribeVpcs(Dictionary<string, string[]> p)
    {
        var filterIds = ParseMemberList(p, "VpcId");
        var sb = new StringBuilder();
        foreach (var vpc in _vpcs.Values.ToList())
        {
            if (filterIds.Count > 0 && !filterIds.Contains((string)vpc["VpcId"])) continue;
            sb.Append(VpcXml(vpc));
        }
        return Xml(200, "DescribeVpcsResponse", $"<vpcSet>{sb}</vpcSet>");
    }

    private ServiceResponse CreateVpc(Dictionary<string, string[]> p)
    {
        var cidr = P(p, "CidrBlock", "10.0.0.0/16");
        var vpcId = NewId("vpc-");
        var accountId = AccountContext.GetAccountId();

        var aclId = NewId("acl-");
        _networkAcls[aclId] = new Dictionary<string, object>
        {
            ["NetworkAclId"] = aclId, ["VpcId"] = vpcId, ["IsDefault"] = true,
            ["Entries"] = new List<Dictionary<string, object>>
            {
                new() { ["RuleNumber"] = 100, ["Protocol"] = "-1", ["RuleAction"] = "allow", ["Egress"] = false, ["CidrBlock"] = "0.0.0.0/0" },
                new() { ["RuleNumber"] = 32767, ["Protocol"] = "-1", ["RuleAction"] = "deny", ["Egress"] = false, ["CidrBlock"] = "0.0.0.0/0" },
                new() { ["RuleNumber"] = 100, ["Protocol"] = "-1", ["RuleAction"] = "allow", ["Egress"] = true, ["CidrBlock"] = "0.0.0.0/0" },
                new() { ["RuleNumber"] = 32767, ["Protocol"] = "-1", ["RuleAction"] = "deny", ["Egress"] = true, ["CidrBlock"] = "0.0.0.0/0" },
            },
            ["Associations"] = new List<Dictionary<string, object>>(), ["Tags"] = new List<object>(), ["OwnerId"] = accountId,
        };

        var rtbId = NewId("rtb-");
        var rtbAssocId = NewId("rtbassoc-");
        _routeTables[rtbId] = new Dictionary<string, object>
        {
            ["RouteTableId"] = rtbId, ["VpcId"] = vpcId, ["OwnerId"] = accountId,
            ["Routes"] = new List<Dictionary<string, string>>
            {
                new() { ["DestinationCidrBlock"] = cidr, ["GatewayId"] = "local", ["State"] = "active", ["Origin"] = "CreateRouteTable" },
            },
            ["Associations"] = new List<Dictionary<string, object>>
            {
                new() { ["RouteTableAssociationId"] = rtbAssocId, ["RouteTableId"] = rtbId, ["Main"] = true },
            },
        };

        var sgId = NewId("sg-");
        _securityGroups[sgId] = new Dictionary<string, object>
        {
            ["GroupId"] = sgId, ["GroupName"] = "default", ["Description"] = "default VPC security group",
            ["VpcId"] = vpcId, ["OwnerId"] = accountId,
            ["IpPermissions"] = new List<Dictionary<string, object>>(),
            ["IpPermissionsEgress"] = new List<Dictionary<string, object>>
            {
                new() { ["IpProtocol"] = "-1",
                    ["IpRanges"] = new List<Dictionary<string, string>> { new() { ["CidrIp"] = "0.0.0.0/0" } },
                    ["Ipv6Ranges"] = new List<object>(), ["PrefixListIds"] = new List<object>(),
                    ["UserIdGroupPairs"] = new List<object>() },
            },
        };

        _vpcs[vpcId] = new Dictionary<string, object>
        {
            ["VpcId"] = vpcId, ["CidrBlock"] = cidr, ["State"] = "available", ["IsDefault"] = false,
            ["DhcpOptionsId"] = "dopt-00000001", ["InstanceTenancy"] = P(p, "InstanceTenancy", "default"),
            ["OwnerId"] = accountId, ["DefaultNetworkAclId"] = aclId,
            ["DefaultSecurityGroupId"] = sgId, ["MainRouteTableId"] = rtbId,
        };
        return Xml(200, "CreateVpcResponse", VpcXml(_vpcs[vpcId], "vpc"));
    }

    private ServiceResponse DeleteVpc(Dictionary<string, string[]> p)
    {
        var vpcId = P(p, "VpcId");
        if (!_vpcs.ContainsKey(vpcId))
            return Error("InvalidVpcID.NotFound", $"The vpc ID '{vpcId}' does not exist", 400);
        _vpcs.TryRemove(vpcId, out _);
        return Xml(200, "DeleteVpcResponse", "<return>true</return>");
    }

    private ServiceResponse DescribeVpcAttribute(Dictionary<string, string[]> p)
    {
        var vpcId = P(p, "VpcId");
        var attribute = P(p, "Attribute");
        if (!_vpcs.TryGetValue(vpcId, out var vpc))
            return Error("InvalidVpcID.NotFound", $"The vpc ID '{vpcId}' does not exist", 400);

        if (attribute == "enableDnsSupport")
        {
            var val = vpc.GetValueOrDefault("EnableDnsSupport", true);
            return Xml(200, "DescribeVpcAttributeResponse", $"<vpcId>{vpcId}</vpcId><enableDnsSupport><value>{(Equals(val, true) ? "true" : "false")}</value></enableDnsSupport>");
        }
        if (attribute == "enableDnsHostnames")
        {
            var val = vpc.GetValueOrDefault("EnableDnsHostnames", false);
            return Xml(200, "DescribeVpcAttributeResponse", $"<vpcId>{vpcId}</vpcId><enableDnsHostnames><value>{(Equals(val, true) ? "true" : "false")}</value></enableDnsHostnames>");
        }
        if (attribute == "enableNetworkAddressUsageMetrics")
            return Xml(200, "DescribeVpcAttributeResponse", $"<vpcId>{vpcId}</vpcId><enableNetworkAddressUsageMetrics><value>false</value></enableNetworkAddressUsageMetrics>");
        return Xml(200, "DescribeVpcAttributeResponse", $"<vpcId>{vpcId}</vpcId>");
    }

    private ServiceResponse ModifyVpcAttribute(Dictionary<string, string[]> p)
    {
        var vpcId = P(p, "VpcId");
        if (!_vpcs.TryGetValue(vpcId, out var vpc))
            return Error("InvalidVpcID.NotFound", $"The vpc ID '{vpcId}' does not exist", 400);
        var dns = P(p, "EnableDnsSupport.Value");
        if (!string.IsNullOrEmpty(dns)) vpc["EnableDnsSupport"] = string.Equals(dns, "true", StringComparison.OrdinalIgnoreCase);
        var hostnames = P(p, "EnableDnsHostnames.Value");
        if (!string.IsNullOrEmpty(hostnames)) vpc["EnableDnsHostnames"] = string.Equals(hostnames, "true", StringComparison.OrdinalIgnoreCase);
        return Xml(200, "ModifyVpcAttributeResponse", "<return>true</return>");
    }

    // ── Subnets ────────────────────────────────────────────────────────────────

    private ServiceResponse DescribeSubnets(Dictionary<string, string[]> p)
    {
        var filterIds = ParseMemberList(p, "SubnetId");
        var filters = ParseFilters(p);
        var sb = new StringBuilder();
        foreach (var subnet in _subnets.Values.ToList())
        {
            if (filterIds.Count > 0 && !filterIds.Contains((string)subnet["SubnetId"])) continue;
            if (filters.TryGetValue("vpc-id", out var vpcFilter) && !vpcFilter.Contains((string)subnet["VpcId"])) continue;
            if (filters.TryGetValue("availability-zone", out var azFilter) && !azFilter.Contains((string)subnet["AvailabilityZone"])) continue;
            if (filters.TryGetValue("subnet-id", out var sidFilter) && !sidFilter.Contains((string)subnet["SubnetId"])) continue;
            if (filters.TryGetValue("default-for-az", out var defFilter))
            {
                var val = Equals(subnet.GetValueOrDefault("DefaultForAz", false), true) ? "true" : "false";
                if (!defFilter.Contains(val)) continue;
            }
            sb.Append(SubnetXml(subnet));
        }
        return Xml(200, "DescribeSubnetsResponse", $"<subnetSet>{sb}</subnetSet>");
    }

    private ServiceResponse CreateSubnet(Dictionary<string, string[]> p)
    {
        var vpcId = P(p, "VpcId", DefaultVpcId);
        var cidr = P(p, "CidrBlock", "10.0.1.0/24");
        var az = P(p, "AvailabilityZone", Region + "a");
        var subnetId = NewId("subnet-");
        _subnets[subnetId] = new Dictionary<string, object>
        {
            ["SubnetId"] = subnetId, ["VpcId"] = vpcId, ["CidrBlock"] = cidr,
            ["AvailabilityZone"] = az, ["AvailableIpAddressCount"] = 251,
            ["State"] = "available", ["DefaultForAz"] = false,
            ["MapPublicIpOnLaunch"] = false, ["OwnerId"] = AccountContext.GetAccountId(),
        };
        return Xml(200, "CreateSubnetResponse", SubnetXml(_subnets[subnetId], "subnet"));
    }

    private ServiceResponse DeleteSubnet(Dictionary<string, string[]> p)
    {
        var subnetId = P(p, "SubnetId");
        if (!_subnets.ContainsKey(subnetId))
            return Error("InvalidSubnetID.NotFound", $"The subnet ID '{subnetId}' does not exist", 400);
        _subnets.TryRemove(subnetId, out _);
        return Xml(200, "DeleteSubnetResponse", "<return>true</return>");
    }

    private ServiceResponse ModifySubnetAttribute(Dictionary<string, string[]> p)
    {
        var subnetId = P(p, "SubnetId");
        if (!_subnets.TryGetValue(subnetId, out var subnet))
            return Error("InvalidSubnetID.NotFound", $"The subnet ID '{subnetId}' does not exist", 400);
        var val = P(p, "MapPublicIpOnLaunch.Value");
        if (!string.IsNullOrEmpty(val))
            subnet["MapPublicIpOnLaunch"] = string.Equals(val, "true", StringComparison.OrdinalIgnoreCase);
        return Xml(200, "ModifySubnetAttributeResponse", "<return>true</return>");
    }

    // ── Internet Gateways ──────────────────────────────────────────────────────

    private ServiceResponse CreateInternetGateway(Dictionary<string, string[]> p)
    {
        var igwId = NewId("igw-");
        _internetGateways[igwId] = new Dictionary<string, object>
        {
            ["InternetGatewayId"] = igwId, ["OwnerId"] = AccountContext.GetAccountId(),
            ["Attachments"] = new List<Dictionary<string, string>>(),
        };
        return Xml(200, "CreateInternetGatewayResponse", IgwXml(_internetGateways[igwId], "internetGateway"));
    }

    private ServiceResponse DeleteInternetGateway(Dictionary<string, string[]> p)
    {
        var igwId = P(p, "InternetGatewayId");
        if (!_internetGateways.ContainsKey(igwId))
            return Error("InvalidInternetGatewayID.NotFound", $"The internet gateway ID '{igwId}' does not exist", 400);
        _internetGateways.TryRemove(igwId, out _);
        return Xml(200, "DeleteInternetGatewayResponse", "<return>true</return>");
    }

    private ServiceResponse DescribeInternetGateways(Dictionary<string, string[]> p)
    {
        var filterIds = ParseMemberList(p, "InternetGatewayId");
        var sb = new StringBuilder();
        foreach (var igw in _internetGateways.Values.ToList())
        {
            if (filterIds.Count > 0 && !filterIds.Contains((string)igw["InternetGatewayId"])) continue;
            sb.Append(IgwXml(igw));
        }
        return Xml(200, "DescribeInternetGatewaysResponse", $"<internetGatewaySet>{sb}</internetGatewaySet>");
    }

    private ServiceResponse AttachInternetGateway(Dictionary<string, string[]> p)
    {
        var igwId = P(p, "InternetGatewayId");
        var vpcId = P(p, "VpcId");
        if (!_internetGateways.TryGetValue(igwId, out var igw))
            return Error("InvalidInternetGatewayID.NotFound", $"The internet gateway ID '{igwId}' does not exist", 400);
        igw["Attachments"] = new List<Dictionary<string, string>> { new() { ["VpcId"] = vpcId, ["State"] = "available" } };
        return Xml(200, "AttachInternetGatewayResponse", "<return>true</return>");
    }

    private ServiceResponse DetachInternetGateway(Dictionary<string, string[]> p)
    {
        var igwId = P(p, "InternetGatewayId");
        if (_internetGateways.TryGetValue(igwId, out var igw))
            igw["Attachments"] = new List<Dictionary<string, string>>();
        return Xml(200, "DetachInternetGatewayResponse", "<return>true</return>");
    }

    // ── Availability Zones ─────────────────────────────────────────────────────

    private ServiceResponse DescribeAvailabilityZones(Dictionary<string, string[]> p)
    {
        var region = Region;
        var sb = new StringBuilder();
        foreach (var az in new[] { region + "a", region + "b", region + "c" })
            sb.Append($"<item><zoneName>{az}</zoneName><zoneState>available</zoneState><regionName>{region}</regionName><zoneId>{az}</zoneId></item>");
        return Xml(200, "DescribeAvailabilityZonesResponse", $"<availabilityZoneInfo>{sb}</availabilityZoneInfo>");
    }

    // ── Elastic IPs ────────────────────────────────────────────────────────────

    private ServiceResponse AllocateAddress(Dictionary<string, string[]> p)
    {
        var domain = P(p, "Domain", "vpc");
        var allocId = NewId("eipalloc-");
        var publicIp = RandomIp("52.");
        _addresses[allocId] = new Dictionary<string, object>
        {
            ["AllocationId"] = allocId, ["PublicIp"] = publicIp, ["Domain"] = domain,
            ["AssociationId"] = "", ["InstanceId"] = "",
        };
        return Xml(200, "AllocateAddressResponse", $"<publicIp>{publicIp}</publicIp><domain>{domain}</domain><allocationId>{allocId}</allocationId>");
    }

    private ServiceResponse ReleaseAddress(Dictionary<string, string[]> p)
    {
        var allocId = P(p, "AllocationId");
        if (!string.IsNullOrEmpty(allocId) && _addresses.ContainsKey(allocId))
            _addresses.TryRemove(allocId, out _);
        return Xml(200, "ReleaseAddressResponse", "<return>true</return>");
    }

    private ServiceResponse AssociateAddress(Dictionary<string, string[]> p)
    {
        var allocId = P(p, "AllocationId");
        var instanceId = P(p, "InstanceId");
        if (!_addresses.TryGetValue(allocId, out var addr))
            return Error("InvalidAllocationID.NotFound", $"The allocation ID '{allocId}' does not exist", 400);
        var assocId = NewId("eipassoc-");
        addr["AssociationId"] = assocId;
        addr["InstanceId"] = instanceId;
        return Xml(200, "AssociateAddressResponse", $"<return>true</return><associationId>{assocId}</associationId>");
    }

    private ServiceResponse DisassociateAddress(Dictionary<string, string[]> p)
    {
        var assocId = P(p, "AssociationId");
        foreach (var addr in _addresses.Values)
        {
            if ((string)addr["AssociationId"] == assocId)
            {
                addr["AssociationId"] = "";
                addr["InstanceId"] = "";
                break;
            }
        }
        return Xml(200, "DisassociateAddressResponse", "<return>true</return>");
    }

    private ServiceResponse DescribeAddresses(Dictionary<string, string[]> p)
    {
        var filterIds = ParseMemberList(p, "AllocationId");
        var sb = new StringBuilder();
        foreach (var addr in _addresses.Values.ToList())
        {
            if (filterIds.Count > 0 && !filterIds.Contains((string)addr["AllocationId"])) continue;
            var assoc = !string.IsNullOrEmpty((string)addr["AssociationId"]) ? $"<associationId>{addr["AssociationId"]}</associationId>" : "";
            var inst = !string.IsNullOrEmpty((string)addr["InstanceId"]) ? $"<instanceId>{addr["InstanceId"]}</instanceId>" : "";
            sb.Append($"<item><allocationId>{addr["AllocationId"]}</allocationId><publicIp>{addr["PublicIp"]}</publicIp><domain>{addr["Domain"]}</domain>{assoc}{inst}</item>");
        }
        return Xml(200, "DescribeAddressesResponse", $"<addressesSet>{sb}</addressesSet>");
    }

    private ServiceResponse DescribeAddressesAttribute(Dictionary<string, string[]> p)
    {
        var ids = ParseMemberList(p, "AllocationId");
        var sb = new StringBuilder();
        foreach (var aid in ids)
            sb.Append($"<item><allocationId>{aid}</allocationId><ptrRecord></ptrRecord></item>");
        return Xml(200, "DescribeAddressesAttributeResponse", $"<addressSet>{sb}</addressSet>");
    }

    // ── Tags ───────────────────────────────────────────────────────────────────

    private ServiceResponse CreateTags(Dictionary<string, string[]> p)
    {
        var resourceIds = ParseMemberList(p, "ResourceId");
        var tagsToAdd = ParseTagParams(p);
        foreach (var rid in resourceIds)
        {
            if (!_tags.TryGetValue(rid, out var existing))
            {
                existing = new List<Dictionary<string, string>>();
                _tags[rid] = existing;
            }
            foreach (var tag in tagsToAdd)
            {
                var idx = existing.FindIndex(t => t["Key"] == tag["Key"]);
                if (idx >= 0) existing[idx] = tag;
                else existing.Add(tag);
            }
        }
        return Xml(200, "CreateTagsResponse", "<return>true</return>");
    }

    private ServiceResponse DeleteTags(Dictionary<string, string[]> p)
    {
        var resourceIds = ParseMemberList(p, "ResourceId");
        var tagsToRemove = ParseTagParams(p);
        var keysToRemove = tagsToRemove.Select(t => t["Key"]).ToHashSet();
        foreach (var rid in resourceIds)
        {
            if (_tags.TryGetValue(rid, out var existing))
                _tags[rid] = existing.Where(t => !keysToRemove.Contains(t["Key"])).ToList();
        }
        return Xml(200, "DeleteTagsResponse", "<return>true</return>");
    }

    private ServiceResponse DescribeTags(Dictionary<string, string[]> p)
    {
        var filters = ParseFilters(p);
        var filterResourceIds = filters.GetValueOrDefault("resource-id")?.ToHashSet();
        var filterKeys = filters.GetValueOrDefault("key")?.ToHashSet();
        var filterValues = filters.GetValueOrDefault("value")?.ToHashSet();

        var sb = new StringBuilder();
        foreach (var (rid, tagList) in _tags.Items.ToList())
        {
            if (filterResourceIds != null && !filterResourceIds.Contains(rid)) continue;
            var resourceType = GuessResourceType(rid);
            foreach (var tag in tagList)
            {
                if (filterKeys != null && !filterKeys.Contains(tag["Key"])) continue;
                if (filterValues != null && !filterValues.Contains(tag.GetValueOrDefault("Value", ""))) continue;
                sb.Append($"<item><resourceId>{rid}</resourceId><resourceType>{resourceType}</resourceType><key>{Esc(tag["Key"])}</key><value>{Esc(tag.GetValueOrDefault("Value", ""))}</value></item>");
            }
        }
        return Xml(200, "DescribeTagsResponse", $"<tagSet>{sb}</tagSet>");
    }
    // ── Route Tables ───────────────────────────────────────────────────────────

    private ServiceResponse CreateRouteTable(Dictionary<string, string[]> p)
    {
        var vpcId = P(p, "VpcId", DefaultVpcId);
        var rtbId = NewId("rtb-");
        var vpcCidr = _vpcs.TryGetValue(vpcId, out var vpcRec) ? (string)vpcRec["CidrBlock"] : "10.0.0.0/16";
        _routeTables[rtbId] = new Dictionary<string, object>
        {
            ["RouteTableId"] = rtbId, ["VpcId"] = vpcId, ["OwnerId"] = AccountContext.GetAccountId(),
            ["Routes"] = new List<Dictionary<string, string>>
            {
                new() { ["DestinationCidrBlock"] = vpcCidr, ["GatewayId"] = "local", ["State"] = "active", ["Origin"] = "CreateRouteTable" },
            },
            ["Associations"] = new List<Dictionary<string, object>>(),
        };
        return Xml(200, "CreateRouteTableResponse", RtbXml(_routeTables[rtbId], "routeTable"));
    }

    private ServiceResponse DeleteRouteTable(Dictionary<string, string[]> p)
    {
        var rtbId = P(p, "RouteTableId");
        if (!_routeTables.ContainsKey(rtbId))
            return Error("InvalidRouteTableID.NotFound", $"The route table '{rtbId}' does not exist", 400);
        _routeTables.TryRemove(rtbId, out _);
        return Xml(200, "DeleteRouteTableResponse", "<return>true</return>");
    }

    private ServiceResponse DescribeRouteTables(Dictionary<string, string[]> p)
    {
        var filterIds = ParseMemberList(p, "RouteTableId");
        var filters = ParseFilters(p);
        var sb = new StringBuilder();

        foreach (var rtb in _routeTables.Values.ToList())
        {
            if (filterIds.Count > 0 && !filterIds.Contains((string)rtb["RouteTableId"])) continue;

            var assocs = (List<Dictionary<string, object>>)rtb["Associations"];

            if (filters.TryGetValue("association.route-table-association-id", out var assocFilter))
            {
                var assocIds = assocs.Select(a => (string)a["RouteTableAssociationId"]).ToList();
                if (!assocFilter.Any(af => assocIds.Contains(af))) continue;
            }
            if (filters.TryGetValue("association.subnet-id", out var subnetFilter))
            {
                var subnetIds = assocs.Select(a => a.GetValueOrDefault("SubnetId")?.ToString() ?? "").ToList();
                if (!subnetFilter.Any(sf => subnetIds.Contains(sf))) continue;
            }
            if (filters.TryGetValue("association.main", out var mainFilter))
            {
                var wantMain = string.Equals(mainFilter[0], "true", StringComparison.OrdinalIgnoreCase);
                var hasMain = assocs.Any(a => Equals(a.GetValueOrDefault("Main"), true));
                if (hasMain != wantMain) continue;
            }
            if (filters.TryGetValue("vpc-id", out var vpcFilter) && !vpcFilter.Contains((string)rtb["VpcId"])) continue;

            sb.Append(RtbXml(rtb));
        }
        return Xml(200, "DescribeRouteTablesResponse", $"<routeTableSet>{sb}</routeTableSet>");
    }

    private ServiceResponse AssociateRouteTable(Dictionary<string, string[]> p)
    {
        var rtbId = P(p, "RouteTableId");
        var subnetId = P(p, "SubnetId");
        if (!_routeTables.TryGetValue(rtbId, out var rtb))
            return Error("InvalidRouteTableID.NotFound", $"The route table '{rtbId}' does not exist", 400);
        var assocId = NewId("rtbassoc-");
        var assocs = (List<Dictionary<string, object>>)rtb["Associations"];
        assocs.Add(new Dictionary<string, object>
        {
            ["RouteTableAssociationId"] = assocId, ["RouteTableId"] = rtbId,
            ["SubnetId"] = subnetId, ["Main"] = false,
        });
        return Xml(200, "AssociateRouteTableResponse", $"<associationId>{assocId}</associationId>");
    }

    private ServiceResponse DisassociateRouteTable(Dictionary<string, string[]> p)
    {
        var assocId = P(p, "AssociationId");
        foreach (var rtb in _routeTables.Values)
        {
            var assocs = (List<Dictionary<string, object>>)rtb["Associations"];
            assocs.RemoveAll(a => (string)a["RouteTableAssociationId"] == assocId);
        }
        return Xml(200, "DisassociateRouteTableResponse", "<return>true</return>");
    }

    private ServiceResponse ReplaceRouteTableAssociation(Dictionary<string, string[]> p)
    {
        var assocId = P(p, "AssociationId");
        var newRtbId = P(p, "RouteTableId");
        if (!_routeTables.ContainsKey(newRtbId))
            return Error("InvalidRouteTableID.NotFound", $"The route table '{newRtbId}' does not exist", 400);

        var newAssocId = NewId("rtbassoc-");
        foreach (var rtb in _routeTables.Values.ToList())
        {
            var assocs = (List<Dictionary<string, object>>)rtb["Associations"];
            for (var i = 0; i < assocs.Count; i++)
            {
                if ((string)assocs[i]["RouteTableAssociationId"] == assocId)
                {
                    var subnetId = assocs[i].GetValueOrDefault("SubnetId")?.ToString() ?? "";
                    var isMain = Equals(assocs[i].GetValueOrDefault("Main"), true);
                    assocs.RemoveAt(i);
                    var targetAssocs = (List<Dictionary<string, object>>)_routeTables[newRtbId]["Associations"];
                    targetAssocs.Add(new Dictionary<string, object>
                    {
                        ["RouteTableAssociationId"] = newAssocId, ["RouteTableId"] = newRtbId,
                        ["SubnetId"] = subnetId, ["Main"] = isMain,
                    });
                    return Xml(200, "ReplaceRouteTableAssociationResponse", $"<newAssociationId>{newAssocId}</newAssociationId>");
                }
            }
        }
        return Error("InvalidAssociationID.NotFound", $"Association '{assocId}' not found", 400);
    }

    private ServiceResponse CreateRoute(Dictionary<string, string[]> p)
    {
        var rtbId = P(p, "RouteTableId");
        if (!_routeTables.TryGetValue(rtbId, out var rtb))
            return Error("InvalidRouteTableID.NotFound", $"The route table '{rtbId}' does not exist", 400);
        var dest = P(p, "DestinationCidrBlock");
        var route = new Dictionary<string, string> { ["DestinationCidrBlock"] = dest, ["State"] = "active", ["Origin"] = "CreateRoute" };
        var gwId = P(p, "GatewayId");
        var natId = P(p, "NatGatewayId");
        if (!string.IsNullOrEmpty(gwId)) route["GatewayId"] = gwId;
        else if (!string.IsNullOrEmpty(natId)) route["NatGatewayId"] = natId;
        else route["GatewayId"] = "local";
        ((List<Dictionary<string, string>>)rtb["Routes"]).Add(route);
        return Xml(200, "CreateRouteResponse", "<return>true</return>");
    }

    private ServiceResponse ReplaceRoute(Dictionary<string, string[]> p)
    {
        var rtbId = P(p, "RouteTableId");
        if (!_routeTables.TryGetValue(rtbId, out var rtb))
            return Error("InvalidRouteTableID.NotFound", $"The route table '{rtbId}' does not exist", 400);
        var dest = P(p, "DestinationCidrBlock");
        var routes = (List<Dictionary<string, string>>)rtb["Routes"];
        foreach (var route in routes)
        {
            if (route.GetValueOrDefault("DestinationCidrBlock") == dest)
            {
                route.Remove("GatewayId"); route.Remove("NatGatewayId");
                var gwId = P(p, "GatewayId");
                var natId = P(p, "NatGatewayId");
                if (!string.IsNullOrEmpty(gwId)) route["GatewayId"] = gwId;
                else if (!string.IsNullOrEmpty(natId)) route["NatGatewayId"] = natId;
                else route["GatewayId"] = "local";
                break;
            }
        }
        return Xml(200, "ReplaceRouteResponse", "<return>true</return>");
    }

    private ServiceResponse DeleteRoute(Dictionary<string, string[]> p)
    {
        var rtbId = P(p, "RouteTableId");
        if (!_routeTables.TryGetValue(rtbId, out var rtb))
            return Error("InvalidRouteTableID.NotFound", $"The route table '{rtbId}' does not exist", 400);
        var dest = P(p, "DestinationCidrBlock");
        var routes = (List<Dictionary<string, string>>)rtb["Routes"];
        routes.RemoveAll(r => r.GetValueOrDefault("DestinationCidrBlock") == dest);
        return Xml(200, "DeleteRouteResponse", "<return>true</return>");
    }

    // ── Network Interfaces ─────────────────────────────────────────────────────

    private ServiceResponse CreateNetworkInterface(Dictionary<string, string[]> p)
    {
        var subnetId = P(p, "SubnetId", DefaultSubnetId);
        var description = P(p, "Description");
        var eniId = NewId("eni-");
        var privateIp = RandomIp("10.0");
        var az = _subnets.TryGetValue(subnetId, out var subRec) ? (string)subRec["AvailabilityZone"] : Region + "a";
        var vpcId = subRec != null ? (string)subRec["VpcId"] : DefaultVpcId;
        _networkInterfaces[eniId] = new Dictionary<string, object>
        {
            ["NetworkInterfaceId"] = eniId, ["SubnetId"] = subnetId, ["VpcId"] = vpcId,
            ["AvailabilityZone"] = az, ["Description"] = description,
            ["OwnerId"] = AccountContext.GetAccountId(), ["Status"] = "available",
            ["PrivateIpAddress"] = privateIp, ["MacAddress"] = RandomMac(),
        };
        return Xml(200, "CreateNetworkInterfaceResponse", EniXml(_networkInterfaces[eniId], "networkInterface"));
    }

    private ServiceResponse DeleteNetworkInterface(Dictionary<string, string[]> p)
    {
        var eniId = P(p, "NetworkInterfaceId");
        if (!_networkInterfaces.ContainsKey(eniId))
            return Error("InvalidNetworkInterfaceID.NotFound", $"The network interface '{eniId}' does not exist", 400);
        _networkInterfaces.TryRemove(eniId, out _);
        return Xml(200, "DeleteNetworkInterfaceResponse", "<return>true</return>");
    }

    private ServiceResponse DescribeNetworkInterfaces(Dictionary<string, string[]> p)
    {
        var filterIds = ParseMemberList(p, "NetworkInterfaceId");
        var sb = new StringBuilder();
        foreach (var eni in _networkInterfaces.Values.ToList())
        {
            if (filterIds.Count > 0 && !filterIds.Contains((string)eni["NetworkInterfaceId"])) continue;
            sb.Append(EniXml(eni));
        }
        return Xml(200, "DescribeNetworkInterfacesResponse", $"<networkInterfaceSet>{sb}</networkInterfaceSet>");
    }

    private ServiceResponse AttachNetworkInterface(Dictionary<string, string[]> p)
    {
        var eniId = P(p, "NetworkInterfaceId");
        var instanceId = P(p, "InstanceId");
        var deviceIndex = P(p, "DeviceIndex", "1");
        if (!_networkInterfaces.TryGetValue(eniId, out var eni))
            return Error("InvalidNetworkInterfaceID.NotFound", $"The network interface '{eniId}' does not exist", 400);
        var attachmentId = NewId("eni-attach-");
        eni["Status"] = "in-use";
        eni["AttachmentId"] = attachmentId;
        eni["AttachInstanceId"] = instanceId;
        eni["DeviceIndex"] = int.Parse(deviceIndex);
        return Xml(200, "AttachNetworkInterfaceResponse", $"<attachmentId>{attachmentId}</attachmentId>");
    }

    private ServiceResponse DetachNetworkInterface(Dictionary<string, string[]> p)
    {
        var attachmentId = P(p, "AttachmentId");
        foreach (var eni in _networkInterfaces.Values)
        {
            if (eni.GetValueOrDefault("AttachmentId")?.ToString() == attachmentId)
            {
                eni["Status"] = "available";
                eni.Remove("AttachmentId");
                eni.Remove("AttachInstanceId");
                eni.Remove("DeviceIndex");
                break;
            }
        }
        return Xml(200, "DetachNetworkInterfaceResponse", "<return>true</return>");
    }

    // ── VPC Endpoints ──────────────────────────────────────────────────────────

    private ServiceResponse CreateVpcEndpoint(Dictionary<string, string[]> p)
    {
        var vpcId = P(p, "VpcId", DefaultVpcId);
        var serviceName = P(p, "ServiceName");
        var endpointType = P(p, "VpcEndpointType", "Gateway");
        var vpceId = NewId("vpce-");
        _vpcEndpoints[vpceId] = new Dictionary<string, object>
        {
            ["VpcEndpointId"] = vpceId, ["VpcEndpointType"] = endpointType,
            ["VpcId"] = vpcId, ["ServiceName"] = serviceName, ["State"] = "available",
            ["RouteTableIds"] = ParseMemberList(p, "RouteTableId"),
            ["SubnetIds"] = ParseMemberList(p, "SubnetId"),
            ["OwnerId"] = AccountContext.GetAccountId(),
        };
        return Xml(200, "CreateVpcEndpointResponse", VpceXml(_vpcEndpoints[vpceId], "vpcEndpoint"));
    }

    private ServiceResponse DeleteVpcEndpoints(Dictionary<string, string[]> p)
    {
        var ids = ParseMemberList(p, "VpcEndpointId");
        foreach (var id in ids) _vpcEndpoints.TryRemove(id, out _);
        return Xml(200, "DeleteVpcEndpointsResponse", "<unsuccessful/>");
    }

    private ServiceResponse DescribeVpcEndpoints(Dictionary<string, string[]> p)
    {
        var filterIds = ParseMemberList(p, "VpcEndpointId");
        var sb = new StringBuilder();
        foreach (var ep in _vpcEndpoints.Values.ToList())
        {
            if (filterIds.Count > 0 && !filterIds.Contains((string)ep["VpcEndpointId"])) continue;
            sb.Append(VpceXml(ep));
        }
        return Xml(200, "DescribeVpcEndpointsResponse", $"<vpcEndpointSet>{sb}</vpcEndpointSet>");
    }

    private ServiceResponse ModifyVpcEndpoint(Dictionary<string, string[]> p)
    {
        var vpceId = P(p, "VpcEndpointId");
        if (!_vpcEndpoints.TryGetValue(vpceId, out var ep))
            return Error("InvalidVpcEndpointId.NotFound", $"The VPC endpoint '{vpceId}' does not exist", 400);
        var addRtbs = ParseMemberList(p, "AddRouteTableId");
        var rmRtbs = ParseMemberList(p, "RemoveRouteTableId");
        var rtbIds = (List<string>)ep["RouteTableIds"];
        if (addRtbs.Count > 0) rtbIds = rtbIds.Union(addRtbs).ToList();
        if (rmRtbs.Count > 0) rtbIds = rtbIds.Where(r => !rmRtbs.Contains(r)).ToList();
        ep["RouteTableIds"] = rtbIds;
        return Xml(200, "ModifyVpcEndpointResponse", "<return>true</return>");
    }

    private ServiceResponse DescribePrefixLists(Dictionary<string, string[]> p)
    {
        var filterIds = ParseMemberList(p, "PrefixListId");
        var filters = ParseFilters(p);
        var region = Region;
        var sb = new StringBuilder();

        // Built-in AWS service prefix lists
        var builtIn = new[]
        {
            ("pl-63a5400a", $"com.amazonaws.{region}.s3"),
            ("pl-02cd2c6b", $"com.amazonaws.{region}.dynamodb"),
        };
        foreach (var (plId, name) in builtIn)
        {
            if (filterIds.Count > 0 && !filterIds.Contains(plId)) continue;
            if (filters.TryGetValue("prefix-list-name", out var plnFilter) && !plnFilter.Contains(name)) continue;
            sb.Append($"<item><prefixListId>{plId}</prefixListId><prefixListName>{name}</prefixListName><cidrSet><item><cidr>0.0.0.0/0</cidr></item></cidrSet></item>");
        }

        // User-created managed prefix lists
        foreach (var pl in _prefixLists.Values.ToList())
        {
            var id = (string)pl["PrefixListId"];
            if (filterIds.Count > 0 && !filterIds.Contains(id)) continue;
            var plName = (string)pl.GetValueOrDefault("PrefixListName", "")!;
            if (filters.TryGetValue("prefix-list-name", out var plnFilter2) && !plnFilter2.Contains(plName)) continue;
            var entries = (List<Dictionary<string, string>>)pl["Entries"];
            var entriesXml = new StringBuilder();
            foreach (var e in entries)
                entriesXml.Append($"<item><cidr>{e["Cidr"]}</cidr></item>");
            sb.Append($"<item><prefixListId>{id}</prefixListId><prefixListName>{plName}</prefixListName><cidrSet>{entriesXml}</cidrSet></item>");
        }

        return Xml(200, "DescribePrefixListsResponse", $"<prefixListSet>{sb}</prefixListSet>");
    }

    // ── EBS Volumes ───────────────────────────────────────────────────────────

    private ServiceResponse CreateVolume(Dictionary<string, string[]> p)
    {
        var volId = NewId("vol-");
        var az = P(p, "AvailabilityZone", $"{Region}a");
        var size = int.Parse(P(p, "Size", "8"));
        var volType = P(p, "VolumeType", "gp2");
        var snapshotId = P(p, "SnapshotId");
        var iopsStr = P(p, "Iops");
        var encrypted = P(p, "Encrypted", "false");
        var now = NowTs();
        var iops = !string.IsNullOrEmpty(iopsStr)
            ? int.Parse(iopsStr)
            : (volType is "gp3" or "io1" or "io2" ? 3000 : 0);
        _volumes[volId] = new Dictionary<string, object>
        {
            ["VolumeId"] = volId, ["Size"] = size, ["AvailabilityZone"] = az,
            ["State"] = "available", ["VolumeType"] = volType, ["SnapshotId"] = snapshotId,
            ["Iops"] = iops, ["Encrypted"] = encrypted.Equals("true", StringComparison.OrdinalIgnoreCase),
            ["CreateTime"] = now, ["Attachments"] = new List<Dictionary<string, object>>(),
            ["MultiAttachEnabled"] = false, ["Throughput"] = volType == "gp3" ? 125 : 0,
        };
        return Xml(200, "CreateVolumeResponse", VolumeInnerXml(_volumes[volId]));
    }

    private ServiceResponse DeleteVolume(Dictionary<string, string[]> p)
    {
        var volId = P(p, "VolumeId");
        if (!_volumes.TryGetValue(volId, out var vol))
            return Error("InvalidVolume.NotFound", $"The volume '{volId}' does not exist.", 400);
        var attachments = (List<Dictionary<string, object>>)vol["Attachments"];
        if (attachments.Count > 0)
            return Error("VolumeInUse", $"Volume {volId} is currently attached.", 400);
        _volumes.TryRemove(volId, out _);
        return Xml(200, "DeleteVolumeResponse", "<return>true</return>");
    }

    private ServiceResponse DescribeVolumes(Dictionary<string, string[]> p)
    {
        var filterIds = ParseMemberList(p, "VolumeId");
        var sb = new StringBuilder();
        foreach (var vol in _volumes.Values.ToList())
        {
            if (filterIds.Count > 0 && !filterIds.Contains((string)vol["VolumeId"])) continue;
            sb.Append($"<item>{VolumeInnerXml(vol)}</item>");
        }
        return Xml(200, "DescribeVolumesResponse", $"<volumeSet>{sb}</volumeSet>");
    }

    private ServiceResponse DescribeVolumeStatus(Dictionary<string, string[]> p)
    {
        var filterIds = ParseMemberList(p, "VolumeId");
        var sb = new StringBuilder();
        foreach (var vol in _volumes.Values.ToList())
        {
            if (filterIds.Count > 0 && !filterIds.Contains((string)vol["VolumeId"])) continue;
            sb.Append($"<item><volumeId>{vol["VolumeId"]}</volumeId><availabilityZone>{vol["AvailabilityZone"]}</availabilityZone><volumeStatus><status>ok</status><details><item><name>io-enabled</name><status>passed</status></item></details></volumeStatus><actionsSet/><eventsSet/></item>");
        }
        return Xml(200, "DescribeVolumeStatusResponse", $"<volumeStatusSet>{sb}</volumeStatusSet>");
    }

    private ServiceResponse AttachVolume(Dictionary<string, string[]> p)
    {
        var volId = P(p, "VolumeId");
        var instanceId = P(p, "InstanceId");
        var device = P(p, "Device", "/dev/xvdf");
        if (!_volumes.TryGetValue(volId, out var vol))
            return Error("InvalidVolume.NotFound", $"The volume '{volId}' does not exist.", 400);
        if (!_instances.ContainsKey(instanceId))
            return Error("InvalidInstanceID.NotFound", $"The instance ID '{instanceId}' does not exist.", 400);
        var now = NowTs();
        var attachment = new Dictionary<string, object>
        {
            ["VolumeId"] = volId, ["InstanceId"] = instanceId, ["Device"] = device,
            ["State"] = "attached", ["AttachTime"] = now, ["DeleteOnTermination"] = false,
        };
        vol["Attachments"] = new List<Dictionary<string, object>> { attachment };
        vol["State"] = "in-use";
        return Xml(200, "AttachVolumeResponse", $"<volumeId>{volId}</volumeId><instanceId>{instanceId}</instanceId><device>{device}</device><status>attached</status><attachTime>{now}</attachTime><deleteOnTermination>false</deleteOnTermination>");
    }

    private ServiceResponse DetachVolume(Dictionary<string, string[]> p)
    {
        var volId = P(p, "VolumeId");
        if (!_volumes.TryGetValue(volId, out var vol))
            return Error("InvalidVolume.NotFound", $"The volume '{volId}' does not exist.", 400);
        vol["Attachments"] = new List<Dictionary<string, object>>();
        vol["State"] = "available";
        return Xml(200, "DetachVolumeResponse", $"<volumeId>{volId}</volumeId><status>detached</status>");
    }

    private ServiceResponse ModifyVolume(Dictionary<string, string[]> p)
    {
        var volId = P(p, "VolumeId");
        if (!_volumes.TryGetValue(volId, out var vol))
            return Error("InvalidVolume.NotFound", $"The volume '{volId}' does not exist.", 400);
        var sizeStr = P(p, "Size");
        if (!string.IsNullOrEmpty(sizeStr)) vol["Size"] = int.Parse(sizeStr);
        var vtStr = P(p, "VolumeType");
        if (!string.IsNullOrEmpty(vtStr)) vol["VolumeType"] = vtStr;
        var iopsStr = P(p, "Iops");
        if (!string.IsNullOrEmpty(iopsStr)) vol["Iops"] = int.Parse(iopsStr);
        var now = NowTs();
        return Xml(200, "ModifyVolumeResponse", $"<volumeModification><volumeId>{volId}</volumeId><modificationState>completed</modificationState><targetSize>{vol["Size"]}</targetSize><targetVolumeType>{vol["VolumeType"]}</targetVolumeType><targetIops>{vol["Iops"]}</targetIops><startTime>{now}</startTime><endTime>{now}</endTime><progress>100</progress></volumeModification>");
    }

    private ServiceResponse DescribeVolumesModifications(Dictionary<string, string[]> p)
    {
        var filterIds = ParseMemberList(p, "VolumeId");
        var sb = new StringBuilder();
        foreach (var vol in _volumes.Values.ToList())
        {
            if (filterIds.Count > 0 && !filterIds.Contains((string)vol["VolumeId"])) continue;
            var now = NowTs();
            sb.Append($"<item><volumeId>{vol["VolumeId"]}</volumeId><modificationState>completed</modificationState><targetSize>{vol["Size"]}</targetSize><targetVolumeType>{vol["VolumeType"]}</targetVolumeType><targetIops>{vol["Iops"]}</targetIops><startTime>{now}</startTime><endTime>{now}</endTime><progress>100</progress></item>");
        }
        return Xml(200, "DescribeVolumesModificationsResponse", $"<volumeModificationSet>{sb}</volumeModificationSet>");
    }

    private ServiceResponse DescribeVolumeAttribute(Dictionary<string, string[]> p)
    {
        var volId = P(p, "VolumeId");
        return Xml(200, "DescribeVolumeAttributeResponse", $"<volumeId>{volId}</volumeId><autoEnableIO><value>false</value></autoEnableIO>");
    }

    // ── EBS Snapshots ─────────────────────────────────────────────────────────

    private ServiceResponse CreateSnapshot(Dictionary<string, string[]> p)
    {
        var volId = P(p, "VolumeId");
        var description = P(p, "Description");
        if (!_volumes.TryGetValue(volId, out var vol))
            return Error("InvalidVolume.NotFound", $"The volume '{volId}' does not exist.", 400);
        var snapId = NewId("snap-");
        var now = NowTs();
        var accountId = AccountContext.GetAccountId();
        _snapshots[snapId] = new Dictionary<string, object>
        {
            ["SnapshotId"] = snapId, ["VolumeId"] = volId, ["VolumeSize"] = (int)vol["Size"],
            ["Description"] = description, ["State"] = "completed", ["StartTime"] = now,
            ["Progress"] = "100%", ["OwnerId"] = accountId, ["Encrypted"] = (bool)vol["Encrypted"],
            ["StorageTier"] = "standard",
        };
        return Xml(200, "CreateSnapshotResponse", SnapshotInnerXml(_snapshots[snapId]));
    }

    private ServiceResponse DeleteSnapshot(Dictionary<string, string[]> p)
    {
        var snapId = P(p, "SnapshotId");
        if (!_snapshots.ContainsKey(snapId))
            return Error("InvalidSnapshot.NotFound", $"The snapshot '{snapId}' does not exist.", 400);
        _snapshots.TryRemove(snapId, out _);
        return Xml(200, "DeleteSnapshotResponse", "<return>true</return>");
    }

    private ServiceResponse DescribeSnapshots(Dictionary<string, string[]> p)
    {
        var filterIds = ParseMemberList(p, "SnapshotId");
        var ownerIds = ParseMemberList(p, "Owner");
        var sb = new StringBuilder();
        foreach (var snap in _snapshots.Values.ToList())
        {
            if (filterIds.Count > 0 && !filterIds.Contains((string)snap["SnapshotId"])) continue;
            if (ownerIds.Count > 0 && !ownerIds.Contains((string)snap["OwnerId"]) && !ownerIds.Contains("self")) continue;
            sb.Append($"<item>{SnapshotInnerXml(snap)}</item>");
        }
        return Xml(200, "DescribeSnapshotsResponse", $"<snapshotSet>{sb}</snapshotSet>");
    }

    private ServiceResponse CopySnapshot(Dictionary<string, string[]> p)
    {
        var sourceSnapId = P(p, "SourceSnapshotId");
        var description = P(p, "Description");
        if (!_snapshots.TryGetValue(sourceSnapId, out var source))
            return Error("InvalidSnapshot.NotFound", $"The snapshot '{sourceSnapId}' does not exist.", 400);
        var newSnapId = NewId("snap-");
        var now = NowTs();
        var copy = new Dictionary<string, object>(source)
        {
            ["SnapshotId"] = newSnapId,
            ["StartTime"] = now,
        };
        if (!string.IsNullOrEmpty(description)) copy["Description"] = description;
        _snapshots[newSnapId] = copy;
        return Xml(200, "CopySnapshotResponse", $"<snapshotId>{newSnapId}</snapshotId>");
    }

    private ServiceResponse ModifySnapshotAttribute(Dictionary<string, string[]> p)
    {
        var snapId = P(p, "SnapshotId");
        if (!_snapshots.TryGetValue(snapId, out var snap))
            return Error("InvalidSnapshot.NotFound", $"Snapshot '{snapId}' not found", 400);
        var op = P(p, "OperationType");
        var userIds = ParseMemberList(p, "UserId");
        if (!snap.ContainsKey("CreateVolumePermissions"))
            snap["CreateVolumePermissions"] = new List<Dictionary<string, string>>();
        var perms = (List<Dictionary<string, string>>)snap["CreateVolumePermissions"];
        if (op == "add")
        {
            foreach (var uid in userIds)
            {
                if (!perms.Any(pp => pp.GetValueOrDefault("UserId") == uid))
                    perms.Add(new Dictionary<string, string> { ["UserId"] = uid });
            }
        }
        else if (op == "remove")
        {
            perms.RemoveAll(pp => userIds.Contains(pp.GetValueOrDefault("UserId") ?? ""));
        }
        return Xml(200, "ModifySnapshotAttributeResponse", "<return>true</return>");
    }

    private ServiceResponse DescribeSnapshotAttribute(Dictionary<string, string[]> p)
    {
        var snapId = P(p, "SnapshotId");
        var permsXml = new StringBuilder();
        if (_snapshots.TryGetValue(snapId, out var snap) && snap.ContainsKey("CreateVolumePermissions"))
        {
            foreach (var pp in (List<Dictionary<string, string>>)snap["CreateVolumePermissions"])
                permsXml.Append($"<item><userId>{pp["UserId"]}</userId></item>");
        }
        return Xml(200, "DescribeSnapshotAttributeResponse", $"<snapshotId>{snapId}</snapshotId><createVolumePermission>{permsXml}</createVolumePermission>");
    }

    // ── NAT Gateways ──────────────────────────────────────────────────────────

    private ServiceResponse CreateNatGateway(Dictionary<string, string[]> p)
    {
        var subnetId = P(p, "SubnetId");
        var allocId = P(p, "AllocationId");
        var connectivity = P(p, "ConnectivityType", "public");
        if (string.IsNullOrEmpty(subnetId))
            return Error("MissingParameter", "SubnetId is required", 400);
        var natId = NewId("nat-");
        var vpcId = _subnets.TryGetValue(subnetId, out var subnet) ? (string)subnet["VpcId"] : DefaultVpcId;
        var tags = ParseTagParams(p);
        var now = NowTs();
        _natGateways[natId] = new Dictionary<string, object>
        {
            ["NatGatewayId"] = natId, ["SubnetId"] = subnetId, ["VpcId"] = vpcId,
            ["AllocationId"] = allocId, ["ConnectivityType"] = connectivity,
            ["State"] = "available", ["CreateTime"] = now, ["Tags"] = tags,
        };
        if (tags.Count > 0) _tags[natId] = tags;
        return Xml(200, "CreateNatGatewayResponse", $"<natGateway><natGatewayId>{natId}</natGatewayId><subnetId>{subnetId}</subnetId><vpcId>{vpcId}</vpcId><state>available</state><connectivityType>{connectivity}</connectivityType><createTime>{now}</createTime><natGatewayAddressSet/><tagSet/></natGateway>");
    }

    private ServiceResponse DescribeNatGateways(Dictionary<string, string[]> p)
    {
        var filters = ParseFilters(p);
        var ids = ParseMemberList(p, "NatGatewayId");
        var sb = new StringBuilder();
        foreach (var nat in _natGateways.Values.ToList())
        {
            if (ids.Count > 0 && !ids.Contains((string)nat["NatGatewayId"])) continue;
            if (filters.TryGetValue("state", out var stateF) && !stateF.Contains((string)nat["State"])) continue;
            if (filters.TryGetValue("vpc-id", out var vpcF) && !vpcF.Contains((string)nat["VpcId"])) continue;
            if (filters.TryGetValue("subnet-id", out var subF) && !subF.Contains((string)nat["SubnetId"])) continue;
            sb.Append($"<item><natGatewayId>{nat["NatGatewayId"]}</natGatewayId><subnetId>{nat["SubnetId"]}</subnetId><vpcId>{nat["VpcId"]}</vpcId><state>{nat["State"]}</state><connectivityType>{nat["ConnectivityType"]}</connectivityType><createTime>{nat["CreateTime"]}</createTime><natGatewayAddressSet/><tagSet/></item>");
        }
        return Xml(200, "DescribeNatGatewaysResponse", $"<natGatewaySet>{sb}</natGatewaySet>");
    }

    private ServiceResponse DeleteNatGateway(Dictionary<string, string[]> p)
    {
        var natId = P(p, "NatGatewayId");
        if (!_natGateways.TryGetValue(natId, out var nat))
            return Error("NatGatewayNotFound", $"NatGateway {natId} not found", 400);
        nat["State"] = "deleted";
        return Xml(200, "DeleteNatGatewayResponse", $"<natGatewayId>{natId}</natGatewayId>");
    }

    // ── Network ACLs ──────────────────────────────────────────────────────────

    private ServiceResponse CreateNetworkAcl(Dictionary<string, string[]> p)
    {
        var vpcId = P(p, "VpcId");
        if (string.IsNullOrEmpty(vpcId))
            return Error("MissingParameter", "VpcId is required", 400);
        var aclId = NewId("acl-");
        var accountId = AccountContext.GetAccountId();
        var tags = ParseTagParams(p);
        _networkAcls[aclId] = new Dictionary<string, object>
        {
            ["NetworkAclId"] = aclId, ["VpcId"] = vpcId, ["IsDefault"] = false,
            ["Entries"] = new List<Dictionary<string, object>>(),
            ["Associations"] = new List<Dictionary<string, object>>(),
            ["Tags"] = tags, ["OwnerId"] = accountId,
        };
        if (tags.Count > 0) _tags[aclId] = tags;
        return Xml(200, "CreateNetworkAclResponse", $"<networkAcl><networkAclId>{aclId}</networkAclId><vpcId>{vpcId}</vpcId><default>false</default><entrySet/><associationSet/><tagSet/><ownerId>{accountId}</ownerId></networkAcl>");
    }

    private ServiceResponse DescribeNetworkAcls(Dictionary<string, string[]> p)
    {
        var filters = ParseFilters(p);
        var ids = ParseMemberList(p, "NetworkAclId");
        var sb = new StringBuilder();
        foreach (var acl in _networkAcls.Values.ToList())
        {
            if (ids.Count > 0 && !ids.Contains((string)acl["NetworkAclId"])) continue;
            if (filters.TryGetValue("vpc-id", out var vpcF) && !vpcF.Contains((string)acl["VpcId"])) continue;
            if (filters.TryGetValue("default", out var defF))
            {
                var wantDefault = defF[0].Equals("true", StringComparison.OrdinalIgnoreCase);
                if ((bool)acl["IsDefault"] != wantDefault) continue;
            }
            var entries = (List<Dictionary<string, object>>)acl["Entries"];
            var entriesSb = new StringBuilder();
            foreach (var e in entries)
                entriesSb.Append($"<item><ruleNumber>{e["RuleNumber"]}</ruleNumber><protocol>{e["Protocol"]}</protocol><ruleAction>{e["RuleAction"]}</ruleAction><egress>{((bool)e["Egress"] ? "true" : "false")}</egress><cidrBlock>{e.GetValueOrDefault("CidrBlock", "0.0.0.0/0")}</cidrBlock></item>");
            var assocs = (List<Dictionary<string, object>>)acl["Associations"];
            var assocsSb = new StringBuilder();
            foreach (var a in assocs)
                assocsSb.Append($"<item><networkAclAssociationId>{a["NetworkAclAssociationId"]}</networkAclAssociationId><networkAclId>{acl["NetworkAclId"]}</networkAclId><subnetId>{a["SubnetId"]}</subnetId></item>");
            var isDefault = (bool)acl["IsDefault"] ? "true" : "false";
            sb.Append($"<item><networkAclId>{acl["NetworkAclId"]}</networkAclId><vpcId>{acl["VpcId"]}</vpcId><default>{isDefault}</default><entrySet>{entriesSb}</entrySet><associationSet>{assocsSb}</associationSet><tagSet/><ownerId>{acl["OwnerId"]}</ownerId></item>");
        }
        return Xml(200, "DescribeNetworkAclsResponse", $"<networkAclSet>{sb}</networkAclSet>");
    }

    private ServiceResponse DeleteNetworkAcl(Dictionary<string, string[]> p)
    {
        var aclId = P(p, "NetworkAclId");
        if (!_networkAcls.ContainsKey(aclId))
            return Error("InvalidNetworkAclID.NotFound", $"The network ACL '{aclId}' does not exist", 400);
        _networkAcls.TryRemove(aclId, out _);
        return Xml(200, "DeleteNetworkAclResponse", "<return>true</return>");
    }

    private ServiceResponse CreateNetworkAclEntry(Dictionary<string, string[]> p)
    {
        var aclId = P(p, "NetworkAclId");
        if (!_networkAcls.TryGetValue(aclId, out var acl))
            return Error("InvalidNetworkAclID.NotFound", $"The network ACL '{aclId}' does not exist", 400);
        var ruleNumStr = P(p, "RuleNumber", "100");
        var entry = new Dictionary<string, object>
        {
            ["RuleNumber"] = int.Parse(ruleNumStr),
            ["Protocol"] = P(p, "Protocol", "-1"),
            ["RuleAction"] = P(p, "RuleAction", "allow"),
            ["Egress"] = P(p, "Egress") == "true",
            ["CidrBlock"] = P(p, "CidrBlock", "0.0.0.0/0"),
        };
        ((List<Dictionary<string, object>>)acl["Entries"]).Add(entry);
        return Xml(200, "CreateNetworkAclEntryResponse", "<return>true</return>");
    }

    private ServiceResponse DeleteNetworkAclEntry(Dictionary<string, string[]> p)
    {
        var aclId = P(p, "NetworkAclId");
        var ruleNum = int.Parse(P(p, "RuleNumber", "0"));
        var egress = P(p, "Egress") == "true";
        if (!_networkAcls.TryGetValue(aclId, out var acl))
            return Error("InvalidNetworkAclID.NotFound", $"The network ACL '{aclId}' does not exist", 400);
        var entries = (List<Dictionary<string, object>>)acl["Entries"];
        entries.RemoveAll(e => (int)e["RuleNumber"] == ruleNum && (bool)e["Egress"] == egress);
        return Xml(200, "DeleteNetworkAclEntryResponse", "<return>true</return>");
    }

    private ServiceResponse ReplaceNetworkAclEntry(Dictionary<string, string[]> p)
    {
        var aclId = P(p, "NetworkAclId");
        var ruleNum = int.Parse(P(p, "RuleNumber", "0"));
        var egress = P(p, "Egress") == "true";
        if (!_networkAcls.TryGetValue(aclId, out var acl))
            return Error("InvalidNetworkAclID.NotFound", $"The network ACL '{aclId}' does not exist", 400);
        var entries = (List<Dictionary<string, object>>)acl["Entries"];
        entries.RemoveAll(e => (int)e["RuleNumber"] == ruleNum && (bool)e["Egress"] == egress);
        entries.Add(new Dictionary<string, object>
        {
            ["RuleNumber"] = ruleNum,
            ["Protocol"] = P(p, "Protocol", "-1"),
            ["RuleAction"] = P(p, "RuleAction", "allow"),
            ["Egress"] = egress,
            ["CidrBlock"] = P(p, "CidrBlock", "0.0.0.0/0"),
        });
        return Xml(200, "ReplaceNetworkAclEntryResponse", "<return>true</return>");
    }

    private ServiceResponse ReplaceNetworkAclAssociation(Dictionary<string, string[]> p)
    {
        var newAclId = P(p, "NetworkAclId");
        var assocId = P(p, "AssociationId");
        if (!_networkAcls.ContainsKey(newAclId))
            return Error("InvalidNetworkAclID.NotFound", $"The network ACL '{newAclId}' does not exist", 400);
        var newAssocId = NewId("aclassoc-");
        foreach (var acl in _networkAcls.Values.ToList())
        {
            var assocs = (List<Dictionary<string, object>>)acl["Associations"];
            assocs.RemoveAll(a => (string)a["NetworkAclAssociationId"] == assocId);
        }
        ((List<Dictionary<string, object>>)_networkAcls[newAclId]["Associations"]).Add(
            new Dictionary<string, object> { ["NetworkAclAssociationId"] = newAssocId, ["SubnetId"] = "" });
        return Xml(200, "ReplaceNetworkAclAssociationResponse", $"<newAssociationId>{newAssocId}</newAssociationId>");
    }

    // ── Flow Logs ─────────────────────────────────────────────────────────────

    private ServiceResponse CreateFlowLogs(Dictionary<string, string[]> p)
    {
        var resourceIds = ParseMemberList(p, "ResourceId");
        var resourceType = P(p, "ResourceType", "VPC");
        var trafficType = P(p, "TrafficType", "ALL");
        var logDestType = P(p, "LogDestinationType", "cloud-watch-logs");
        var logDest = P(p, "LogDestination");
        if (string.IsNullOrEmpty(logDest)) logDest = P(p, "LogGroupName");
        var created = new List<string>();
        foreach (var rid in resourceIds)
        {
            var flId = NewId("fl-");
            _flowLogs[flId] = new Dictionary<string, object>
            {
                ["FlowLogId"] = flId, ["ResourceId"] = rid, ["ResourceType"] = resourceType,
                ["TrafficType"] = trafficType, ["LogDestinationType"] = logDestType,
                ["LogDestination"] = logDest, ["FlowLogStatus"] = "ACTIVE", ["CreationTime"] = NowTs(),
            };
            created.Add(flId);
        }
        var idsSb = new StringBuilder();
        foreach (var fid in created) idsSb.Append($"<item>{fid}</item>");
        return Xml(200, "CreateFlowLogsResponse", $"<flowLogIdSet>{idsSb}</flowLogIdSet><unsuccessful/>");
    }

    private ServiceResponse DescribeFlowLogs(Dictionary<string, string[]> p)
    {
        var ids = ParseMemberList(p, "FlowLogId");
        var filters = ParseFilters(p);
        var sb = new StringBuilder();
        foreach (var fl in _flowLogs.Values.ToList())
        {
            if (ids.Count > 0 && !ids.Contains((string)fl["FlowLogId"])) continue;
            if (filters.TryGetValue("resource-id", out var resF) && !resF.Contains((string)fl["ResourceId"])) continue;
            sb.Append($"<item><flowLogId>{fl["FlowLogId"]}</flowLogId><resourceId>{fl["ResourceId"]}</resourceId><trafficType>{fl["TrafficType"]}</trafficType><logDestinationType>{fl["LogDestinationType"]}</logDestinationType><logDestination>{fl.GetValueOrDefault("LogDestination", "")}</logDestination><flowLogStatus>{fl["FlowLogStatus"]}</flowLogStatus><creationTime>{fl["CreationTime"]}</creationTime></item>");
        }
        return Xml(200, "DescribeFlowLogsResponse", $"<flowLogSet>{sb}</flowLogSet>");
    }

    private ServiceResponse DeleteFlowLogs(Dictionary<string, string[]> p)
    {
        var ids = ParseMemberList(p, "FlowLogId");
        foreach (var fid in ids) _flowLogs.TryRemove(fid, out _);
        return Xml(200, "DeleteFlowLogsResponse", "<unsuccessful/>");
    }

    // ── VPC Peering ───────────────────────────────────────────────────────────

    private ServiceResponse CreateVpcPeeringConnection(Dictionary<string, string[]> p)
    {
        var vpcId = P(p, "VpcId");
        var peerVpcId = P(p, "PeerVpcId");
        var peerOwnerId = P(p, "PeerOwnerId");
        var peerRegion = P(p, "PeerRegion");
        var accountId = AccountContext.GetAccountId();
        if (string.IsNullOrEmpty(peerOwnerId)) peerOwnerId = accountId;
        if (string.IsNullOrEmpty(peerRegion)) peerRegion = Region;
        if (string.IsNullOrEmpty(vpcId) || string.IsNullOrEmpty(peerVpcId))
            return Error("MissingParameter", "VpcId and PeerVpcId are required", 400);
        var pcxId = NewId("pcx-");
        _vpcPeering[pcxId] = new Dictionary<string, object>
        {
            ["VpcPeeringConnectionId"] = pcxId,
            ["RequesterVpcInfo"] = new Dictionary<string, object> { ["VpcId"] = vpcId, ["OwnerId"] = accountId, ["Region"] = Region },
            ["AccepterVpcInfo"] = new Dictionary<string, object> { ["VpcId"] = peerVpcId, ["OwnerId"] = peerOwnerId, ["Region"] = peerRegion },
            ["Status"] = new Dictionary<string, object> { ["Code"] = "pending-acceptance", ["Message"] = "Pending Acceptance by " + peerOwnerId },
            ["ExpirationTime"] = NowTs(), ["Tags"] = new List<Dictionary<string, string>>(),
        };
        return Xml(200, "CreateVpcPeeringConnectionResponse", $"<vpcPeeringConnection><vpcPeeringConnectionId>{pcxId}</vpcPeeringConnectionId><requesterVpcInfo><vpcId>{vpcId}</vpcId><ownerId>{accountId}</ownerId><region>{Region}</region></requesterVpcInfo><accepterVpcInfo><vpcId>{peerVpcId}</vpcId><ownerId>{peerOwnerId}</ownerId><region>{peerRegion}</region></accepterVpcInfo><status><code>pending-acceptance</code></status><tagSet/></vpcPeeringConnection>");
    }

    private ServiceResponse AcceptVpcPeeringConnection(Dictionary<string, string[]> p)
    {
        var pcxId = P(p, "VpcPeeringConnectionId");
        if (!_vpcPeering.TryGetValue(pcxId, out var pcx))
            return Error("InvalidVpcPeeringConnectionID.NotFound", $"The VPC peering connection '{pcxId}' does not exist", 400);
        pcx["Status"] = new Dictionary<string, object> { ["Code"] = "active", ["Message"] = "Active" };
        var req = (Dictionary<string, object>)pcx["RequesterVpcInfo"];
        var acc = (Dictionary<string, object>)pcx["AccepterVpcInfo"];
        return Xml(200, "AcceptVpcPeeringConnectionResponse", $"<vpcPeeringConnection><vpcPeeringConnectionId>{pcxId}</vpcPeeringConnectionId><requesterVpcInfo><vpcId>{req["VpcId"]}</vpcId><ownerId>{req["OwnerId"]}</ownerId><region>{req["Region"]}</region></requesterVpcInfo><accepterVpcInfo><vpcId>{acc["VpcId"]}</vpcId><ownerId>{acc["OwnerId"]}</ownerId><region>{acc["Region"]}</region></accepterVpcInfo><status><code>active</code></status><tagSet/></vpcPeeringConnection>");
    }

    private ServiceResponse DescribeVpcPeeringConnections(Dictionary<string, string[]> p)
    {
        var ids = ParseMemberList(p, "VpcPeeringConnectionId");
        var filters = ParseFilters(p);
        var sb = new StringBuilder();
        foreach (var pcx in _vpcPeering.Values.ToList())
        {
            if (ids.Count > 0 && !ids.Contains((string)pcx["VpcPeeringConnectionId"])) continue;
            var status = (Dictionary<string, object>)pcx["Status"];
            if (filters.TryGetValue("status-code", out var scF) && !scF.Contains((string)status["Code"])) continue;
            var req = (Dictionary<string, object>)pcx["RequesterVpcInfo"];
            var acc = (Dictionary<string, object>)pcx["AccepterVpcInfo"];
            sb.Append($"<item><vpcPeeringConnectionId>{pcx["VpcPeeringConnectionId"]}</vpcPeeringConnectionId><requesterVpcInfo><vpcId>{req["VpcId"]}</vpcId><ownerId>{req["OwnerId"]}</ownerId><region>{req["Region"]}</region></requesterVpcInfo><accepterVpcInfo><vpcId>{acc["VpcId"]}</vpcId><ownerId>{acc["OwnerId"]}</ownerId><region>{acc["Region"]}</region></accepterVpcInfo><status><code>{status["Code"]}</code><message>{status["Message"]}</message></status><tagSet/></item>");
        }
        return Xml(200, "DescribeVpcPeeringConnectionsResponse", $"<vpcPeeringConnectionSet>{sb}</vpcPeeringConnectionSet>");
    }

    private ServiceResponse DeleteVpcPeeringConnection(Dictionary<string, string[]> p)
    {
        var pcxId = P(p, "VpcPeeringConnectionId");
        if (!_vpcPeering.TryGetValue(pcxId, out var pcx))
            return Error("InvalidVpcPeeringConnectionID.NotFound", $"The VPC peering connection '{pcxId}' does not exist", 400);
        pcx["Status"] = new Dictionary<string, object> { ["Code"] = "deleted", ["Message"] = "Deleted" };
        return Xml(200, "DeleteVpcPeeringConnectionResponse", "<return>true</return>");
    }

    // ── DHCP Options ──────────────────────────────────────────────────────────

    private ServiceResponse CreateDhcpOptions(Dictionary<string, string[]> p)
    {
        var configs = new List<Dictionary<string, object>>();
        for (var i = 1; ; i++)
        {
            var key = P(p, $"DhcpConfiguration.{i}.Key");
            if (string.IsNullOrEmpty(key)) break;
            var vals = new List<string>();
            for (var j = 1; ; j++)
            {
                var v = P(p, $"DhcpConfiguration.{i}.Value.{j}");
                if (string.IsNullOrEmpty(v)) break;
                vals.Add(v);
            }
            configs.Add(new Dictionary<string, object> { ["Key"] = key, ["Values"] = vals });
        }
        var doptId = NewId("dopt-");
        var accountId = AccountContext.GetAccountId();
        var tags = ParseTagParams(p);
        _dhcpOptions[doptId] = new Dictionary<string, object>
        {
            ["DhcpOptionsId"] = doptId, ["DhcpConfigurations"] = configs,
            ["OwnerId"] = accountId, ["Tags"] = tags,
        };
        if (tags.Count > 0) _tags[doptId] = tags;
        var configsXml = new StringBuilder();
        foreach (var c in configs)
        {
            var valsSb = new StringBuilder();
            foreach (var v in (List<string>)c["Values"])
                valsSb.Append($"<item><value>{v}</value></item>");
            configsXml.Append($"<item><key>{c["Key"]}</key><valueSet>{valsSb}</valueSet></item>");
        }
        return Xml(200, "CreateDhcpOptionsResponse", $"<dhcpOptions><dhcpOptionsId>{doptId}</dhcpOptionsId><dhcpConfigurationSet>{configsXml}</dhcpConfigurationSet><ownerId>{accountId}</ownerId><tagSet/></dhcpOptions>");
    }

    private ServiceResponse AssociateDhcpOptions(Dictionary<string, string[]> p)
    {
        var doptId = P(p, "DhcpOptionsId");
        var vpcId = P(p, "VpcId");
        if (!_vpcs.ContainsKey(vpcId))
            return Error("InvalidVpcID.NotFound", $"The VPC '{vpcId}' does not exist", 400);
        if (doptId != "default" && !_dhcpOptions.ContainsKey(doptId))
            return Error("InvalidDhcpOptionsID.NotFound", $"The dhcp options '{doptId}' does not exist", 400);
        _vpcs[vpcId]["DhcpOptionsId"] = doptId;
        return Xml(200, "AssociateDhcpOptionsResponse", "<return>true</return>");
    }

    private ServiceResponse DescribeDhcpOptions(Dictionary<string, string[]> p)
    {
        var ids = ParseMemberList(p, "DhcpOptionsId");
        var sb = new StringBuilder();
        foreach (var dopt in _dhcpOptions.Values.ToList())
        {
            if (ids.Count > 0 && !ids.Contains((string)dopt["DhcpOptionsId"])) continue;
            var configs = (List<Dictionary<string, object>>)dopt["DhcpConfigurations"];
            var configsXml = new StringBuilder();
            foreach (var c in configs)
            {
                var valsSb = new StringBuilder();
                foreach (var v in (List<string>)c["Values"])
                    valsSb.Append($"<item><value>{v}</value></item>");
                configsXml.Append($"<item><key>{c["Key"]}</key><valueSet>{valsSb}</valueSet></item>");
            }
            sb.Append($"<item><dhcpOptionsId>{dopt["DhcpOptionsId"]}</dhcpOptionsId><dhcpConfigurationSet>{configsXml}</dhcpConfigurationSet><ownerId>{dopt["OwnerId"]}</ownerId><tagSet/></item>");
        }
        return Xml(200, "DescribeDhcpOptionsResponse", $"<dhcpOptionsSet>{sb}</dhcpOptionsSet>");
    }

    private ServiceResponse DeleteDhcpOptions(Dictionary<string, string[]> p)
    {
        var doptId = P(p, "DhcpOptionsId");
        if (!_dhcpOptions.ContainsKey(doptId))
            return Error("InvalidDhcpOptionsID.NotFound", $"The dhcp options '{doptId}' does not exist", 400);
        _dhcpOptions.TryRemove(doptId, out _);
        return Xml(200, "DeleteDhcpOptionsResponse", "<return>true</return>");
    }

    // ── Egress-Only Internet Gateways ─────────────────────────────────────────

    private ServiceResponse CreateEgressOnlyInternetGateway(Dictionary<string, string[]> p)
    {
        var vpcId = P(p, "VpcId");
        if (string.IsNullOrEmpty(vpcId))
            return Error("MissingParameter", "VpcId is required", 400);
        var eigwId = NewId("eigw-");
        var tags = ParseTagParams(p);
        _egressIgws[eigwId] = new Dictionary<string, object>
        {
            ["EgressOnlyInternetGatewayId"] = eigwId, ["VpcId"] = vpcId,
            ["State"] = "attached", ["Tags"] = tags,
        };
        if (tags.Count > 0) _tags[eigwId] = tags;
        return Xml(200, "CreateEgressOnlyInternetGatewayResponse", $"<egressOnlyInternetGateway><egressOnlyInternetGatewayId>{eigwId}</egressOnlyInternetGatewayId><attachmentSet><item><vpcId>{vpcId}</vpcId><state>attached</state></item></attachmentSet><tagSet/></egressOnlyInternetGateway>");
    }

    private ServiceResponse DescribeEgressOnlyInternetGateways(Dictionary<string, string[]> p)
    {
        var ids = ParseMemberList(p, "EgressOnlyInternetGatewayId");
        var sb = new StringBuilder();
        foreach (var eigw in _egressIgws.Values.ToList())
        {
            if (ids.Count > 0 && !ids.Contains((string)eigw["EgressOnlyInternetGatewayId"])) continue;
            sb.Append($"<item><egressOnlyInternetGatewayId>{eigw["EgressOnlyInternetGatewayId"]}</egressOnlyInternetGatewayId><attachmentSet><item><vpcId>{eigw["VpcId"]}</vpcId><state>{eigw["State"]}</state></item></attachmentSet><tagSet/></item>");
        }
        return Xml(200, "DescribeEgressOnlyInternetGatewaysResponse", $"<egressOnlyInternetGatewaySet>{sb}</egressOnlyInternetGatewaySet>");
    }

    private ServiceResponse DeleteEgressOnlyInternetGateway(Dictionary<string, string[]> p)
    {
        var eigwId = P(p, "EgressOnlyInternetGatewayId");
        if (!_egressIgws.ContainsKey(eigwId))
            return Error("InvalidGatewayID.NotFound", $"The egress only internet gateway '{eigwId}' does not exist", 400);
        _egressIgws.TryRemove(eigwId, out _);
        return Xml(200, "DeleteEgressOnlyInternetGatewayResponse", "<returnCode>true</returnCode>");
    }

    // ── Managed Prefix Lists ──────────────────────────────────────────────────

    private ServiceResponse CreateManagedPrefixList(Dictionary<string, string[]> p)
    {
        var name = P(p, "PrefixListName");
        var maxEntries = int.Parse(P(p, "MaxEntries", "10"));
        var af = P(p, "AddressFamily", "IPv4");
        var plId = NewId("pl-");
        var entries = new List<Dictionary<string, string>>();
        for (var i = 1; ; i++)
        {
            var cidr = P(p, $"Entry.{i}.Cidr");
            if (string.IsNullOrEmpty(cidr)) break;
            entries.Add(new Dictionary<string, string> { ["Cidr"] = cidr, ["Description"] = P(p, $"Entry.{i}.Description") });
        }
        var accountId = AccountContext.GetAccountId();
        var tags = ParseTagParams(p);
        _prefixLists[plId] = new Dictionary<string, object>
        {
            ["PrefixListId"] = plId, ["PrefixListName"] = name, ["State"] = "create-complete",
            ["AddressFamily"] = af, ["MaxEntries"] = maxEntries, ["Version"] = 1,
            ["Entries"] = entries, ["Tags"] = tags, ["OwnerId"] = accountId,
            ["PrefixListArn"] = $"arn:aws:ec2:{Region}:{accountId}:prefix-list/{plId}",
        };
        if (tags.Count > 0) _tags[plId] = tags;
        return Xml(200, "CreateManagedPrefixListResponse", PrefixListXml(_prefixLists[plId], "prefixList"));
    }

    private ServiceResponse DescribeManagedPrefixLists(Dictionary<string, string[]> p)
    {
        var filterIds = ParseMemberList(p, "PrefixListId");
        var filters = ParseFilters(p);
        var sb = new StringBuilder();
        foreach (var pl in _prefixLists.Values.ToList())
        {
            if (filterIds.Count > 0 && !filterIds.Contains((string)pl["PrefixListId"])) continue;
            if (filters.TryGetValue("prefix-list-name", out var plnF) && !plnF.Contains((string)pl.GetValueOrDefault("PrefixListName", "")!)) continue;
            sb.Append(PrefixListXml(pl, "item"));
        }
        return Xml(200, "DescribeManagedPrefixListsResponse", $"<prefixListSet>{sb}</prefixListSet>");
    }

    private ServiceResponse GetManagedPrefixListEntries(Dictionary<string, string[]> p)
    {
        var plId = P(p, "PrefixListId");
        if (!_prefixLists.TryGetValue(plId, out var pl))
            return Error("InvalidPrefixListID.NotFound", $"Prefix list '{plId}' not found", 400);
        var entries = (List<Dictionary<string, string>>)pl["Entries"];
        var sb = new StringBuilder();
        foreach (var e in entries)
            sb.Append($"<item><cidr>{e["Cidr"]}</cidr><description>{e.GetValueOrDefault("Description", "")}</description></item>");
        return Xml(200, "GetManagedPrefixListEntriesResponse", $"<entrySet>{sb}</entrySet>");
    }

    private ServiceResponse ModifyManagedPrefixList(Dictionary<string, string[]> p)
    {
        var plId = P(p, "PrefixListId");
        if (!_prefixLists.TryGetValue(plId, out var pl))
            return Error("InvalidPrefixListID.NotFound", $"Prefix list '{plId}' not found", 400);
        var name = P(p, "PrefixListName");
        if (!string.IsNullOrEmpty(name)) pl["PrefixListName"] = name;
        var maxE = P(p, "MaxEntries");
        if (!string.IsNullOrEmpty(maxE)) pl["MaxEntries"] = int.Parse(maxE);
        var entries = (List<Dictionary<string, string>>)pl["Entries"];
        for (var i = 1; ; i++)
        {
            var cidr = P(p, $"AddEntry.{i}.Cidr");
            if (string.IsNullOrEmpty(cidr)) break;
            entries.Add(new Dictionary<string, string> { ["Cidr"] = cidr, ["Description"] = P(p, $"AddEntry.{i}.Description") });
        }
        var rmCidrs = new HashSet<string>();
        for (var i = 1; ; i++)
        {
            var cidr = P(p, $"RemoveEntry.{i}.Cidr");
            if (string.IsNullOrEmpty(cidr)) break;
            rmCidrs.Add(cidr);
        }
        if (rmCidrs.Count > 0)
            entries.RemoveAll(e => rmCidrs.Contains(e["Cidr"]));
        pl["Version"] = (int)pl.GetValueOrDefault("Version", 1)! + 1;
        return Xml(200, "ModifyManagedPrefixListResponse", PrefixListXml(pl, "prefixList"));
    }

    private ServiceResponse DeleteManagedPrefixList(Dictionary<string, string[]> p)
    {
        var plId = P(p, "PrefixListId");
        if (!_prefixLists.ContainsKey(plId))
            return Error("InvalidPrefixListID.NotFound", $"Prefix list '{plId}' not found", 400);
        _prefixLists.TryRemove(plId, out _);
        return Xml(200, "DeleteManagedPrefixListResponse", "<return>true</return>");
    }

    // ── VPN Gateways ──────────────────────────────────────────────────────────

    private ServiceResponse CreateVpnGateway(Dictionary<string, string[]> p)
    {
        var gwType = P(p, "Type", "ipsec.1");
        var az = P(p, "AvailabilityZone");
        var asn = P(p, "AmazonSideAsn", "64512");
        var vgwId = NewId("vgw-");
        var accountId = AccountContext.GetAccountId();
        var tags = ParseTagParams(p);
        _vpnGateways[vgwId] = new Dictionary<string, object>
        {
            ["VpnGatewayId"] = vgwId, ["Type"] = gwType, ["State"] = "available",
            ["AvailabilityZone"] = az, ["AmazonSideAsn"] = asn,
            ["Attachments"] = new List<Dictionary<string, object>>(), ["Tags"] = tags, ["OwnerId"] = accountId,
        };
        if (tags.Count > 0) _tags[vgwId] = tags;
        return Xml(200, "CreateVpnGatewayResponse", VgwXml(_vpnGateways[vgwId], "vpnGateway"));
    }

    private ServiceResponse DescribeVpnGateways(Dictionary<string, string[]> p)
    {
        var filterIds = ParseMemberList(p, "VpnGatewayId");
        var filters = ParseFilters(p);
        var sb = new StringBuilder();
        foreach (var vgw in _vpnGateways.Values.ToList())
        {
            if (filterIds.Count > 0 && !filterIds.Contains((string)vgw["VpnGatewayId"])) continue;
            if (filters.TryGetValue("attachment.vpc-id", out var avF))
            {
                var vpcIds = ((List<Dictionary<string, object>>)vgw["Attachments"]).Select(a => (string)a["VpcId"]).ToList();
                if (!avF.Any(v => vpcIds.Contains(v))) continue;
            }
            sb.Append(VgwXml(vgw, "item"));
        }
        return Xml(200, "DescribeVpnGatewaysResponse", $"<vpnGatewaySet>{sb}</vpnGatewaySet>");
    }

    private ServiceResponse AttachVpnGateway(Dictionary<string, string[]> p)
    {
        var vgwId = P(p, "VpnGatewayId");
        var vpcId = P(p, "VpcId");
        if (!_vpnGateways.TryGetValue(vgwId, out var vgw))
            return Error("InvalidVpnGatewayID.NotFound", $"VPN gateway '{vgwId}' not found", 400);
        vgw["Attachments"] = new List<Dictionary<string, object>>
        {
            new() { ["VpcId"] = vpcId, ["State"] = "attached" },
        };
        return Xml(200, "AttachVpnGatewayResponse", $"<attachment><vpcId>{vpcId}</vpcId><state>attached</state></attachment>");
    }

    private ServiceResponse DetachVpnGateway(Dictionary<string, string[]> p)
    {
        var vgwId = P(p, "VpnGatewayId");
        if (!_vpnGateways.TryGetValue(vgwId, out var vgw))
            return Error("InvalidVpnGatewayID.NotFound", $"VPN gateway '{vgwId}' not found", 400);
        vgw["Attachments"] = new List<Dictionary<string, object>>();
        vgw["State"] = "detached";
        return Xml(200, "DetachVpnGatewayResponse", "<return>true</return>");
    }

    private ServiceResponse DeleteVpnGateway(Dictionary<string, string[]> p)
    {
        var vgwId = P(p, "VpnGatewayId");
        if (!_vpnGateways.ContainsKey(vgwId))
            return Error("InvalidVpnGatewayID.NotFound", $"VPN gateway '{vgwId}' not found", 400);
        _vpnGateways.TryRemove(vgwId, out _);
        return Xml(200, "DeleteVpnGatewayResponse", "<return>true</return>");
    }

    private ServiceResponse EnableVgwRoutePropagation(Dictionary<string, string[]> p)
    {
        var rtbId = P(p, "RouteTableId");
        var vgwId = P(p, "GatewayId");
        if (!_routeTables.TryGetValue(rtbId, out var rtb))
            return Error("InvalidRouteTableID.NotFound", $"Route table '{rtbId}' not found", 400);
        if (!rtb.ContainsKey("PropagatingVgws"))
            rtb["PropagatingVgws"] = new List<string>();
        var propagating = (List<string>)rtb["PropagatingVgws"];
        if (!propagating.Contains(vgwId)) propagating.Add(vgwId);
        return Xml(200, "EnableVgwRoutePropagationResponse", "<return>true</return>");
    }

    private ServiceResponse DisableVgwRoutePropagation(Dictionary<string, string[]> p)
    {
        var rtbId = P(p, "RouteTableId");
        var vgwId = P(p, "GatewayId");
        if (!_routeTables.TryGetValue(rtbId, out var rtb))
            return Error("InvalidRouteTableID.NotFound", $"Route table '{rtbId}' not found", 400);
        if (rtb.TryGetValue("PropagatingVgws", out var pvObj))
        {
            var propagating = (List<string>)pvObj;
            propagating.Remove(vgwId);
        }
        return Xml(200, "DisableVgwRoutePropagationResponse", "<return>true</return>");
    }

    // ── Customer Gateways ─────────────────────────────────────────────────────

    private ServiceResponse CreateCustomerGateway(Dictionary<string, string[]> p)
    {
        var bgpAsn = P(p, "BgpAsn", "65000");
        var ipAddress = P(p, "IpAddress");
        if (string.IsNullOrEmpty(ipAddress)) ipAddress = P(p, "PublicIp");
        var gwType = P(p, "Type", "ipsec.1");
        var cgwId = NewId("cgw-");
        var accountId = AccountContext.GetAccountId();
        var tags = ParseTagParams(p);
        _customerGateways[cgwId] = new Dictionary<string, object>
        {
            ["CustomerGatewayId"] = cgwId, ["BgpAsn"] = bgpAsn, ["IpAddress"] = ipAddress,
            ["Type"] = gwType, ["State"] = "available", ["Tags"] = tags, ["OwnerId"] = accountId,
        };
        if (tags.Count > 0) _tags[cgwId] = tags;
        return Xml(200, "CreateCustomerGatewayResponse", CgwXml(_customerGateways[cgwId], "customerGateway"));
    }

    private ServiceResponse DescribeCustomerGateways(Dictionary<string, string[]> p)
    {
        var filterIds = ParseMemberList(p, "CustomerGatewayId");
        var sb = new StringBuilder();
        foreach (var cgw in _customerGateways.Values.ToList())
        {
            if (filterIds.Count > 0 && !filterIds.Contains((string)cgw["CustomerGatewayId"])) continue;
            sb.Append(CgwXml(cgw, "item"));
        }
        return Xml(200, "DescribeCustomerGatewaysResponse", $"<customerGatewaySet>{sb}</customerGatewaySet>");
    }

    private ServiceResponse DeleteCustomerGateway(Dictionary<string, string[]> p)
    {
        var cgwId = P(p, "CustomerGatewayId");
        if (!_customerGateways.ContainsKey(cgwId))
            return Error("InvalidCustomerGatewayID.NotFound", $"Customer gateway '{cgwId}' not found", 400);
        _customerGateways.TryRemove(cgwId, out _);
        return Xml(200, "DeleteCustomerGatewayResponse", "<return>true</return>");
    }

    // ── Launch Templates ──────────────────────────────────────────────────────

    private ServiceResponse CreateLaunchTemplate(Dictionary<string, string[]> p)
    {
        var name = P(p, "LaunchTemplateName");
        if (string.IsNullOrEmpty(name))
            return Error("MissingParameter", "LaunchTemplateName is required", 400);
        foreach (var lt in _launchTemplates.Values.ToList())
        {
            if ((string)lt["LaunchTemplateName"] == name)
                return Error("InvalidLaunchTemplateName.AlreadyExistsException", $"Launch template name already in use: {name}", 400);
        }
        var ltId = NewId("lt-");
        var ltData = ParseLtData(p, "LaunchTemplateData");
        var verDesc = P(p, "VersionDescription");
        var now = NowTs();
        var version = new Dictionary<string, object>
        {
            ["LaunchTemplateId"] = ltId, ["LaunchTemplateName"] = name,
            ["VersionNumber"] = 1, ["VersionDescription"] = verDesc,
            ["DefaultVersion"] = true, ["CreateTime"] = now, ["LaunchTemplateData"] = ltData,
        };
        var ltRecord = new Dictionary<string, object>
        {
            ["LaunchTemplateId"] = ltId, ["LaunchTemplateName"] = name,
            ["CreateTime"] = now, ["DefaultVersionNumber"] = 1, ["LatestVersionNumber"] = 1,
            ["Versions"] = new List<Dictionary<string, object>> { version },
        };
        // Parse tag specifications for the template itself
        var tags = new List<Dictionary<string, string>>();
        for (var i = 1; ; i++)
        {
            var rtype = P(p, $"TagSpecification.{i}.ResourceType");
            if (string.IsNullOrEmpty(rtype)) break;
            if (rtype == "launch-template")
            {
                for (var j = 1; ; j++)
                {
                    var tk = P(p, $"TagSpecification.{i}.Tag.{j}.Key");
                    if (string.IsNullOrEmpty(tk)) break;
                    tags.Add(new Dictionary<string, string> { ["Key"] = tk, ["Value"] = P(p, $"TagSpecification.{i}.Tag.{j}.Value") });
                }
            }
        }
        if (tags.Count > 0)
        {
            ltRecord["Tags"] = tags;
            _tags[ltId] = tags;
        }
        _launchTemplates[ltId] = ltRecord;
        var tagsSb = new StringBuilder();
        foreach (var t in tags)
            tagsSb.Append($"<item><key>{Esc(t["Key"])}</key><value>{Esc(t.GetValueOrDefault("Value", "")!)}</value></item>");
        var accountId = AccountContext.GetAccountId();
        return Xml(200, "CreateLaunchTemplateResponse", $"<launchTemplate><launchTemplateId>{ltId}</launchTemplateId><launchTemplateName>{Esc(name)}</launchTemplateName><createTime>{now}</createTime><createdBy>arn:aws:iam::{accountId}:root</createdBy><defaultVersionNumber>1</defaultVersionNumber><latestVersionNumber>1</latestVersionNumber><tags>{tagsSb}</tags></launchTemplate>");
    }

    private ServiceResponse CreateLaunchTemplateVersion(Dictionary<string, string[]> p)
    {
        var ltId = P(p, "LaunchTemplateId");
        var ltName = P(p, "LaunchTemplateName");
        Dictionary<string, object>? lt = null;
        if (!string.IsNullOrEmpty(ltId))
        {
            _launchTemplates.TryGetValue(ltId, out lt);
        }
        else if (!string.IsNullOrEmpty(ltName))
        {
            lt = _launchTemplates.Values.FirstOrDefault(t => (string)t["LaunchTemplateName"] == ltName);
        }
        if (lt == null)
            return Error("InvalidLaunchTemplateId.NotFoundException", "The specified launch template does not exist", 400);
        var ltData = ParseLtData(p, "LaunchTemplateData");
        var sourceVer = P(p, "SourceVersion");
        if (!string.IsNullOrEmpty(sourceVer))
        {
            var versions = (List<Dictionary<string, object>>)lt["Versions"];
            var src = versions.FirstOrDefault(v => v["VersionNumber"].ToString() == sourceVer);
            if (src != null)
            {
                var merged = new Dictionary<string, object>((Dictionary<string, object>)src.GetValueOrDefault("LaunchTemplateData", new Dictionary<string, object>())!);
                foreach (var kv in ltData) merged[kv.Key] = kv.Value;
                ltData = merged;
            }
        }
        var verNum = (int)lt["LatestVersionNumber"] + 1;
        var verDesc = P(p, "VersionDescription");
        var now = NowTs();
        var version = new Dictionary<string, object>
        {
            ["LaunchTemplateId"] = (string)lt["LaunchTemplateId"],
            ["LaunchTemplateName"] = (string)lt["LaunchTemplateName"],
            ["VersionNumber"] = verNum, ["VersionDescription"] = verDesc,
            ["DefaultVersion"] = verNum == (int)lt["DefaultVersionNumber"],
            ["CreateTime"] = now, ["LaunchTemplateData"] = ltData,
        };
        ((List<Dictionary<string, object>>)lt["Versions"]).Add(version);
        lt["LatestVersionNumber"] = verNum;
        return Xml(200, "CreateLaunchTemplateVersionResponse", $"<launchTemplateVersion>{LtVersionXml(version)}</launchTemplateVersion>");
    }

    private ServiceResponse DescribeLaunchTemplates(Dictionary<string, string[]> p)
    {
        var ltIds = ParseMemberList(p, "LaunchTemplateId");
        var ltNames = ParseMemberList(p, "LaunchTemplateName");
        var filters = ParseFilters(p);
        var sb = new StringBuilder();
        var accountId = AccountContext.GetAccountId();
        foreach (var lt in _launchTemplates.Values.ToList())
        {
            if (ltIds.Count > 0 && !ltIds.Contains((string)lt["LaunchTemplateId"])) continue;
            if (ltNames.Count > 0 && !ltNames.Contains((string)lt["LaunchTemplateName"])) continue;
            if (filters.TryGetValue("launch-template-name", out var ltnF) && !ltnF.Contains((string)lt["LaunchTemplateName"])) continue;
            var tagsSb = new StringBuilder();
            var ltTags = lt.ContainsKey("Tags")
                ? (List<Dictionary<string, string>>)lt["Tags"]
                : (_tags.TryGetValue((string)lt["LaunchTemplateId"], out var t) ? t : new List<Dictionary<string, string>>());
            foreach (var tag in ltTags)
                tagsSb.Append($"<item><key>{Esc(tag["Key"])}</key><value>{Esc(tag.GetValueOrDefault("Value", "")!)}</value></item>");
            sb.Append($"<item><launchTemplateId>{lt["LaunchTemplateId"]}</launchTemplateId><launchTemplateName>{Esc((string)lt["LaunchTemplateName"])}</launchTemplateName><createTime>{lt["CreateTime"]}</createTime><createdBy>arn:aws:iam::{accountId}:root</createdBy><defaultVersionNumber>{lt["DefaultVersionNumber"]}</defaultVersionNumber><latestVersionNumber>{lt["LatestVersionNumber"]}</latestVersionNumber><tags>{tagsSb}</tags></item>");
        }
        return Xml(200, "DescribeLaunchTemplatesResponse", $"<launchTemplates>{sb}</launchTemplates>");
    }

    private ServiceResponse DescribeLaunchTemplateVersions(Dictionary<string, string[]> p)
    {
        var ltId = P(p, "LaunchTemplateId");
        var ltName = P(p, "LaunchTemplateName");
        Dictionary<string, object>? lt = null;
        if (!string.IsNullOrEmpty(ltId))
            _launchTemplates.TryGetValue(ltId, out lt);
        else if (!string.IsNullOrEmpty(ltName))
            lt = _launchTemplates.Values.FirstOrDefault(t => (string)t["LaunchTemplateName"] == ltName);
        if (lt == null)
            return Error("InvalidLaunchTemplateId.NotFoundException", "The specified launch template does not exist", 400);
        var reqVersions = ParseMemberList(p, "LaunchTemplateVersion");
        var versions = (List<Dictionary<string, object>>)lt["Versions"];
        if (reqVersions.Count > 0)
        {
            var filtered = new List<Dictionary<string, object>>();
            foreach (var rv in reqVersions)
            {
                if (rv == "$Latest")
                {
                    var latest = versions.FirstOrDefault(v => (int)v["VersionNumber"] == (int)lt["LatestVersionNumber"]);
                    if (latest != null) filtered.Add(latest);
                }
                else if (rv == "$Default")
                {
                    var def = versions.FirstOrDefault(v => (int)v["VersionNumber"] == (int)lt["DefaultVersionNumber"]);
                    if (def != null) filtered.Add(def);
                }
                else
                {
                    var match = versions.FirstOrDefault(v => v["VersionNumber"].ToString() == rv);
                    if (match != null) filtered.Add(match);
                }
            }
            versions = filtered;
        }
        var sb = new StringBuilder();
        foreach (var v in versions) sb.Append(LtVersionXml(v));
        return Xml(200, "DescribeLaunchTemplateVersionsResponse", $"<launchTemplateVersionSet>{sb}</launchTemplateVersionSet>");
    }

    private ServiceResponse ModifyLaunchTemplate(Dictionary<string, string[]> p)
    {
        var ltId = P(p, "LaunchTemplateId");
        var ltName = P(p, "LaunchTemplateName");
        Dictionary<string, object>? lt = null;
        if (!string.IsNullOrEmpty(ltId))
            _launchTemplates.TryGetValue(ltId, out lt);
        else if (!string.IsNullOrEmpty(ltName))
            lt = _launchTemplates.Values.FirstOrDefault(t => (string)t["LaunchTemplateName"] == ltName);
        if (lt == null)
            return Error("InvalidLaunchTemplateId.NotFoundException", "The specified launch template does not exist", 400);
        var defaultVer = P(p, "SetDefaultVersion");
        if (!string.IsNullOrEmpty(defaultVer))
        {
            var verNum = int.Parse(defaultVer);
            var versions = (List<Dictionary<string, object>>)lt["Versions"];
            if (!versions.Any(v => (int)v["VersionNumber"] == verNum))
                return Error("InvalidLaunchTemplateId.VersionNotFound", $"Version {verNum} does not exist", 400);
            lt["DefaultVersionNumber"] = verNum;
            foreach (var v in versions) v["DefaultVersion"] = (int)v["VersionNumber"] == verNum;
        }
        var accountId = AccountContext.GetAccountId();
        return Xml(200, "ModifyLaunchTemplateResponse", $"<launchTemplate><launchTemplateId>{lt["LaunchTemplateId"]}</launchTemplateId><launchTemplateName>{Esc((string)lt["LaunchTemplateName"])}</launchTemplateName><createTime>{lt["CreateTime"]}</createTime><createdBy>arn:aws:iam::{accountId}:root</createdBy><defaultVersionNumber>{lt["DefaultVersionNumber"]}</defaultVersionNumber><latestVersionNumber>{lt["LatestVersionNumber"]}</latestVersionNumber></launchTemplate>");
    }

    private ServiceResponse DeleteLaunchTemplate(Dictionary<string, string[]> p)
    {
        var ltId = P(p, "LaunchTemplateId");
        var ltName = P(p, "LaunchTemplateName");
        Dictionary<string, object>? lt = null;
        if (!string.IsNullOrEmpty(ltId))
        {
            _launchTemplates.TryGetValue(ltId, out lt);
        }
        else if (!string.IsNullOrEmpty(ltName))
        {
            lt = _launchTemplates.Values.FirstOrDefault(t => (string)t["LaunchTemplateName"] == ltName);
            if (lt != null) ltId = (string)lt["LaunchTemplateId"];
        }
        if (lt == null)
            return Error("InvalidLaunchTemplateId.NotFoundException", "The specified launch template does not exist", 400);
        _launchTemplates.TryRemove(ltId!, out _);
        _tags.TryRemove(ltId!, out _);
        return Xml(200, "DeleteLaunchTemplateResponse", $"<launchTemplate><launchTemplateId>{lt["LaunchTemplateId"]}</launchTemplateId><launchTemplateName>{Esc((string)lt["LaunchTemplateName"])}</launchTemplateName><createTime>{lt["CreateTime"]}</createTime><defaultVersionNumber>{lt["DefaultVersionNumber"]}</defaultVersionNumber><latestVersionNumber>{lt["LatestVersionNumber"]}</latestVersionNumber></launchTemplate>");
    }

    // ── XML helpers ───────────────────────────────────────────────────────────

    private string InstanceXml(Dictionary<string, object> inst)
    {
        var sgs = new StringBuilder();
        foreach (var sg in (List<Dictionary<string, string>>)inst["SecurityGroups"])
            sgs.Append($"<item><groupId>{sg["GroupId"]}</groupId><groupName>{sg["GroupName"]}</groupName></item>");
        var tagsSb = new StringBuilder();
        if (_tags.TryGetValue((string)inst["InstanceId"], out var instTags))
        {
            foreach (var t in instTags)
                tagsSb.Append($"<item><key>{Esc(t["Key"])}</key><value>{Esc(t["Value"])}</value></item>");
        }
        var state = (Dictionary<string, object>)inst["State"];
        var placement = (Dictionary<string, object>)inst["Placement"];
        var monitoring = (Dictionary<string, object>)inst["Monitoring"];
        return $"<item><instanceId>{inst["InstanceId"]}</instanceId><imageId>{inst["ImageId"]}</imageId><instanceState><code>{state["Code"]}</code><name>{state["Name"]}</name></instanceState><instanceType>{inst["InstanceType"]}</instanceType><keyName>{inst.GetValueOrDefault("KeyName", "")}</keyName><launchTime>{inst["LaunchTime"]}</launchTime><placement><availabilityZone>{placement["AvailabilityZone"]}</availabilityZone><tenancy>{placement["Tenancy"]}</tenancy></placement><privateDnsName>{inst["PrivateDnsName"]}</privateDnsName><privateIpAddress>{inst["PrivateIpAddress"]}</privateIpAddress><publicDnsName>{inst["PublicDnsName"]}</publicDnsName><publicIpAddress>{inst["PublicIpAddress"]}</publicIpAddress><subnetId>{inst["SubnetId"]}</subnetId><vpcId>{inst["VpcId"]}</vpcId><architecture>{inst["Architecture"]}</architecture><rootDeviceType>{inst["RootDeviceType"]}</rootDeviceType><rootDeviceName>{inst["RootDeviceName"]}</rootDeviceName><virtualizationType>{inst["Virtualization"]}</virtualizationType><hypervisor>{inst["Hypervisor"]}</hypervisor><monitoring><state>{monitoring["State"]}</state></monitoring><groupSet>{sgs}</groupSet><tagSet>{tagsSb}</tagSet><amiLaunchIndex>{inst["AmiLaunchIndex"]}</amiLaunchIndex></item>";
    }

    private static string SgXml(Dictionary<string, object> sg)
    {
        var ingress = new StringBuilder();
        foreach (var r in (List<Dictionary<string, object>>)sg["IpPermissions"])
            ingress.Append(PermXml(r));
        var egress = new StringBuilder();
        foreach (var r in (List<Dictionary<string, object>>)sg["IpPermissionsEgress"])
            egress.Append(PermXml(r));
        return $"<item><ownerId>{sg["OwnerId"]}</ownerId><groupId>{sg["GroupId"]}</groupId><groupName>{sg["GroupName"]}</groupName><groupDescription>{sg["Description"]}</groupDescription><vpcId>{sg["VpcId"]}</vpcId><ipPermissions>{ingress}</ipPermissions><ipPermissionsEgress>{egress}</ipPermissionsEgress><tagSet/></item>";
    }

    private static string PermXml(Dictionary<string, object> r)
    {
        var ranges = new StringBuilder();
        foreach (var ip in (List<Dictionary<string, string>>)r.GetValueOrDefault("IpRanges", new List<Dictionary<string, string>>())!)
            ranges.Append($"<item><cidrIp>{ip["CidrIp"]}</cidrIp></item>");
        var fromPort = r.ContainsKey("FromPort") ? $"<fromPort>{r["FromPort"]}</fromPort>" : "";
        var toPort = r.ContainsKey("ToPort") ? $"<toPort>{r["ToPort"]}</toPort>" : "";
        return $"<item><ipProtocol>{r.GetValueOrDefault("IpProtocol", "-1")}</ipProtocol>{fromPort}{toPort}<ipRanges>{ranges}</ipRanges><ipv6Ranges/><prefixListIds/><groups/></item>";
    }

    private static string VpcXml(Dictionary<string, object> vpc)
    {
        return VpcXml(vpc, "item");
    }

    private static string VpcXml(Dictionary<string, object> vpc, string tag)
    {
        var isDefault = (bool)vpc["IsDefault"] ? "true" : "false";
        var extra = new StringBuilder();
        if (vpc.TryGetValue("DefaultNetworkAclId", out var dna) && !string.IsNullOrEmpty((string)dna))
            extra.Append($"<defaultNetworkAclId>{dna}</defaultNetworkAclId>");
        if (vpc.TryGetValue("DefaultSecurityGroupId", out var dsg) && !string.IsNullOrEmpty((string)dsg))
            extra.Append($"<defaultSecurityGroupId>{dsg}</defaultSecurityGroupId>");
        if (vpc.TryGetValue("MainRouteTableId", out var mrt) && !string.IsNullOrEmpty((string)mrt))
            extra.Append($"<mainRouteTableId>{mrt}</mainRouteTableId>");
        return $"<{tag}><vpcId>{vpc["VpcId"]}</vpcId><state>{vpc["State"]}</state><cidrBlock>{vpc["CidrBlock"]}</cidrBlock><dhcpOptionsId>{vpc["DhcpOptionsId"]}</dhcpOptionsId><instanceTenancy>{vpc["InstanceTenancy"]}</instanceTenancy><isDefault>{isDefault}</isDefault><ownerId>{vpc["OwnerId"]}</ownerId>{extra}<tagSet/></{tag}>";
    }

    private static string SubnetXml(Dictionary<string, object> subnet)
    {
        return SubnetXml(subnet, "item");
    }

    private static string SubnetXml(Dictionary<string, object> subnet, string tag)
    {
        var region = MicroStackOptions.Instance.Region;
        var accountId = AccountContext.GetAccountId();
        var defaultForAz = (bool)subnet["DefaultForAz"] ? "true" : "false";
        var mapPublic = (bool)subnet["MapPublicIpOnLaunch"] ? "true" : "false";
        return $"<{tag}><subnetId>{subnet["SubnetId"]}</subnetId><subnetArn>arn:aws:ec2:{region}:{accountId}:subnet/{subnet["SubnetId"]}</subnetArn><state>{subnet["State"]}</state><vpcId>{subnet["VpcId"]}</vpcId><cidrBlock>{subnet["CidrBlock"]}</cidrBlock><availableIpAddressCount>{subnet["AvailableIpAddressCount"]}</availableIpAddressCount><availabilityZone>{subnet["AvailabilityZone"]}</availabilityZone><defaultForAz>{defaultForAz}</defaultForAz><mapPublicIpOnLaunch>{mapPublic}</mapPublicIpOnLaunch><ownerId>{subnet["OwnerId"]}</ownerId><tagSet/></{tag}>";
    }

    private static string IgwXml(Dictionary<string, object> igw)
    {
        return IgwXml(igw, "item");
    }

    private static string IgwXml(Dictionary<string, object> igw, string tag)
    {
        var attachments = new StringBuilder();
        foreach (var a in (List<Dictionary<string, string>>)igw["Attachments"])
            attachments.Append($"<item><vpcId>{a["VpcId"]}</vpcId><state>{a["State"]}</state></item>");
        return $"<{tag}><internetGatewayId>{igw["InternetGatewayId"]}</internetGatewayId><ownerId>{igw["OwnerId"]}</ownerId><attachmentSet>{attachments}</attachmentSet><tagSet/></{tag}>";
    }

    private static string RtbXml(Dictionary<string, object> rtb)
    {
        return RtbXml(rtb, "item");
    }

    private static string RtbXml(Dictionary<string, object> rtb, string tag)
    {
        var routes = (List<Dictionary<string, string>>)rtb["Routes"];
        var routesSb = new StringBuilder();
        foreach (var r in routes)
        {
            var target = new StringBuilder();
            if (r.TryGetValue("GatewayId", out var gw) && !string.IsNullOrEmpty(gw))
                target.Append($"<gatewayId>{gw}</gatewayId>");
            if (r.TryGetValue("NatGatewayId", out var nat) && !string.IsNullOrEmpty(nat))
                target.Append($"<natGatewayId>{nat}</natGatewayId>");
            if (r.TryGetValue("InstanceId", out var iid) && !string.IsNullOrEmpty(iid))
                target.Append($"<instanceId>{iid}</instanceId>");
            if (r.TryGetValue("VpcPeeringConnectionId", out var pcx) && !string.IsNullOrEmpty(pcx))
                target.Append($"<vpcPeeringConnectionId>{pcx}</vpcPeeringConnectionId>");
            if (r.TryGetValue("TransitGatewayId", out var tgw) && !string.IsNullOrEmpty(tgw))
                target.Append($"<transitGatewayId>{tgw}</transitGatewayId>");
            routesSb.Append($"<item><destinationCidrBlock>{r.GetValueOrDefault("DestinationCidrBlock", "")}</destinationCidrBlock>{target}<state>{r.GetValueOrDefault("State", "active")}</state><origin>{r.GetValueOrDefault("Origin", "")}</origin></item>");
        }
        var assocs = (List<Dictionary<string, object>>)rtb["Associations"];
        var assocsSb = new StringBuilder();
        foreach (var a in assocs)
        {
            var subnetPart = a.TryGetValue("SubnetId", out var sn) && !string.IsNullOrEmpty(sn as string) ? $"<subnetId>{sn}</subnetId>" : "";
            var isMain = a.ContainsKey("Main") && (bool)a["Main"] ? "true" : "false";
            assocsSb.Append($"<item><routeTableAssociationId>{a["RouteTableAssociationId"]}</routeTableAssociationId><routeTableId>{a["RouteTableId"]}</routeTableId><main>{isMain}</main>{subnetPart}<associationState><state>associated</state></associationState></item>");
        }
        return $"<{tag}><routeTableId>{rtb["RouteTableId"]}</routeTableId><vpcId>{rtb["VpcId"]}</vpcId><ownerId>{rtb["OwnerId"]}</ownerId><routeSet>{routesSb}</routeSet><associationSet>{assocsSb}</associationSet><propagatingVgwSet/><tagSet/></{tag}>";
    }

    private static string EniXml(Dictionary<string, object> eni)
    {
        return EniXml(eni, "item");
    }

    private static string EniXml(Dictionary<string, object> eni, string tag)
    {
        var groups = new StringBuilder();
        foreach (var g in (List<Dictionary<string, string>>)eni.GetValueOrDefault("Groups", new List<Dictionary<string, string>>())!)
            groups.Append($"<item><groupId>{g["GroupId"]}</groupId><groupName>{g["GroupName"]}</groupName></item>");
        var attachment = "";
        if (eni.TryGetValue("Attachment", out var attObj) && attObj != null)
        {
            var a = (Dictionary<string, object>)attObj;
            attachment = $"<attachment><attachmentId>{a["AttachmentId"]}</attachmentId><instanceId>{a.GetValueOrDefault("InstanceId", "")}</instanceId><deviceIndex>{a.GetValueOrDefault("DeviceIndex", 0)}</deviceIndex><status>{a.GetValueOrDefault("Status", "attached")}</status></attachment>";
        }
        var privateIp = (string)eni["PrivateIpAddress"];
        var region = MicroStackOptions.Instance.Region;
        var sourceDestCheck = eni.TryGetValue("SourceDestCheck", out var sdc) && sdc is bool sdcBool ? (sdcBool ? "true" : "false") : "true";
        return $"<{tag}><networkInterfaceId>{eni["NetworkInterfaceId"]}</networkInterfaceId><subnetId>{eni["SubnetId"]}</subnetId><vpcId>{eni["VpcId"]}</vpcId><availabilityZone>{eni.GetValueOrDefault("AvailabilityZone", region + "a")}</availabilityZone><description>{eni["Description"]}</description><ownerId>{eni["OwnerId"]}</ownerId><status>{eni["Status"]}</status><privateIpAddress>{privateIp}</privateIpAddress><sourceDestCheck>{sourceDestCheck}</sourceDestCheck><interfaceType>{eni.GetValueOrDefault("InterfaceType", "interface")}</interfaceType><macAddress>{eni["MacAddress"]}</macAddress><groupSet>{groups}</groupSet><privateIpAddressesSet><item><privateIpAddress>{privateIp}</privateIpAddress><primary>true</primary></item></privateIpAddressesSet>{attachment}<tagSet/></{tag}>";
    }

    private static string VpceXml(Dictionary<string, object> ep)
    {
        return VpceXml(ep, "item");
    }

    private static string VpceXml(Dictionary<string, object> ep, string tag)
    {
        var rtbIds = new StringBuilder();
        foreach (var r in (List<string>)ep.GetValueOrDefault("RouteTableIds", new List<string>())!)
            rtbIds.Append($"<item>{r}</item>");
        var subnetIds = new StringBuilder();
        foreach (var s in (List<string>)ep.GetValueOrDefault("SubnetIds", new List<string>())!)
            subnetIds.Append($"<item>{s}</item>");
        return $"<{tag}><vpcEndpointId>{ep["VpcEndpointId"]}</vpcEndpointId><vpcEndpointType>{ep["VpcEndpointType"]}</vpcEndpointType><vpcId>{ep["VpcId"]}</vpcId><serviceName>{ep["ServiceName"]}</serviceName><state>{ep["State"]}</state><ownerId>{ep["OwnerId"]}</ownerId><routeTableIdSet>{rtbIds}</routeTableIdSet><subnetIdSet>{subnetIds}</subnetIdSet><tagSet/></{tag}>";
    }

    private static string VolumeInnerXml(Dictionary<string, object> vol)
    {
        var attachments = new StringBuilder();
        foreach (var a in (List<Dictionary<string, object>>)vol["Attachments"])
        {
            var dot = (bool)a["DeleteOnTermination"] ? "true" : "false";
            attachments.Append($"<item><volumeId>{a["VolumeId"]}</volumeId><instanceId>{a["InstanceId"]}</instanceId><device>{a["Device"]}</device><status>{a["State"]}</status><attachTime>{a["AttachTime"]}</attachTime><deleteOnTermination>{dot}</deleteOnTermination></item>");
        }
        var snap = !string.IsNullOrEmpty((string)vol["SnapshotId"]) ? $"<snapshotId>{vol["SnapshotId"]}</snapshotId>" : "<snapshotId/>";
        var iops = (int)vol["Iops"] != 0 ? $"<iops>{vol["Iops"]}</iops>" : "";
        var encrypted = (bool)vol["Encrypted"] ? "true" : "false";
        var multiAttach = (bool)vol["MultiAttachEnabled"] ? "true" : "false";
        return $"<volumeId>{vol["VolumeId"]}</volumeId><size>{vol["Size"]}</size><availabilityZone>{vol["AvailabilityZone"]}</availabilityZone><status>{vol["State"]}</status><createTime>{vol["CreateTime"]}</createTime><volumeType>{vol["VolumeType"]}</volumeType>{snap}{iops}<encrypted>{encrypted}</encrypted><multiAttachEnabled>{multiAttach}</multiAttachEnabled><attachmentSet>{attachments}</attachmentSet><tagSet/>";
    }

    private static string SnapshotInnerXml(Dictionary<string, object> snap)
    {
        var encrypted = (bool)snap["Encrypted"] ? "true" : "false";
        return $"<snapshotId>{snap["SnapshotId"]}</snapshotId><volumeId>{snap["VolumeId"]}</volumeId><status>{snap["State"]}</status><startTime>{snap["StartTime"]}</startTime><progress>{snap["Progress"]}</progress><ownerId>{snap["OwnerId"]}</ownerId><volumeSize>{snap["VolumeSize"]}</volumeSize><description>{Esc((string)snap["Description"])}</description><encrypted>{encrypted}</encrypted><storageTier>{snap["StorageTier"]}</storageTier><tagSet/>";
    }

    private static string PrefixListXml(Dictionary<string, object> pl, string tag)
    {
        var accountId = AccountContext.GetAccountId();
        return $"<{tag}><prefixListId>{pl["PrefixListId"]}</prefixListId><prefixListName>{pl.GetValueOrDefault("PrefixListName", "")}</prefixListName><state>{pl.GetValueOrDefault("State", "create-complete")}</state><addressFamily>{pl.GetValueOrDefault("AddressFamily", "IPv4")}</addressFamily><maxEntries>{pl.GetValueOrDefault("MaxEntries", 10)}</maxEntries><version>{pl.GetValueOrDefault("Version", 1)}</version><prefixListArn>{pl.GetValueOrDefault("PrefixListArn", "")}</prefixListArn><ownerId>{pl.GetValueOrDefault("OwnerId", accountId)}</ownerId><tagSet/></{tag}>";
    }

    private static string VgwXml(Dictionary<string, object> vgw, string tag)
    {
        var attachments = new StringBuilder();
        foreach (var a in (List<Dictionary<string, object>>)vgw["Attachments"])
            attachments.Append($"<item><vpcId>{a["VpcId"]}</vpcId><state>{a["State"]}</state></item>");
        return $"<{tag}><vpnGatewayId>{vgw["VpnGatewayId"]}</vpnGatewayId><state>{vgw["State"]}</state><type>{vgw["Type"]}</type><availabilityZone>{vgw.GetValueOrDefault("AvailabilityZone", "")}</availabilityZone><amazonSideAsn>{vgw.GetValueOrDefault("AmazonSideAsn", "64512")}</amazonSideAsn><attachments>{attachments}</attachments><tagSet/></{tag}>";
    }

    private static string CgwXml(Dictionary<string, object> cgw, string tag)
    {
        return $"<{tag}><customerGatewayId>{cgw["CustomerGatewayId"]}</customerGatewayId><bgpAsn>{cgw["BgpAsn"]}</bgpAsn><ipAddress>{cgw["IpAddress"]}</ipAddress><type>{cgw["Type"]}</type><state>{cgw["State"]}</state><tagSet/></{tag}>";
    }

    // ── Launch Template helpers ───────────────────────────────────────────────

    private static Dictionary<string, object> ParseLtData(Dictionary<string, string[]> p, string prefix)
    {
        var data = new Dictionary<string, object>();
        var img = P(p, $"{prefix}.ImageId");
        if (!string.IsNullOrEmpty(img)) data["ImageId"] = img;
        var itype = P(p, $"{prefix}.InstanceType");
        if (!string.IsNullOrEmpty(itype)) data["InstanceType"] = itype;
        var key = P(p, $"{prefix}.KeyName");
        if (!string.IsNullOrEmpty(key)) data["KeyName"] = key;
        var ud = P(p, $"{prefix}.UserData");
        if (!string.IsNullOrEmpty(ud)) data["UserData"] = ud;
        // Security group IDs
        var sgIds = new List<string>();
        for (var i = 1; ; i++)
        {
            var sg = P(p, $"{prefix}.SecurityGroupId.{i}");
            if (string.IsNullOrEmpty(sg)) break;
            sgIds.Add(sg);
        }
        if (sgIds.Count > 0) data["SecurityGroupIds"] = sgIds;
        // Security groups by name
        var sgNames = new List<string>();
        for (var i = 1; ; i++)
        {
            var sg = P(p, $"{prefix}.SecurityGroup.{i}");
            if (string.IsNullOrEmpty(sg)) break;
            sgNames.Add(sg);
        }
        if (sgNames.Count > 0) data["SecurityGroups"] = sgNames;
        // Block device mappings
        var bdms = new List<Dictionary<string, object>>();
        for (var i = 1; ; i++)
        {
            var dev = P(p, $"{prefix}.BlockDeviceMapping.{i}.DeviceName");
            if (string.IsNullOrEmpty(dev)) break;
            var bdm = new Dictionary<string, object> { ["DeviceName"] = dev };
            var ebs = new Dictionary<string, object>();
            var volSize = P(p, $"{prefix}.BlockDeviceMapping.{i}.Ebs.VolumeSize");
            if (!string.IsNullOrEmpty(volSize)) ebs["VolumeSize"] = int.Parse(volSize);
            var volType = P(p, $"{prefix}.BlockDeviceMapping.{i}.Ebs.VolumeType");
            if (!string.IsNullOrEmpty(volType)) ebs["VolumeType"] = volType;
            var encr = P(p, $"{prefix}.BlockDeviceMapping.{i}.Ebs.Encrypted");
            if (!string.IsNullOrEmpty(encr)) ebs["Encrypted"] = encr.Equals("true", StringComparison.OrdinalIgnoreCase);
            var delOn = P(p, $"{prefix}.BlockDeviceMapping.{i}.Ebs.DeleteOnTermination");
            if (!string.IsNullOrEmpty(delOn)) ebs["DeleteOnTermination"] = delOn.Equals("true", StringComparison.OrdinalIgnoreCase);
            var snapId = P(p, $"{prefix}.BlockDeviceMapping.{i}.Ebs.SnapshotId");
            if (!string.IsNullOrEmpty(snapId)) ebs["SnapshotId"] = snapId;
            var iopsStr = P(p, $"{prefix}.BlockDeviceMapping.{i}.Ebs.Iops");
            if (!string.IsNullOrEmpty(iopsStr)) ebs["Iops"] = int.Parse(iopsStr);
            var throughputStr = P(p, $"{prefix}.BlockDeviceMapping.{i}.Ebs.Throughput");
            if (!string.IsNullOrEmpty(throughputStr)) ebs["Throughput"] = int.Parse(throughputStr);
            if (ebs.Count > 0) bdm["Ebs"] = ebs;
            bdms.Add(bdm);
        }
        if (bdms.Count > 0) data["BlockDeviceMappings"] = bdms;
        // Network interfaces
        var nis = new List<Dictionary<string, object>>();
        for (var i = 1; ; i++)
        {
            var devIdx = P(p, $"{prefix}.NetworkInterface.{i}.DeviceIndex");
            var sub = P(p, $"{prefix}.NetworkInterface.{i}.SubnetId");
            if (string.IsNullOrEmpty(devIdx) && string.IsNullOrEmpty(sub)) break;
            var ni = new Dictionary<string, object>();
            if (!string.IsNullOrEmpty(devIdx)) ni["DeviceIndex"] = int.Parse(devIdx);
            if (!string.IsNullOrEmpty(sub)) ni["SubnetId"] = sub;
            var assocPub = P(p, $"{prefix}.NetworkInterface.{i}.AssociatePublicIpAddress");
            if (!string.IsNullOrEmpty(assocPub)) ni["AssociatePublicIpAddress"] = assocPub.Equals("true", StringComparison.OrdinalIgnoreCase);
            var desc = P(p, $"{prefix}.NetworkInterface.{i}.Description");
            if (!string.IsNullOrEmpty(desc)) ni["Description"] = desc;
            var niGroups = new List<string>();
            for (var j = 1; ; j++)
            {
                var g = P(p, $"{prefix}.NetworkInterface.{i}.Groups.SecurityGroupId.{j}");
                if (string.IsNullOrEmpty(g)) g = P(p, $"{prefix}.NetworkInterface.{i}.SecurityGroupId.{j}");
                if (string.IsNullOrEmpty(g)) break;
                niGroups.Add(g);
            }
            if (niGroups.Count > 0) ni["Groups"] = niGroups;
            nis.Add(ni);
        }
        if (nis.Count > 0) data["NetworkInterfaces"] = nis;
        // IamInstanceProfile
        var iamArn = P(p, $"{prefix}.IamInstanceProfile.Arn");
        var iamName = P(p, $"{prefix}.IamInstanceProfile.Name");
        if (!string.IsNullOrEmpty(iamArn) || !string.IsNullOrEmpty(iamName))
        {
            var iip = new Dictionary<string, object>();
            if (!string.IsNullOrEmpty(iamArn)) iip["Arn"] = iamArn;
            if (!string.IsNullOrEmpty(iamName)) iip["Name"] = iamName;
            data["IamInstanceProfile"] = iip;
        }
        // TagSpecifications
        var tagSpecs = new List<Dictionary<string, object>>();
        for (var i = 1; ; i++)
        {
            var rtype = P(p, $"{prefix}.TagSpecification.{i}.ResourceType");
            if (string.IsNullOrEmpty(rtype)) break;
            var ts = new Dictionary<string, object> { ["ResourceType"] = rtype, ["Tags"] = new List<Dictionary<string, string>>() };
            for (var j = 1; ; j++)
            {
                var tk = P(p, $"{prefix}.TagSpecification.{i}.Tag.{j}.Key");
                if (string.IsNullOrEmpty(tk)) break;
                ((List<Dictionary<string, string>>)ts["Tags"]).Add(new Dictionary<string, string> { ["Key"] = tk, ["Value"] = P(p, $"{prefix}.TagSpecification.{i}.Tag.{j}.Value") });
            }
            tagSpecs.Add(ts);
        }
        if (tagSpecs.Count > 0) data["TagSpecifications"] = tagSpecs;
        // Monitoring
        var monitoring = P(p, $"{prefix}.Monitoring.Enabled");
        if (!string.IsNullOrEmpty(monitoring))
            data["Monitoring"] = new Dictionary<string, object> { ["Enabled"] = monitoring.Equals("true", StringComparison.OrdinalIgnoreCase) };
        // DisableApiTermination
        var disableApi = P(p, $"{prefix}.DisableApiTermination");
        if (!string.IsNullOrEmpty(disableApi))
            data["DisableApiTermination"] = disableApi.Equals("true", StringComparison.OrdinalIgnoreCase);
        // EbsOptimized
        var ebsOpt = P(p, $"{prefix}.EbsOptimized");
        if (!string.IsNullOrEmpty(ebsOpt))
            data["EbsOptimized"] = ebsOpt.Equals("true", StringComparison.OrdinalIgnoreCase);
        return data;
    }

    private static string LtDataXml(Dictionary<string, object> data)
    {
        var sb = new StringBuilder();
        if (data.TryGetValue("ImageId", out var img)) sb.Append($"<imageId>{Esc((string)img)}</imageId>");
        if (data.TryGetValue("InstanceType", out var it)) sb.Append($"<instanceType>{Esc((string)it)}</instanceType>");
        if (data.TryGetValue("KeyName", out var kn)) sb.Append($"<keyName>{Esc((string)kn)}</keyName>");
        if (data.TryGetValue("UserData", out var ud)) sb.Append($"<userData>{Esc((string)ud)}</userData>");
        if (data.TryGetValue("EbsOptimized", out var eo)) sb.Append($"<ebsOptimized>{((bool)eo ? "true" : "false")}</ebsOptimized>");
        if (data.TryGetValue("DisableApiTermination", out var dat)) sb.Append($"<disableApiTermination>{((bool)dat ? "true" : "false")}</disableApiTermination>");
        if (data.TryGetValue("SecurityGroupIds", out var sgi))
        {
            var inner = new StringBuilder();
            foreach (var s in (List<string>)sgi) inner.Append($"<item>{Esc(s)}</item>");
            sb.Append($"<securityGroupIdSet>{inner}</securityGroupIdSet>");
        }
        if (data.TryGetValue("SecurityGroups", out var sgn))
        {
            var inner = new StringBuilder();
            foreach (var s in (List<string>)sgn) inner.Append($"<item>{Esc(s)}</item>");
            sb.Append($"<securityGroupSet>{inner}</securityGroupSet>");
        }
        if (data.TryGetValue("BlockDeviceMappings", out var bdmsObj))
        {
            var inner = new StringBuilder();
            foreach (var bdm in (List<Dictionary<string, object>>)bdmsObj)
            {
                inner.Append($"<item><deviceName>{Esc((string)bdm["DeviceName"])}</deviceName>");
                if (bdm.TryGetValue("Ebs", out var ebsObj))
                {
                    var ebs = (Dictionary<string, object>)ebsObj;
                    inner.Append("<ebs>");
                    if (ebs.TryGetValue("VolumeSize", out var vs)) inner.Append($"<volumeSize>{vs}</volumeSize>");
                    if (ebs.TryGetValue("VolumeType", out var vt)) inner.Append($"<volumeType>{Esc((string)vt)}</volumeType>");
                    if (ebs.TryGetValue("Encrypted", out var enc)) inner.Append($"<encrypted>{((bool)enc ? "true" : "false")}</encrypted>");
                    if (ebs.TryGetValue("DeleteOnTermination", out var del)) inner.Append($"<deleteOnTermination>{((bool)del ? "true" : "false")}</deleteOnTermination>");
                    if (ebs.TryGetValue("SnapshotId", out var sn)) inner.Append($"<snapshotId>{Esc((string)sn)}</snapshotId>");
                    if (ebs.TryGetValue("Iops", out var iopsVal)) inner.Append($"<iops>{iopsVal}</iops>");
                    if (ebs.TryGetValue("Throughput", out var tp)) inner.Append($"<throughput>{tp}</throughput>");
                    inner.Append("</ebs>");
                }
                inner.Append("</item>");
            }
            sb.Append($"<blockDeviceMappingSet>{inner}</blockDeviceMappingSet>");
        }
        if (data.TryGetValue("NetworkInterfaces", out var nisObj))
        {
            var inner = new StringBuilder();
            foreach (var ni in (List<Dictionary<string, object>>)nisObj)
            {
                inner.Append("<item>");
                if (ni.TryGetValue("DeviceIndex", out var di)) inner.Append($"<deviceIndex>{di}</deviceIndex>");
                if (ni.TryGetValue("SubnetId", out var sn)) inner.Append($"<subnetId>{Esc((string)sn)}</subnetId>");
                if (ni.TryGetValue("AssociatePublicIpAddress", out var ap)) inner.Append($"<associatePublicIpAddress>{((bool)ap ? "true" : "false")}</associatePublicIpAddress>");
                if (ni.TryGetValue("Description", out var d)) inner.Append($"<description>{Esc((string)d)}</description>");
                if (ni.TryGetValue("Groups", out var g))
                {
                    var gi = new StringBuilder();
                    foreach (var gId in (List<string>)g) gi.Append($"<item>{Esc(gId)}</item>");
                    inner.Append($"<groupSet>{gi}</groupSet>");
                }
                inner.Append("</item>");
            }
            sb.Append($"<networkInterfaceSet>{inner}</networkInterfaceSet>");
        }
        if (data.TryGetValue("IamInstanceProfile", out var iipObj))
        {
            var iip = (Dictionary<string, object>)iipObj;
            sb.Append("<iamInstanceProfile>");
            if (iip.TryGetValue("Arn", out var arn)) sb.Append($"<arn>{Esc((string)arn)}</arn>");
            if (iip.TryGetValue("Name", out var n)) sb.Append($"<name>{Esc((string)n)}</name>");
            sb.Append("</iamInstanceProfile>");
        }
        if (data.TryGetValue("TagSpecifications", out var tsObj))
        {
            var inner = new StringBuilder();
            foreach (var ts in (List<Dictionary<string, object>>)tsObj)
            {
                inner.Append($"<item><resourceType>{Esc((string)ts["ResourceType"])}</resourceType><tagSet>");
                foreach (var t in (List<Dictionary<string, string>>)ts["Tags"])
                    inner.Append($"<item><key>{Esc(t["Key"])}</key><value>{Esc(t.GetValueOrDefault("Value", "")!)}</value></item>");
                inner.Append("</tagSet></item>");
            }
            sb.Append($"<tagSpecificationSet>{inner}</tagSpecificationSet>");
        }
        if (data.TryGetValue("Monitoring", out var monObj))
        {
            var mon = (Dictionary<string, object>)monObj;
            var enabled = mon.TryGetValue("Enabled", out var e) && (bool)e ? "true" : "false";
            sb.Append($"<monitoring><enabled>{enabled}</enabled></monitoring>");
        }
        return sb.ToString();
    }

    private static string LtVersionXml(Dictionary<string, object> ver)
    {
        var accountId = AccountContext.GetAccountId();
        var ltData = ver.TryGetValue("LaunchTemplateData", out var ld) ? LtDataXml((Dictionary<string, object>)ld) : "";
        var isDefault = ver.TryGetValue("DefaultVersion", out var dv) && (bool)dv ? "true" : "false";
        return $"<item><launchTemplateId>{Esc((string)ver["LaunchTemplateId"])}</launchTemplateId><launchTemplateName>{Esc((string)ver["LaunchTemplateName"])}</launchTemplateName><versionNumber>{ver["VersionNumber"]}</versionNumber><versionDescription>{Esc((string)ver.GetValueOrDefault("VersionDescription", "")!)}</versionDescription><defaultVersion>{isDefault}</defaultVersion><createTime>{ver["CreateTime"]}</createTime><createdBy>arn:aws:iam::{accountId}:root</createdBy><launchTemplateData>{ltData}</launchTemplateData></item>";
    }

    // ── Parse helpers ─────────────────────────────────────────────────────────

    private static Dictionary<string, string[]> ParseParams(ServiceRequest request)
    {
        var result = new Dictionary<string, string[]>(StringComparer.Ordinal);
        if (request.Body.Length == 0) return result;
        var body = System.Text.Encoding.UTF8.GetString(request.Body);
        if (string.IsNullOrEmpty(body)) return result;

        foreach (var pair in body.Split('&'))
        {
            var eqIdx = pair.IndexOf('=');
            if (eqIdx < 0) continue;
            var key = HttpUtility.UrlDecode(pair[..eqIdx]);
            var val = HttpUtility.UrlDecode(pair[(eqIdx + 1)..]);
            if (result.TryGetValue(key, out var existing))
            {
                var newArr = new string[existing.Length + 1];
                existing.CopyTo(newArr, 0);
                newArr[existing.Length] = val;
                result[key] = newArr;
            }
            else
            {
                result[key] = [val];
            }
        }
        return result;
    }

    private static string P(Dictionary<string, string[]> p, string key)
    {
        return p.TryGetValue(key, out var vals) && vals.Length > 0 ? vals[0] : "";
    }

    private static string P(Dictionary<string, string[]> p, string key, string defaultValue)
    {
        var val = P(p, key);
        return string.IsNullOrEmpty(val) ? defaultValue : val;
    }

    private static List<string> ParseMemberList(Dictionary<string, string[]> p, string prefix)
    {
        var items = new List<string>();
        for (var i = 1; ; i++)
        {
            var val = P(p, $"{prefix}.{i}");
            if (string.IsNullOrEmpty(val)) break;
            items.Add(val);
        }
        return items;
    }

    private static Dictionary<string, List<string>> ParseFilters(Dictionary<string, string[]> p)
    {
        var filters = new Dictionary<string, List<string>>(StringComparer.Ordinal);
        for (var i = 1; ; i++)
        {
            var name = P(p, $"Filter.{i}.Name");
            if (string.IsNullOrEmpty(name)) break;
            var vals = new List<string>();
            for (var j = 1; ; j++)
            {
                var v = P(p, $"Filter.{i}.Value.{j}");
                if (string.IsNullOrEmpty(v)) break;
                vals.Add(v);
            }
            filters[name] = vals;
        }
        return filters;
    }

    private static List<Dictionary<string, object>> ParseIpPermissions(Dictionary<string, string[]> p, string prefix)
    {
        var rules = new List<Dictionary<string, object>>();
        for (var i = 1; ; i++)
        {
            var proto = P(p, $"{prefix}.{i}.IpProtocol");
            if (string.IsNullOrEmpty(proto)) break;
            var rule = new Dictionary<string, object>
            {
                ["IpProtocol"] = proto,
                ["IpRanges"] = new List<Dictionary<string, string>>(),
                ["Ipv6Ranges"] = new List<Dictionary<string, string>>(),
                ["PrefixListIds"] = new List<string>(),
                ["UserIdGroupPairs"] = new List<Dictionary<string, string>>(),
            };
            var fromPort = P(p, $"{prefix}.{i}.FromPort");
            if (!string.IsNullOrEmpty(fromPort)) rule["FromPort"] = int.Parse(fromPort);
            var toPort = P(p, $"{prefix}.{i}.ToPort");
            if (!string.IsNullOrEmpty(toPort)) rule["ToPort"] = int.Parse(toPort);
            for (var j = 1; ; j++)
            {
                var cidr = P(p, $"{prefix}.{i}.IpRanges.{j}.CidrIp");
                if (string.IsNullOrEmpty(cidr)) break;
                ((List<Dictionary<string, string>>)rule["IpRanges"]).Add(new Dictionary<string, string> { ["CidrIp"] = cidr });
            }
            rules.Add(rule);
        }
        return rules;
    }

    private static List<Dictionary<string, string>> ParseTagParams(Dictionary<string, string[]> p)
    {
        var tags = new List<Dictionary<string, string>>();
        for (var i = 1; ; i++)
        {
            var key = P(p, $"Tag.{i}.Key");
            if (string.IsNullOrEmpty(key)) break;
            tags.Add(new Dictionary<string, string> { ["Key"] = key, ["Value"] = P(p, $"Tag.{i}.Value") });
        }
        return tags;
    }

    private static bool MatchesInstanceFilters(Dictionary<string, object> inst, Dictionary<string, List<string>> filters)
    {
        foreach (var (name, vals) in filters)
        {
            if (name == "instance-state-name")
            {
                var state = (Dictionary<string, object>)inst["State"];
                if (!vals.Contains((string)state["Name"])) return false;
            }
            else if (name == "instance-type")
            {
                if (!vals.Contains((string)inst["InstanceType"])) return false;
            }
            else if (name == "image-id")
            {
                if (!vals.Contains((string)inst["ImageId"])) return false;
            }
        }
        return true;
    }

    // ── Utility helpers ───────────────────────────────────────────────────────

    private static string NewId(string prefix)
    {
        return prefix + Guid.NewGuid().ToString("N")[..17];
    }

    private static string RandomIp(string prefix)
    {
        return $"{prefix}{Random.Shared.Next(1, 255)}.{Random.Shared.Next(1, 255)}";
    }

    private static string RandomFingerprint()
    {
        var bytes = new byte[20];
        Random.Shared.NextBytes(bytes);
        return string.Join(":", bytes.Select(b => b.ToString("x2")));
    }

    private static string RandomMac()
    {
        var bytes = new byte[6];
        Random.Shared.NextBytes(bytes);
        return string.Join(":", bytes.Select(b => b.ToString("x2")));
    }

    private static string NowTs()
    {
        return DateTime.UtcNow.ToString("yyyy-MM-ddTHH:mm:ss.000Z");
    }

    private static string GuessResourceType(string resourceId)
    {
        ReadOnlySpan<(string Prefix, string Type)> prefixMap =
        [
            ("i-", "instance"), ("sg-", "security-group"), ("vpc-", "vpc"),
            ("subnet-", "subnet"), ("igw-", "internet-gateway"), ("eipalloc-", "elastic-ip"),
            ("rtb-", "route-table"), ("eni-", "network-interface"), ("vpce-", "vpc-endpoint"),
            ("vol-", "volume"), ("snap-", "snapshot"), ("acl-", "network-acl"),
            ("nat-", "natgateway"), ("dopt-", "dhcp-options"), ("eigw-", "egress-only-internet-gateway"),
            ("lt-", "launch-template"), ("pl-", "managed-prefix-list"), ("vgw-", "vpn-gateway"),
            ("cgw-", "customer-gateway"), ("ami-", "image"), ("tgw-", "transit-gateway"),
        ];
        foreach (var (prefix, type) in prefixMap)
        {
            if (resourceId.StartsWith(prefix, StringComparison.Ordinal)) return type;
        }
        return "resource";
    }

    private static string Esc(string value)
    {
        if (string.IsNullOrEmpty(value)) return "";
        return value
            .Replace("&", "&amp;")
            .Replace("<", "&lt;")
            .Replace(">", "&gt;")
            .Replace("\"", "&quot;")
            .Replace("'", "&apos;");
    }

    private static bool PermEquals(Dictionary<string, object> a, Dictionary<string, object> b)
    {
        if ((string)a.GetValueOrDefault("IpProtocol", "")! != (string)b.GetValueOrDefault("IpProtocol", "")!) return false;
        var aRanges = (List<Dictionary<string, string>>)a.GetValueOrDefault("IpRanges", new List<Dictionary<string, string>>())!;
        var bRanges = (List<Dictionary<string, string>>)b.GetValueOrDefault("IpRanges", new List<Dictionary<string, string>>())!;
        if (aRanges.Count != bRanges.Count) return false;
        for (var i = 0; i < aRanges.Count; i++)
        {
            if (aRanges[i].GetValueOrDefault("CidrIp", "") != bRanges[i].GetValueOrDefault("CidrIp", "")) return false;
        }
        return true;
    }

    // ── XML / Error response builders ─────────────────────────────────────────

    private static readonly IReadOnlyDictionary<string, string> XmlHeaders =
        new Dictionary<string, string> { ["Content-Type"] = "application/xml" };

    private static ServiceResponse Xml(int status, string rootTag, string inner)
    {
        var requestId = Guid.NewGuid().ToString();
        var body = $"<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<{rootTag} xmlns=\"{Ec2Ns}\">\n    {inner}\n    <requestId>{requestId}</requestId>\n</{rootTag}>";
        return new ServiceResponse(status, XmlHeaders, System.Text.Encoding.UTF8.GetBytes(body));
    }

    private static ServiceResponse Error(string code, string message, int status)
    {
        var requestId = Guid.NewGuid().ToString();
        var body = $"<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<Response>\n    <Errors><Error>\n        <Code>{code}</Code>\n        <Message>{message}</Message>\n    </Error></Errors>\n    <RequestID>{requestId}</RequestID>\n</Response>";
        return new ServiceResponse(status, XmlHeaders, System.Text.Encoding.UTF8.GetBytes(body));
    }
}