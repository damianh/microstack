using Amazon;
using Amazon.Runtime;
using Amazon.WAFV2;
using Amazon.WAFV2.Model;

namespace MicroStack.Tests;

/// <summary>
/// Integration tests for the WAF v2 service handler.
/// Uses the AWS SDK for .NET pointed at the in-process MicroStack server.
///
/// Mirrors coverage from ministack/tests/test_waf.py.
/// </summary>
public sealed class WafTests : IClassFixture<MicroStackFixture>, IAsyncLifetime
{
    private readonly MicroStackFixture _fixture;
    private readonly AmazonWAFV2Client _waf;

    public WafTests(MicroStackFixture fixture)
    {
        _fixture = fixture;
        _waf = CreateWafClient(fixture);
    }

    private static AmazonWAFV2Client CreateWafClient(MicroStackFixture fixture)
    {
        var innerHandler = fixture.Factory.Server.CreateHandler();
        var httpClient = new HttpClient(new CanonicalizeUriHandler(innerHandler))
        {
            BaseAddress = new Uri("http://localhost/"),
        };

        var config = new AmazonWAFV2Config
        {
            RegionEndpoint = RegionEndpoint.USEast1,
            ServiceURL = "http://localhost/",
            HttpClientFactory = new FixedHttpClientFactory(httpClient),
        };

        return new AmazonWAFV2Client(
            new BasicAWSCredentials("test", "test"), config);
    }

    public async ValueTask InitializeAsync()
    {
        await _fixture.HttpClient.PostAsync("/_ministack/reset", null);
    }

    public ValueTask DisposeAsync()
    {
        _waf.Dispose();
        return ValueTask.CompletedTask;
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // WebACL CRUD
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task WebAclCrud()
    {
        var createResp = await _waf.CreateWebACLAsync(new CreateWebACLRequest
        {
            Name = "test-acl",
            Scope = "REGIONAL",
            DefaultAction = new DefaultAction { Allow = new AllowAction() },
            VisibilityConfig = new VisibilityConfig
            {
                SampledRequestsEnabled = true,
                CloudWatchMetricsEnabled = false,
                MetricName = "test",
            },
        });

        var uid = createResp.Summary.Id;
        createResp.Summary.Name.ShouldBe("test-acl");

        var getResp = await _waf.GetWebACLAsync(new GetWebACLRequest
        {
            Name = "test-acl",
            Scope = "REGIONAL",
            Id = uid,
        });
        getResp.WebACL.Name.ShouldBe("test-acl");

        var listResp = await _waf.ListWebACLsAsync(new ListWebACLsRequest
        {
            Scope = "REGIONAL",
        });
        var ids = listResp.WebACLs.ConvertAll(a => a.Id);
        ids.ShouldContain(uid);

        await _waf.DeleteWebACLAsync(new DeleteWebACLRequest
        {
            Name = "test-acl",
            Scope = "REGIONAL",
            Id = uid,
            LockToken = createResp.Summary.LockToken,
        });

        var list2 = await _waf.ListWebACLsAsync(new ListWebACLsRequest
        {
            Scope = "REGIONAL",
        });
        var ids2 = list2.WebACLs.ConvertAll(a => a.Id);
        ids2.ShouldNotContain(uid);
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // UpdateWebACL
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task UpdateWebAcl()
    {
        var createResp = await _waf.CreateWebACLAsync(new CreateWebACLRequest
        {
            Name = "update-acl",
            Scope = "REGIONAL",
            DefaultAction = new DefaultAction { Block = new BlockAction() },
            VisibilityConfig = new VisibilityConfig
            {
                SampledRequestsEnabled = false,
                CloudWatchMetricsEnabled = false,
                MetricName = "m",
            },
        });

        var uid = createResp.Summary.Id;
        var lockToken = createResp.Summary.LockToken;

        var updateResp = await _waf.UpdateWebACLAsync(new UpdateWebACLRequest
        {
            Name = "update-acl",
            Scope = "REGIONAL",
            Id = uid,
            LockToken = lockToken,
            DefaultAction = new DefaultAction { Allow = new AllowAction() },
            VisibilityConfig = new VisibilityConfig
            {
                SampledRequestsEnabled = false,
                CloudWatchMetricsEnabled = false,
                MetricName = "m",
            },
        });

        updateResp.NextLockToken.ShouldNotBeNull();
        (updateResp.NextLockToken.Length > 0).ShouldBe(true);
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Associate / Disassociate WebACL
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task AssociateDisassociateWebAcl()
    {
        var createResp = await _waf.CreateWebACLAsync(new CreateWebACLRequest
        {
            Name = "assoc-acl",
            Scope = "REGIONAL",
            DefaultAction = new DefaultAction { Allow = new AllowAction() },
            VisibilityConfig = new VisibilityConfig
            {
                SampledRequestsEnabled = false,
                CloudWatchMetricsEnabled = false,
                MetricName = "m",
            },
        });

        var aclArn = createResp.Summary.ARN;
        var resourceArn = "arn:aws:elasticloadbalancing:us-east-1:000000000000:loadbalancer/app/test/abc";

        await _waf.AssociateWebACLAsync(new AssociateWebACLRequest
        {
            WebACLArn = aclArn,
            ResourceArn = resourceArn,
        });

        var getResp = await _waf.GetWebACLForResourceAsync(new GetWebACLForResourceRequest
        {
            ResourceArn = resourceArn,
        });
        getResp.WebACL.ARN.ShouldBe(aclArn);

        await _waf.DisassociateWebACLAsync(new DisassociateWebACLRequest
        {
            ResourceArn = resourceArn,
        });

        var ex = await Should.ThrowAsync<WAFNonexistentItemException>(() =>
            _waf.GetWebACLForResourceAsync(new GetWebACLForResourceRequest
            {
                ResourceArn = resourceArn,
            }));
        ex.ShouldNotBeNull();
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // IPSet CRUD
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task IpSetCrud()
    {
        var createResp = await _waf.CreateIPSetAsync(new CreateIPSetRequest
        {
            Name = "test-ipset",
            Scope = "REGIONAL",
            IPAddressVersion = "IPV4",
            Addresses = ["1.2.3.4/32"],
        });

        var uid = createResp.Summary.Id;
        var lockToken = createResp.Summary.LockToken;

        var getResp = await _waf.GetIPSetAsync(new GetIPSetRequest
        {
            Name = "test-ipset",
            Scope = "REGIONAL",
            Id = uid,
        });
        getResp.IPSet.Addresses.ShouldContain("1.2.3.4/32");

        var updateResp = await _waf.UpdateIPSetAsync(new UpdateIPSetRequest
        {
            Name = "test-ipset",
            Scope = "REGIONAL",
            Id = uid,
            LockToken = lockToken,
            Addresses = ["5.6.7.8/32"],
        });
        updateResp.NextLockToken.ShouldNotBeNull();

        var listResp = await _waf.ListIPSetsAsync(new ListIPSetsRequest
        {
            Scope = "REGIONAL",
        });
        var ids = listResp.IPSets.ConvertAll(s => s.Id);
        ids.ShouldContain(uid);

        await _waf.DeleteIPSetAsync(new DeleteIPSetRequest
        {
            Name = "test-ipset",
            Scope = "REGIONAL",
            Id = uid,
            LockToken = updateResp.NextLockToken,
        });

        var list2 = await _waf.ListIPSetsAsync(new ListIPSetsRequest
        {
            Scope = "REGIONAL",
        });
        var ids2 = list2.IPSets.ConvertAll(s => s.Id);
        ids2.ShouldNotContain(uid);
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // RuleGroup CRUD
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task RuleGroupCrud()
    {
        var createResp = await _waf.CreateRuleGroupAsync(new CreateRuleGroupRequest
        {
            Name = "test-rg",
            Scope = "REGIONAL",
            Capacity = 100,
            VisibilityConfig = new VisibilityConfig
            {
                SampledRequestsEnabled = false,
                CloudWatchMetricsEnabled = false,
                MetricName = "m",
            },
        });

        var uid = createResp.Summary.Id;
        var lockToken = createResp.Summary.LockToken;

        var getResp = await _waf.GetRuleGroupAsync(new GetRuleGroupRequest
        {
            Name = "test-rg",
            Scope = "REGIONAL",
            Id = uid,
        });
        getResp.RuleGroup.Name.ShouldBe("test-rg");

        var updateResp = await _waf.UpdateRuleGroupAsync(new UpdateRuleGroupRequest
        {
            Name = "test-rg",
            Scope = "REGIONAL",
            Id = uid,
            LockToken = lockToken,
            VisibilityConfig = new VisibilityConfig
            {
                SampledRequestsEnabled = false,
                CloudWatchMetricsEnabled = false,
                MetricName = "m2",
            },
        });
        updateResp.NextLockToken.ShouldNotBeNull();

        var listResp = await _waf.ListRuleGroupsAsync(new ListRuleGroupsRequest
        {
            Scope = "REGIONAL",
        });
        var ids = listResp.RuleGroups.ConvertAll(r => r.Id);
        ids.ShouldContain(uid);

        await _waf.DeleteRuleGroupAsync(new DeleteRuleGroupRequest
        {
            Name = "test-rg",
            Scope = "REGIONAL",
            Id = uid,
            LockToken = updateResp.NextLockToken,
        });

        var list2 = await _waf.ListRuleGroupsAsync(new ListRuleGroupsRequest
        {
            Scope = "REGIONAL",
        });
        var ids2 = list2.RuleGroups.ConvertAll(r => r.Id);
        ids2.ShouldNotContain(uid);
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Tags
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task TagOperations()
    {
        var createResp = await _waf.CreateWebACLAsync(new CreateWebACLRequest
        {
            Name = "tag-acl",
            Scope = "REGIONAL",
            DefaultAction = new DefaultAction { Allow = new AllowAction() },
            VisibilityConfig = new VisibilityConfig
            {
                SampledRequestsEnabled = false,
                CloudWatchMetricsEnabled = false,
                MetricName = "m",
            },
            Tags = [new Tag { Key = "env", Value = "test" }],
        });

        var arn = createResp.Summary.ARN;

        var tagsResp = await _waf.ListTagsForResourceAsync(new ListTagsForResourceRequest
        {
            ResourceARN = arn,
        });
        tagsResp.TagInfoForResource.TagList.ShouldContain(t => t.Key == "env");

        await _waf.TagResourceAsync(new TagResourceRequest
        {
            ResourceARN = arn,
            Tags = [new Tag { Key = "team", Value = "security" }],
        });

        var tags2 = await _waf.ListTagsForResourceAsync(new ListTagsForResourceRequest
        {
            ResourceARN = arn,
        });
        tags2.TagInfoForResource.TagList.ShouldContain(t => t.Key == "team");

        await _waf.UntagResourceAsync(new UntagResourceRequest
        {
            ResourceARN = arn,
            TagKeys = ["env"],
        });

        var tags3 = await _waf.ListTagsForResourceAsync(new ListTagsForResourceRequest
        {
            ResourceARN = arn,
        });
        tags3.TagInfoForResource.TagList.ShouldNotContain(t => t.Key == "env");
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // CheckCapacity
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task CheckCapacity()
    {
        var resp = await _waf.CheckCapacityAsync(new CheckCapacityRequest
        {
            Scope = "REGIONAL",
            Rules =
            [
                new Rule
                {
                    Name = "rate-rule",
                    Priority = 1,
                    Statement = new Statement
                    {
                        RateBasedStatement = new RateBasedStatement
                        {
                            Limit = 1000,
                            AggregateKeyType = RateBasedStatementAggregateKeyType.IP,
                        },
                    },
                    Action = new RuleAction { Block = new BlockAction() },
                    VisibilityConfig = new VisibilityConfig
                    {
                        SampledRequestsEnabled = false,
                        CloudWatchMetricsEnabled = false,
                        MetricName = "rate",
                    },
                },
            ],
        });

        (resp.Capacity > 0).ShouldBe(true);
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // DescribeManagedRuleGroup
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task DescribeManagedRuleGroup()
    {
        var resp = await _waf.DescribeManagedRuleGroupAsync(new DescribeManagedRuleGroupRequest
        {
            VendorName = "AWS",
            Name = "AWSManagedRulesCommonRuleSet",
            Scope = "REGIONAL",
        });

        (resp.Capacity > 0).ShouldBe(true);
        resp.Rules.ShouldNotBeNull();
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // ListResourcesForWebACL
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task ListResourcesForWebAcl()
    {
        var createResp = await _waf.CreateWebACLAsync(new CreateWebACLRequest
        {
            Name = "res-list-acl",
            Scope = "REGIONAL",
            DefaultAction = new DefaultAction { Allow = new AllowAction() },
            VisibilityConfig = new VisibilityConfig
            {
                SampledRequestsEnabled = false,
                CloudWatchMetricsEnabled = false,
                MetricName = "m",
            },
        });

        var aclArn = createResp.Summary.ARN;
        var resourceArn = "arn:aws:elasticloadbalancing:us-east-1:000000000000:loadbalancer/app/waf-test/xyz";

        await _waf.AssociateWebACLAsync(new AssociateWebACLRequest
        {
            WebACLArn = aclArn,
            ResourceArn = resourceArn,
        });

        var listResp = await _waf.ListResourcesForWebACLAsync(new ListResourcesForWebACLRequest
        {
            WebACLArn = aclArn,
            ResourceType = ResourceType.APPLICATION_LOAD_BALANCER,
        });

        listResp.ResourceArns.ShouldContain(resourceArn);
    }
}
