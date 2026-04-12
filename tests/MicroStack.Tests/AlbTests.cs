using Amazon;
using Amazon.ElasticLoadBalancingV2;
using Amazon.ElasticLoadBalancingV2.Model;
using Amazon.Runtime;

namespace MicroStack.Tests;

public sealed class AlbTests : IClassFixture<MicroStackFixture>, IAsyncLifetime
{
    private readonly MicroStackFixture _fixture;
    private readonly AmazonElasticLoadBalancingV2Client _elb;

    public AlbTests(MicroStackFixture fixture)
    {
        _fixture = fixture;
        _elb = CreateElbClient(fixture);
    }

    private static AmazonElasticLoadBalancingV2Client CreateElbClient(MicroStackFixture fixture)
    {
        var innerHandler = fixture.Factory.Server.CreateHandler();
        var httpClient = new HttpClient(new CanonicalizeUriHandler(innerHandler))
        {
            BaseAddress = new Uri("http://localhost/"),
        };
        var config = new AmazonElasticLoadBalancingV2Config
        {
            RegionEndpoint = RegionEndpoint.USEast1,
            ServiceURL = "http://localhost/",
            HttpClientFactory = new FixedHttpClientFactory(httpClient),
        };
        return new AmazonElasticLoadBalancingV2Client(
            new BasicAWSCredentials("test", "test"), config);
    }

    public async Task InitializeAsync()
    {
        await _fixture.HttpClient.PostAsync("/_ministack/reset", null);
    }

    public Task DisposeAsync()
    {
        _elb.Dispose();
        return Task.CompletedTask;
    }

    // ── Load Balancer CRUD ──────────────────────────────────────────────────

    [Fact]
    public async Task CreateDescribeDeleteLoadBalancer()
    {
        var createResp = await _elb.CreateLoadBalancerAsync(new CreateLoadBalancerRequest
        {
            Name = "qa-alb",
            Type = LoadBalancerTypeEnum.Application,
            Scheme = LoadBalancerSchemeEnum.InternetFacing,
        });
        var lb = createResp.LoadBalancers[0];
        var lbArn = lb.LoadBalancerArn;
        Assert.StartsWith("arn:aws:elasticloadbalancing", lbArn);
        Assert.Equal("qa-alb", lb.LoadBalancerName);
        Assert.Equal(LoadBalancerTypeEnum.Application, lb.Type);
        Assert.Equal(LoadBalancerSchemeEnum.InternetFacing, lb.Scheme);
        Assert.NotNull(lb.DNSName);
        Assert.NotEmpty(lb.DNSName);
        Assert.Equal("active", lb.State.Code.Value);

        var descResp = await _elb.DescribeLoadBalancersAsync(new DescribeLoadBalancersRequest
        {
            LoadBalancerArns = [lbArn],
        });
        Assert.Single(descResp.LoadBalancers);
        Assert.Equal(lbArn, descResp.LoadBalancers[0].LoadBalancerArn);

        await _elb.DeleteLoadBalancerAsync(new DeleteLoadBalancerRequest
        {
            LoadBalancerArn = lbArn,
        });

        var descAfter = await _elb.DescribeLoadBalancersAsync(new DescribeLoadBalancersRequest());
        Assert.DoesNotContain(descAfter.LoadBalancers ?? [], l => l.LoadBalancerArn == lbArn);
    }

    [Fact]
    public async Task DescribeLoadBalancerByName()
    {
        await _elb.CreateLoadBalancerAsync(new CreateLoadBalancerRequest
        {
            Name = "qa-alb-named",
        });

        var resp = await _elb.DescribeLoadBalancersAsync(new DescribeLoadBalancersRequest
        {
            Names = ["qa-alb-named"],
        });
        Assert.Single(resp.LoadBalancers);
        Assert.Equal("qa-alb-named", resp.LoadBalancers[0].LoadBalancerName);

        await _elb.DeleteLoadBalancerAsync(new DeleteLoadBalancerRequest
        {
            LoadBalancerArn = resp.LoadBalancers[0].LoadBalancerArn,
        });
    }

    [Fact]
    public async Task DuplicateLoadBalancerNameThrows()
    {
        var createResp = await _elb.CreateLoadBalancerAsync(new CreateLoadBalancerRequest
        {
            Name = "qa-alb-dup",
        });
        var lbArn = createResp.LoadBalancers[0].LoadBalancerArn;

        try
        {
            var ex = await Assert.ThrowsAsync<DuplicateLoadBalancerNameException>(async () =>
            {
                await _elb.CreateLoadBalancerAsync(new CreateLoadBalancerRequest
                {
                    Name = "qa-alb-dup",
                });
            });
            Assert.Contains("DuplicateLoadBalancerName", ex.ErrorCode);
        }
        finally
        {
            await _elb.DeleteLoadBalancerAsync(new DeleteLoadBalancerRequest
            {
                LoadBalancerArn = lbArn,
            });
        }
    }

    // ── Load Balancer Attributes ────────────────────────────────────────────

    [Fact]
    public async Task LoadBalancerAttributes()
    {
        var lbArn = (await _elb.CreateLoadBalancerAsync(new CreateLoadBalancerRequest
        {
            Name = "qa-alb-attrs",
        })).LoadBalancers[0].LoadBalancerArn;

        var attrs = (await _elb.DescribeLoadBalancerAttributesAsync(
            new DescribeLoadBalancerAttributesRequest
            {
                LoadBalancerArn = lbArn,
            })).Attributes;

        var keys = attrs.Select(a => a.Key).ToHashSet();
        Assert.Contains("idle_timeout.timeout_seconds", keys);

        await _elb.ModifyLoadBalancerAttributesAsync(
            new ModifyLoadBalancerAttributesRequest
            {
                LoadBalancerArn = lbArn,
                Attributes =
                [
                    new LoadBalancerAttribute
                    {
                        Key = "idle_timeout.timeout_seconds",
                        Value = "120",
                    },
                ],
            });

        var updated = (await _elb.DescribeLoadBalancerAttributesAsync(
            new DescribeLoadBalancerAttributesRequest
            {
                LoadBalancerArn = lbArn,
            })).Attributes;

        var val = updated.First(a => a.Key == "idle_timeout.timeout_seconds").Value;
        Assert.Equal("120", val);

        await _elb.DeleteLoadBalancerAsync(new DeleteLoadBalancerRequest
        {
            LoadBalancerArn = lbArn,
        });
    }

    // ── Target Group CRUD ───────────────────────────────────────────────────

    [Fact]
    public async Task CreateDescribeDeleteTargetGroup()
    {
        var createResp = await _elb.CreateTargetGroupAsync(new CreateTargetGroupRequest
        {
            Name = "qa-tg",
            Protocol = ProtocolEnum.HTTP,
            Port = 80,
            VpcId = "vpc-00000001",
            HealthCheckPath = "/health",
        });
        var tg = createResp.TargetGroups[0];
        var tgArn = tg.TargetGroupArn;
        Assert.StartsWith("arn:aws:elasticloadbalancing", tgArn);
        Assert.Equal("qa-tg", tg.TargetGroupName);
        Assert.Equal("/health", tg.HealthCheckPath);

        var descResp = await _elb.DescribeTargetGroupsAsync(new DescribeTargetGroupsRequest
        {
            TargetGroupArns = [tgArn],
        });
        Assert.Single(descResp.TargetGroups);
        Assert.Equal(tgArn, descResp.TargetGroups[0].TargetGroupArn);

        await _elb.DeleteTargetGroupAsync(new DeleteTargetGroupRequest
        {
            TargetGroupArn = tgArn,
        });

        var descAfter = await _elb.DescribeTargetGroupsAsync(new DescribeTargetGroupsRequest());
        Assert.DoesNotContain(descAfter.TargetGroups ?? [], t => t.TargetGroupArn == tgArn);
    }

    [Fact]
    public async Task DuplicateTargetGroupNameThrows()
    {
        var createResp = await _elb.CreateTargetGroupAsync(new CreateTargetGroupRequest
        {
            Name = "qa-tg-dup",
            Protocol = ProtocolEnum.HTTP,
            Port = 80,
            VpcId = "vpc-00000001",
        });
        var tgArn = createResp.TargetGroups[0].TargetGroupArn;

        try
        {
            var ex = await Assert.ThrowsAsync<DuplicateTargetGroupNameException>(async () =>
            {
                await _elb.CreateTargetGroupAsync(new CreateTargetGroupRequest
                {
                    Name = "qa-tg-dup",
                    Protocol = ProtocolEnum.HTTP,
                    Port = 80,
                    VpcId = "vpc-00000001",
                });
            });
            Assert.Contains("DuplicateTargetGroupName", ex.ErrorCode);
        }
        finally
        {
            await _elb.DeleteTargetGroupAsync(new DeleteTargetGroupRequest
            {
                TargetGroupArn = tgArn,
            });
        }
    }

    // ── Target Group Attributes ─────────────────────────────────────────────

    [Fact]
    public async Task TargetGroupAttributes()
    {
        var tgArn = (await _elb.CreateTargetGroupAsync(new CreateTargetGroupRequest
        {
            Name = "qa-tg-attrs",
            Protocol = ProtocolEnum.HTTP,
            Port = 80,
            VpcId = "vpc-00000001",
        })).TargetGroups[0].TargetGroupArn;

        var attrs = (await _elb.DescribeTargetGroupAttributesAsync(
            new DescribeTargetGroupAttributesRequest
            {
                TargetGroupArn = tgArn,
            })).Attributes;

        var keys = attrs.Select(a => a.Key).ToHashSet();
        Assert.Contains("deregistration_delay.timeout_seconds", keys);

        await _elb.ModifyTargetGroupAttributesAsync(
            new ModifyTargetGroupAttributesRequest
            {
                TargetGroupArn = tgArn,
                Attributes =
                [
                    new TargetGroupAttribute
                    {
                        Key = "deregistration_delay.timeout_seconds",
                        Value = "60",
                    },
                ],
            });

        var updated = (await _elb.DescribeTargetGroupAttributesAsync(
            new DescribeTargetGroupAttributesRequest
            {
                TargetGroupArn = tgArn,
            })).Attributes;

        var val = updated.First(a => a.Key == "deregistration_delay.timeout_seconds").Value;
        Assert.Equal("60", val);

        await _elb.DeleteTargetGroupAsync(new DeleteTargetGroupRequest
        {
            TargetGroupArn = tgArn,
        });
    }

    // ── Modify Target Group ─────────────────────────────────────────────────

    [Fact]
    public async Task ModifyTargetGroupUpdatesHealthCheckPath()
    {
        var tgArn = (await _elb.CreateTargetGroupAsync(new CreateTargetGroupRequest
        {
            Name = "qa-tg-modify",
            Protocol = ProtocolEnum.HTTP,
            Port = 80,
            VpcId = "vpc-00000001",
            HealthCheckPath = "/",
        })).TargetGroups[0].TargetGroupArn;

        await _elb.ModifyTargetGroupAsync(new ModifyTargetGroupRequest
        {
            TargetGroupArn = tgArn,
            HealthCheckPath = "/new-health",
        });

        var desc = await _elb.DescribeTargetGroupsAsync(new DescribeTargetGroupsRequest
        {
            TargetGroupArns = [tgArn],
        });
        Assert.Equal("/new-health", desc.TargetGroups[0].HealthCheckPath);

        await _elb.DeleteTargetGroupAsync(new DeleteTargetGroupRequest
        {
            TargetGroupArn = tgArn,
        });
    }

    // ── Listener CRUD ───────────────────────────────────────────────────────

    [Fact]
    public async Task ListenerCrud()
    {
        var lbArn = (await _elb.CreateLoadBalancerAsync(new CreateLoadBalancerRequest
        {
            Name = "qa-alb-listener",
        })).LoadBalancers[0].LoadBalancerArn;

        var tgArn = (await _elb.CreateTargetGroupAsync(new CreateTargetGroupRequest
        {
            Name = "qa-tg-l",
            Protocol = ProtocolEnum.HTTP,
            Port = 80,
            VpcId = "vpc-00000001",
        })).TargetGroups[0].TargetGroupArn;

        var lResp = await _elb.CreateListenerAsync(new CreateListenerRequest
        {
            LoadBalancerArn = lbArn,
            Protocol = ProtocolEnum.HTTP,
            Port = 80,
            DefaultActions =
            [
                new Amazon.ElasticLoadBalancingV2.Model.Action
                {
                    Type = ActionTypeEnum.Forward,
                    TargetGroupArn = tgArn,
                },
            ],
        });
        var listener = lResp.Listeners[0];
        var lArn = listener.ListenerArn;
        Assert.StartsWith("arn:aws:elasticloadbalancing", lArn);
        Assert.Equal(80, listener.Port);
        Assert.Equal(ProtocolEnum.HTTP, listener.Protocol);

        var descResp = await _elb.DescribeListenersAsync(new DescribeListenersRequest
        {
            LoadBalancerArn = lbArn,
        });
        Assert.Contains(descResp.Listeners, l => l.ListenerArn == lArn);

        // TG should now reference the LB
        var tgDesc = (await _elb.DescribeTargetGroupsAsync(new DescribeTargetGroupsRequest
        {
            TargetGroupArns = [tgArn],
        })).TargetGroups[0];
        Assert.Contains(lbArn, tgDesc.LoadBalancerArns);

        // Modify listener port
        await _elb.ModifyListenerAsync(new ModifyListenerRequest
        {
            ListenerArn = lArn,
            Port = 8080,
        });
        var updated = (await _elb.DescribeListenersAsync(new DescribeListenersRequest
        {
            ListenerArns = [lArn],
        })).Listeners[0];
        Assert.Equal(8080, updated.Port);

        // Delete listener
        await _elb.DeleteListenerAsync(new DeleteListenerRequest
        {
            ListenerArn = lArn,
        });
        var descAfter = await _elb.DescribeListenersAsync(new DescribeListenersRequest
        {
            LoadBalancerArn = lbArn,
        });
        Assert.DoesNotContain(descAfter.Listeners ?? [], l => l.ListenerArn == lArn);

        await _elb.DeleteTargetGroupAsync(new DeleteTargetGroupRequest { TargetGroupArn = tgArn });
        await _elb.DeleteLoadBalancerAsync(new DeleteLoadBalancerRequest { LoadBalancerArn = lbArn });
    }

    // ── Rule CRUD ───────────────────────────────────────────────────────────

    [Fact]
    public async Task RuleCrud()
    {
        var lbArn = (await _elb.CreateLoadBalancerAsync(new CreateLoadBalancerRequest
        {
            Name = "qa-alb-rules",
        })).LoadBalancers[0].LoadBalancerArn;

        var tgArn = (await _elb.CreateTargetGroupAsync(new CreateTargetGroupRequest
        {
            Name = "qa-tg-r",
            Protocol = ProtocolEnum.HTTP,
            Port = 80,
            VpcId = "vpc-00000001",
        })).TargetGroups[0].TargetGroupArn;

        var lArn = (await _elb.CreateListenerAsync(new CreateListenerRequest
        {
            LoadBalancerArn = lbArn,
            Protocol = ProtocolEnum.HTTP,
            Port = 80,
            DefaultActions =
            [
                new Amazon.ElasticLoadBalancingV2.Model.Action
                {
                    Type = ActionTypeEnum.Forward,
                    TargetGroupArn = tgArn,
                },
            ],
        })).Listeners[0].ListenerArn;

        // Default rule should exist
        var rules = (await _elb.DescribeRulesAsync(new DescribeRulesRequest
        {
            ListenerArn = lArn,
        })).Rules;
        Assert.Contains(rules, r => r.IsDefault == true);

        // Create a custom rule
        var ruleResp = await _elb.CreateRuleAsync(new CreateRuleRequest
        {
            ListenerArn = lArn,
            Priority = 10,
            Conditions =
            [
                new RuleCondition
                {
                    Field = "path-pattern",
                    Values = ["/api/*"],
                },
            ],
            Actions =
            [
                new Amazon.ElasticLoadBalancingV2.Model.Action
                {
                    Type = ActionTypeEnum.Forward,
                    TargetGroupArn = tgArn,
                },
            ],
        });
        var rule = ruleResp.Rules[0];
        var rArn = rule.RuleArn;
        Assert.False(rule.IsDefault == true);
        Assert.Equal("10", rule.Priority);

        var rules2 = (await _elb.DescribeRulesAsync(new DescribeRulesRequest
        {
            ListenerArn = lArn,
        })).Rules;
        Assert.Contains(rules2, r => r.RuleArn == rArn);

        // Delete the rule
        await _elb.DeleteRuleAsync(new DeleteRuleRequest { RuleArn = rArn });
        var rules3 = (await _elb.DescribeRulesAsync(new DescribeRulesRequest
        {
            ListenerArn = lArn,
        })).Rules;
        Assert.DoesNotContain(rules3, r => r.RuleArn == rArn);

        await _elb.DeleteListenerAsync(new DeleteListenerRequest { ListenerArn = lArn });
        await _elb.DeleteTargetGroupAsync(new DeleteTargetGroupRequest { TargetGroupArn = tgArn });
        await _elb.DeleteLoadBalancerAsync(new DeleteLoadBalancerRequest { LoadBalancerArn = lbArn });
    }

    [Fact]
    public async Task DeleteDefaultRuleThrows()
    {
        var lbArn = (await _elb.CreateLoadBalancerAsync(new CreateLoadBalancerRequest
        {
            Name = "qa-alb-defrule",
        })).LoadBalancers[0].LoadBalancerArn;

        var tgArn = (await _elb.CreateTargetGroupAsync(new CreateTargetGroupRequest
        {
            Name = "qa-tg-defrule",
            Protocol = ProtocolEnum.HTTP,
            Port = 80,
            VpcId = "vpc-00000001",
        })).TargetGroups[0].TargetGroupArn;

        var lArn = (await _elb.CreateListenerAsync(new CreateListenerRequest
        {
            LoadBalancerArn = lbArn,
            Protocol = ProtocolEnum.HTTP,
            Port = 80,
            DefaultActions =
            [
                new Amazon.ElasticLoadBalancingV2.Model.Action
                {
                    Type = ActionTypeEnum.Forward,
                    TargetGroupArn = tgArn,
                },
            ],
        })).Listeners[0].ListenerArn;

        var rules = (await _elb.DescribeRulesAsync(new DescribeRulesRequest
        {
            ListenerArn = lArn,
        })).Rules;
        var defaultRule = rules.First(r => r.IsDefault == true);

        await Assert.ThrowsAsync<OperationNotPermittedException>(async () =>
        {
            await _elb.DeleteRuleAsync(new DeleteRuleRequest { RuleArn = defaultRule.RuleArn });
        });

        await _elb.DeleteListenerAsync(new DeleteListenerRequest { ListenerArn = lArn });
        await _elb.DeleteTargetGroupAsync(new DeleteTargetGroupRequest { TargetGroupArn = tgArn });
        await _elb.DeleteLoadBalancerAsync(new DeleteLoadBalancerRequest { LoadBalancerArn = lbArn });
    }

    [Fact]
    public async Task SetRulePriorities()
    {
        var lbArn = (await _elb.CreateLoadBalancerAsync(new CreateLoadBalancerRequest
        {
            Name = "qa-alb-setpri",
        })).LoadBalancers[0].LoadBalancerArn;

        var tgArn = (await _elb.CreateTargetGroupAsync(new CreateTargetGroupRequest
        {
            Name = "qa-tg-setpri",
            Protocol = ProtocolEnum.HTTP,
            Port = 80,
            VpcId = "vpc-00000001",
        })).TargetGroups[0].TargetGroupArn;

        var lArn = (await _elb.CreateListenerAsync(new CreateListenerRequest
        {
            LoadBalancerArn = lbArn,
            Protocol = ProtocolEnum.HTTP,
            Port = 80,
            DefaultActions =
            [
                new Amazon.ElasticLoadBalancingV2.Model.Action
                {
                    Type = ActionTypeEnum.Forward,
                    TargetGroupArn = tgArn,
                },
            ],
        })).Listeners[0].ListenerArn;

        var rArn = (await _elb.CreateRuleAsync(new CreateRuleRequest
        {
            ListenerArn = lArn,
            Priority = 10,
            Conditions =
            [
                new RuleCondition { Field = "path-pattern", Values = ["/api/*"] },
            ],
            Actions =
            [
                new Amazon.ElasticLoadBalancingV2.Model.Action
                {
                    Type = ActionTypeEnum.Forward,
                    TargetGroupArn = tgArn,
                },
            ],
        })).Rules[0].RuleArn;

        var resp = await _elb.SetRulePrioritiesAsync(new SetRulePrioritiesRequest
        {
            RulePriorities =
            [
                new RulePriorityPair { RuleArn = rArn, Priority = 20 },
            ],
        });
        Assert.Contains(resp.Rules, r => r.RuleArn == rArn && r.Priority == "20");

        await _elb.DeleteRuleAsync(new DeleteRuleRequest { RuleArn = rArn });
        await _elb.DeleteListenerAsync(new DeleteListenerRequest { ListenerArn = lArn });
        await _elb.DeleteTargetGroupAsync(new DeleteTargetGroupRequest { TargetGroupArn = tgArn });
        await _elb.DeleteLoadBalancerAsync(new DeleteLoadBalancerRequest { LoadBalancerArn = lbArn });
    }

    // ── Register / Deregister Targets ───────────────────────────────────────

    [Fact]
    public async Task RegisterDeregisterTargets()
    {
        var tgArn = (await _elb.CreateTargetGroupAsync(new CreateTargetGroupRequest
        {
            Name = "qa-tg-targets",
            Protocol = ProtocolEnum.HTTP,
            Port = 80,
            VpcId = "vpc-00000001",
        })).TargetGroups[0].TargetGroupArn;

        await _elb.RegisterTargetsAsync(new RegisterTargetsRequest
        {
            TargetGroupArn = tgArn,
            Targets =
            [
                new TargetDescription { Id = "i-0001", Port = 80 },
                new TargetDescription { Id = "i-0002", Port = 80 },
            ],
        });

        var health = await _elb.DescribeTargetHealthAsync(new DescribeTargetHealthRequest
        {
            TargetGroupArn = tgArn,
        });
        Assert.Equal(2, health.TargetHealthDescriptions.Count);
        var ids = health.TargetHealthDescriptions.Select(d => d.Target.Id).ToHashSet();
        Assert.Contains("i-0001", ids);
        Assert.Contains("i-0002", ids);
        Assert.All(health.TargetHealthDescriptions,
            d => Assert.Equal("healthy", d.TargetHealth.State.Value));

        await _elb.DeregisterTargetsAsync(new DeregisterTargetsRequest
        {
            TargetGroupArn = tgArn,
            Targets = [new TargetDescription { Id = "i-0001" }],
        });

        var health2 = await _elb.DescribeTargetHealthAsync(new DescribeTargetHealthRequest
        {
            TargetGroupArn = tgArn,
        });
        Assert.Single(health2.TargetHealthDescriptions);
        Assert.Equal("i-0002", health2.TargetHealthDescriptions[0].Target.Id);

        await _elb.DeleteTargetGroupAsync(new DeleteTargetGroupRequest
        {
            TargetGroupArn = tgArn,
        });
    }

    [Fact]
    public async Task RegisterDuplicateTargetIsIdempotent()
    {
        var tgArn = (await _elb.CreateTargetGroupAsync(new CreateTargetGroupRequest
        {
            Name = "qa-tg-dedup",
            Protocol = ProtocolEnum.HTTP,
            Port = 80,
            VpcId = "vpc-00000001",
        })).TargetGroups[0].TargetGroupArn;

        await _elb.RegisterTargetsAsync(new RegisterTargetsRequest
        {
            TargetGroupArn = tgArn,
            Targets = [new TargetDescription { Id = "i-0001", Port = 80 }],
        });

        // Register same target again
        await _elb.RegisterTargetsAsync(new RegisterTargetsRequest
        {
            TargetGroupArn = tgArn,
            Targets = [new TargetDescription { Id = "i-0001", Port = 80 }],
        });

        var health = await _elb.DescribeTargetHealthAsync(new DescribeTargetHealthRequest
        {
            TargetGroupArn = tgArn,
        });
        Assert.Single(health.TargetHealthDescriptions);

        await _elb.DeleteTargetGroupAsync(new DeleteTargetGroupRequest
        {
            TargetGroupArn = tgArn,
        });
    }

    // ── Tags ────────────────────────────────────────────────────────────────

    [Fact]
    public async Task TagOperations()
    {
        var lbArn = (await _elb.CreateLoadBalancerAsync(new CreateLoadBalancerRequest
        {
            Name = "qa-alb-tags",
            Tags =
            [
                new Tag { Key = "env", Value = "test" },
            ],
        })).LoadBalancers[0].LoadBalancerArn;

        await _elb.AddTagsAsync(new AddTagsRequest
        {
            ResourceArns = [lbArn],
            Tags = [new Tag { Key = "team", Value = "infra" }],
        });

        var desc = await _elb.DescribeTagsAsync(new DescribeTagsRequest
        {
            ResourceArns = [lbArn],
        });
        var tagMap = desc.TagDescriptions[0].Tags.ToDictionary(t => t.Key, t => t.Value);
        Assert.Equal("test", tagMap["env"]);
        Assert.Equal("infra", tagMap["team"]);

        await _elb.RemoveTagsAsync(new RemoveTagsRequest
        {
            ResourceArns = [lbArn],
            TagKeys = ["env"],
        });

        var desc2 = await _elb.DescribeTagsAsync(new DescribeTagsRequest
        {
            ResourceArns = [lbArn],
        });
        var tagMap2 = desc2.TagDescriptions[0].Tags.ToDictionary(t => t.Key, t => t.Value);
        Assert.False(tagMap2.ContainsKey("env"));
        Assert.Equal("infra", tagMap2["team"]);

        await _elb.DeleteLoadBalancerAsync(new DeleteLoadBalancerRequest
        {
            LoadBalancerArn = lbArn,
        });
    }

    [Fact]
    public async Task AddTagsOverwritesExistingKey()
    {
        var lbArn = (await _elb.CreateLoadBalancerAsync(new CreateLoadBalancerRequest
        {
            Name = "qa-alb-tag-overwrite",
            Tags = [new Tag { Key = "env", Value = "dev" }],
        })).LoadBalancers[0].LoadBalancerArn;

        await _elb.AddTagsAsync(new AddTagsRequest
        {
            ResourceArns = [lbArn],
            Tags = [new Tag { Key = "env", Value = "prod" }],
        });

        var desc = await _elb.DescribeTagsAsync(new DescribeTagsRequest
        {
            ResourceArns = [lbArn],
        });
        var tagMap = desc.TagDescriptions[0].Tags.ToDictionary(t => t.Key, t => t.Value);
        Assert.Equal("prod", tagMap["env"]);

        await _elb.DeleteLoadBalancerAsync(new DeleteLoadBalancerRequest
        {
            LoadBalancerArn = lbArn,
        });
    }

    // ── SetSecurityGroups ───────────────────────────────────────────────────

    [Fact]
    public async Task SetSecurityGroups()
    {
        var lbArn = (await _elb.CreateLoadBalancerAsync(new CreateLoadBalancerRequest
        {
            Name = "qa-alb-sg",
        })).LoadBalancers[0].LoadBalancerArn;

        var resp = await _elb.SetSecurityGroupsAsync(new SetSecurityGroupsRequest
        {
            LoadBalancerArn = lbArn,
            SecurityGroups = ["sg-111", "sg-222"],
        });
        Assert.Contains("sg-111", resp.SecurityGroupIds);
        Assert.Contains("sg-222", resp.SecurityGroupIds);

        await _elb.DeleteLoadBalancerAsync(new DeleteLoadBalancerRequest
        {
            LoadBalancerArn = lbArn,
        });
    }

    // ── SetSubnets ──────────────────────────────────────────────────────────

    [Fact]
    public async Task SetSubnets()
    {
        var lbArn = (await _elb.CreateLoadBalancerAsync(new CreateLoadBalancerRequest
        {
            Name = "qa-alb-subnets",
        })).LoadBalancers[0].LoadBalancerArn;

        var resp = await _elb.SetSubnetsAsync(new SetSubnetsRequest
        {
            LoadBalancerArn = lbArn,
            Subnets = ["subnet-aaa", "subnet-bbb"],
        });
        Assert.Equal(2, resp.AvailabilityZones.Count);
        Assert.Contains(resp.AvailabilityZones, az => az.SubnetId == "subnet-aaa");
        Assert.Contains(resp.AvailabilityZones, az => az.SubnetId == "subnet-bbb");

        await _elb.DeleteLoadBalancerAsync(new DeleteLoadBalancerRequest
        {
            LoadBalancerArn = lbArn,
        });
    }

    // ── Listener with FixedResponse ─────────────────────────────────────────

    [Fact]
    public async Task ListenerWithFixedResponseAction()
    {
        var lbArn = (await _elb.CreateLoadBalancerAsync(new CreateLoadBalancerRequest
        {
            Name = "qa-alb-fixed",
        })).LoadBalancers[0].LoadBalancerArn;

        var lResp = await _elb.CreateListenerAsync(new CreateListenerRequest
        {
            LoadBalancerArn = lbArn,
            Protocol = ProtocolEnum.HTTP,
            Port = 80,
            DefaultActions =
            [
                new Amazon.ElasticLoadBalancingV2.Model.Action
                {
                    Type = ActionTypeEnum.FixedResponse,
                    FixedResponseConfig = new FixedResponseActionConfig
                    {
                        StatusCode = "200",
                        ContentType = "text/plain",
                        MessageBody = "maintenance",
                    },
                },
            ],
        });
        var listener = lResp.Listeners[0];
        Assert.Equal("fixed-response", listener.DefaultActions[0].Type.Value);
        Assert.Equal("200", listener.DefaultActions[0].FixedResponseConfig.StatusCode);
        Assert.Equal("maintenance", listener.DefaultActions[0].FixedResponseConfig.MessageBody);

        await _elb.DeleteListenerAsync(new DeleteListenerRequest { ListenerArn = listener.ListenerArn });
        await _elb.DeleteLoadBalancerAsync(new DeleteLoadBalancerRequest { LoadBalancerArn = lbArn });
    }

    // ── Listener with RedirectConfig ────────────────────────────────────────

    [Fact]
    public async Task ListenerWithRedirectAction()
    {
        var lbArn = (await _elb.CreateLoadBalancerAsync(new CreateLoadBalancerRequest
        {
            Name = "qa-alb-redirect",
        })).LoadBalancers[0].LoadBalancerArn;

        var lResp = await _elb.CreateListenerAsync(new CreateListenerRequest
        {
            LoadBalancerArn = lbArn,
            Protocol = ProtocolEnum.HTTP,
            Port = 80,
            DefaultActions =
            [
                new Amazon.ElasticLoadBalancingV2.Model.Action
                {
                    Type = ActionTypeEnum.Redirect,
                    RedirectConfig = new RedirectActionConfig
                    {
                        Protocol = "HTTPS",
                        Host = "example.com",
                        Path = "/new",
                        StatusCode = "HTTP_301",
                    },
                },
            ],
        });
        var listener = lResp.Listeners[0];
        Assert.Equal("redirect", listener.DefaultActions[0].Type.Value);
        Assert.Equal("HTTP_301", listener.DefaultActions[0].RedirectConfig.StatusCode);
        Assert.Equal("example.com", listener.DefaultActions[0].RedirectConfig.Host);

        await _elb.DeleteListenerAsync(new DeleteListenerRequest { ListenerArn = listener.ListenerArn });
        await _elb.DeleteLoadBalancerAsync(new DeleteLoadBalancerRequest { LoadBalancerArn = lbArn });
    }

    // ── Modify Rule ─────────────────────────────────────────────────────────

    [Fact]
    public async Task ModifyRuleUpdatesConditionsAndActions()
    {
        var lbArn = (await _elb.CreateLoadBalancerAsync(new CreateLoadBalancerRequest
        {
            Name = "qa-alb-modrule",
        })).LoadBalancers[0].LoadBalancerArn;

        var tgArn = (await _elb.CreateTargetGroupAsync(new CreateTargetGroupRequest
        {
            Name = "qa-tg-modrule",
            Protocol = ProtocolEnum.HTTP,
            Port = 80,
            VpcId = "vpc-00000001",
        })).TargetGroups[0].TargetGroupArn;

        var lArn = (await _elb.CreateListenerAsync(new CreateListenerRequest
        {
            LoadBalancerArn = lbArn,
            Protocol = ProtocolEnum.HTTP,
            Port = 80,
            DefaultActions =
            [
                new Amazon.ElasticLoadBalancingV2.Model.Action
                {
                    Type = ActionTypeEnum.Forward,
                    TargetGroupArn = tgArn,
                },
            ],
        })).Listeners[0].ListenerArn;

        var rArn = (await _elb.CreateRuleAsync(new CreateRuleRequest
        {
            ListenerArn = lArn,
            Priority = 10,
            Conditions =
            [
                new RuleCondition { Field = "path-pattern", Values = ["/old/*"] },
            ],
            Actions =
            [
                new Amazon.ElasticLoadBalancingV2.Model.Action
                {
                    Type = ActionTypeEnum.Forward,
                    TargetGroupArn = tgArn,
                },
            ],
        })).Rules[0].RuleArn;

        var modResp = await _elb.ModifyRuleAsync(new ModifyRuleRequest
        {
            RuleArn = rArn,
            Conditions =
            [
                new RuleCondition { Field = "path-pattern", Values = ["/new/*"] },
            ],
        });
        Assert.Equal("/new/*", modResp.Rules[0].Conditions[0].Values[0]);

        await _elb.DeleteRuleAsync(new DeleteRuleRequest { RuleArn = rArn });
        await _elb.DeleteListenerAsync(new DeleteListenerRequest { ListenerArn = lArn });
        await _elb.DeleteTargetGroupAsync(new DeleteTargetGroupRequest { TargetGroupArn = tgArn });
        await _elb.DeleteLoadBalancerAsync(new DeleteLoadBalancerRequest { LoadBalancerArn = lbArn });
    }

    // ── DescribeRules by ARN ────────────────────────────────────────────────

    [Fact]
    public async Task DescribeRulesByArn()
    {
        var lbArn = (await _elb.CreateLoadBalancerAsync(new CreateLoadBalancerRequest
        {
            Name = "qa-alb-rulearn",
        })).LoadBalancers[0].LoadBalancerArn;

        var tgArn = (await _elb.CreateTargetGroupAsync(new CreateTargetGroupRequest
        {
            Name = "qa-tg-rulearn",
            Protocol = ProtocolEnum.HTTP,
            Port = 80,
            VpcId = "vpc-00000001",
        })).TargetGroups[0].TargetGroupArn;

        var lArn = (await _elb.CreateListenerAsync(new CreateListenerRequest
        {
            LoadBalancerArn = lbArn,
            Protocol = ProtocolEnum.HTTP,
            Port = 80,
            DefaultActions =
            [
                new Amazon.ElasticLoadBalancingV2.Model.Action
                {
                    Type = ActionTypeEnum.Forward,
                    TargetGroupArn = tgArn,
                },
            ],
        })).Listeners[0].ListenerArn;

        var rArn = (await _elb.CreateRuleAsync(new CreateRuleRequest
        {
            ListenerArn = lArn,
            Priority = 5,
            Conditions =
            [
                new RuleCondition { Field = "path-pattern", Values = ["/test/*"] },
            ],
            Actions =
            [
                new Amazon.ElasticLoadBalancingV2.Model.Action
                {
                    Type = ActionTypeEnum.Forward,
                    TargetGroupArn = tgArn,
                },
            ],
        })).Rules[0].RuleArn;

        var resp = await _elb.DescribeRulesAsync(new DescribeRulesRequest
        {
            RuleArns = [rArn],
        });
        Assert.Single(resp.Rules);
        Assert.Equal(rArn, resp.Rules[0].RuleArn);

        await _elb.DeleteRuleAsync(new DeleteRuleRequest { RuleArn = rArn });
        await _elb.DeleteListenerAsync(new DeleteListenerRequest { ListenerArn = lArn });
        await _elb.DeleteTargetGroupAsync(new DeleteTargetGroupRequest { TargetGroupArn = tgArn });
        await _elb.DeleteLoadBalancerAsync(new DeleteLoadBalancerRequest { LoadBalancerArn = lbArn });
    }

    // ── Describe Target Health with filter ──────────────────────────────────

    [Fact]
    public async Task DescribeTargetHealthWithFilter()
    {
        var tgArn = (await _elb.CreateTargetGroupAsync(new CreateTargetGroupRequest
        {
            Name = "qa-tg-healthfilter",
            Protocol = ProtocolEnum.HTTP,
            Port = 80,
            VpcId = "vpc-00000001",
        })).TargetGroups[0].TargetGroupArn;

        await _elb.RegisterTargetsAsync(new RegisterTargetsRequest
        {
            TargetGroupArn = tgArn,
            Targets =
            [
                new TargetDescription { Id = "i-0001", Port = 80 },
                new TargetDescription { Id = "i-0002", Port = 80 },
                new TargetDescription { Id = "i-0003", Port = 80 },
            ],
        });

        var health = await _elb.DescribeTargetHealthAsync(new DescribeTargetHealthRequest
        {
            TargetGroupArn = tgArn,
            Targets = [new TargetDescription { Id = "i-0002" }],
        });
        Assert.Single(health.TargetHealthDescriptions);
        Assert.Equal("i-0002", health.TargetHealthDescriptions[0].Target.Id);

        await _elb.DeleteTargetGroupAsync(new DeleteTargetGroupRequest
        {
            TargetGroupArn = tgArn,
        });
    }

    // ── DescribeListeners by ARN ────────────────────────────────────────────

    [Fact]
    public async Task DescribeListenersByArn()
    {
        var lbArn = (await _elb.CreateLoadBalancerAsync(new CreateLoadBalancerRequest
        {
            Name = "qa-alb-larn",
        })).LoadBalancers[0].LoadBalancerArn;

        var tgArn = (await _elb.CreateTargetGroupAsync(new CreateTargetGroupRequest
        {
            Name = "qa-tg-larn",
            Protocol = ProtocolEnum.HTTP,
            Port = 80,
            VpcId = "vpc-00000001",
        })).TargetGroups[0].TargetGroupArn;

        var lArn = (await _elb.CreateListenerAsync(new CreateListenerRequest
        {
            LoadBalancerArn = lbArn,
            Protocol = ProtocolEnum.HTTP,
            Port = 80,
            DefaultActions =
            [
                new Amazon.ElasticLoadBalancingV2.Model.Action
                {
                    Type = ActionTypeEnum.Forward,
                    TargetGroupArn = tgArn,
                },
            ],
        })).Listeners[0].ListenerArn;

        var resp = await _elb.DescribeListenersAsync(new DescribeListenersRequest
        {
            ListenerArns = [lArn],
        });
        Assert.Single(resp.Listeners);
        Assert.Equal(lArn, resp.Listeners[0].ListenerArn);

        await _elb.DeleteListenerAsync(new DeleteListenerRequest { ListenerArn = lArn });
        await _elb.DeleteTargetGroupAsync(new DeleteTargetGroupRequest { TargetGroupArn = tgArn });
        await _elb.DeleteLoadBalancerAsync(new DeleteLoadBalancerRequest { LoadBalancerArn = lbArn });
    }

    // ── Delete listener removes associated rules ────────────────────────────

    [Fact]
    public async Task DeleteListenerRemovesAssociatedRules()
    {
        var lbArn = (await _elb.CreateLoadBalancerAsync(new CreateLoadBalancerRequest
        {
            Name = "qa-alb-delrules",
        })).LoadBalancers[0].LoadBalancerArn;

        var tgArn = (await _elb.CreateTargetGroupAsync(new CreateTargetGroupRequest
        {
            Name = "qa-tg-delrules",
            Protocol = ProtocolEnum.HTTP,
            Port = 80,
            VpcId = "vpc-00000001",
        })).TargetGroups[0].TargetGroupArn;

        var lArn = (await _elb.CreateListenerAsync(new CreateListenerRequest
        {
            LoadBalancerArn = lbArn,
            Protocol = ProtocolEnum.HTTP,
            Port = 80,
            DefaultActions =
            [
                new Amazon.ElasticLoadBalancingV2.Model.Action
                {
                    Type = ActionTypeEnum.Forward,
                    TargetGroupArn = tgArn,
                },
            ],
        })).Listeners[0].ListenerArn;

        // Create a custom rule
        await _elb.CreateRuleAsync(new CreateRuleRequest
        {
            ListenerArn = lArn,
            Priority = 10,
            Conditions =
            [
                new RuleCondition { Field = "path-pattern", Values = ["/api/*"] },
            ],
            Actions =
            [
                new Amazon.ElasticLoadBalancingV2.Model.Action
                {
                    Type = ActionTypeEnum.Forward,
                    TargetGroupArn = tgArn,
                },
            ],
        });

        // Verify rules exist
        var rules = (await _elb.DescribeRulesAsync(new DescribeRulesRequest
        {
            ListenerArn = lArn,
        })).Rules;
        Assert.True(rules.Count >= 2); // default + custom

        // Delete listener
        await _elb.DeleteListenerAsync(new DeleteListenerRequest { ListenerArn = lArn });

        // All rules for that listener should be gone
        var allRules = (await _elb.DescribeRulesAsync(new DescribeRulesRequest())).Rules ?? [];
        Assert.DoesNotContain(allRules, r => r.RuleArn.Contains(lArn.Split('/')[^1]));

        await _elb.DeleteTargetGroupAsync(new DeleteTargetGroupRequest { TargetGroupArn = tgArn });
        await _elb.DeleteLoadBalancerAsync(new DeleteLoadBalancerRequest { LoadBalancerArn = lbArn });
    }

    // ── Modify listener default actions ─────────────────────────────────────

    [Fact]
    public async Task ModifyListenerDefaultActions()
    {
        var lbArn = (await _elb.CreateLoadBalancerAsync(new CreateLoadBalancerRequest
        {
            Name = "qa-alb-modact",
        })).LoadBalancers[0].LoadBalancerArn;

        var tgArn = (await _elb.CreateTargetGroupAsync(new CreateTargetGroupRequest
        {
            Name = "qa-tg-modact",
            Protocol = ProtocolEnum.HTTP,
            Port = 80,
            VpcId = "vpc-00000001",
        })).TargetGroups[0].TargetGroupArn;

        var lArn = (await _elb.CreateListenerAsync(new CreateListenerRequest
        {
            LoadBalancerArn = lbArn,
            Protocol = ProtocolEnum.HTTP,
            Port = 80,
            DefaultActions =
            [
                new Amazon.ElasticLoadBalancingV2.Model.Action
                {
                    Type = ActionTypeEnum.Forward,
                    TargetGroupArn = tgArn,
                },
            ],
        })).Listeners[0].ListenerArn;

        // Modify to fixed-response
        var modResp = await _elb.ModifyListenerAsync(new ModifyListenerRequest
        {
            ListenerArn = lArn,
            DefaultActions =
            [
                new Amazon.ElasticLoadBalancingV2.Model.Action
                {
                    Type = ActionTypeEnum.FixedResponse,
                    FixedResponseConfig = new FixedResponseActionConfig
                    {
                        StatusCode = "503",
                        ContentType = "text/plain",
                        MessageBody = "down",
                    },
                },
            ],
        });
        Assert.Equal("fixed-response", modResp.Listeners[0].DefaultActions[0].Type.Value);

        await _elb.DeleteListenerAsync(new DeleteListenerRequest { ListenerArn = lArn });
        await _elb.DeleteTargetGroupAsync(new DeleteTargetGroupRequest { TargetGroupArn = tgArn });
        await _elb.DeleteLoadBalancerAsync(new DeleteLoadBalancerRequest { LoadBalancerArn = lbArn });
    }

    // ── DescribeTargetGroups by name ────────────────────────────────────────

    [Fact]
    public async Task DescribeTargetGroupsByName()
    {
        var tgArn = (await _elb.CreateTargetGroupAsync(new CreateTargetGroupRequest
        {
            Name = "qa-tg-byname",
            Protocol = ProtocolEnum.HTTP,
            Port = 80,
            VpcId = "vpc-00000001",
        })).TargetGroups[0].TargetGroupArn;

        var resp = await _elb.DescribeTargetGroupsAsync(new DescribeTargetGroupsRequest
        {
            Names = ["qa-tg-byname"],
        });
        Assert.Single(resp.TargetGroups);
        Assert.Equal("qa-tg-byname", resp.TargetGroups[0].TargetGroupName);

        await _elb.DeleteTargetGroupAsync(new DeleteTargetGroupRequest
        {
            TargetGroupArn = tgArn,
        });
    }

    // ── DescribeTargetGroups by LB ARN ──────────────────────────────────────

    [Fact]
    public async Task DescribeTargetGroupsByLoadBalancerArn()
    {
        var lbArn = (await _elb.CreateLoadBalancerAsync(new CreateLoadBalancerRequest
        {
            Name = "qa-alb-tgfilter",
        })).LoadBalancers[0].LoadBalancerArn;

        var tgArn = (await _elb.CreateTargetGroupAsync(new CreateTargetGroupRequest
        {
            Name = "qa-tg-tgfilter",
            Protocol = ProtocolEnum.HTTP,
            Port = 80,
            VpcId = "vpc-00000001",
        })).TargetGroups[0].TargetGroupArn;

        // Before linking, filter by LB should return empty
        var before = await _elb.DescribeTargetGroupsAsync(new DescribeTargetGroupsRequest
        {
            LoadBalancerArn = lbArn,
        });
        Assert.DoesNotContain(before.TargetGroups ?? [], tg => tg.TargetGroupArn == tgArn);

        // Create listener to link TG to LB
        var lArn = (await _elb.CreateListenerAsync(new CreateListenerRequest
        {
            LoadBalancerArn = lbArn,
            Protocol = ProtocolEnum.HTTP,
            Port = 80,
            DefaultActions =
            [
                new Amazon.ElasticLoadBalancingV2.Model.Action
                {
                    Type = ActionTypeEnum.Forward,
                    TargetGroupArn = tgArn,
                },
            ],
        })).Listeners[0].ListenerArn;

        var after = await _elb.DescribeTargetGroupsAsync(new DescribeTargetGroupsRequest
        {
            LoadBalancerArn = lbArn,
        });
        Assert.Contains(after.TargetGroups, tg => tg.TargetGroupArn == tgArn);

        await _elb.DeleteListenerAsync(new DeleteListenerRequest { ListenerArn = lArn });
        await _elb.DeleteTargetGroupAsync(new DeleteTargetGroupRequest { TargetGroupArn = tgArn });
        await _elb.DeleteLoadBalancerAsync(new DeleteLoadBalancerRequest { LoadBalancerArn = lbArn });
    }

    // ── Invalid action returns error ────────────────────────────────────────

    [Fact]
    public async Task DescribeLoadBalancerAttributesForNonexistentLbThrows()
    {
        await Assert.ThrowsAsync<LoadBalancerNotFoundException>(async () =>
        {
            await _elb.DescribeLoadBalancerAttributesAsync(
                new DescribeLoadBalancerAttributesRequest
                {
                    LoadBalancerArn = "arn:aws:elasticloadbalancing:us-east-1:000000000000:loadbalancer/app/nonexistent/1234",
                });
        });
    }

    [Fact]
    public async Task DescribeTargetGroupAttributesForNonexistentTgThrows()
    {
        await Assert.ThrowsAsync<TargetGroupNotFoundException>(async () =>
        {
            await _elb.DescribeTargetGroupAttributesAsync(
                new DescribeTargetGroupAttributesRequest
                {
                    TargetGroupArn = "arn:aws:elasticloadbalancing:us-east-1:000000000000:targetgroup/nonexistent/1234",
                });
        });
    }

    // ── Tags for target group ───────────────────────────────────────────────

    [Fact]
    public async Task TagsOnTargetGroup()
    {
        var tgArn = (await _elb.CreateTargetGroupAsync(new CreateTargetGroupRequest
        {
            Name = "qa-tg-tags",
            Protocol = ProtocolEnum.HTTP,
            Port = 80,
            VpcId = "vpc-00000001",
            Tags = [new Tag { Key = "purpose", Value = "testing" }],
        })).TargetGroups[0].TargetGroupArn;

        var desc = await _elb.DescribeTagsAsync(new DescribeTagsRequest
        {
            ResourceArns = [tgArn],
        });
        var tagMap = desc.TagDescriptions[0].Tags.ToDictionary(t => t.Key, t => t.Value);
        Assert.Equal("testing", tagMap["purpose"]);

        await _elb.DeleteTargetGroupAsync(new DeleteTargetGroupRequest
        {
            TargetGroupArn = tgArn,
        });
    }

    // ── DescribeTags for multiple resources ─────────────────────────────────

    [Fact]
    public async Task DescribeTagsMultipleResources()
    {
        var lb1Arn = (await _elb.CreateLoadBalancerAsync(new CreateLoadBalancerRequest
        {
            Name = "qa-alb-multi-tags-1",
            Tags = [new Tag { Key = "env", Value = "dev" }],
        })).LoadBalancers[0].LoadBalancerArn;

        var lb2Arn = (await _elb.CreateLoadBalancerAsync(new CreateLoadBalancerRequest
        {
            Name = "qa-alb-multi-tags-2",
            Tags = [new Tag { Key = "env", Value = "prod" }],
        })).LoadBalancers[0].LoadBalancerArn;

        var desc = await _elb.DescribeTagsAsync(new DescribeTagsRequest
        {
            ResourceArns = [lb1Arn, lb2Arn],
        });
        Assert.Equal(2, desc.TagDescriptions.Count);

        var td1 = desc.TagDescriptions.First(td => td.ResourceArn == lb1Arn);
        Assert.Equal("dev", td1.Tags.First(t => t.Key == "env").Value);

        var td2 = desc.TagDescriptions.First(td => td.ResourceArn == lb2Arn);
        Assert.Equal("prod", td2.Tags.First(t => t.Key == "env").Value);

        await _elb.DeleteLoadBalancerAsync(new DeleteLoadBalancerRequest { LoadBalancerArn = lb1Arn });
        await _elb.DeleteLoadBalancerAsync(new DeleteLoadBalancerRequest { LoadBalancerArn = lb2Arn });
    }
}
