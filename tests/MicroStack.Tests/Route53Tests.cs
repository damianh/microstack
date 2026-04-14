using Amazon;
using Amazon.Route53;
using Amazon.Route53.Model;
using Amazon.Runtime;

namespace MicroStack.Tests;

/// <summary>
/// Integration tests for the Route53 service handler.
/// Uses the AWS SDK for .NET pointed at the in-process MicroStack server.
///
/// Mirrors coverage from ministack/tests/test_route53.py.
/// </summary>
public sealed class Route53Tests : IClassFixture<MicroStackFixture>, IAsyncLifetime
{
    private readonly MicroStackFixture _fixture;
    private readonly AmazonRoute53Client _r53;

    public Route53Tests(MicroStackFixture fixture)
    {
        _fixture = fixture;
        _r53 = CreateClient(fixture);
    }

    private static AmazonRoute53Client CreateClient(MicroStackFixture fixture)
    {
        var innerHandler = fixture.Factory.Server.CreateHandler();
        var httpClient = new HttpClient(new CanonicalizeUriHandler(innerHandler))
        {
            BaseAddress = new Uri("http://localhost/"),
        };

        var config = new AmazonRoute53Config
        {
            RegionEndpoint = RegionEndpoint.USEast1,
            ServiceURL = "http://localhost/",
            HttpClientFactory = new FixedHttpClientFactory(httpClient),
        };

        return new AmazonRoute53Client(
            new BasicAWSCredentials("test", "test"), config);
    }

    public async Task InitializeAsync()
    {
        await _fixture.HttpClient.PostAsync("/_ministack/reset", null);
    }

    public Task DisposeAsync()
    {
        _r53.Dispose();
        return Task.CompletedTask;
    }

    // ── Helpers ──────────────────────────────────────────────────────────────────

    private async Task<string> CreateZone(string name, string callerRef)
    {
        var resp = await _r53.CreateHostedZoneAsync(new CreateHostedZoneRequest
        {
            Name = name,
            CallerReference = callerRef,
        });
        return resp.HostedZone.Id.Split('/')[^1];
    }

    // ── Hosted zone tests ────────────────────────────────────────────────────────

    [Fact]
    public async Task CreateAndGetHostedZone()
    {
        var resp = await _r53.CreateHostedZoneAsync(new CreateHostedZoneRequest
        {
            Name = "example.com",
            CallerReference = "ref-create-1",
        });

        ((int)resp.HttpStatusCode).ShouldBe(201);
        var hz = resp.HostedZone;
        var zoneId = hz.Id.Split('/')[^1];
        hz.Name.ShouldBe("example.com.");
        resp.DelegationSet.ShouldNotBeNull();
        resp.DelegationSet.NameServers.Count.ShouldBe(4);

        var getResp = await _r53.GetHostedZoneAsync(new GetHostedZoneRequest
        {
            Id = zoneId,
        });
        getResp.HostedZone.Name.ShouldBe("example.com.");
        getResp.HostedZone.ResourceRecordSetCount.ShouldBe(2); // SOA + NS
    }

    [Fact]
    public async Task CreateZoneIdempotency()
    {
        await _r53.CreateHostedZoneAsync(new CreateHostedZoneRequest
        {
            Name = "idempotent.com",
            CallerReference = "ref-idem-1",
        });

        var resp2 = await _r53.CreateHostedZoneAsync(new CreateHostedZoneRequest
        {
            Name = "idempotent.com",
            CallerReference = "ref-idem-1",
        });
        // Same CallerReference → same zone returned, not a new one
        resp2.HostedZone.Name.ShouldBe("idempotent.com.");
    }

    [Fact]
    public async Task ListHostedZones()
    {
        await _r53.CreateHostedZoneAsync(new CreateHostedZoneRequest
        {
            Name = "list-test.com",
            CallerReference = "ref-list-1",
        });

        var resp = await _r53.ListHostedZonesAsync(new ListHostedZonesRequest());
        var names = resp.HostedZones.Select(hz => hz.Name).ToList();
        names.ShouldContain("list-test.com.");
    }

    [Fact]
    public async Task ListHostedZonesByName()
    {
        await _r53.CreateHostedZoneAsync(new CreateHostedZoneRequest
        {
            Name = "byname-alpha.com",
            CallerReference = "ref-bn-1",
        });
        await _r53.CreateHostedZoneAsync(new CreateHostedZoneRequest
        {
            Name = "byname-beta.com",
            CallerReference = "ref-bn-2",
        });

        var resp = await _r53.ListHostedZonesByNameAsync(new ListHostedZonesByNameRequest
        {
            DNSName = "byname-alpha.com",
        });
        resp.HostedZones[0].Name.ShouldBe("byname-alpha.com.");
    }

    [Fact]
    public async Task DeleteHostedZone()
    {
        var resp = await _r53.CreateHostedZoneAsync(new CreateHostedZoneRequest
        {
            Name = "delete-me.com",
            CallerReference = "ref-del-1",
        });
        var zoneId = resp.HostedZone.Id.Split('/')[^1];

        await _r53.DeleteHostedZoneAsync(new DeleteHostedZoneRequest
        {
            Id = zoneId,
        });

        var ex = await Should.ThrowAsync<NoSuchHostedZoneException>(() =>
            _r53.GetHostedZoneAsync(new GetHostedZoneRequest { Id = zoneId }));
        ex.ErrorCode.ShouldContain("NoSuchHostedZone");
    }

    [Fact]
    public async Task ChangeResourceRecordSetsCreate()
    {
        var zoneId = await CreateZone("records.com", "ref-rrs-1");

        var changeResp = await _r53.ChangeResourceRecordSetsAsync(new ChangeResourceRecordSetsRequest
        {
            HostedZoneId = zoneId,
            ChangeBatch = new ChangeBatch
            {
                Changes =
                [
                    new Change
                    {
                        Action = ChangeAction.CREATE,
                        ResourceRecordSet = new ResourceRecordSet
                        {
                            Name = "www.records.com",
                            Type = RRType.A,
                            TTL = 300,
                            ResourceRecords = [new ResourceRecord { Value = "1.2.3.4" }],
                        },
                    },
                ],
            },
        });

        changeResp.ChangeInfo.Status.Value.ShouldBe("INSYNC");
    }

    [Fact]
    public async Task ListResourceRecordSets()
    {
        var zoneId = await CreateZone("listrrs.com", "ref-lrrs-1");

        await _r53.ChangeResourceRecordSetsAsync(new ChangeResourceRecordSetsRequest
        {
            HostedZoneId = zoneId,
            ChangeBatch = new ChangeBatch
            {
                Changes =
                [
                    new Change
                    {
                        Action = ChangeAction.CREATE,
                        ResourceRecordSet = new ResourceRecordSet
                        {
                            Name = "mail.listrrs.com",
                            Type = RRType.MX,
                            TTL = 300,
                            ResourceRecords = [new ResourceRecord { Value = "10 mail.example.com." }],
                        },
                    },
                ],
            },
        });

        var listResp = await _r53.ListResourceRecordSetsAsync(new ListResourceRecordSetsRequest
        {
            HostedZoneId = zoneId,
        });

        var types = listResp.ResourceRecordSets.Select(rrs => rrs.Type.Value).ToList();
        types.ShouldContain("MX");
        types.ShouldContain("SOA");
        types.ShouldContain("NS");
    }

    [Fact]
    public async Task ListResourceRecordSetsStartNameUsesReversedLabelOrder()
    {
        var parentResp = await _r53.CreateHostedZoneAsync(new CreateHostedZoneRequest
        {
            Name = "parent-zone.com",
            CallerReference = "ref-parent-zone",
        });
        var parentZoneId = parentResp.HostedZone.Id.Split('/')[^1];

        var childResp = await _r53.CreateHostedZoneAsync(new CreateHostedZoneRequest
        {
            Name = "child.parent-zone.com",
            CallerReference = "ref-child-zone",
        });
        var childZoneId = childResp.HostedZone.Id.Split('/')[^1];

        // Get child NS records
        var childRecords = await _r53.ListResourceRecordSetsAsync(new ListResourceRecordSetsRequest
        {
            HostedZoneId = childZoneId,
        });
        var childNs = childRecords.ResourceRecordSets
            .First(rrs => rrs.Name == "child.parent-zone.com." && rrs.Type == RRType.NS);

        // Add child NS delegation to parent
        await _r53.ChangeResourceRecordSetsAsync(new ChangeResourceRecordSetsRequest
        {
            HostedZoneId = parentZoneId,
            ChangeBatch = new ChangeBatch
            {
                Changes =
                [
                    new Change
                    {
                        Action = ChangeAction.CREATE,
                        ResourceRecordSet = new ResourceRecordSet
                        {
                            Name = "child.parent-zone.com",
                            Type = RRType.NS,
                            TTL = childNs.TTL,
                            ResourceRecords = childNs.ResourceRecords,
                        },
                    },
                ],
            },
        });

        var listResp = await _r53.ListResourceRecordSetsAsync(new ListResourceRecordSetsRequest
        {
            HostedZoneId = parentZoneId,
            StartRecordName = "child.parent-zone.com.",
            StartRecordType = RRType.NS,
        });

        var returned = listResp.ResourceRecordSets;
        returned[0].Name.ShouldBe("child.parent-zone.com.");
        returned[0].Type.ShouldBe(RRType.NS);
        // Parent NS for "parent-zone.com." should not be in results
        returned.ShouldNotContain(rrs => rrs.Name == "parent-zone.com." && rrs.Type == RRType.NS);
    }

    [Fact]
    public async Task ListResourceRecordSetsTruncatedNextRecordUsesNextPageStart()
    {
        var zoneId = await CreateZone("pagination-zone.com", "ref-next-record");

        await _r53.ChangeResourceRecordSetsAsync(new ChangeResourceRecordSetsRequest
        {
            HostedZoneId = zoneId,
            ChangeBatch = new ChangeBatch
            {
                Changes =
                [
                    new Change
                    {
                        Action = ChangeAction.CREATE,
                        ResourceRecordSet = new ResourceRecordSet
                        {
                            Name = "token.pagination-zone.com",
                            Type = RRType.TXT,
                            TTL = 60,
                            ResourceRecords = [new ResourceRecord { Value = "\"target.pagination-zone.com\"" }],
                        },
                    },
                    new Change
                    {
                        Action = ChangeAction.CREATE,
                        ResourceRecordSet = new ResourceRecordSet
                        {
                            Name = "zz-next.pagination-zone.com",
                            Type = RRType.NS,
                            TTL = 120,
                            ResourceRecords =
                            [
                                new ResourceRecord { Value = "ns-1.example.com." },
                                new ResourceRecord { Value = "ns-2.example.com." },
                                new ResourceRecord { Value = "ns-3.example.com." },
                                new ResourceRecord { Value = "ns-4.example.com." },
                            ],
                        },
                    },
                ],
            },
        });

        var listResp = await _r53.ListResourceRecordSetsAsync(new ListResourceRecordSetsRequest
        {
            HostedZoneId = zoneId,
            StartRecordName = "token.pagination-zone.com.",
            StartRecordType = RRType.TXT,
            MaxItems = "1",
        });

        listResp.ResourceRecordSets[0].Name.ShouldBe("token.pagination-zone.com.");
        listResp.ResourceRecordSets[0].Type.ShouldBe(RRType.TXT);
        listResp.IsTruncated.ShouldBe(true);
        listResp.NextRecordName.ShouldBe("zz-next.pagination-zone.com.");
        listResp.NextRecordType.Value.ShouldBe("NS");
    }

    [Fact]
    public async Task ListResourceRecordSetsPaginationAdvancesWithNextRecordCursor()
    {
        var zoneId = await CreateZone("cursor-zone.com", "ref-cursor-pagination");

        await _r53.ChangeResourceRecordSetsAsync(new ChangeResourceRecordSetsRequest
        {
            HostedZoneId = zoneId,
            ChangeBatch = new ChangeBatch
            {
                Changes =
                [
                    new Change
                    {
                        Action = ChangeAction.CREATE,
                        ResourceRecordSet = new ResourceRecordSet
                        {
                            Name = "token.cursor-zone.com",
                            Type = RRType.TXT,
                            TTL = 60,
                            ResourceRecords = [new ResourceRecord { Value = "\"target.cursor-zone.com\"" }],
                        },
                    },
                    new Change
                    {
                        Action = ChangeAction.CREATE,
                        ResourceRecordSet = new ResourceRecordSet
                        {
                            Name = "zz-next.cursor-zone.com",
                            Type = RRType.NS,
                            TTL = 120,
                            ResourceRecords =
                            [
                                new ResourceRecord { Value = "ns-1.example.com." },
                                new ResourceRecord { Value = "ns-2.example.com." },
                                new ResourceRecord { Value = "ns-3.example.com." },
                                new ResourceRecord { Value = "ns-4.example.com." },
                            ],
                        },
                    },
                ],
            },
        });

        var firstPage = await _r53.ListResourceRecordSetsAsync(new ListResourceRecordSetsRequest
        {
            HostedZoneId = zoneId,
            StartRecordName = "token.cursor-zone.com.",
            StartRecordType = RRType.TXT,
            MaxItems = "1",
        });

        firstPage.ResourceRecordSets[0].Name.ShouldBe("token.cursor-zone.com.");
        firstPage.ResourceRecordSets[0].Type.ShouldBe(RRType.TXT);
        firstPage.IsTruncated.ShouldBe(true);

        var secondPage = await _r53.ListResourceRecordSetsAsync(new ListResourceRecordSetsRequest
        {
            HostedZoneId = zoneId,
            StartRecordName = firstPage.NextRecordName,
            StartRecordType = firstPage.NextRecordType,
            MaxItems = "1",
        });

        secondPage.ResourceRecordSets[0].Name.ShouldBe("zz-next.cursor-zone.com.");
        secondPage.ResourceRecordSets[0].Type.ShouldBe(RRType.NS);
        secondPage.ResourceRecordSets[0].Name.ShouldNotBe(firstPage.ResourceRecordSets[0].Name);
        secondPage.IsTruncated.ShouldBe(false);
    }

    [Fact]
    public async Task UpsertRecord()
    {
        var zoneId = await CreateZone("upsert.com", "ref-ups-1");

        foreach (var ip in new[] { "1.1.1.1", "2.2.2.2" })
        {
            await _r53.ChangeResourceRecordSetsAsync(new ChangeResourceRecordSetsRequest
            {
                HostedZoneId = zoneId,
                ChangeBatch = new ChangeBatch
                {
                    Changes =
                    [
                        new Change
                        {
                            Action = ChangeAction.UPSERT,
                            ResourceRecordSet = new ResourceRecordSet
                            {
                                Name = "www.upsert.com",
                                Type = RRType.A,
                                TTL = 60,
                                ResourceRecords = [new ResourceRecord { Value = ip }],
                            },
                        },
                    ],
                },
            });
        }

        var listResp = await _r53.ListResourceRecordSetsAsync(new ListResourceRecordSetsRequest
        {
            HostedZoneId = zoneId,
        });
        var aRecords = listResp.ResourceRecordSets.Where(rrs => rrs.Type == RRType.A).ToList();
        aRecords.ShouldHaveSingleItem();
        aRecords[0].ResourceRecords[0].Value.ShouldBe("2.2.2.2");
    }

    [Fact]
    public async Task DeleteRecord()
    {
        var zoneId = await CreateZone("delrec.com", "ref-dr-1");

        await _r53.ChangeResourceRecordSetsAsync(new ChangeResourceRecordSetsRequest
        {
            HostedZoneId = zoneId,
            ChangeBatch = new ChangeBatch
            {
                Changes =
                [
                    new Change
                    {
                        Action = ChangeAction.CREATE,
                        ResourceRecordSet = new ResourceRecordSet
                        {
                            Name = "www.delrec.com",
                            Type = RRType.A,
                            TTL = 300,
                            ResourceRecords = [new ResourceRecord { Value = "5.5.5.5" }],
                        },
                    },
                ],
            },
        });

        await _r53.ChangeResourceRecordSetsAsync(new ChangeResourceRecordSetsRequest
        {
            HostedZoneId = zoneId,
            ChangeBatch = new ChangeBatch
            {
                Changes =
                [
                    new Change
                    {
                        Action = ChangeAction.DELETE,
                        ResourceRecordSet = new ResourceRecordSet
                        {
                            Name = "www.delrec.com",
                            Type = RRType.A,
                            TTL = 300,
                            ResourceRecords = [new ResourceRecord { Value = "5.5.5.5" }],
                        },
                    },
                ],
            },
        });

        var listResp = await _r53.ListResourceRecordSetsAsync(new ListResourceRecordSetsRequest
        {
            HostedZoneId = zoneId,
        });
        var aRecords = listResp.ResourceRecordSets.Where(rrs => rrs.Type == RRType.A).ToList();
        aRecords.ShouldBeEmpty();
    }

    [Fact]
    public async Task GetChange()
    {
        var zoneId = await CreateZone("change-status.com", "ref-cs-1");

        var changeResp = await _r53.ChangeResourceRecordSetsAsync(new ChangeResourceRecordSetsRequest
        {
            HostedZoneId = zoneId,
            ChangeBatch = new ChangeBatch
            {
                Changes =
                [
                    new Change
                    {
                        Action = ChangeAction.CREATE,
                        ResourceRecordSet = new ResourceRecordSet
                        {
                            Name = "a.change-status.com",
                            Type = RRType.A,
                            TTL = 60,
                            ResourceRecords = [new ResourceRecord { Value = "9.9.9.9" }],
                        },
                    },
                ],
            },
        });

        var changeId = changeResp.ChangeInfo.Id.Split('/')[^1];
        var getChange = await _r53.GetChangeAsync(new GetChangeRequest
        {
            Id = changeId,
        });
        getChange.ChangeInfo.Status.ShouldBe(ChangeStatus.INSYNC);
    }

    [Fact]
    public async Task CreateHealthCheck()
    {
        var resp = await _r53.CreateHealthCheckAsync(new CreateHealthCheckRequest
        {
            CallerReference = "ref-hc-1",
            HealthCheckConfig = new HealthCheckConfig
            {
                IPAddress = "1.2.3.4",
                Port = 80,
                Type = HealthCheckType.HTTP,
                ResourcePath = "/health",
                RequestInterval = 30,
                FailureThreshold = 3,
            },
        });

        ((int)resp.HttpStatusCode).ShouldBe(201);
        var hcId = resp.HealthCheck.Id;
        resp.HealthCheck.HealthCheckConfig.Type.ShouldBe(HealthCheckType.HTTP);

        var getResp = await _r53.GetHealthCheckAsync(new GetHealthCheckRequest
        {
            HealthCheckId = hcId,
        });
        getResp.HealthCheck.Id.ShouldBe(hcId);
    }

    [Fact]
    public async Task ListHealthChecks()
    {
        await _r53.CreateHealthCheckAsync(new CreateHealthCheckRequest
        {
            CallerReference = "ref-hcl-1",
            HealthCheckConfig = new HealthCheckConfig
            {
                IPAddress = "2.2.2.2",
                Port = 443,
                Type = HealthCheckType.HTTPS,
            },
        });

        var resp = await _r53.ListHealthChecksAsync(new ListHealthChecksRequest());
        (resp.HealthChecks.Count >= 1).ShouldBe(true);
    }

    [Fact]
    public async Task DeleteHealthCheck()
    {
        var resp = await _r53.CreateHealthCheckAsync(new CreateHealthCheckRequest
        {
            CallerReference = "ref-hcd-1",
            HealthCheckConfig = new HealthCheckConfig
            {
                IPAddress = "3.3.3.3",
                Port = 80,
                Type = HealthCheckType.HTTP,
            },
        });
        var hcId = resp.HealthCheck.Id;

        await _r53.DeleteHealthCheckAsync(new DeleteHealthCheckRequest
        {
            HealthCheckId = hcId,
        });

        var ex = await Should.ThrowAsync<NoSuchHealthCheckException>(() =>
            _r53.GetHealthCheckAsync(new GetHealthCheckRequest { HealthCheckId = hcId }));
        ex.ErrorCode.ShouldContain("NoSuchHealthCheck");
    }

    [Fact]
    public async Task TagsForHostedZone()
    {
        var zoneId = await CreateZone("tagged.com", "ref-tag-1");

        await _r53.ChangeTagsForResourceAsync(new ChangeTagsForResourceRequest
        {
            ResourceType = TagResourceType.Hostedzone,
            ResourceId = zoneId,
            AddTags =
            [
                new Tag { Key = "env", Value = "test" },
                new Tag { Key = "team", Value = "infra" },
            ],
        });

        var tagsResp = await _r53.ListTagsForResourceAsync(new ListTagsForResourceRequest
        {
            ResourceType = TagResourceType.Hostedzone,
            ResourceId = zoneId,
        });
        var tags = tagsResp.ResourceTagSet.Tags.ToDictionary(t => t.Key, t => t.Value);
        tags["env"].ShouldBe("test");
        tags["team"].ShouldBe("infra");

        // Remove a tag
        await _r53.ChangeTagsForResourceAsync(new ChangeTagsForResourceRequest
        {
            ResourceType = TagResourceType.Hostedzone,
            ResourceId = zoneId,
            RemoveTagKeys = ["team"],
        });

        var tagsResp2 = await _r53.ListTagsForResourceAsync(new ListTagsForResourceRequest
        {
            ResourceType = TagResourceType.Hostedzone,
            ResourceId = zoneId,
        });
        var keys2 = tagsResp2.ResourceTagSet.Tags.Select(t => t.Key).ToList();
        keys2.ShouldContain("env");
        keys2.ShouldNotContain("team");
    }

    [Fact]
    public async Task NoSuchHostedZone()
    {
        var ex = await Should.ThrowAsync<NoSuchHostedZoneException>(() =>
            _r53.GetHostedZoneAsync(new GetHostedZoneRequest { Id = "ZNOTEXIST1234" }));
        ex.ErrorCode.ShouldContain("NoSuchHostedZone");
    }

    [Fact]
    public async Task AliasRecord()
    {
        var zoneId = await CreateZone("alias.com", "ref-alias-1");

        await _r53.ChangeResourceRecordSetsAsync(new ChangeResourceRecordSetsRequest
        {
            HostedZoneId = zoneId,
            ChangeBatch = new ChangeBatch
            {
                Changes =
                [
                    new Change
                    {
                        Action = ChangeAction.CREATE,
                        ResourceRecordSet = new ResourceRecordSet
                        {
                            Name = "www.alias.com",
                            Type = RRType.A,
                            AliasTarget = new AliasTarget
                            {
                                HostedZoneId = "Z2FDTNDATAQYW2",
                                DNSName = "d1234.cloudfront.net",
                                EvaluateTargetHealth = false,
                            },
                        },
                    },
                ],
            },
        });

        var listResp = await _r53.ListResourceRecordSetsAsync(new ListResourceRecordSetsRequest
        {
            HostedZoneId = zoneId,
        });
        var aliasRecs = listResp.ResourceRecordSets
            .Where(rrs => rrs.Type == RRType.A && rrs.AliasTarget is not null)
            .ToList();
        aliasRecs.ShouldHaveSingleItem();
        aliasRecs[0].AliasTarget.DNSName.ShouldBe("d1234.cloudfront.net.");
    }

    [Fact]
    public async Task DeleteZoneWithRecordsFails()
    {
        var zoneId = await CreateZone("qa-r53-nonempty.com.", $"qa-nonempty-{DateTimeOffset.UtcNow.ToUnixTimeSeconds()}");

        await _r53.ChangeResourceRecordSetsAsync(new ChangeResourceRecordSetsRequest
        {
            HostedZoneId = zoneId,
            ChangeBatch = new ChangeBatch
            {
                Changes =
                [
                    new Change
                    {
                        Action = ChangeAction.CREATE,
                        ResourceRecordSet = new ResourceRecordSet
                        {
                            Name = "www.qa-r53-nonempty.com.",
                            Type = RRType.A,
                            TTL = 300,
                            ResourceRecords = [new ResourceRecord { Value = "1.2.3.4" }],
                        },
                    },
                ],
            },
        });

        var ex = await Should.ThrowAsync<HostedZoneNotEmptyException>(() =>
            _r53.DeleteHostedZoneAsync(new DeleteHostedZoneRequest { Id = zoneId }));
        ex.ErrorCode.ShouldContain("HostedZoneNotEmpty");
    }

    [Fact]
    public async Task UpsertIsIdempotent()
    {
        var zoneId = await CreateZone("qa-r53-upsert.com.", $"qa-upsert-{DateTimeOffset.UtcNow.ToUnixTimeSeconds()}");

        foreach (var ip in new[] { "1.1.1.1", "2.2.2.2" })
        {
            await _r53.ChangeResourceRecordSetsAsync(new ChangeResourceRecordSetsRequest
            {
                HostedZoneId = zoneId,
                ChangeBatch = new ChangeBatch
                {
                    Changes =
                    [
                        new Change
                        {
                            Action = ChangeAction.UPSERT,
                            ResourceRecordSet = new ResourceRecordSet
                            {
                                Name = "api.qa-r53-upsert.com.",
                                Type = RRType.A,
                                TTL = 60,
                                ResourceRecords = [new ResourceRecord { Value = ip }],
                            },
                        },
                    ],
                },
            });
        }

        var records = await _r53.ListResourceRecordSetsAsync(new ListResourceRecordSetsRequest
        {
            HostedZoneId = zoneId,
        });
        var aRecords = records.ResourceRecordSets
            .Where(r => r.Name == "api.qa-r53-upsert.com." && r.Type == RRType.A)
            .ToList();
        aRecords.ShouldHaveSingleItem();
        aRecords[0].ResourceRecords[0].Value.ShouldBe("2.2.2.2");
    }

    [Fact]
    public async Task CreateRecordDuplicateFails()
    {
        var zoneId = await CreateZone("qa-r53-dup.com.", $"qa-dup-{DateTimeOffset.UtcNow.ToUnixTimeSeconds()}");

        await _r53.ChangeResourceRecordSetsAsync(new ChangeResourceRecordSetsRequest
        {
            HostedZoneId = zoneId,
            ChangeBatch = new ChangeBatch
            {
                Changes =
                [
                    new Change
                    {
                        Action = ChangeAction.CREATE,
                        ResourceRecordSet = new ResourceRecordSet
                        {
                            Name = "dup.qa-r53-dup.com.",
                            Type = RRType.A,
                            TTL = 60,
                            ResourceRecords = [new ResourceRecord { Value = "1.1.1.1" }],
                        },
                    },
                ],
            },
        });

        var ex = await Should.ThrowAsync<InvalidChangeBatchException>(() =>
            _r53.ChangeResourceRecordSetsAsync(new ChangeResourceRecordSetsRequest
            {
                HostedZoneId = zoneId,
                ChangeBatch = new ChangeBatch
                {
                    Changes =
                    [
                        new Change
                        {
                            Action = ChangeAction.CREATE,
                            ResourceRecordSet = new ResourceRecordSet
                            {
                                Name = "dup.qa-r53-dup.com.",
                                Type = RRType.A,
                                TTL = 60,
                                ResourceRecords = [new ResourceRecord { Value = "2.2.2.2" }],
                            },
                        },
                    ],
                },
            }));
        ex.ErrorCode.ShouldContain("InvalidChangeBatch");
    }

    [Fact]
    public async Task GetHostedZoneCount()
    {
        await CreateZone("count-test-1.com", "ref-count-1");
        await CreateZone("count-test-2.com", "ref-count-2");

        var resp = await _r53.GetHostedZoneCountAsync(new GetHostedZoneCountRequest());
        (resp.HostedZoneCount >= 2).ShouldBe(true);
    }

    [Fact]
    public async Task HealthCheckIdempotency()
    {
        var resp1 = await _r53.CreateHealthCheckAsync(new CreateHealthCheckRequest
        {
            CallerReference = "ref-hc-idem-1",
            HealthCheckConfig = new HealthCheckConfig
            {
                IPAddress = "4.4.4.4",
                Port = 80,
                Type = HealthCheckType.HTTP,
            },
        });

        var resp2 = await _r53.CreateHealthCheckAsync(new CreateHealthCheckRequest
        {
            CallerReference = "ref-hc-idem-1",
            HealthCheckConfig = new HealthCheckConfig
            {
                IPAddress = "4.4.4.4",
                Port = 80,
                Type = HealthCheckType.HTTP,
            },
        });

        // Same CallerReference → same health check returned
        resp2.HealthCheck.Id.ShouldBe(resp1.HealthCheck.Id);
    }

    [Fact]
    public async Task UpdateHealthCheck()
    {
        var resp = await _r53.CreateHealthCheckAsync(new CreateHealthCheckRequest
        {
            CallerReference = "ref-hc-update-1",
            HealthCheckConfig = new HealthCheckConfig
            {
                IPAddress = "5.5.5.5",
                Port = 80,
                Type = HealthCheckType.HTTP,
                ResourcePath = "/old-path",
            },
        });
        var hcId = resp.HealthCheck.Id;

        var updateResp = await _r53.UpdateHealthCheckAsync(new UpdateHealthCheckRequest
        {
            HealthCheckId = hcId,
            ResourcePath = "/new-path",
        });

        updateResp.HealthCheck.HealthCheckConfig.ResourcePath.ShouldBe("/new-path");
        updateResp.HealthCheck.HealthCheckVersion.ShouldBe(2);
    }

    [Fact]
    public async Task DeleteDeletedRecordFails()
    {
        var zoneId = await CreateZone("deltwice.com", "ref-deltwice-1");

        await _r53.ChangeResourceRecordSetsAsync(new ChangeResourceRecordSetsRequest
        {
            HostedZoneId = zoneId,
            ChangeBatch = new ChangeBatch
            {
                Changes =
                [
                    new Change
                    {
                        Action = ChangeAction.CREATE,
                        ResourceRecordSet = new ResourceRecordSet
                        {
                            Name = "www.deltwice.com",
                            Type = RRType.A,
                            TTL = 300,
                            ResourceRecords = [new ResourceRecord { Value = "1.1.1.1" }],
                        },
                    },
                ],
            },
        });

        // First delete succeeds
        await _r53.ChangeResourceRecordSetsAsync(new ChangeResourceRecordSetsRequest
        {
            HostedZoneId = zoneId,
            ChangeBatch = new ChangeBatch
            {
                Changes =
                [
                    new Change
                    {
                        Action = ChangeAction.DELETE,
                        ResourceRecordSet = new ResourceRecordSet
                        {
                            Name = "www.deltwice.com",
                            Type = RRType.A,
                            TTL = 300,
                            ResourceRecords = [new ResourceRecord { Value = "1.1.1.1" }],
                        },
                    },
                ],
            },
        });

        // Second delete fails
        var ex = await Should.ThrowAsync<InvalidChangeBatchException>(() =>
            _r53.ChangeResourceRecordSetsAsync(new ChangeResourceRecordSetsRequest
            {
                HostedZoneId = zoneId,
                ChangeBatch = new ChangeBatch
                {
                    Changes =
                    [
                        new Change
                        {
                            Action = ChangeAction.DELETE,
                            ResourceRecordSet = new ResourceRecordSet
                            {
                                Name = "www.deltwice.com",
                                Type = RRType.A,
                                TTL = 300,
                                ResourceRecords = [new ResourceRecord { Value = "1.1.1.1" }],
                            },
                        },
                    ],
                },
            }));
        ex.ErrorCode.ShouldContain("InvalidChangeBatch");
    }

    [Fact]
    public async Task ChangeRecordSetsOnNonExistentZoneFails()
    {
        var ex = await Should.ThrowAsync<NoSuchHostedZoneException>(() =>
            _r53.ChangeResourceRecordSetsAsync(new ChangeResourceRecordSetsRequest
            {
                HostedZoneId = "ZNOTEXIST1234",
                ChangeBatch = new ChangeBatch
                {
                    Changes =
                    [
                        new Change
                        {
                            Action = ChangeAction.CREATE,
                            ResourceRecordSet = new ResourceRecordSet
                            {
                                Name = "test.example.com",
                                Type = RRType.A,
                                TTL = 300,
                                ResourceRecords = [new ResourceRecord { Value = "1.1.1.1" }],
                            },
                        },
                    ],
                },
            }));
        ex.ErrorCode.ShouldContain("NoSuchHostedZone");
    }

    [Fact]
    public async Task ListResourceRecordSetsOnNonExistentZoneFails()
    {
        var ex = await Should.ThrowAsync<NoSuchHostedZoneException>(() =>
            _r53.ListResourceRecordSetsAsync(new ListResourceRecordSetsRequest
            {
                HostedZoneId = "ZNOTEXIST1234",
            }));
        ex.ErrorCode.ShouldContain("NoSuchHostedZone");
    }

    [Fact]
    public async Task CreateHostedZoneWithConfig()
    {
        var resp = await _r53.CreateHostedZoneAsync(new CreateHostedZoneRequest
        {
            Name = "configured.com",
            CallerReference = "ref-config-1",
            HostedZoneConfig = new HostedZoneConfig
            {
                Comment = "My test zone",
                PrivateZone = false,
            },
        });

        resp.HostedZone.Name.ShouldBe("configured.com.");
        resp.HostedZone.Config.Comment.ShouldBe("My test zone");
    }

    [Fact]
    public async Task MultipleRecordTypes()
    {
        var zoneId = await CreateZone("multi.com", "ref-multi-1");

        await _r53.ChangeResourceRecordSetsAsync(new ChangeResourceRecordSetsRequest
        {
            HostedZoneId = zoneId,
            ChangeBatch = new ChangeBatch
            {
                Changes =
                [
                    new Change
                    {
                        Action = ChangeAction.CREATE,
                        ResourceRecordSet = new ResourceRecordSet
                        {
                            Name = "www.multi.com",
                            Type = RRType.A,
                            TTL = 300,
                            ResourceRecords = [new ResourceRecord { Value = "1.2.3.4" }],
                        },
                    },
                    new Change
                    {
                        Action = ChangeAction.CREATE,
                        ResourceRecordSet = new ResourceRecordSet
                        {
                            Name = "www.multi.com",
                            Type = RRType.AAAA,
                            TTL = 300,
                            ResourceRecords = [new ResourceRecord { Value = "::1" }],
                        },
                    },
                    new Change
                    {
                        Action = ChangeAction.CREATE,
                        ResourceRecordSet = new ResourceRecordSet
                        {
                            Name = "multi.com",
                            Type = RRType.MX,
                            TTL = 300,
                            ResourceRecords = [new ResourceRecord { Value = "10 mail.multi.com." }],
                        },
                    },
                    new Change
                    {
                        Action = ChangeAction.CREATE,
                        ResourceRecordSet = new ResourceRecordSet
                        {
                            Name = "multi.com",
                            Type = RRType.TXT,
                            TTL = 300,
                            ResourceRecords = [new ResourceRecord { Value = "\"v=spf1 include:example.com ~all\"" }],
                        },
                    },
                ],
            },
        });

        var listResp = await _r53.ListResourceRecordSetsAsync(new ListResourceRecordSetsRequest
        {
            HostedZoneId = zoneId,
        });

        var types = listResp.ResourceRecordSets.Select(rrs => rrs.Type.Value).ToList();
        types.ShouldContain("A");
        types.ShouldContain("AAAA");
        types.ShouldContain("MX");
        types.ShouldContain("TXT");
        types.ShouldContain("SOA");
        types.ShouldContain("NS");
        listResp.ResourceRecordSets.Count.ShouldBe(6);
    }

    [Fact]
    public async Task UpsertCreatesWhenNotExist()
    {
        var zoneId = await CreateZone("upsert-new.com", "ref-upsert-new-1");

        await _r53.ChangeResourceRecordSetsAsync(new ChangeResourceRecordSetsRequest
        {
            HostedZoneId = zoneId,
            ChangeBatch = new ChangeBatch
            {
                Changes =
                [
                    new Change
                    {
                        Action = ChangeAction.UPSERT,
                        ResourceRecordSet = new ResourceRecordSet
                        {
                            Name = "new.upsert-new.com",
                            Type = RRType.CNAME,
                            TTL = 300,
                            ResourceRecords = [new ResourceRecord { Value = "target.example.com." }],
                        },
                    },
                ],
            },
        });

        var listResp = await _r53.ListResourceRecordSetsAsync(new ListResourceRecordSetsRequest
        {
            HostedZoneId = zoneId,
        });
        var cnameRecords = listResp.ResourceRecordSets
            .Where(rrs => rrs.Type == RRType.CNAME).ToList();
        cnameRecords.ShouldHaveSingleItem();
        cnameRecords[0].ResourceRecords[0].Value.ShouldBe("target.example.com.");
    }
}
