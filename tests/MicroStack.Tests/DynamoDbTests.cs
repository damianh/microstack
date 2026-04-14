using Amazon;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using Amazon.Runtime;
using Microsoft.AspNetCore.Mvc.Testing;

namespace MicroStack.Tests;

/// <summary>
/// Integration tests for the DynamoDB service handler.
/// Uses the AWS SDK for .NET v4 pointed at the in-process MicroStack server.
///
/// Mirrors coverage from ministack/tests/test_dynamodb.py.
/// </summary>
public sealed class DynamoDbTests : IClassFixture<MicroStackFixture>, IAsyncLifetime
{
    private readonly MicroStackFixture _fixture;
    private readonly AmazonDynamoDBClient _ddb;

    public DynamoDbTests(MicroStackFixture fixture)
    {
        _fixture = fixture;
        _ddb = CreateDdbClient(fixture);
    }

    private static AmazonDynamoDBClient CreateDdbClient(MicroStackFixture fixture)
    {
        var innerHandler = fixture.Factory.Server.CreateHandler();
        var httpClient = new HttpClient(new CanonicalizeUriHandler(innerHandler))
        {
            BaseAddress = new Uri("http://localhost/"),
        };

        var config = new AmazonDynamoDBConfig
        {
            RegionEndpoint    = RegionEndpoint.USEast1,
            ServiceURL        = "http://localhost/",
            HttpClientFactory = new FixedHttpClientFactory(httpClient),
        };

        return new AmazonDynamoDBClient(new BasicAWSCredentials("test", "test"), config);
    }

    public async Task InitializeAsync()
    {
        await _fixture.HttpClient.PostAsync("/_ministack/reset", null);
    }

    public Task DisposeAsync()
    {
        _ddb.Dispose();
        return Task.CompletedTask;
    }

    // ── Helpers ─────────────────────────────────────────────────────────────────

    private static CreateTableRequest HashOnlyTable(string name) => new()
    {
        TableName            = name,
        KeySchema            = [new KeySchemaElement("pk", KeyType.HASH)],
        AttributeDefinitions = [new AttributeDefinition("pk", ScalarAttributeType.S)],
        BillingMode          = BillingMode.PAY_PER_REQUEST,
    };

    private static CreateTableRequest CompositeTable(string name) => new()
    {
        TableName            = name,
        KeySchema            = [new KeySchemaElement("pk", KeyType.HASH), new KeySchemaElement("sk", KeyType.RANGE)],
        AttributeDefinitions = [new AttributeDefinition("pk", ScalarAttributeType.S), new AttributeDefinition("sk", ScalarAttributeType.S)],
        BillingMode          = BillingMode.PAY_PER_REQUEST,
    };

    private static Dictionary<string, AttributeValue> StrAttr(string key, string value) =>
        new() { [key] = new AttributeValue { S = value } };

    // ── Table operations ─────────────────────────────────────────────────────────

    [Fact]
    public async Task CreateTable_HashOnly()
    {
        var resp = await _ddb.CreateTableAsync(HashOnlyTable("t_hash_only"));
        var desc = resp.TableDescription;
        desc.TableName.ShouldBe("t_hash_only");
        desc.TableStatus.ShouldBe(TableStatus.ACTIVE);
        desc.KeySchema.ShouldContain(k => k.KeyType == KeyType.HASH);
    }

    [Fact]
    public async Task CreateTable_Composite()
    {
        var resp = await _ddb.CreateTableAsync(CompositeTable("t_composite"));
        var types = resp.TableDescription.KeySchema.Select(k => k.KeyType).ToHashSet();
        types.ShouldContain(KeyType.HASH);
        types.ShouldContain(KeyType.RANGE);
    }

    [Fact]
    public async Task CreateTable_Duplicate_ThrowsResourceInUseException()
    {
        await _ddb.CreateTableAsync(HashOnlyTable("t_hash_only"));
        var ex = await Should.ThrowAsync<ResourceInUseException>(() =>
            _ddb.CreateTableAsync(HashOnlyTable("t_hash_only")));
        ex.ShouldNotBeNull();
    }

    [Fact]
    public async Task DeleteTable_RemovesTable()
    {
        await _ddb.CreateTableAsync(HashOnlyTable("t_to_delete"));
        var resp = await _ddb.DeleteTableAsync("t_to_delete");
        resp.TableDescription.TableStatus.ShouldBe(TableStatus.DELETING);

        var list = await _ddb.ListTablesAsync();
        list.TableNames.ShouldNotContain("t_to_delete");
    }

    [Fact]
    public async Task DeleteTable_NotFound_ThrowsResourceNotFoundException()
    {
        await Should.ThrowAsync<ResourceNotFoundException>(() =>
            _ddb.DeleteTableAsync("t_nonexistent_xyz"));
    }

    [Fact]
    public async Task DescribeTable_WithGsiAndLsi()
    {
        await _ddb.CreateTableAsync(new CreateTableRequest
        {
            TableName            = "t_describe_gsi",
            KeySchema            = [new KeySchemaElement("pk", KeyType.HASH), new KeySchemaElement("sk", KeyType.RANGE)],
            AttributeDefinitions = [
                new AttributeDefinition("pk", ScalarAttributeType.S),
                new AttributeDefinition("sk", ScalarAttributeType.S),
                new AttributeDefinition("gsi_pk", ScalarAttributeType.S),
            ],
            GlobalSecondaryIndexes = [new GlobalSecondaryIndex
            {
                IndexName  = "gsi1",
                KeySchema  = [new KeySchemaElement("gsi_pk", KeyType.HASH)],
                Projection = new Projection { ProjectionType = ProjectionType.ALL },
            }],
            LocalSecondaryIndexes = [new LocalSecondaryIndex
            {
                IndexName  = "lsi1",
                KeySchema  = [new KeySchemaElement("pk", KeyType.HASH), new KeySchemaElement("sk", KeyType.RANGE)],
                Projection = new Projection { ProjectionType = ProjectionType.ALL },
            }],
            BillingMode = BillingMode.PAY_PER_REQUEST,
        });

        var resp  = await _ddb.DescribeTableAsync("t_describe_gsi");
        var table = resp.Table;
        table.TableName.ShouldBe("t_describe_gsi");
        table.GlobalSecondaryIndexes.ShouldHaveSingleItem();
        table.GlobalSecondaryIndexes[0].IndexName.ShouldBe("gsi1");
        table.LocalSecondaryIndexes.ShouldHaveSingleItem();
        table.LocalSecondaryIndexes[0].IndexName.ShouldBe("lsi1");
    }

    [Fact]
    public async Task ListTables_WithPagination()
    {
        for (var i = 0; i < 3; i++)
        {
            try { await _ddb.CreateTableAsync(HashOnlyTable($"t_list_{i}")); }
            catch (ResourceInUseException) { }
        }

        var resp = await _ddb.ListTablesAsync(new ListTablesRequest { Limit = 2 });
        (resp.TableNames.Count <= 2).ShouldBe(true);

        if (resp.LastEvaluatedTableName is not null)
        {
            var resp2 = await _ddb.ListTablesAsync(new ListTablesRequest
            {
                ExclusiveStartTableName = resp.LastEvaluatedTableName,
                Limit = 100,
            });
            (resp2.TableNames.Count >= 1).ShouldBe(true);
        }
    }

    [Fact]
    public async Task UpdateTable_BillingMode()
    {
        var name = $"intg-updtbl-{Guid.NewGuid():N}";
        await _ddb.CreateTableAsync(HashOnlyTable(name));

        var resp = await _ddb.UpdateTableAsync(new UpdateTableRequest
        {
            TableName            = name,
            BillingMode          = BillingMode.PROVISIONED,
            ProvisionedThroughput = new ProvisionedThroughput { ReadCapacityUnits = 5, WriteCapacityUnits = 5 },
        });
        resp.TableDescription.TableName.ShouldBe(name);
        await _ddb.DeleteTableAsync(name);
    }

    // ── Item operations ──────────────────────────────────────────────────────────

    [Fact]
    public async Task PutGetItem_AllTypes()
    {
        await _ddb.CreateTableAsync(HashOnlyTable("t_hash_only"));
        await _ddb.PutItemAsync("t_hash_only", new Dictionary<string, AttributeValue>
        {
            ["pk"]        = new() { S    = "allTypes" },
            ["str_attr"]  = new() { S    = "hello" },
            ["num_attr"]  = new() { N    = "42" },
            ["bool_attr"] = new() { BOOL = true },
            ["null_attr"] = new() { NULL = true },
            ["list_attr"] = new() { L    = [new AttributeValue { S = "a" }, new AttributeValue { N = "1" }] },
            ["map_attr"]  = new() { M    = new Dictionary<string, AttributeValue> { ["nested"] = new() { S = "value" } } },
            ["ss_attr"]   = new() { SS   = ["x", "y"] },
            ["ns_attr"]   = new() { NS   = ["1", "2", "3"] },
        });

        var resp = await _ddb.GetItemAsync("t_hash_only", StrAttr("pk", "allTypes"));
        var item = resp.Item;
        item["str_attr"].S.ShouldBe("hello");
        item["num_attr"].N.ShouldBe("42");
        item["bool_attr"].BOOL.ShouldBe(true);
        item["null_attr"].NULL.ShouldBe(true);
        item["list_attr"].L.Count.ShouldBe(2);
        item["map_attr"].M["nested"].S.ShouldBe("value");
        item["ss_attr"].SS.ToHashSet().ShouldBe(new HashSet<string> { "x", "y" });
        item["ns_attr"].NS.ToHashSet().ShouldBe(new HashSet<string> { "1", "2", "3" });
    }

    [Fact]
    public async Task BasicCrud_PutGetDelete()
    {
        await _ddb.CreateTableAsync(HashOnlyTable("t_crud"));

        await _ddb.PutItemAsync("t_crud", new Dictionary<string, AttributeValue>
        {
            ["pk"]   = new() { S = "key1" },
            ["data"] = new() { S = "value1" },
        });

        var get1 = await _ddb.GetItemAsync("t_crud", StrAttr("pk", "key1"));
        get1.Item["data"].S.ShouldBe("value1");

        await _ddb.DeleteItemAsync("t_crud", StrAttr("pk", "key1"));

        var get2 = await _ddb.GetItemAsync("t_crud", StrAttr("pk", "key1"));
        get2.Item.ShouldBeNull();
    }

    [Fact]
    public async Task PutItem_ConditionExpression_AttributeNotExists_Succeeds()
    {
        await _ddb.CreateTableAsync(HashOnlyTable("t_hash_only"));
        await _ddb.PutItemAsync(new PutItemRequest
        {
            TableName           = "t_hash_only",
            Item                = new Dictionary<string, AttributeValue> { ["pk"] = new() { S = "cond_new" }, ["val"] = new() { S = "first" } },
            ConditionExpression = "attribute_not_exists(pk)",
        });

        var resp = await _ddb.GetItemAsync("t_hash_only", StrAttr("pk", "cond_new"));
        resp.Item["val"].S.ShouldBe("first");
    }

    [Fact]
    public async Task PutItem_ConditionExpression_AttributeNotExists_FailsWhenExists()
    {
        await _ddb.CreateTableAsync(HashOnlyTable("t_hash_only"));
        await _ddb.PutItemAsync("t_hash_only", new Dictionary<string, AttributeValue>
            { ["pk"] = new() { S = "cond_fail" }, ["val"] = new() { S = "v1" } });

        await Should.ThrowAsync<ConditionalCheckFailedException>(() =>
            _ddb.PutItemAsync(new PutItemRequest
            {
                TableName           = "t_hash_only",
                Item                = new Dictionary<string, AttributeValue> { ["pk"] = new() { S = "cond_fail" }, ["val"] = new() { S = "v2" } },
                ConditionExpression = "attribute_not_exists(pk)",
            }));
    }

    [Fact]
    public async Task DeleteItem_ReturnAllOld()
    {
        await _ddb.CreateTableAsync(HashOnlyTable("t_hash_only"));
        await _ddb.PutItemAsync("t_hash_only", new Dictionary<string, AttributeValue>
            { ["pk"] = new() { S = "ret_old" }, ["data"] = new() { S = "precious" } });

        var resp = await _ddb.DeleteItemAsync(new DeleteItemRequest
        {
            TableName    = "t_hash_only",
            Key          = StrAttr("pk", "ret_old"),
            ReturnValues = ReturnValue.ALL_OLD,
        });
        resp.Attributes["data"].S.ShouldBe("precious");
    }

    [Fact]
    public async Task GetItem_MissingSortKey_ThrowsValidationException()
    {
        await _ddb.CreateTableAsync(new CreateTableRequest
        {
            TableName            = "t_get_missing_sk",
            KeySchema            = [new KeySchemaElement("pk", KeyType.HASH), new KeySchemaElement("sk", KeyType.RANGE)],
            AttributeDefinitions = [new AttributeDefinition("pk", ScalarAttributeType.S), new AttributeDefinition("sk", ScalarAttributeType.S)],
            BillingMode          = BillingMode.PAY_PER_REQUEST,
        });

        var ex = await Should.ThrowAsync<AmazonDynamoDBException>(() =>
            _ddb.GetItemAsync("t_get_missing_sk", StrAttr("pk", "q_pk")));
        (ex.ErrorCode ?? ex.Message).ShouldContain("ValidationException");
    }

    [Fact]
    public async Task GetItem_WrongKeyType_ThrowsValidationException()
    {
        await _ddb.CreateTableAsync(HashOnlyTable("t_get_wrong_type"));
        await _ddb.PutItemAsync("t_get_wrong_type", new Dictionary<string, AttributeValue>
            { ["pk"] = new() { S = "typed-key" } });

        var ex = await Should.ThrowAsync<AmazonDynamoDBException>(() =>
            _ddb.GetItemAsync("t_get_wrong_type",
                new Dictionary<string, AttributeValue> { ["pk"] = new() { N = "123" } }));
        (ex.ErrorCode ?? ex.Message).ShouldContain("ValidationException");
    }

    [Fact]
    public async Task UpdateItem_ExtraKeyAttribute_ThrowsValidationException()
    {
        await _ddb.CreateTableAsync(HashOnlyTable("t_update_extra_key"));

        var ex = await Should.ThrowAsync<AmazonDynamoDBException>(() =>
            _ddb.UpdateItemAsync(new UpdateItemRequest
            {
                TableName                = "t_update_extra_key",
                Key                      = new Dictionary<string, AttributeValue>
                {
                    ["pk"] = new() { S = "k1" },
                    ["sk"] = new() { S = "unexpected" },
                },
                UpdateExpression         = "SET v = :v",
                ExpressionAttributeValues = new Dictionary<string, AttributeValue> { [":v"] = new() { S = "x" } },
            }));
        (ex.ErrorCode ?? ex.Message).ShouldContain("ValidationException");
    }

    [Fact]
    public async Task UpdateItem_Set_ReturnsAllNew()
    {
        await _ddb.CreateTableAsync(HashOnlyTable("t_hash_only"));
        await _ddb.PutItemAsync("t_hash_only", new Dictionary<string, AttributeValue>
            { ["pk"] = new() { S = "upd_set" }, ["count"] = new() { N = "0" } });

        var resp = await _ddb.UpdateItemAsync(new UpdateItemRequest
        {
            TableName                = "t_hash_only",
            Key                      = StrAttr("pk", "upd_set"),
            UpdateExpression         = "SET #c = :val",
            ExpressionAttributeNames  = new Dictionary<string, string> { ["#c"] = "count" },
            ExpressionAttributeValues = new Dictionary<string, AttributeValue> { [":val"] = new() { N = "10" } },
            ReturnValues             = ReturnValue.ALL_NEW,
        });
        resp.Attributes["count"].N.ShouldBe("10");
    }

    [Fact]
    public async Task UpdateItem_Remove_ReturnsAllNew()
    {
        await _ddb.CreateTableAsync(HashOnlyTable("t_hash_only"));
        await _ddb.PutItemAsync("t_hash_only", new Dictionary<string, AttributeValue>
        {
            ["pk"]    = new() { S = "upd_rem" },
            ["extra"] = new() { S = "bye" },
            ["keep"]  = new() { S = "stay" },
        });

        var resp = await _ddb.UpdateItemAsync(new UpdateItemRequest
        {
            TableName        = "t_hash_only",
            Key              = StrAttr("pk", "upd_rem"),
            UpdateExpression = "REMOVE extra",
            ReturnValues     = ReturnValue.ALL_NEW,
        });
        resp.Attributes.ContainsKey("extra").ShouldBe(false);
        resp.Attributes["keep"].S.ShouldBe("stay");
    }

    [Fact]
    public async Task UpdateItem_Add_IncrementsNumber()
    {
        await _ddb.CreateTableAsync(HashOnlyTable("t_hash_only"));
        await _ddb.PutItemAsync("t_hash_only", new Dictionary<string, AttributeValue>
            { ["pk"] = new() { S = "upd_add" }, ["counter"] = new() { N = "5" } });

        var resp = await _ddb.UpdateItemAsync(new UpdateItemRequest
        {
            TableName                = "t_hash_only",
            Key                      = StrAttr("pk", "upd_add"),
            UpdateExpression         = "ADD counter :inc",
            ExpressionAttributeValues = new Dictionary<string, AttributeValue> { [":inc"] = new() { N = "3" } },
            ReturnValues             = ReturnValue.ALL_NEW,
        });
        resp.Attributes["counter"].N.ShouldBe("8");
    }

    [Fact]
    public async Task UpdateItem_ReturnAllOld()
    {
        await _ddb.CreateTableAsync(HashOnlyTable("t_hash_only"));
        await _ddb.PutItemAsync("t_hash_only", new Dictionary<string, AttributeValue>
            { ["pk"] = new() { S = "upd_old" }, ["v"] = new() { N = "1" } });

        var resp = await _ddb.UpdateItemAsync(new UpdateItemRequest
        {
            TableName                = "t_hash_only",
            Key                      = StrAttr("pk", "upd_old"),
            UpdateExpression         = "SET v = :new",
            ExpressionAttributeValues = new Dictionary<string, AttributeValue> { [":new"] = new() { N = "99" } },
            ReturnValues             = ReturnValue.ALL_OLD,
        });
        resp.Attributes["v"].N.ShouldBe("1");
    }

    [Fact]
    public async Task UpdateItem_ConditionOnMissingItem_ThrowsConditionalCheckFailedException()
    {
        await _ddb.CreateTableAsync(HashOnlyTable("t_update_cond_missing"));

        await Should.ThrowAsync<ConditionalCheckFailedException>(() =>
            _ddb.UpdateItemAsync(new UpdateItemRequest
            {
                TableName                = "t_update_cond_missing",
                Key                      = StrAttr("pk", "missing-update-item"),
                UpdateExpression         = "SET v = :v",
                ExpressionAttributeValues = new Dictionary<string, AttributeValue> { [":v"] = new() { S = "x" } },
                ConditionExpression      = "attribute_exists(pk)",
                ReturnValues             = ReturnValue.ALL_NEW,
            }));
    }

    [Fact]
    public async Task UpdateItem_UpdatedNew_ReturnsOnlyChangedAttributes()
    {
        await _ddb.CreateTableAsync(HashOnlyTable("qa-ddb-updated-new"));
        await _ddb.PutItemAsync("qa-ddb-updated-new", new Dictionary<string, AttributeValue>
        {
            ["pk"] = new() { S = "k1" },
            ["a"]  = new() { S = "old" },
            ["b"]  = new() { N = "1" },
        });

        var resp = await _ddb.UpdateItemAsync(new UpdateItemRequest
        {
            TableName                = "qa-ddb-updated-new",
            Key                      = StrAttr("pk", "k1"),
            UpdateExpression         = "SET a = :new",
            ExpressionAttributeValues = new Dictionary<string, AttributeValue> { [":new"] = new() { S = "new" } },
            ReturnValues             = ReturnValue.UPDATED_NEW,
        });
        resp.Attributes.ContainsKey("a").ShouldBe(true);
        resp.Attributes["a"].S.ShouldBe("new");
        resp.Attributes.ContainsKey("b").ShouldBe(false);
    }

    [Fact]
    public async Task UpdateItem_UpdatedOld_ReturnsOldChangedAttributes()
    {
        await _ddb.CreateTableAsync(HashOnlyTable("qa-ddb-updated-old"));
        await _ddb.PutItemAsync("qa-ddb-updated-old", new Dictionary<string, AttributeValue>
            { ["pk"] = new() { S = "k1" }, ["score"] = new() { N = "10" } });

        var resp = await _ddb.UpdateItemAsync(new UpdateItemRequest
        {
            TableName                = "qa-ddb-updated-old",
            Key                      = StrAttr("pk", "k1"),
            UpdateExpression         = "SET score = :new",
            ExpressionAttributeValues = new Dictionary<string, AttributeValue> { [":new"] = new() { N = "20" } },
            ReturnValues             = ReturnValue.UPDATED_OLD,
        });
        resp.Attributes["score"].N.ShouldBe("10");
    }

    // ── Query ────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task Query_PkOnly_ReturnsAllItems()
    {
        await _ddb.CreateTableAsync(CompositeTable("t_composite"));
        for (var i = 0; i < 3; i++)
            await _ddb.PutItemAsync("t_composite", new Dictionary<string, AttributeValue>
            {
                ["pk"] = new() { S = "q_pk" },
                ["sk"] = new() { S = $"sk_{i}" },
                ["n"]  = new() { N = i.ToString() },
            });

        var resp = await _ddb.QueryAsync(new QueryRequest
        {
            TableName                = "t_composite",
            KeyConditionExpression   = "pk = :pk",
            ExpressionAttributeValues = new Dictionary<string, AttributeValue> { [":pk"] = new() { S = "q_pk" } },
        });
        resp.Count.ShouldBe(3);
    }

    [Fact]
    public async Task Query_PkAndSk_BeginsWith()
    {
        await _ddb.CreateTableAsync(CompositeTable("t_composite"));
        for (var i = 0; i < 5; i++)
            await _ddb.PutItemAsync("t_composite", new Dictionary<string, AttributeValue>
            {
                ["pk"] = new() { S = "q_sk" },
                ["sk"] = new() { S = $"item_{i:D3}" },
            });

        var resp = await _ddb.QueryAsync(new QueryRequest
        {
            TableName                = "t_composite",
            KeyConditionExpression   = "pk = :pk AND begins_with(sk, :prefix)",
            ExpressionAttributeValues = new Dictionary<string, AttributeValue>
            {
                [":pk"]     = new() { S = "q_sk" },
                [":prefix"] = new() { S = "item_00" },
            },
        });
        (resp.Count >= 1).ShouldBe(true);
        foreach (var item in resp.Items)
            item["sk"].S.ShouldStartWith("item_00");
    }

    [Fact]
    public async Task Query_PkAndSk_Between()
    {
        await _ddb.CreateTableAsync(CompositeTable("t_composite"));
        for (var i = 0; i < 5; i++)
            await _ddb.PutItemAsync("t_composite", new Dictionary<string, AttributeValue>
            {
                ["pk"] = new() { S = "q_sk2" },
                ["sk"] = new() { S = $"item_{i:D3}" },
            });

        var resp = await _ddb.QueryAsync(new QueryRequest
        {
            TableName                = "t_composite",
            KeyConditionExpression   = "pk = :pk AND sk BETWEEN :lo AND :hi",
            ExpressionAttributeValues = new Dictionary<string, AttributeValue>
            {
                [":pk"] = new() { S = "q_sk2" },
                [":lo"] = new() { S = "item_001" },
                [":hi"] = new() { S = "item_003" },
            },
        });
        (resp.Count >= 1).ShouldBe(true);
        foreach (var item in resp.Items)
        {
            var sk = item["sk"].S;
            (string.Compare(sk, "item_001", StringComparison.Ordinal) >= 0).ShouldBe(true);
            (string.Compare(sk, "item_003", StringComparison.Ordinal) <= 0).ShouldBe(true);
        }
    }

    [Fact]
    public async Task Query_WithFilterExpression_ReducesCount()
    {
        await _ddb.CreateTableAsync(CompositeTable("t_composite"));
        for (var i = 0; i < 5; i++)
            await _ddb.PutItemAsync("t_composite", new Dictionary<string, AttributeValue>
            {
                ["pk"]  = new() { S = "q_filt" },
                ["sk"]  = new() { S = $"f_{i}" },
                ["val"] = new() { N = i.ToString() },
            });

        var resp = await _ddb.QueryAsync(new QueryRequest
        {
            TableName                = "t_composite",
            KeyConditionExpression   = "pk = :pk",
            FilterExpression         = "val > :min",
            ExpressionAttributeValues = new Dictionary<string, AttributeValue>
            {
                [":pk"]  = new() { S = "q_filt" },
                [":min"] = new() { N = "2" },
            },
        });
        resp.Count.ShouldBe(2);
        resp.ScannedCount.ShouldBe(5);
    }

    [Fact]
    public async Task Query_Pagination()
    {
        await _ddb.CreateTableAsync(CompositeTable("t_composite"));
        for (var i = 0; i < 6; i++)
            await _ddb.PutItemAsync("t_composite", new Dictionary<string, AttributeValue>
            {
                ["pk"] = new() { S = "q_page" },
                ["sk"] = new() { S = $"p_{i:D3}" },
            });

        var resp1 = await _ddb.QueryAsync(new QueryRequest
        {
            TableName                = "t_composite",
            KeyConditionExpression   = "pk = :pk",
            ExpressionAttributeValues = new Dictionary<string, AttributeValue> { [":pk"] = new() { S = "q_page" } },
            Limit                    = 3,
        });
        resp1.Count.ShouldBe(3);
        resp1.LastEvaluatedKey.ShouldNotBeEmpty();

        var resp2 = await _ddb.QueryAsync(new QueryRequest
        {
            TableName                = "t_composite",
            KeyConditionExpression   = "pk = :pk",
            ExpressionAttributeValues = new Dictionary<string, AttributeValue> { [":pk"] = new() { S = "q_page" } },
            ExclusiveStartKey        = resp1.LastEvaluatedKey,
            Limit                    = 3,
        });
        resp2.Count.ShouldBe(3);

        var page1Sks = resp1.Items.Select(it => it["sk"].S).ToHashSet();
        var page2Sks = resp2.Items.Select(it => it["sk"].S).ToHashSet();
        page1Sks.Intersect(page2Sks).ShouldBeEmpty();
    }

    [Fact]
    public async Task Query_WithFilterExpression_NumericSort()
    {
        await _ddb.CreateTableAsync(new CreateTableRequest
        {
            TableName            = "qa-ddb-filter",
            KeySchema            = [new KeySchemaElement("pk", KeyType.HASH), new KeySchemaElement("sk", KeyType.RANGE)],
            AttributeDefinitions = [new AttributeDefinition("pk", ScalarAttributeType.S), new AttributeDefinition("sk", ScalarAttributeType.N)],
            BillingMode          = BillingMode.PAY_PER_REQUEST,
        });

        for (var i = 0; i < 5; i++)
            await _ddb.PutItemAsync("qa-ddb-filter", new Dictionary<string, AttributeValue>
            {
                ["pk"]     = new() { S    = "user1" },
                ["sk"]     = new() { N    = i.ToString() },
                ["active"] = new() { BOOL = i % 2 == 0 },
            });

        var resp = await _ddb.QueryAsync(new QueryRequest
        {
            TableName                = "qa-ddb-filter",
            KeyConditionExpression   = "pk = :pk",
            FilterExpression         = "active = :t",
            ExpressionAttributeValues = new Dictionary<string, AttributeValue>
            {
                [":pk"] = new() { S    = "user1" },
                [":t"]  = new() { BOOL = true },
            },
        });
        resp.Count.ShouldBe(3);
        resp.ScannedCount.ShouldBe(5);
    }

    [Fact]
    public async Task Query_LegacyQueryFilter()
    {
        await _ddb.CreateTableAsync(new CreateTableRequest
        {
            TableName            = "intg-ddb-queryfilter",
            KeySchema            = [new KeySchemaElement("pk", KeyType.HASH), new KeySchemaElement("sk", KeyType.RANGE)],
            AttributeDefinitions = [new AttributeDefinition("pk", ScalarAttributeType.S), new AttributeDefinition("sk", ScalarAttributeType.S)],
            BillingMode          = BillingMode.PAY_PER_REQUEST,
        });

        for (var i = 0; i < 5; i++)
            await _ddb.PutItemAsync("intg-ddb-queryfilter", new Dictionary<string, AttributeValue>
            {
                ["pk"]     = new() { S = "qf_pk" },
                ["sk"]     = new() { S = $"sk_{i}" },
                ["status"] = new() { S = i < 3 ? "active" : "inactive" },
            });

        var resp = await _ddb.QueryAsync(new QueryRequest
        {
            TableName                = "intg-ddb-queryfilter",
            KeyConditionExpression   = "pk = :pk",
            ExpressionAttributeValues = new Dictionary<string, AttributeValue> { [":pk"] = new() { S = "qf_pk" } },
            QueryFilter              = new Dictionary<string, Condition>
            {
                ["status"] = new Condition
                {
                    AttributeValueList  = [new AttributeValue { S = "active" }],
                    ComparisonOperator  = ComparisonOperator.EQ,
                },
            },
        });
        resp.Count.ShouldBe(3);
        resp.ScannedCount.ShouldBe(5);
        foreach (var item in resp.Items)
            item["status"].S.ShouldBe("active");
    }

    // ── Scan ─────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task Scan_AllItems()
    {
        await _ddb.CreateTableAsync(HashOnlyTable("t_scan"));
        for (var i = 0; i < 8; i++)
            await _ddb.PutItemAsync("t_scan", new Dictionary<string, AttributeValue>
                { ["pk"] = new() { S = $"sc_{i}" }, ["n"] = new() { N = i.ToString() } });

        var resp = await _ddb.ScanAsync(new ScanRequest { TableName = "t_scan" });
        resp.Count.ShouldBe(8);
        resp.Items.Count.ShouldBe(8);
    }

    [Fact]
    public async Task Scan_WithFilterExpression()
    {
        await _ddb.CreateTableAsync(HashOnlyTable("t_scan"));
        for (var i = 0; i < 8; i++)
            await _ddb.PutItemAsync("t_scan", new Dictionary<string, AttributeValue>
                { ["pk"] = new() { S = $"sc_{i}" }, ["n"] = new() { N = i.ToString() } });

        var resp = await _ddb.ScanAsync(new ScanRequest
        {
            TableName                = "t_scan",
            FilterExpression         = "n >= :min",
            ExpressionAttributeValues = new Dictionary<string, AttributeValue> { [":min"] = new() { N = "5" } },
        });
        resp.Count.ShouldBe(3);
        foreach (var item in resp.Items)
            (int.Parse(item["n"].N) >= 5).ShouldBe(true);
    }

    [Fact]
    public async Task Scan_WithPagination()
    {
        await _ddb.CreateTableAsync(HashOnlyTable("qa-ddb-scan-page"));
        for (var i = 0; i < 10; i++)
            await _ddb.PutItemAsync("qa-ddb-scan-page", new Dictionary<string, AttributeValue>
                { ["pk"] = new() { S = $"item{i:D2}" } });

        var allItems = new List<Dictionary<string, AttributeValue>>();
        Dictionary<string, AttributeValue>? lek = null;
        do
        {
            var req = new ScanRequest { TableName = "qa-ddb-scan-page", Limit = 3 };
            if (lek is not null) req.ExclusiveStartKey = lek;
            var resp = await _ddb.ScanAsync(req);
            allItems.AddRange(resp.Items);
            lek = resp.LastEvaluatedKey?.Count > 0 ? resp.LastEvaluatedKey : null;
        } while (lek is not null);

        allItems.Count.ShouldBe(10);
    }

    [Fact]
    public async Task Scan_PaginationHashOnly_NoDuplicates()
    {
        await _ddb.CreateTableAsync(HashOnlyTable("t_hash_paginate"));
        for (var i = 0; i < 5; i++)
            await _ddb.PutItemAsync("t_hash_paginate", new Dictionary<string, AttributeValue>
                { ["pk"] = new() { S = $"item_{i:D3}" }, ["v"] = new() { N = i.ToString() } });

        var resp1 = await _ddb.ScanAsync(new ScanRequest { TableName = "t_hash_paginate", Limit = 3 });
        resp1.Count.ShouldBe(3);
        resp1.LastEvaluatedKey.ShouldNotBeEmpty();

        var resp2 = await _ddb.ScanAsync(new ScanRequest
        {
            TableName         = "t_hash_paginate",
            Limit             = 3,
            ExclusiveStartKey = resp1.LastEvaluatedKey,
        });
        resp2.Count.ShouldBe(2);

        var all = resp1.Items.Concat(resp2.Items).Select(it => it["pk"].S).ToHashSet();
        all.Count.ShouldBe(5);
    }

    [Fact]
    public async Task Scan_LegacyScanFilter()
    {
        await _ddb.CreateTableAsync(HashOnlyTable("intg-ddb-scanfilter"));
        for (var i = 0; i < 5; i++)
            await _ddb.PutItemAsync("intg-ddb-scanfilter", new Dictionary<string, AttributeValue>
            {
                ["pk"]    = new() { S = $"sf_{i}" },
                ["color"] = new() { S = i % 2 == 0 ? "red" : "blue" },
            });

        var resp = await _ddb.ScanAsync(new ScanRequest
        {
            TableName = "intg-ddb-scanfilter",
            ScanFilter = new Dictionary<string, Condition>
            {
                ["color"] = new Condition
                {
                    AttributeValueList = [new AttributeValue { S = "red" }],
                    ComparisonOperator = ComparisonOperator.EQ,
                },
            },
        });
        resp.Count.ShouldBe(3);
        foreach (var item in resp.Items)
            item["color"].S.ShouldBe("red");
    }

    // ── Batch operations ─────────────────────────────────────────────────────────

    [Fact]
    public async Task BatchWriteItem_PutsItems()
    {
        await _ddb.CreateTableAsync(HashOnlyTable("t_bw"));
        await _ddb.BatchWriteItemAsync(new BatchWriteItemRequest
        {
            RequestItems = new Dictionary<string, List<WriteRequest>>
            {
                ["t_bw"] = Enumerable.Range(0, 10).Select(i => new WriteRequest
                {
                    PutRequest = new PutRequest
                    {
                        Item = new Dictionary<string, AttributeValue>
                        {
                            ["pk"]   = new() { S = $"bw_{i}" },
                            ["data"] = new() { S = $"d{i}" },
                        },
                    },
                }).ToList(),
            },
        });

        var scan = await _ddb.ScanAsync(new ScanRequest { TableName = "t_bw" });
        scan.Count.ShouldBe(10);
    }

    [Fact]
    public async Task BatchGetItem_ReturnsItems()
    {
        await _ddb.CreateTableAsync(HashOnlyTable("t_bw"));
        for (var i = 0; i < 10; i++)
            await _ddb.PutItemAsync("t_bw", new Dictionary<string, AttributeValue>
                { ["pk"] = new() { S = $"bw_{i}" }, ["data"] = new() { S = $"d{i}" } });

        var resp = await _ddb.BatchGetItemAsync(new BatchGetItemRequest
        {
            RequestItems = new Dictionary<string, KeysAndAttributes>
            {
                ["t_bw"] = new KeysAndAttributes
                {
                    Keys = Enumerable.Range(0, 5).Select(i =>
                        new Dictionary<string, AttributeValue> { ["pk"] = new() { S = $"bw_{i}" } })
                        .ToList(),
                },
            },
        });
        resp.Responses["t_bw"].Count.ShouldBe(5);
    }

    [Fact]
    public async Task BatchGetItem_MissingTable_ReturnsUnprocessedKeys()
    {
        var resp = await _ddb.BatchGetItemAsync(new BatchGetItemRequest
        {
            RequestItems = new Dictionary<string, KeysAndAttributes>
            {
                ["qa-ddb-nonexistent-xyz"] = new KeysAndAttributes
                {
                    Keys = [new Dictionary<string, AttributeValue> { ["pk"] = new() { S = "k1" } }],
                },
            },
        });
        resp.UnprocessedKeys.ContainsKey("qa-ddb-nonexistent-xyz").ShouldBe(true);
    }

    [Fact]
    public async Task BatchWriteItem_WithConsumedCapacity()
    {
        await _ddb.CreateTableAsync(HashOnlyTable("batch-cap-regression"));
        var resp = await _ddb.BatchWriteItemAsync(new BatchWriteItemRequest
        {
            RequestItems = new Dictionary<string, List<WriteRequest>>
            {
                ["batch-cap-regression"] = [new WriteRequest
                {
                    PutRequest = new PutRequest { Item = new Dictionary<string, AttributeValue> { ["pk"] = new() { S = "k1" } } },
                }],
            },
            ReturnConsumedCapacity = ReturnConsumedCapacity.TOTAL,
        });
        resp.ConsumedCapacity.ShouldNotBeEmpty();
        resp.ConsumedCapacity[0].TableName.ShouldBe("batch-cap-regression");
        resp.ConsumedCapacity[0].CapacityUnits.ShouldBe(1.0);
        await _ddb.DeleteTableAsync("batch-cap-regression");
    }

    [Fact]
    public async Task PutItem_WithGsi_ReturnsCorrectConsumedCapacity()
    {
        await _ddb.CreateTableAsync(new CreateTableRequest
        {
            TableName            = "gsi-cap-put",
            AttributeDefinitions = [
                new AttributeDefinition("pk", ScalarAttributeType.S),
                new AttributeDefinition("sk", ScalarAttributeType.S),
                new AttributeDefinition("last_name", ScalarAttributeType.S),
            ],
            KeySchema            = [new KeySchemaElement("pk", KeyType.HASH), new KeySchemaElement("sk", KeyType.RANGE)],
            GlobalSecondaryIndexes = [new GlobalSecondaryIndex
            {
                IndexName  = "last_name-index",
                KeySchema  = [new KeySchemaElement("last_name", KeyType.HASH)],
                Projection = new Projection { ProjectionType = ProjectionType.ALL },
            }],
            BillingMode = BillingMode.PAY_PER_REQUEST,
        });

        var resp = await _ddb.PutItemAsync(new PutItemRequest
        {
            TableName              = "gsi-cap-put",
            Item                   = new Dictionary<string, AttributeValue>
            {
                ["pk"]        = new() { S = "p1" },
                ["sk"]        = new() { S = "s1" },
                ["last_name"] = new() { S = "Smith" },
            },
            ReturnConsumedCapacity = ReturnConsumedCapacity.TOTAL,
        });
        resp.ConsumedCapacity.CapacityUnits.ShouldBe(2.0);
        await _ddb.DeleteTableAsync("gsi-cap-put");
    }

    [Fact]
    public async Task BatchWriteItem_WithGsi_ReturnsCorrectConsumedCapacity()
    {
        await _ddb.CreateTableAsync(new CreateTableRequest
        {
            TableName            = "gsi-cap-batch",
            AttributeDefinitions = [
                new AttributeDefinition("pk", ScalarAttributeType.S),
                new AttributeDefinition("sk", ScalarAttributeType.S),
                new AttributeDefinition("age", ScalarAttributeType.N),
            ],
            KeySchema            = [new KeySchemaElement("pk", KeyType.HASH), new KeySchemaElement("sk", KeyType.RANGE)],
            GlobalSecondaryIndexes = [new GlobalSecondaryIndex
            {
                IndexName  = "age-index",
                KeySchema  = [new KeySchemaElement("age", KeyType.HASH)],
                Projection = new Projection { ProjectionType = ProjectionType.ALL },
            }],
            BillingMode = BillingMode.PAY_PER_REQUEST,
        });

        var resp = await _ddb.BatchWriteItemAsync(new BatchWriteItemRequest
        {
            RequestItems = new Dictionary<string, List<WriteRequest>>
            {
                ["gsi-cap-batch"] = [
                    new WriteRequest { PutRequest = new PutRequest { Item = new Dictionary<string, AttributeValue> { ["pk"] = new() { S = "p2" }, ["sk"] = new() { S = "s2" }, ["age"] = new() { N = "25" } } } },
                    new WriteRequest { PutRequest = new PutRequest { Item = new Dictionary<string, AttributeValue> { ["pk"] = new() { S = "p3" }, ["sk"] = new() { S = "s3" }, ["age"] = new() { N = "26" } } } },
                ],
            },
            ReturnConsumedCapacity = ReturnConsumedCapacity.TOTAL,
        });
        resp.ConsumedCapacity[0].CapacityUnits.ShouldBe(4.0);
        await _ddb.DeleteTableAsync("gsi-cap-batch");
    }

    // ── Transactions ─────────────────────────────────────────────────────────────

    [Fact]
    public async Task TransactWriteItems_PutAndDelete()
    {
        await _ddb.CreateTableAsync(HashOnlyTable("t_tx"));
        await _ddb.TransactWriteItemsAsync(new TransactWriteItemsRequest
        {
            TransactItems = [
                new TransactWriteItem { Put = new Put { TableName = "t_tx", Item = new Dictionary<string, AttributeValue> { ["pk"] = new() { S = "tx1" }, ["v"] = new() { S = "a" } } } },
                new TransactWriteItem { Put = new Put { TableName = "t_tx", Item = new Dictionary<string, AttributeValue> { ["pk"] = new() { S = "tx2" }, ["v"] = new() { S = "b" } } } },
                new TransactWriteItem { Put = new Put { TableName = "t_tx", Item = new Dictionary<string, AttributeValue> { ["pk"] = new() { S = "tx3" }, ["v"] = new() { S = "c" } } } },
            ],
        });

        var scan = await _ddb.ScanAsync(new ScanRequest { TableName = "t_tx" });
        scan.Count.ShouldBe(3);

        await _ddb.TransactWriteItemsAsync(new TransactWriteItemsRequest
        {
            TransactItems = [
                new TransactWriteItem { Delete = new Delete { TableName = "t_tx", Key = StrAttr("pk", "tx3") } },
                new TransactWriteItem
                {
                    Update = new Update
                    {
                        TableName                = "t_tx",
                        Key                      = StrAttr("pk", "tx1"),
                        UpdateExpression         = "SET v = :new",
                        ExpressionAttributeValues = new Dictionary<string, AttributeValue> { [":new"] = new() { S = "updated" } },
                    },
                },
            ],
        });

        var item = (await _ddb.GetItemAsync("t_tx", StrAttr("pk", "tx1"))).Item;
        item["v"].S.ShouldBe("updated");

        var gone = await _ddb.GetItemAsync("t_tx", StrAttr("pk", "tx3"));
        gone.Item.ShouldBeNull();
    }

    [Fact]
    public async Task TransactGetItems_ReturnsItems()
    {
        await _ddb.CreateTableAsync(HashOnlyTable("t_tx"));
        await _ddb.PutItemAsync("t_tx", new Dictionary<string, AttributeValue> { ["pk"] = new() { S = "tx1" }, ["v"] = new() { S = "a" } });
        await _ddb.PutItemAsync("t_tx", new Dictionary<string, AttributeValue> { ["pk"] = new() { S = "tx2" }, ["v"] = new() { S = "b" } });

        var resp = await _ddb.TransactGetItemsAsync(new TransactGetItemsRequest
        {
            TransactItems = [
                new TransactGetItem { Get = new Get { TableName = "t_tx", Key = StrAttr("pk", "tx1") } },
                new TransactGetItem { Get = new Get { TableName = "t_tx", Key = StrAttr("pk", "tx2") } },
            ],
        });
        resp.Responses.Count.ShouldBe(2);
        resp.Responses[0].Item["pk"].S.ShouldBe("tx1");
        resp.Responses[1].Item["pk"].S.ShouldBe("tx2");
    }

    [Fact]
    public async Task TransactWriteItems_ConditionFails_CancelsTransaction()
    {
        await _ddb.CreateTableAsync(HashOnlyTable("qa-ddb-transact"));
        await _ddb.PutItemAsync("qa-ddb-transact", new Dictionary<string, AttributeValue>
            { ["pk"] = new() { S = "existing" } });

        await Should.ThrowAsync<TransactionCanceledException>(() =>
            _ddb.TransactWriteItemsAsync(new TransactWriteItemsRequest
            {
                TransactItems = [
                    new TransactWriteItem
                    {
                        Put = new Put
                        {
                            TableName = "qa-ddb-transact",
                            Item      = new Dictionary<string, AttributeValue> { ["pk"] = new() { S = "new-item" } },
                        },
                    },
                    new TransactWriteItem
                    {
                        Put = new Put
                        {
                            TableName           = "qa-ddb-transact",
                            Item                = new Dictionary<string, AttributeValue> { ["pk"] = new() { S = "existing" }, ["data"] = new() { S = "x" } },
                            ConditionExpression = "attribute_not_exists(pk)",
                        },
                    },
                ],
            }));

        var resp = await _ddb.GetItemAsync("qa-ddb-transact", StrAttr("pk", "new-item"));
        resp.Item.ShouldBeNull();
    }

    // ── GSI Query ────────────────────────────────────────────────────────────────

    [Fact]
    public async Task Query_Gsi()
    {
        await _ddb.CreateTableAsync(new CreateTableRequest
        {
            TableName            = "t_gsi_q",
            KeySchema            = [new KeySchemaElement("pk", KeyType.HASH)],
            AttributeDefinitions = [
                new AttributeDefinition("pk", ScalarAttributeType.S),
                new AttributeDefinition("gsi_pk", ScalarAttributeType.S),
            ],
            GlobalSecondaryIndexes = [new GlobalSecondaryIndex
            {
                IndexName  = "gsi_index",
                KeySchema  = [new KeySchemaElement("gsi_pk", KeyType.HASH)],
                Projection = new Projection { ProjectionType = ProjectionType.ALL },
            }],
            BillingMode = BillingMode.PAY_PER_REQUEST,
        });

        for (var i = 0; i < 4; i++)
            await _ddb.PutItemAsync("t_gsi_q", new Dictionary<string, AttributeValue>
            {
                ["pk"]     = new() { S = $"main_{i}" },
                ["gsi_pk"] = new() { S = "shared_gsi" },
                ["data"]   = new() { N = i.ToString() },
            });

        await _ddb.PutItemAsync("t_gsi_q", new Dictionary<string, AttributeValue>
        {
            ["pk"]     = new() { S = "main_other" },
            ["gsi_pk"] = new() { S = "other_gsi" },
            ["data"]   = new() { N = "99" },
        });

        var resp = await _ddb.QueryAsync(new QueryRequest
        {
            TableName                = "t_gsi_q",
            IndexName                = "gsi_index",
            KeyConditionExpression   = "gsi_pk = :gpk",
            ExpressionAttributeValues = new Dictionary<string, AttributeValue> { [":gpk"] = new() { S = "shared_gsi" } },
        });
        resp.Count.ShouldBe(4);
        foreach (var item in resp.Items)
            item["gsi_pk"].S.ShouldBe("shared_gsi");
    }

    // ── TTL ──────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task Ttl_EnableDisable()
    {
        var table = $"intg-ttl-{Guid.NewGuid():N}";
        await _ddb.CreateTableAsync(HashOnlyTable(table));

        var desc1 = await _ddb.DescribeTimeToLiveAsync(table);
        desc1.TimeToLiveDescription.TimeToLiveStatus.ShouldBe(TimeToLiveStatus.DISABLED);

        await _ddb.UpdateTimeToLiveAsync(new UpdateTimeToLiveRequest
        {
            TableName                  = table,
            TimeToLiveSpecification    = new TimeToLiveSpecification { Enabled = true, AttributeName = "expires_at" },
        });

        var desc2 = await _ddb.DescribeTimeToLiveAsync(table);
        desc2.TimeToLiveDescription.TimeToLiveStatus.ShouldBe(TimeToLiveStatus.ENABLED);
        desc2.TimeToLiveDescription.AttributeName.ShouldBe("expires_at");

        await _ddb.UpdateTimeToLiveAsync(new UpdateTimeToLiveRequest
        {
            TableName                  = table,
            TimeToLiveSpecification    = new TimeToLiveSpecification { Enabled = false, AttributeName = "expires_at" },
        });

        var desc3 = await _ddb.DescribeTimeToLiveAsync(table);
        desc3.TimeToLiveDescription.TimeToLiveStatus.ShouldBe(TimeToLiveStatus.DISABLED);
        await _ddb.DeleteTableAsync(table);
    }

    [Fact]
    public async Task Ttl_ExpiredItemIsPresent_BeforeReaper()
    {
        var table = $"intg-ttl-exp-{Guid.NewGuid():N}";
        await _ddb.CreateTableAsync(HashOnlyTable(table));
        await _ddb.UpdateTimeToLiveAsync(new UpdateTimeToLiveRequest
        {
            TableName               = table,
            TimeToLiveSpecification = new TimeToLiveSpecification { Enabled = true, AttributeName = "expires_at" },
        });

        var past = DateTimeOffset.UtcNow.AddSeconds(-10).ToUnixTimeSeconds();
        await _ddb.PutItemAsync(table, new Dictionary<string, AttributeValue>
        {
            ["pk"]         = new() { S = "expired-item" },
            ["expires_at"] = new() { N = past.ToString() },
            ["data"]       = new() { S = "should-be-gone" },
        });

        var resp = await _ddb.GetItemAsync(table, StrAttr("pk", "expired-item"));
        resp.Item.ShouldNotBeEmpty();

        var ttlDesc = await _ddb.DescribeTimeToLiveAsync(table);
        ttlDesc.TimeToLiveDescription.TimeToLiveStatus.ShouldBe(TimeToLiveStatus.ENABLED);
        ttlDesc.TimeToLiveDescription.AttributeName.ShouldBe("expires_at");
    }

    // ── Continuous backups (PITR) ────────────────────────────────────────────────

    [Fact]
    public async Task DescribeContinuousBackups_DefaultDisabled()
    {
        await _ddb.CreateTableAsync(HashOnlyTable("ddb-pitr-tbl"));

        var resp = await _ddb.DescribeContinuousBackupsAsync(new DescribeContinuousBackupsRequest { TableName = "ddb-pitr-tbl" });
        var cbs  = resp.ContinuousBackupsDescription;
        cbs.ContinuousBackupsStatus.ShouldBe(ContinuousBackupsStatus.ENABLED);
        cbs.PointInTimeRecoveryDescription.PointInTimeRecoveryStatus.ShouldBe(PointInTimeRecoveryStatus.DISABLED);
    }

    [Fact]
    public async Task UpdateContinuousBackups_EnablesPitr()
    {
        await _ddb.CreateTableAsync(HashOnlyTable("ddb-pitr-tbl"));
        await _ddb.UpdateContinuousBackupsAsync(new UpdateContinuousBackupsRequest
        {
            TableName                          = "ddb-pitr-tbl",
            PointInTimeRecoverySpecification   = new PointInTimeRecoverySpecification { PointInTimeRecoveryEnabled = true },
        });

        var resp = await _ddb.DescribeContinuousBackupsAsync(new DescribeContinuousBackupsRequest { TableName = "ddb-pitr-tbl" });
        resp.ContinuousBackupsDescription.PointInTimeRecoveryDescription.PointInTimeRecoveryStatus.ShouldBe(PointInTimeRecoveryStatus.ENABLED);
    }

    // ── Tags ─────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task TagUntagResource()
    {
        await _ddb.CreateTableAsync(HashOnlyTable("ddb-tag-test"));
        var describe = await _ddb.DescribeTableAsync("ddb-tag-test");
        var arn = describe.Table.TableArn;

        await _ddb.TagResourceAsync(new TagResourceRequest
        {
            ResourceArn = arn,
            Tags = [new Tag { Key = "env", Value = "test" }, new Tag { Key = "team", Value = "platform" }],
        });

        var tags1 = (await _ddb.ListTagsOfResourceAsync(new ListTagsOfResourceRequest { ResourceArn = arn })).Tags;
        var keys1 = tags1.Select(t => t.Key).ToHashSet();
        keys1.ShouldContain("env");
        keys1.ShouldContain("team");

        await _ddb.UntagResourceAsync(new UntagResourceRequest { ResourceArn = arn, TagKeys = ["team"] });

        var tags2 = (await _ddb.ListTagsOfResourceAsync(new ListTagsOfResourceRequest { ResourceArn = arn })).Tags;
        var keys2 = tags2.Select(t => t.Key).ToHashSet();
        keys2.ShouldContain("env");
        keys2.ShouldNotContain("team");
    }

    // ── Endpoints ────────────────────────────────────────────────────────────────

    [Fact]
    public async Task DescribeEndpoints_ReturnsEndpoints()
    {
        var resp = await _ddb.DescribeEndpointsAsync(new DescribeEndpointsRequest());
        resp.Endpoints.ShouldNotBeEmpty();
        resp.Endpoints[0].Address.ShouldNotBeEmpty();
    }

    // ── Streams ──────────────────────────────────────────────────────────────────

    [Fact]
    public async Task StreamSpecification_TableHasStreamArn()
    {
        var tableName = "stream-arn-test";
        await _ddb.CreateTableAsync(new CreateTableRequest
        {
            TableName            = tableName,
            KeySchema            = [new KeySchemaElement("pk", KeyType.HASH)],
            AttributeDefinitions = [new AttributeDefinition("pk", ScalarAttributeType.S)],
            BillingMode          = BillingMode.PAY_PER_REQUEST,
            StreamSpecification  = new StreamSpecification { StreamEnabled = true, StreamViewType = StreamViewType.NEW_AND_OLD_IMAGES },
        });

        var desc = (await _ddb.DescribeTableAsync(tableName)).Table;
        (!string.IsNullOrEmpty(desc.LatestStreamArn) ||
            (desc.StreamSpecification?.StreamEnabled == true)).ShouldBe(true);

        await _ddb.PutItemAsync(tableName, new Dictionary<string, AttributeValue>
            { ["pk"] = new() { S = "k1" }, ["val"] = new() { S = "v1" } });

        await _ddb.UpdateItemAsync(new UpdateItemRequest
        {
            TableName                = tableName,
            Key                      = StrAttr("pk", "k1"),
            UpdateExpression         = "SET val = :v",
            ExpressionAttributeValues = new Dictionary<string, AttributeValue> { [":v"] = new() { S = "v2" } },
        });

        await _ddb.DeleteItemAsync(tableName, StrAttr("pk", "k1"));

        var get = await _ddb.GetItemAsync(tableName, StrAttr("pk", "k1"));
        get.Item.ShouldBeNull();
    }
}
