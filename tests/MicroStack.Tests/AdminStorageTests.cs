using System.Text;
using System.Text.Json;
using MicroStack.Admin.Contracts;
using MicroStack.Internal;
using MicroStack.Internal.Admin;
using MicroStack.Services.DynamoDb;
using MicroStack.Services.Efs;
using MicroStack.Services.ElastiCache;
using MicroStack.Services.Rds;
using MicroStack.Services.RdsData;
using MicroStack.Services.S3;
using MicroStack.Services.S3Files;

namespace MicroStack.Tests;

public sealed class AdminStorageTests : IDisposable
{
    public void Dispose() => AccountContext.Reset();

    [Fact]
    public async Task S3_ProjectsPrefixesObjectsVersionsAndLazyContent()
    {
        var handler = new S3ServiceHandler();
        await Send(handler, "PUT", "/admin-storage");
        await Send(handler, "PUT", "/admin-storage/a/value.json",
            """{"value":"old"}"""u8.ToArray(), ("content-type", "application/json"));
        await Send(handler, "PUT", "/admin-storage", "<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>"u8.ToArray(),
            query: new Dictionary<string, string[]> { ["versioning"] = [] });
        await Send(handler, "PUT", "/admin-storage/a/versioned.txt", "first"u8.ToArray(), ("content-type", "text/plain"));

        var source = (IAdminResourceSource)handler;
        source.GetAdminResourceKinds("s3").Select(x => x.Id)
            .ShouldBe(["buckets"]);
        var bucket = source.GetAdminResources("s3").Single();
        var prefix = bucket.ReadChildren!().Single(x => x.Resource.Key.Kind == "prefixes");
        var json = prefix.ReadChildren!().Single(x => x.Resource.Key.Id == "a/value.json");
        json.ReadContent!().Kind.ShouldBe("json");

        await Send(handler, "PUT", "/admin-storage/a/value.json",
            """{"value":"new"}"""u8.ToArray(), ("content-type", "application/json"));
        json.ReadContent!().Text!.ShouldContain("\"new\"");

        var versioned = prefix.ReadChildren!().Single(x => x.Resource.Key.Id == "a/versioned.txt");
        versioned.ReadChildren!().Single().Resource.Key.Kind.ShouldBe("versions");
    }

    [Fact]
    public async Task S3_ContentLimitIsBytesAndBinaryIsMetadataOnly()
    {
        var handler = new S3ServiceHandler();
        await Send(handler, "PUT", "/limits-bucket");
        await Send(handler, "PUT", "/limits-bucket/exact.txt",
            Enumerable.Repeat((byte)'a', AdminData.PreviewMaxBytes).ToArray(), ("content-type", "text/plain"));
        await Send(handler, "PUT", "/limits-bucket/unicode.txt",
            Encoding.UTF8.GetBytes(new string('\u00e9', AdminData.PreviewMaxBytes / 2 + 1)), ("content-type", "text/plain"));
        await Send(handler, "PUT", "/limits-bucket/blob.bin", [0, 1, 2], ("content-type", "application/octet-stream"));

        var children = ((IAdminResourceSource)handler).GetAdminResources("s3").Single().ReadChildren!().ToArray();
        children.Single(x => x.Resource.Key.Id == "exact.txt").ReadContent!().Kind.ShouldBe("text");
        children.Single(x => x.Resource.Key.Id == "unicode.txt").ReadContent!().Kind.ShouldBe("oversized");
        var binary = children.Single(x => x.Resource.Key.Id == "blob.bin").ReadContent!();
        binary.Kind.ShouldBe("binary");
        binary.Text.ShouldBeNull();
        binary.Length.ShouldBe(3);
    }

    [Fact]
    public async Task S3_SummariesKeepFullIdentityFriendlyNamesAndLiveVersionMetadata()
    {
        var handler = new S3ServiceHandler();
        await Send(handler, "PUT", "/parity-bucket");
        var source = (IAdminResourceSource)handler;
        var bucket = source.GetAdminResources("s3").Single();
        bucket.Resource.Type.ShouldBe("Bucket");
        bucket.Resource.Status.ShouldBeNull();
        bucket.ChildKinds.Select(x => x.Id).ShouldBe(["prefixes", "objects"]);
        bucket.ChildKinds.ShouldAllBe(x => !x.IsRoot);
        bucket.ReadSummary!().Single(x => x.Name == "ObjectCount").Value.ShouldBe("0");
        bucket.ReadChildren!().ShouldBeEmpty();

        await Send(handler, "PUT", "/parity-bucket",
            "<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>"u8.ToArray(),
            query: new Dictionary<string, string[]> { ["versioning"] = [] });
        await Send(handler, "PUT", "/parity-bucket/2026/日本語/value.json",
            "not valid json"u8.ToArray(), ("content-type", "application/json"));

        bucket.ReadSummary!().Single(x => x.Name == "ObjectCount").Value.ShouldBe("1");
        bucket.ReadSummary!().Single(x => x.Name == "Versioning").Value.ShouldBe("Enabled");
        var year = bucket.ReadChildren!().Single();
        var prefix = year.ReadChildren!().Single();
        prefix.Resource.Key.Id.ShouldBe("2026/日本語/");
        prefix.Resource.Name.ShouldBe("日本語/");
        prefix.Resource.Type.ShouldBe("Prefix");
        prefix.ChildKinds.Select(x => x.Id).ShouldBe(["prefixes", "objects"]);
        var obj = prefix.ReadChildren!().Single();
        obj.Resource.Key.Id.ShouldBe("2026/日本語/value.json");
        obj.Resource.Name.ShouldBe("value.json");
        obj.Resource.Type.ShouldBe("Object");
        obj.Resource.Summary.Select(x => x.Name).ShouldBe(["ContentType", "Length", "LastModified"]);
        obj.Resource.Summary.Single(x => x.Name == "Length").Value.ShouldBe("14");
        AssertIsoUtc(obj.Resource.Summary.Single(x => x.Name == "LastModified"));
        AssertIsoUtc(bucket.ReadFields!().Single(x => x.Name == "CreationDate"));
        obj.ReadFields!().Single(x => x.Name == "ETag").Secondary.ShouldBeTrue();
        obj.ReadFields!().Single(x => x.Name == "ContentType").Secondary.ShouldBeFalse();
        obj.ReadContent!().Kind.ShouldBe("unavailable");

        var version = obj.ReadChildren!().Single();
        version.Resource.Type.ShouldBe("Version");
        version.Resource.Key.Kind.ShouldBe("versions");
        version.ReadSummary!().ShouldBe(obj.Resource.Summary);
        await Send(handler, "PUT", "/parity-bucket/2026/日本語/value.json",
            """{"ok":true}"""u8.ToArray(), ("content-type", "application/json"));
        obj.ReadSummary!().Single(x => x.Name == "Length").Value.ShouldBe("11");
        version.ReadSummary!().ShouldBeEmpty();
        version.ReadFields!().ShouldBeEmpty();
        version.ReadContent!().Kind.ShouldBe("unavailable");
        obj.ReadChildren!().Single().Resource.Key.Id.ShouldNotBe(version.Resource.Key.Id);
    }

    [Fact]
    public async Task S3_ConfigurationUsesRetainedValuesAndMasksSensitiveTags()
    {
        var handler = new S3ServiceHandler();
        await Send(handler, "PUT", "/settings-bucket");
        await Send(handler, "PUT", "/settings-bucket",
            "<ServerSideEncryptionConfiguration><Rule><ApplyServerSideEncryptionByDefault><SSEAlgorithm>AES256</SSEAlgorithm></ApplyServerSideEncryptionByDefault></Rule></ServerSideEncryptionConfiguration>"u8.ToArray(),
            query: new Dictionary<string, string[]> { ["encryption"] = [] });
        await Send(handler, "PUT", "/settings-bucket",
            "<Tagging><TagSet><Tag><Key>environment</Key><Value>local</Value></Tag><Tag><Key>secret</Key><Value>do-not-display</Value></Tag></TagSet></Tagging>"u8.ToArray(),
            query: new Dictionary<string, string[]> { ["tagging"] = [] });
        await Send(handler, "PUT", "/settings-bucket/file.txt", "hello"u8.ToArray(),
            ("content-type", "text/plain"), ("x-amz-meta-token", "do-not-display"),
            ("cache-control", "max-age=60"));

        var bucket = ((IAdminResourceSource)handler).GetAdminResources("s3").Single();
        var fields = bucket.ReadFields!();
        fields.Single(x => x.Name == "Versioning").Value.ShouldBe("Disabled");
        fields.Single(x => x.Name == "EncryptionConfigured").Value.ShouldBe("True");
        fields.Single(x => x.Name == "EncryptionAlgorithm").Value.ShouldBe("AES256");
        fields.Single(x => x.Name == "LifecycleConfigured").Value.ShouldBe("False");
        fields.Single(x => x.Name == "Tags.environment").Value.ShouldBe("local");
        fields.Single(x => x.Name == "Tags.secret").Value.ShouldBe(AdminData.MaskedValue);
        var obj = bucket.ReadChildren!().Single();
        obj.ChildKinds.Single().Id.ShouldBe("versions");
        obj.ReadChildren!().ShouldBeEmpty();
        var token = obj.ReadFields!().Single(x => x.Name == "Metadata.x-amz-meta-token");
        token.Sensitive.ShouldBeTrue();
        token.Value.ShouldBe(AdminData.MaskedValue);
        obj.ReadFields!().Single(x => x.Name == "Headers.cache-control").Value.ShouldBe("max-age=60");
        obj.Resource.Summary.ShouldNotContain(x => x.Name.StartsWith("Metadata.", StringComparison.Ordinal));

        await Send(handler, "DELETE", "/settings-bucket/file.txt");
        obj.ReadSummary!().ShouldBeEmpty();
        obj.ReadFields!().ShouldBeEmpty();
        obj.ReadContent!().Kind.ShouldBe("unavailable");
        bucket.ReadSummary!().Single(x => x.Name == "ObjectCount").Value.ShouldBe("0");
    }

    [Fact]
    public async Task DynamoDb_PreservesTypedCompositeKeysAndDoesNotMutateTtl()
    {
        var handler = new DynamoDbServiceHandler();
        await Ddb(handler, "CreateTable", """
            {"TableName":"typed","KeySchema":[{"AttributeName":"pk","KeyType":"HASH"},{"AttributeName":"sk","KeyType":"RANGE"}],
             "AttributeDefinitions":[{"AttributeName":"pk","AttributeType":"N"},{"AttributeName":"sk","AttributeType":"B"}],
             "BillingMode":"PAY_PER_REQUEST"}
            """);
        await Ddb(handler, "PutItem", """
            {"TableName":"typed","Item":{"pk":{"N":"01"},"sk":{"B":"AQI="},"text":{"S":"hello"},"flag":{"BOOL":true}}}
            """);
        await Ddb(handler, "UpdateTimeToLive", """
            {"TableName":"typed","TimeToLiveSpecification":{"Enabled":true,"AttributeName":"expires"}}
            """);

        var table = ((IAdminResourceSource)handler).GetAdminResources("dynamodb").Single();
        var before = await Ddb(handler, "DescribeTimeToLive", """{"TableName":"typed"}""");
        var item = table.ReadChildren!().Single();
        item.Resource.Name.ShouldContain("""pk={"N":"01"}""");
        item.Resource.Name.ShouldContain("""sk={"B":"AQI="}""");
        item.ReadContent!().Text!.ShouldContain("\"flag\":{\"BOOL\":true}");
        _ = table.ReadFields!();
        var after = await Ddb(handler, "DescribeTimeToLive", """{"TableName":"typed"}""");
        Encoding.UTF8.GetString(after.Body).ShouldBe(Encoding.UTF8.GetString(before.Body));
    }

    [Fact]
    public async Task DynamoDb_SummariesExposeSchemaActualKeysAndRetainedCountWithoutItemBodies()
    {
        var handler = new DynamoDbServiceHandler();
        await Ddb(handler, "CreateTable", """
            {"TableName":"parity","KeySchema":[{"AttributeName":"PK","KeyType":"HASH"},{"AttributeName":"SK","KeyType":"RANGE"}],
             "AttributeDefinitions":[{"AttributeName":"PK","AttributeType":"S"},{"AttributeName":"SK","AttributeType":"N"}],
             "BillingMode":"PAY_PER_REQUEST","StreamSpecification":{"StreamEnabled":true,"StreamViewType":"NEW_IMAGE"},
             "SSESpecification":{"Enabled":true,"SSEType":"KMS","KMSMasterKeyId":"alias/local"}}
            """);
        var table = ((IAdminResourceSource)handler).GetAdminResources("dynamodb").Single();
        table.Resource.Type.ShouldBe("Table");
        table.ChildKinds.Single().Id.ShouldBe("items");
        table.ChildKinds.Single().IsRoot.ShouldBeFalse();
        table.ReadChildren!().ShouldBeEmpty();
        table.ReadSummary!().Single(x => x.Name == "ItemCount").Value.ShouldBe("0");
        table.ReadSummary!().Single(x => x.Name == "PartitionKey").Value.ShouldBe("PK (S)");
        table.ReadSummary!().Single(x => x.Name == "SortKey").Value.ShouldBe("SK (N)");
        await Ddb(handler, "PutItem", """
            {"TableName":"parity","Item":{"PK":{"S":"注文/42"},"SK":{"N":"01"},"payload":{"M":{"list":{"L":[{"BOOL":true},{"NULL":true}]}}},
             "strings":{"SS":["a","b"]},"numbers":{"NS":["1","2"]},"binary":{"B":"AQI="},"binaries":{"BS":["AQI="]}}}
            """);
        await Ddb(handler, "UpdateTimeToLive", """
            {"TableName":"parity","TimeToLiveSpecification":{"Enabled":true,"AttributeName":"expires"}}
            """);
        await Ddb(handler, "TagResource", $$"""
            {"ResourceArn":"{{table.Resource.Arn}}","Tags":[{"Key":"environment","Value":"local"},{"Key":"auth-token","Value":"hidden"}]}
            """);
        var beforeTtl = await Ddb(handler, "DescribeTimeToLive", """{"TableName":"parity"}""");
        table.ReadSummary!().Single(x => x.Name == "ItemCount").Value.ShouldBe("1");
        var item = table.ReadChildren!().Single();
        item.Resource.Type.ShouldBe("Item");
        item.Resource.Summary.Count.ShouldBe(2);
        item.Resource.Summary.Single(x => x.Name == "PK").Format.ShouldBe("dynamodb-attribute");
        item.Resource.Summary.Single(x => x.Name == "SK").Value.ShouldBe("""{"N":"01"}""");
        item.Resource.Summary.ShouldAllBe(x => !x.Secondary);
        item.ReadFields!().Single(x => x.Name == "AttributeCount").Secondary.ShouldBeTrue();
        var fields = table.ReadFields!();
        fields.Single(x => x.Name == "PartitionKeyType").Value.ShouldBe("S");
        var keySchema = fields.Single(x => x.Name == "KeySchema").Value;
        keySchema.ShouldNotBeNull();
        keySchema.ShouldContain("\"KeyType\":\"HASH\"");
        var streamSpecification = fields.Single(x => x.Name == "StreamSpecification").Value;
        streamSpecification.ShouldNotBeNull();
        streamSpecification.ShouldContain("\"StreamEnabled\":true");
        var sseDescription = fields.Single(x => x.Name == "SSEDescription").Value;
        sseDescription.ShouldNotBeNull();
        sseDescription.ShouldContain("alias/local");
        fields.Single(x => x.Name == "Tags.environment").Value.ShouldBe("local");
        fields.Single(x => x.Name == "Tags.auth-token").Value.ShouldBe(AdminData.MaskedValue);
        AssertIsoUtc(fields.Single(x => x.Name == "CreationDateTime"));
        using var content = JsonDocument.Parse(item.ReadContent!().Text!);
        content.RootElement.GetProperty("SK").GetProperty("N").GetString().ShouldBe("01");
        content.RootElement.GetProperty("payload").GetProperty("M").GetProperty("list").GetProperty("L")[0]
            .GetProperty("BOOL").GetBoolean().ShouldBeTrue();
        content.RootElement.GetProperty("strings").GetProperty("SS").GetArrayLength().ShouldBe(2);
        content.RootElement.GetProperty("numbers").GetProperty("NS").GetArrayLength().ShouldBe(2);
        content.RootElement.GetProperty("binary").GetProperty("B").GetString().ShouldBe("AQI=");
        content.RootElement.GetProperty("binaries").GetProperty("BS").GetArrayLength().ShouldBe(1);
        var afterTtl = await Ddb(handler, "DescribeTimeToLive", """{"TableName":"parity"}""");
        afterTtl.Body.ShouldBe(beforeTtl.Body);

        await Ddb(handler, "DeleteItem", """{"TableName":"parity","Key":{"PK":{"S":"注文/42"},"SK":{"N":"01"}}}""");
        table.ReadSummary!().Single(x => x.Name == "ItemCount").Value.ShouldBe("0");
        item.ReadSummary!().ShouldBeEmpty();
        item.ReadFields!().ShouldBeEmpty();
        item.ReadContent!().Kind.ShouldBe("unavailable");
    }

    [Fact]
    public async Task DynamoDb_HashOnlySchemaOmitsInventedSortKeySummary()
    {
        var handler = new DynamoDbServiceHandler();
        await Ddb(handler, "CreateTable", """
            {"TableName":"hash-only","KeySchema":[{"AttributeName":"id","KeyType":"HASH"}],
             "AttributeDefinitions":[{"AttributeName":"id","AttributeType":"B"}]}
            """);
        await Ddb(handler, "PutItem", """{"TableName":"hash-only","Item":{"id":{"B":"AQI="},"body":{"S":"payload-only"}}}""");
        var table = ((IAdminResourceSource)handler).GetAdminResources("dynamodb").Single();
        table.Resource.Summary.Select(x => x.Name).ShouldBe(["PartitionKey", "ItemCount"]);
        var item = table.ReadChildren!().Single();
        item.Resource.Summary.Single().Value.ShouldBe("""{"B":"AQI="}""");
        item.ReadSummary!().ShouldBe(item.Resource.Summary);
    }

    [Fact]
    public async Task ProvidersAreAccountScoped()
    {
        var handler = new S3ServiceHandler();
        AccountContext.SetFromAccessKey("111111111111");
        await Send(handler, "PUT", "/account-bucket");
        ((IAdminResourceSource)handler).GetAdminResources("s3").Count().ShouldBe(1);

        AccountContext.SetFromAccessKey("222222222222");
        ((IAdminResourceSource)handler).GetAdminResources("s3").ShouldBeEmpty();

        AccountContext.SetFromAccessKey("111111111111");
        ((IAdminResourceSource)handler).GetAdminResources("s3").Single()
            .ReadFields!().Single(x => x.Name == "AccountId").Value.ShouldBe("111111111111");
    }

    [Fact]
    public void StorageProvidersExposeOnlyTheirRetainedKinds()
    {
        ((IAdminResourceSource)new EfsServiceHandler()).GetAdminResourceKinds("elasticfilesystem").ShouldNotBeEmpty();
        ((IAdminResourceSource)new S3FilesServiceHandler()).GetAdminResourceKinds("s3files").ShouldNotBeEmpty();
        ((IAdminResourceSource)new RdsServiceHandler()).GetAdminResourceKinds("rds").ShouldNotBeEmpty();
        ((IAdminResourceSource)new ElastiCacheServiceHandler()).GetAdminResourceKinds("elasticache").ShouldNotBeEmpty();

        var data = (IAdminResourceSource)new RdsDataServiceHandler();
        data.GetAdminResourceKinds("rds-data").ShouldBeEmpty();
        data.GetAdminResources("rds-data").ShouldBeEmpty();
        data.GetAdminNotice("rds-data")!.ShouldContain("no database resources");
        data.GetAdminNotice("rds-data")!.Contains("SELECT", StringComparison.OrdinalIgnoreCase).ShouldBeFalse();
    }

    private static Task<ServiceResponse> Ddb(DynamoDbServiceHandler handler, string action, string body) =>
        Send(handler, "POST", "/", Encoding.UTF8.GetBytes(body),
            ("x-amz-target", $"DynamoDB_20120810.{action}"),
            ("content-type", "application/x-amz-json-1.0"));

    private static void AssertIsoUtc(AdminField field)
    {
        field.Format.ShouldBe("datetime");
        field.Value.ShouldNotBeNull();
        field.Value.ShouldEndWith("Z");
        DateTimeOffset.TryParse(field.Value, System.Globalization.CultureInfo.InvariantCulture,
            System.Globalization.DateTimeStyles.RoundtripKind, out var parsed).ShouldBeTrue();
        parsed.Offset.ShouldBe(TimeSpan.Zero);
    }

    private static Task<ServiceResponse> Send(
        IServiceHandler handler, string method, string path, byte[]? body = null,
        params (string Key, string Value)[] headers) =>
        Send(handler, method, path, body, headers, new Dictionary<string, string[]>());

    private static Task<ServiceResponse> Send(
        IServiceHandler handler, string method, string path, byte[]? body,
        (string Key, string Value) header = default,
        IReadOnlyDictionary<string, string[]>? query = null) =>
        Send(handler, method, path, body,
            string.IsNullOrEmpty(header.Key) ? [] : [header], query ?? new Dictionary<string, string[]>());

    private static Task<ServiceResponse> Send(
        IServiceHandler handler, string method, string path, byte[]? body,
        IEnumerable<(string Key, string Value)> headers,
        IReadOnlyDictionary<string, string[]> query)
    {
        var headerMap = headers.ToDictionary(x => x.Key, x => x.Value, StringComparer.OrdinalIgnoreCase);
        return handler.HandleAsync(new ServiceRequest(method, path, headerMap, body ?? [], query));
    }
}
