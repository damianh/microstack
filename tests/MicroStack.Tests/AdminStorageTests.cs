using System.Text;
using System.Text.Json;
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
