using System.Net.Http.Json;
using System.Text;
using System.Text.Json;
using MicroStack.Admin.Contracts;
using MicroStack.Internal;
using MicroStack.Services.DynamoDb;
using MicroStack.Services.S3;
using Microsoft.Extensions.DependencyInjection;

namespace MicroStack.Tests;

public sealed class AdminStorageParityApiTests(MicroStackFixture fixture) : IClassFixture<MicroStackFixture>
{
    private const string Account = "838383838383";

    [Fact]
    public async Task S3_SummaryPagingFiltersFullKeysAndKeepsPrefixVersionPaths()
    {
        var handler = (S3ServiceHandler)fixture.Factory.Services.GetRequiredService<ServiceRegistry>().Resolve("s3")!;
        using (AccountContext.BeginScope(Account))
        {
            await Send(handler, "PUT", "/parity-api-bucket");
            await Send(handler, "PUT", "/parity-api-bucket",
                "<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>",
                query: new Dictionary<string, string[]> { ["versioning"] = [] });
            await Send(handler, "PUT", "/parity-api-bucket/folder/a.json", "body-must-not-appear",
                new Dictionary<string, string> { ["content-type"] = "application/json" });
            await Send(handler, "PUT", "/parity-api-bucket/folder/b.txt", "second-body-must-not-appear",
                new Dictionary<string, string> { ["content-type"] = "text/plain" });
        }

        AdminKey[] bucket = [new("buckets", "parity-api-bucket")];
        AdminKey[] prefix = [.. bucket, new("prefixes", "folder/")];
        var bucketDetail = await fixture.HttpClient.GetFromJsonAsync(
            Url("s3", "resource", bucket), AdminJsonContext.Default.AdminResourceDetail);
        bucketDetail!.Summary.Single(x => x.Name == "ObjectCount").Value.ShouldBe("2");
        bucketDetail.ChildKinds.Select(x => x.Id).ShouldBe(["prefixes", "objects"]);
        var childrenUrl = Url("s3", "children", prefix);
        using var first = await fixture.HttpClient.GetAsync(childrenUrl + "&pageSize=1");
        first.EnsureSuccessStatusCode();
        var json = await first.Content.ReadAsStringAsync();
        json.ShouldNotContain("body-must-not-appear");
        using var page = JsonDocument.Parse(json);
        page.RootElement.GetProperty("knownTotal").ValueKind.ShouldBe(JsonValueKind.Null);
        page.RootElement.GetProperty("items").GetArrayLength().ShouldBe(1);
        var item = page.RootElement.GetProperty("items")[0];
        item.GetProperty("name").GetString().ShouldBe("a.json");
        item.GetProperty("key").GetProperty("id").GetString().ShouldBe("folder/a.json");
        item.GetProperty("summary").GetArrayLength().ShouldBe(3);
        item.TryGetProperty("text", out _).ShouldBeFalse();
        var cursor = page.RootElement.GetProperty("nextCursor").GetString();
        cursor.ShouldNotBeNullOrEmpty();
        using var second = await fixture.HttpClient.GetAsync(childrenUrl + "&pageSize=1&cursor=" + Uri.EscapeDataString(cursor));
        second.EnsureSuccessStatusCode();
        using var secondPage = JsonDocument.Parse(await second.Content.ReadAsStringAsync());
        secondPage.RootElement.GetProperty("items").GetArrayLength().ShouldBe(1);
        secondPage.RootElement.GetProperty("items")[0].GetProperty("name").GetString().ShouldBe("b.txt");
        secondPage.RootElement.GetProperty("nextCursor").ValueKind.ShouldBe(JsonValueKind.Null);

        using var filtered = await fixture.HttpClient.GetAsync(childrenUrl + "&filter=folder%2Fa.json");
        filtered.EnsureSuccessStatusCode();
        using var filteredPage = JsonDocument.Parse(await filtered.Content.ReadAsStringAsync());
        filteredPage.RootElement.GetProperty("knownTotal").ValueKind.ShouldBe(JsonValueKind.Null);
        filteredPage.RootElement.GetProperty("items").GetArrayLength().ShouldBe(1);
        filteredPage.RootElement.GetProperty("items")[0].GetProperty("key").GetProperty("id").GetString()
            .ShouldBe("folder/a.json");
        filteredPage.RootElement.GetProperty("nextCursor").ValueKind.ShouldBe(JsonValueKind.Null);
        AdminKey[] objectPath = [.. prefix, new("objects", "folder/a.json")];
        using var versions = await fixture.HttpClient.GetAsync(Url("s3", "children", objectPath));
        versions.EnsureSuccessStatusCode();
        using var versionPage = JsonDocument.Parse(await versions.Content.ReadAsStringAsync());
        var versionId = versionPage.RootElement.GetProperty("items")[0].GetProperty("key").GetProperty("id").GetString()!;
        AdminKey[] versionPath = [.. objectPath, new("versions", versionId)];
        var version = await fixture.HttpClient.GetFromJsonAsync(
            Url("s3", "resource", versionPath), AdminJsonContext.Default.AdminResourceDetail);
        version!.Resource.Key.Id.ShouldBe(versionId);
        version.Resource.Type.ShouldBe("Version");
        version.HasContent.ShouldBeTrue();
        version.Summary.Single(x => x.Name == "Length").Value.ShouldBe("20");
        using (AccountContext.BeginScope(Account))
        {
            var retained = await Send(handler, "GET", "/parity-api-bucket/folder/a.json");
            Encoding.UTF8.GetString(retained.Body).ShouldBe("body-must-not-appear");
            retained.Headers["x-amz-version-id"].ShouldBe(versionId);
        }
    }

    [Fact]
    public async Task DynamoDb_SummaryPageHasKeysNotPayloadAndCountIsIndependentOfPage()
    {
        var handler = (DynamoDbServiceHandler)fixture.Factory.Services.GetRequiredService<ServiceRegistry>().Resolve("dynamodb")!;
        using (AccountContext.BeginScope(Account))
        {
            await Ddb(handler, "CreateTable", """
                {"TableName":"parity-api-table","KeySchema":[{"AttributeName":"PK","KeyType":"HASH"},{"AttributeName":"SK","KeyType":"RANGE"}],
                 "AttributeDefinitions":[{"AttributeName":"PK","AttributeType":"S"},{"AttributeName":"SK","AttributeType":"N"}]}
                """);
            await Ddb(handler, "PutItem", """
                {"TableName":"parity-api-table","Item":{"PK":{"S":"a"},"SK":{"N":"01"},"payload":{"S":"body-must-not-appear"}}}
                """);
            await Ddb(handler, "PutItem", """
                {"TableName":"parity-api-table","Item":{"PK":{"S":"b"},"SK":{"N":"02"},"payload":{"S":"body-must-not-appear"}}}
                """);
        }
        AdminKey[] tablePath = [new("tables", "parity-api-table")];
        var table = await fixture.HttpClient.GetFromJsonAsync(
            Url("dynamodb", "resource", tablePath), AdminJsonContext.Default.AdminResourceDetail);
        table!.Summary.Single(x => x.Name == "PartitionKey").Value.ShouldBe("PK (S)");
        table.Summary.Single(x => x.Name == "SortKey").Value.ShouldBe("SK (N)");
        table.Summary.Single(x => x.Name == "ItemCount").Value.ShouldBe("2");
        using var children = await fixture.HttpClient.GetAsync(Url("dynamodb", "children", tablePath) + "&pageSize=1");
        children.EnsureSuccessStatusCode();
        var json = await children.Content.ReadAsStringAsync();
        json.ShouldNotContain("body-must-not-appear");
        json.ShouldNotContain("payload");
        using var page = JsonDocument.Parse(json);
        page.RootElement.GetProperty("knownTotal").ValueKind.ShouldBe(JsonValueKind.Null);
        page.RootElement.GetProperty("items").GetArrayLength().ShouldBe(1);
        var item = page.RootElement.GetProperty("items")[0];
        item.GetProperty("summary").GetArrayLength().ShouldBe(2);
        var id = item.GetProperty("key").GetProperty("id").GetString()!;
        var cursor = page.RootElement.GetProperty("nextCursor").GetString();
        cursor.ShouldNotBeNullOrEmpty();
        using var second = await fixture.HttpClient.GetAsync(
            Url("dynamodb", "children", tablePath) + "&pageSize=1&cursor=" + Uri.EscapeDataString(cursor));
        second.EnsureSuccessStatusCode();
        using var secondPage = JsonDocument.Parse(await second.Content.ReadAsStringAsync());
        secondPage.RootElement.GetProperty("items").GetArrayLength().ShouldBe(1);
        secondPage.RootElement.GetProperty("items")[0].GetProperty("key").GetProperty("id").GetString().ShouldNotBe(id);
        secondPage.RootElement.GetProperty("nextCursor").ValueKind.ShouldBe(JsonValueKind.Null);
        using var filtered = await fixture.HttpClient.GetAsync(
            Url("dynamodb", "children", tablePath) + "&filter=" + Uri.EscapeDataString(id));
        filtered.EnsureSuccessStatusCode();
        using var filteredPage = JsonDocument.Parse(await filtered.Content.ReadAsStringAsync());
        filteredPage.RootElement.GetProperty("knownTotal").ValueKind.ShouldBe(JsonValueKind.Null);
        filteredPage.RootElement.GetProperty("items").GetArrayLength().ShouldBe(1);
        filteredPage.RootElement.GetProperty("items")[0].GetProperty("key").GetProperty("id").GetString().ShouldBe(id);
        filteredPage.RootElement.GetProperty("nextCursor").ValueKind.ShouldBe(JsonValueKind.Null);
        var itemDetail = await fixture.HttpClient.GetFromJsonAsync(
            Url("dynamodb", "resource", [.. tablePath, new("items", id)]), AdminJsonContext.Default.AdminResourceDetail);
        itemDetail!.Resource.Key.Id.ShouldBe(id);
        itemDetail.HasContent.ShouldBeTrue();
        itemDetail.Fields.Single(x => x.Name == "AttributeCount").Secondary.ShouldBeTrue();
        using (AccountContext.BeginScope(Account))
        {
            var retained = await Ddb(handler, "GetItem", """
                {"TableName":"parity-api-table","Key":{"PK":{"S":"a"},"SK":{"N":"01"}}}
                """);
            using var retainedItem = JsonDocument.Parse(retained.Body);
            retainedItem.RootElement.GetProperty("Item").GetProperty("payload").GetProperty("S").GetString()
                .ShouldBe("body-must-not-appear");
        }
    }

    private static string Url(string service, string operation, AdminKey[] path) =>
        $"/_microstack/admin/v1/services/{service}/{operation}?accountId={Account}&path=" +
        Uri.EscapeDataString(JsonSerializer.Serialize(path, AdminJsonContext.Default.AdminKeyArray));

    private static Task<ServiceResponse> Ddb(DynamoDbServiceHandler handler, string action, string body) =>
        Send(handler, "POST", "/", body, new Dictionary<string, string>
        {
            ["x-amz-target"] = $"DynamoDB_20120810.{action}",
            ["content-type"] = "application/x-amz-json-1.0"
        });

    private static Task<ServiceResponse> Send(
        IServiceHandler handler, string method, string path, string? body = null,
        IReadOnlyDictionary<string, string>? headers = null, IReadOnlyDictionary<string, string[]>? query = null) =>
        handler.HandleAsync(new ServiceRequest(method, path, headers ?? new Dictionary<string, string>(),
            Encoding.UTF8.GetBytes(body ?? ""), query ?? new Dictionary<string, string[]>()));
}
