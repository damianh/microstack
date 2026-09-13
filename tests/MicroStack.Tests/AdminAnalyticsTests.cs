using System.Text;
using System.Text.Json;
using MicroStack.Internal;
using MicroStack.Internal.Admin;
using MicroStack.Services.Athena;
using MicroStack.Services.Emr;
using MicroStack.Services.Firehose;
using MicroStack.Services.Glue;
using MicroStack.Services.Kinesis;

namespace MicroStack.Tests;

public sealed class AdminAnalyticsTests
{
    [Fact]
    public void AnalyticsHandlersDeclareEveryRootKind()
    {
        Kinds(new AthenaServiceHandler()).ShouldBe(
            ["data-catalog", "named-query", "prepared-statement", "query-execution", "workgroup"],
            ignoreOrder: true);
        Kinds(new EmrServiceHandler()).ShouldBe(["cluster"]);
        Kinds(new FirehoseServiceHandler()).ShouldBe(["delivery-stream"]);
        Kinds(new GlueServiceHandler()).ShouldBe(["crawler", "database", "job", "registry"],
            ignoreOrder: true);
        Kinds(new KinesisServiceHandler()).ShouldBe(["stream"]);
    }

    [Fact]
    public async Task AccountScopedAnalyticsResourcesDoNotLeak()
    {
        var athena = new AthenaServiceHandler();
        using (AccountContext.BeginScope("111111111111"))
        {
            await Send(athena, "AmazonAthena.CreateNamedQuery",
                """{"Name":"mine","Database":"default","QueryString":"SELECT 1"}""");
            athena.GetAdminResources("athena").ShouldContain(x => x.Resource.Key.Kind == "named-query");
        }

        using (AccountContext.BeginScope("222222222222"))
        {
            athena.GetAdminResources("athena").ShouldNotContain(x => x.Resource.Key.Kind == "named-query");
            athena.GetAdminResources("athena")
                .Where(x => x.Resource.Key.Kind is "workgroup" or "data-catalog")
                .ShouldAllBe(x => x.Resource.Scope == "global");
        }
    }

    [Fact]
    public async Task KinesisInspectionDoesNotConsumeOrAdvanceRecords()
    {
        var kinesis = new KinesisServiceHandler();
        await Send(kinesis, "Kinesis_20131202.CreateStream",
            """{"StreamName":"events","ShardCount":1}""");
        await Send(kinesis, "Kinesis_20131202.PutRecord",
            $$"""{"StreamName":"events","PartitionKey":"p","Data":"{{Convert.ToBase64String("payload"u8.ToArray())}}"}""");
        var iteratorResponse = await Send(kinesis, "Kinesis_20131202.GetShardIterator",
            """{"StreamName":"events","ShardId":"shardId-000000000000","ShardIteratorType":"TRIM_HORIZON"}""");
        using var iteratorJson = JsonDocument.Parse(iteratorResponse.Body);
        var iterator = iteratorJson.RootElement.GetProperty("ShardIterator").GetString();

        var stream = kinesis.GetAdminResources("kinesis").Single();
        var shard = stream.ReadChildren!().Single(x => x.Resource.Key.Kind == "shard");
        shard.ReadChildren!().Single().ReadContent!().Text.ShouldBe("payload");

        var records = await Send(kinesis, "Kinesis_20131202.GetRecords",
            $$"""{"ShardIterator":"{{iterator}}"}""");
        using var recordsJson = JsonDocument.Parse(records.Body);
        recordsJson.RootElement.GetProperty("Records").GetArrayLength().ShouldBe(1);
    }

    [Fact]
    public void PreviewAndCredentialProjectionAreBoundedAndMasked()
    {
        AdminData.Text(new string('x', AdminData.PreviewMaxBytes + 1)).Kind.ShouldBe("oversized");
        var projected = AnalyticsAdminData.Json(new Dictionary<string, object?>
        {
            ["Username"] = "visible",
            ["Password"] = "do-not-show",
            ["AccessKey"] = "do-not-show",
        });
        projected.Text.ShouldNotBeNull();
        using var json = JsonDocument.Parse(projected.Text);
        json.RootElement.GetProperty("Username").GetString().ShouldBe("visible");
        json.RootElement.GetProperty("Password").GetString().ShouldBe(AdminData.MaskedValue);
        json.RootElement.GetProperty("AccessKey").GetString().ShouldBe(AdminData.MaskedValue);
        Encoding.UTF8.GetByteCount(projected.Text).ShouldBeLessThanOrEqualTo(AdminData.PreviewMaxBytes);
    }

    private static string[] Kinds(IAdminResourceSource source) =>
        source.GetAdminResourceKinds("ignored").Select(x => x.Id).ToArray();

    private static Task<ServiceResponse> Send(IServiceHandler handler, string target, string json) =>
        handler.HandleAsync(new ServiceRequest("POST", "/",
            new Dictionary<string, string> { ["x-amz-target"] = target },
            Encoding.UTF8.GetBytes(json), new Dictionary<string, string[]>()));
}
