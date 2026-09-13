using System.Text.Json;
using MicroStack.Admin.Contracts;
using MicroStack.Internal.Admin;

namespace MicroStack.Tests;

public sealed class AdminPresentationContractTests
{
    [Fact]
    public void Older_contracts_keep_conservative_presentation_defaults()
    {
        var kind = JsonSerializer.Deserialize(
            """{"id":"queues","label":"Queues"}""", AdminJsonContext.Default.AdminResourceKind)!;
        kind.IsRoot.ShouldBeTrue();

        var detail = JsonSerializer.Deserialize(
            """{"resource":{"key":{"kind":"queues","id":"orders"},"name":"orders"},"hasChildren":true}""",
            AdminJsonContext.Default.AdminResourceDetail)!;
        detail.Resource.Type.ShouldBeNull();
        detail.Resource.Summary.ShouldBeEmpty();
        detail.ChildKinds.ShouldBeEmpty();
        detail.Summary.ShouldBeEmpty();
        detail.ConnectionCount.ShouldBeNull();
        detail.Fields.ShouldBeEmpty();
        detail.RevealableFields.ShouldBeEmpty();

        var service = JsonSerializer.Deserialize(
            """{"id":"sqs","name":"SQS","label":"SQS","category":"Messaging","icon":"sqs","canonicalHandler":"sqs","availability":"available","scope":"account"}""",
            AdminJsonContext.Default.AdminService)!;
        service.Kinds.ShouldBeEmpty();
    }

    [Fact]
    public void Presentation_metadata_roundtrips_through_the_AOT_contract()
    {
        var detail = new AdminResourceDetail(new(new("queues", "orders"), "orders")
        {
            Type = "FIFO queue",
            Summary = [new("Created", "2026-09-13T10:00:00Z", Format: "datetime")]
        })
        {
            HasChildren = true,
            ChildKinds = [new("messages", "Messages") { IsRoot = false }],
            Summary = [new("Visible", "0")],
            Fields = [new("Body MD5", "demo") { Secondary = true }],
            ConnectionCount = 0
        };

        var json = JsonSerializer.Serialize(detail, AdminJsonContext.Default.AdminResourceDetail);
        json.ShouldContain("\"isRoot\":false");
        json.ShouldContain("\"secondary\":true");
        json.ShouldContain("\"connectionCount\":0");
        var restored = JsonSerializer.Deserialize(json, AdminJsonContext.Default.AdminResourceDetail)!;
        restored.Resource.Type.ShouldBe("FIFO queue");
        restored.Resource.Summary.Single().Format.ShouldBe("datetime");
        restored.ChildKinds.Single().IsRoot.ShouldBeFalse();
        restored.Summary.Single().Value.ShouldBe("0");
        restored.Fields.Single().Secondary.ShouldBeTrue();
        restored.ConnectionCount.ShouldBe(0);
    }

    [Fact]
    public void Secondary_sensitive_fields_are_still_masked()
    {
        var field = AdminData.Field("Token", "never-render-this", sensitive: true, secondary: true);
        field.Value.ShouldBe(AdminData.MaskedValue);
        field.Sensitive.ShouldBeTrue();
        field.Secondary.ShouldBeTrue();
    }

    [Fact]
    public void Configured_connection_origin_preserves_opaque_source_paths()
    {
        var source = new AdminKey[]
        {
            new("event-buses", "arn:aws:events:us-east-1:000000000000:event-bus/custom"),
            new("rules", "arn:aws:events:us-east-1:000000000000:rule/custom/order-routing")
        };
        var connection = new AdminConnection("Orders", "targets", "sqs",
            [new("queues", "arn:aws:sqs:us-east-1:000000000000:orders")])
        {
            SourceServiceId = "events",
            SourcePath = source
        };
        var json = JsonSerializer.Serialize(connection, AdminJsonContext.Default.AdminConnection);
        var restored = JsonSerializer.Deserialize(json, AdminJsonContext.Default.AdminConnection)!;
        restored.SourceServiceId.ShouldBe("events");
        restored.SourcePath.ShouldBe(source);
        restored.TargetPath.ShouldBe(connection.TargetPath);
    }
}
