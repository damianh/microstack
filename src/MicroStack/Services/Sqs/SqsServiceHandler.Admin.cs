using System.Globalization;
using System.Text.Json;
using System.Text.Json.Nodes;
using MicroStack.Admin.Contracts;
using MicroStack.Internal;
using MicroStack.Internal.Admin;

namespace MicroStack.Services.Sqs;

internal sealed partial class SqsServiceHandler : IAdminResourceSource, IAdminRelationshipSource
{
    private static readonly AdminResourceKind[] AdminKinds =
    [
        new("queues", "Queues"),
        new("messages", "Messages") { IsRoot = false }
    ];

    public IReadOnlyList<AdminResourceKind> GetAdminResourceKinds(string serviceId) =>
        serviceId == "sqs" ? AdminKinds : [];

    public IEnumerable<AdminNode> GetAdminResources(string serviceId)
    {
        if (serviceId != "sqs")
            return [];

        var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        QueueSnapshot[] queues;
        lock (_lock)
        {
            queues = _queues.Values.Select(queue =>
            {
                var visible = 0;
                var delayed = 0;
                foreach (var message in queue.Messages)
                {
                    if (message.VisibleAtMs <= now)
                        visible++;
                    else if (message.ReceiveCount == 0 && message.FirstReceiveAtMs is null)
                        delayed++;
                }
                return new QueueSnapshot(queue.Name, QueueUrl(QueueEndpoint(), queue.Name), queue.IsFifo,
                    new(queue.Attributes, StringComparer.Ordinal),
                    new(queue.Tags, StringComparer.Ordinal),
                    visible, delayed, queue.Messages.Count - visible - delayed,
                    () => ReadMessages(queue));
            }).ToArray();
        }

        return queues.Select(queue => QueueNode(queue, now)).ToArray();
    }

    private MessageSnapshot[] ReadMessages(SqsQueue queue)
    {
        lock (_lock)
        {
            return queue.Messages.Select(message => new MessageSnapshot(
                    message.Id,
                    message.Body,
                    message.Md5Body,
                    message.Md5Attrs,
                    message.SentAtMs,
                    message.VisibleAtMs,
                    message.ReceiveCount,
                    message.FirstReceiveAtMs,
                    message.MessageAttributes.DeepClone(),
                    new(message.SystemAttributes, StringComparer.Ordinal),
                    message.GroupId,
                    message.DedupId,
                    message.SequenceNumber)).ToArray();
        }
    }

    private static AdminNode QueueNode(QueueSnapshot queue, long capturedAt)
    {
        var arn = queue.Attributes.GetValueOrDefault("QueueArn")
            ?? $"arn:aws:sqs:{_region}:{AccountContext.GetAccountId()}:{queue.Name}";
        var visible = queue.Visible;
        var delayed = queue.Delayed;
        var inFlight = queue.InFlight;
        AdminField[] summary =
        [
            AdminData.Field("Visible messages", visible.ToString(CultureInfo.InvariantCulture)),
            AdminData.Field("Delayed messages", delayed.ToString(CultureInfo.InvariantCulture)),
            AdminData.Field("In-flight messages", inFlight.ToString(CultureInfo.InvariantCulture))
        ];
        return AdminData.Node("queues", arn, queue.Name, arn, "available",
            type: queue.IsFifo ? "FIFO" : "Standard", summary: summary) with
        {
            ReadSummary = () => summary,
            ChildKinds = [AdminKinds[1]],
            ReadFields = () => new[]
            {
                AdminData.Field("Queue URL", queue.Url, format: "uri"),
                AdminData.Field("Type", queue.IsFifo ? "FIFO" : "Standard")
            }.Concat(queue.Attributes.OrderBy(item => item.Key, StringComparer.Ordinal)
                .Select(item => AdminData.Field(item.Key, item.Key switch
                    {
                        "ApproximateNumberOfMessages" => visible.ToString(CultureInfo.InvariantCulture),
                        "ApproximateNumberOfMessagesDelayed" => delayed.ToString(CultureInfo.InvariantCulture),
                        "ApproximateNumberOfMessagesNotVisible" => inFlight.ToString(CultureInfo.InvariantCulture),
                        _ => item.Value
                    },
                    format: item.Key is "Policy" or "RedrivePolicy" or "RedriveAllowPolicy" ? "json" : null,
                    secondary: true)))
                .Concat(queue.Tags.OrderBy(item => item.Key, StringComparer.Ordinal)
                    .Select(item => AdminData.Field($"Tag: {item.Key}", item.Value, secondary: true))).ToArray(),
            ReadChildren = () => queue.ReadMessages()
                .Select(message => MessageNode(message, capturedAt, queue.IsFifo)).ToArray(),
            ReadConnections = () => QueueConnections(queue.Attributes)
        };
    }

    private static AdminNode MessageNode(MessageSnapshot message, long capturedAt, bool isFifo)
    {
        var state = MessageState(message, capturedAt);
        var sent = AdminData.IsoUtc(DateTimeOffset.FromUnixTimeMilliseconds(message.SentAtMs));
        return AdminData.Node("messages", message.Id, message.Id, status: state,
            type: isFifo ? "FIFO message" : "Message",
            summary: [AdminData.Field("Sent", sent, format: "datetime")]) with
        {
            ReadFields = () =>
            [
                AdminData.Field("State", state),
                AdminData.Field("Sent", sent, format: "datetime"),
                AdminData.Field("Visible at", AdminData.IsoUtc(DateTimeOffset.FromUnixTimeMilliseconds(message.VisibleAtMs)), format: "datetime"),
                AdminData.Field("Receive count", message.ReceiveCount.ToString()),
                AdminData.Field("First received", message.FirstReceiveAtMs is long first
                    ? AdminData.IsoUtc(DateTimeOffset.FromUnixTimeMilliseconds(first)) : null, format: "datetime"),
                AdminData.Field("Body MD5", message.Md5Body, secondary: true),
                AdminData.Field("Attributes MD5", message.Md5Attrs, secondary: true),
                AdminData.Field("Message attributes", message.MessageAttributes.ToJsonString(), format: "json", secondary: true),
                AdminData.Field("System attributes", string.Join(", ",
                    message.SystemAttributes.OrderBy(item => item.Key, StringComparer.Ordinal)
                        .Select(item => $"{item.Key}={item.Value}")), secondary: true),
                AdminData.Field("Message group ID", message.GroupId),
                AdminData.Field("Deduplication ID", message.DedupId),
                AdminData.Field("Sequence number", message.SequenceNumber)
            ],
            ReadContent = () => MessageContent(message.Body)
        };
    }

    private static AdminContent MessageContent(string body)
    {
        var text = AdminData.Text(body);
        if (text.Kind == "oversized")
            return text;
        JsonDocument document;
        try
        {
            document = JsonDocument.Parse(body);
        }
        catch (JsonException)
        {
            return text;
        }
        using (document)
        {
            try
            {
                using var stream = new MessagePreviewStream();
                using (var writer = new Utf8JsonWriter(stream, new() { Indented = true }))
                    document.RootElement.WriteTo(writer);
                return AdminData.Text(stream.GetBuffer().AsSpan(0, checked((int)stream.Length)),
                    "application/json") with { Kind = "json" };
            }
            catch (MessagePreviewLimitException)
            {
                return AdminData.Oversized("application/json");
            }
        }
    }

    private sealed class MessagePreviewLimitException : Exception;

    private sealed class MessagePreviewStream : MemoryStream
    {
        public override void Write(byte[] buffer, int offset, int count)
        {
            if (count > AdminData.PreviewMaxBytes - Length)
                throw new MessagePreviewLimitException();
            base.Write(buffer, offset, count);
        }

        public override void Write(ReadOnlySpan<byte> buffer)
        {
            if (buffer.Length > AdminData.PreviewMaxBytes - Length)
                throw new MessagePreviewLimitException();
            base.Write(buffer);
        }
    }

    private static string MessageState(MessageSnapshot message, long now) =>
        message.VisibleAtMs <= now ? "visible"
        : message.ReceiveCount > 0 || message.FirstReceiveAtMs is not null ? "in-flight"
        : "delayed";

    private static IReadOnlyList<AdminConnection> QueueConnections(IReadOnlyDictionary<string, string> attributes)
    {
        if (!attributes.TryGetValue("RedrivePolicy", out var policy) || string.IsNullOrWhiteSpace(policy))
            return [];
        JsonDocument document;
        try
        {
            document = JsonDocument.Parse(policy);
        }
        catch (JsonException)
        {
            return [new("Invalid redrive policy JSON", "redrive", State: "unavailable")];
        }
        using (document)
        {
            if (document.RootElement.ValueKind != JsonValueKind.Object
                || !document.RootElement.TryGetProperty("deadLetterTargetArn", out var target)
                || target.ValueKind != JsonValueKind.String || string.IsNullOrWhiteSpace(target.GetString()))
                return [new("Redrive policy has no valid target ARN", "redrive", State: "unavailable")];
            return [new("Dead-letter queue", "redrive", "sqs", [new("queues", target.GetString()!)], State: "configured")];
        }
    }

    public AdminRelationshipSnapshot GetAdminRelationshipSnapshot()
    {
        lock (_lock)
        {
            var resources = _queues.Values.Select(queue => new AdminRelationshipResource(
                [new("queues", queue.Attributes.GetValueOrDefault("QueueArn")
                    ?? $"arn:aws:sqs:{_region}:{AccountContext.GetAccountId()}:{queue.Name}")],
                queue.Name)).ToArray();
            var edges = _queues.Values.SelectMany(queue =>
            {
                var arn = queue.Attributes.GetValueOrDefault("QueueArn")
                    ?? $"arn:aws:sqs:{_region}:{AccountContext.GetAccountId()}:{queue.Name}";
                return QueueConnections(queue.Attributes).Select(connection =>
                    new AdminConfiguredRelationship("sqs", [new("queues", arn)], queue.Name, connection));
            }).ToArray();
            return new(resources, edges);
        }
    }

    private sealed record QueueSnapshot(
        string Name, string Url, bool IsFifo,
        Dictionary<string, string> Attributes,
        Dictionary<string, string> Tags,
        int Visible, int Delayed, int InFlight, Func<MessageSnapshot[]> ReadMessages);

    private sealed record MessageSnapshot(
        string Id, string Body, string Md5Body, string? Md5Attrs,
        long SentAtMs, long VisibleAtMs, int ReceiveCount, long? FirstReceiveAtMs,
        JsonNode MessageAttributes, Dictionary<string, string> SystemAttributes,
        string? GroupId, string? DedupId, string? SequenceNumber);
}
