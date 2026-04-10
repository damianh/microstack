using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Web;
using MicroStack.Internal;

namespace MicroStack.Services.Sqs;

/// <summary>
/// SQS service handler — supports both legacy Query/XML protocol and JSON protocol
/// (X-Amz-Target: AmazonSQS.*).
///
/// Port of ministack/services/sqs.py.
/// </summary>
internal sealed class SqsServiceHandler : IServiceHandler
{
    // ── Module-level state ──────────────────────────────────────────────────────

    // keyed by account-scoped queue URL
    private readonly AccountScopedDictionary<string, SqsQueue> _queues = new();
    // keyed by queue name → URL
    private readonly AccountScopedDictionary<string, string> _queueNameToUrl = new();
    private readonly Lock _lock = new();

    private static readonly string _region =
        Environment.GetEnvironmentVariable("MINISTACK_REGION") ?? "us-east-1";
    private static readonly string _defaultHost =
        Environment.GetEnvironmentVariable("MINISTACK_HOST") ?? "localhost";
    private static readonly string _defaultPort =
        Environment.GetEnvironmentVariable("GATEWAY_PORT") ?? "4566";

    private const int DedupWindowSeconds = 300; // 5 minutes

    // ── Query-compat error code mapping ────────────────────────────────────────

    private static readonly Dictionary<string, string> QueryCompatCodes = new(StringComparer.Ordinal)
    {
        ["QueueDoesNotExist"]            = "AWS.SimpleQueueService.NonExistentQueue",
        ["QueueNameExists"]              = "QueueAlreadyExists",
        ["TooManyEntriesInBatchRequest"] = "AWS.SimpleQueueService.TooManyEntriesInBatchRequest",
        ["EmptyBatchRequest"]            = "AWS.SimpleQueueService.EmptyBatchRequest",
        ["BatchEntryIdsNotDistinct"]     = "AWS.SimpleQueueService.BatchEntryIdsNotDistinct",
        ["BatchRequestTooLong"]          = "AWS.SimpleQueueService.BatchRequestTooLong",
        ["InvalidBatchEntryId"]          = "AWS.SimpleQueueService.InvalidBatchEntryId",
        ["MessageNotInflight"]           = "AWS.SimpleQueueService.MessageNotInflight",
        ["PurgeQueueInProgress"]         = "AWS.SimpleQueueService.PurgeQueueInProgress",
        ["QueueDeletedRecently"]         = "AWS.SimpleQueueService.QueueDeletedRecently",
        ["UnsupportedOperation"]         = "AWS.SimpleQueueService.UnsupportedOperation",
        ["OverLimit"]                    = "OverLimit",
        ["InvalidIdFormat"]              = "InvalidIdFormat",
        ["InvalidMessageContents"]       = "InvalidMessageContents",
        ["ReceiptHandleIsInvalid"]       = "ReceiptHandleIsInvalid",
        ["InvalidAttributeName"]         = "InvalidAttributeName",
        ["InvalidAttributeValue"]        = "InvalidAttributeValue",
        ["InvalidSecurity"]              = "InvalidSecurity",
        ["InvalidAddress"]               = "InvalidAddress",
        ["RequestThrottled"]             = "RequestThrottled",
        ["ResourceNotFoundException"]    = "ResourceNotFoundException",
    };

    // ── IServiceHandler ─────────────────────────────────────────────────────────

    public string ServiceName => "sqs";

    public async Task<ServiceResponse> HandleAsync(ServiceRequest request)
    {
        var target = request.GetHeader("x-amz-target") ?? "";

        // JSON protocol (X-Amz-Target: AmazonSQS.*)
        if (target.StartsWith("AmazonSQS.", StringComparison.Ordinal))
        {
            var action = target["AmazonSQS.".Length..];
            JsonObject data;
            try
            {
                data = request.Body.Length > 0
                    ? JsonNode.Parse(request.Body)?.AsObject() ?? new JsonObject()
                    : new JsonObject();
            }
            catch
            {
                data = new JsonObject();
            }
            return await HandleJsonAsync(action, data, request.Path);
        }

        // Legacy Query / form-encoded protocol
        var queryParams = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        foreach (var (k, v) in request.QueryParams)
            if (v.Length > 0) queryParams[k] = v[0];

        if (request.Method == "POST" && request.Body.Length > 0)
        {
            var formStr = Encoding.UTF8.GetString(request.Body);
            foreach (var pair in formStr.Split('&', StringSplitOptions.RemoveEmptyEntries))
            {
                var eq = pair.IndexOf('=');
                if (eq < 0) continue;
                var key = HttpUtility.UrlDecode(pair[..eq]);
                var val = HttpUtility.UrlDecode(pair[(eq + 1)..]);
                queryParams[key] = val;
            }
        }

        var queryAction = queryParams.GetValueOrDefault("Action", "");
        if (string.IsNullOrEmpty(queryAction))
            return XmlErrorResponse("MissingAction", "Missing Action parameter", 400);

        return await HandleQueryAsync(queryAction, queryParams, request.Path);
    }

    public void Reset()
    {
        lock (_lock)
        {
            _queues.Clear();
            _queueNameToUrl.Clear();
        }
    }

    public object? GetState()
    {
        // Not implementing deep serialization for persistence in Phase 1.
        // Full persistence will be wired up when state serialization is standardized.
        return null;
    }

    public void RestoreState(object state)
    {
        // Not implementing restore in Phase 1.
    }

    // ── JSON protocol ───────────────────────────────────────────────────────────

    private async Task<ServiceResponse> HandleJsonAsync(string action, JsonObject data, string path)
    {
        try
        {
            var qurl = data["QueueUrl"]?.GetValue<string>() ?? UrlFromPath(path);
            var result = await DispatchAsync(action, data, qurl);
            return JsonOkResponse(result);
        }
        catch (SqsException ex)
        {
            return JsonErrorResponse(ex.Code, ex.Message, ex.Status);
        }
    }

    // ── Query/XML protocol ──────────────────────────────────────────────────────

    private async Task<ServiceResponse> HandleQueryAsync(
        string action, Dictionary<string, string> @params, string path)
    {
        try
        {
            var data = NormaliseParams(action, @params);
            var qurl = data["QueueUrl"]?.GetValue<string>()
                ?? UrlFromPath(path);
            data["QueueUrl"] = qurl;
            var result = await DispatchAsync(action, data, qurl);
            return ToXmlResponse(action, result);
        }
        catch (SqsException ex)
        {
            return XmlErrorResponse(ex.Code, ex.Message, ex.Status);
        }
    }

    // ── Dispatcher ──────────────────────────────────────────────────────────────

    private Task<JsonObject> DispatchAsync(string action, JsonObject data, string qurl)
    {
        return action switch
        {
            "CreateQueue"                  => Task.FromResult(ActCreateQueue(data, qurl)),
            "DeleteQueue"                  => Task.FromResult(ActDeleteQueue(data, qurl)),
            "ListQueues"                   => Task.FromResult(ActListQueues(data, qurl)),
            "GetQueueUrl"                  => Task.FromResult(ActGetQueueUrl(data, qurl)),
            "SendMessage"                  => Task.FromResult(ActSendMessage(data, qurl)),
            "ReceiveMessage"               => ActReceiveMessageAsync(data, qurl),
            "DeleteMessage"                => Task.FromResult(ActDeleteMessage(data, qurl)),
            "ChangeMessageVisibility"      => Task.FromResult(ActChangeVisibility(data, qurl)),
            "ChangeMessageVisibilityBatch" => Task.FromResult(ActChangeVisibilityBatch(data, qurl)),
            "GetQueueAttributes"           => Task.FromResult(ActGetQueueAttributes(data, qurl)),
            "SetQueueAttributes"           => Task.FromResult(ActSetQueueAttributes(data, qurl)),
            "PurgeQueue"                   => Task.FromResult(ActPurgeQueue(data, qurl)),
            "SendMessageBatch"             => Task.FromResult(ActSendMessageBatch(data, qurl)),
            "DeleteMessageBatch"           => Task.FromResult(ActDeleteMessageBatch(data, qurl)),
            "ListQueueTags"                => Task.FromResult(ActListQueueTags(data, qurl)),
            "TagQueue"                     => Task.FromResult(ActTagQueue(data, qurl)),
            "UntagQueue"                   => Task.FromResult(ActUntagQueue(data, qurl)),
            _ => throw new SqsException("InvalidAction",
                $"The action {action} is not valid for this endpoint.")
        };
    }

    // ── Core Actions ────────────────────────────────────────────────────────────

    private JsonObject ActCreateQueue(JsonObject data, string _)
    {
        var name = data["QueueName"]?.GetValue<string>() ?? "";
        if (string.IsNullOrEmpty(name))
            throw new SqsException("MissingParameter",
                "The request must contain the parameter QueueName.");

        var attrs = data["Attributes"]?.AsObject() ?? new JsonObject();
        var isFifo = name.EndsWith(".fifo", StringComparison.Ordinal)
                     || attrs["FifoQueue"]?.GetValue<string>() == "true";

        if (isFifo && !name.EndsWith(".fifo", StringComparison.Ordinal))
            throw new SqsException("InvalidParameterValue",
                "The name of a FIFO queue can only include alphanumeric characters, " +
                "hyphens, or underscores, must end with .fifo suffix.");

        if (name.EndsWith(".fifo", StringComparison.Ordinal))
            isFifo = true;

        var url = QueueUrl(name);
        lock (_lock)
        {
            if (_queues.TryGetValue(url, out SqsQueue? _))
                return new JsonObject { ["QueueUrl"] = url };

            var ts = DateTimeOffset.UtcNow.ToUnixTimeSeconds().ToString();
            var q = new SqsQueue
            {
                Name = name,
                Url  = url,
                IsFifo = isFifo,
                Attributes = new Dictionary<string, string>(StringComparer.Ordinal)
                {
                    ["QueueArn"]                          = $"arn:aws:sqs:{_region}:{AccountContext.GetAccountId()}:{name}",
                    ["CreatedTimestamp"]                  = ts,
                    ["LastModifiedTimestamp"]             = ts,
                    ["VisibilityTimeout"]                 = "30",
                    ["MaximumMessageSize"]                = "262144",
                    ["MessageRetentionPeriod"]            = "345600",
                    ["DelaySeconds"]                      = "0",
                    ["ReceiveMessageWaitTimeSeconds"]     = "0",
                },
            };

            if (isFifo)
            {
                q.Attributes["FifoQueue"] = "true";
                q.Attributes["ContentBasedDeduplication"] =
                    attrs["ContentBasedDeduplication"]?.GetValue<string>() ?? "false";
            }

            // Apply extra attributes from request
            foreach (var kv in attrs)
                q.Attributes[kv.Key] = kv.Value?.GetValue<string>() ?? "";

            // Apply tags passed at creation time
            var tags = data["Tags"]?.AsObject();
            if (tags is not null)
                foreach (var kv in tags)
                    q.Tags[kv.Key] = kv.Value?.GetValue<string>() ?? "";

            _queues[url] = q;
            _queueNameToUrl[name] = url;
        }
        return new JsonObject { ["QueueUrl"] = url };
    }

    private JsonObject ActDeleteQueue(JsonObject data, string qurl)
    {
        var url = data["QueueUrl"]?.GetValue<string>() ?? qurl;
        lock (_lock)
        {
            if (_queues.TryGetValue(url, out var q))
            {
                _queueNameToUrl.TryRemove(q.Name, out _);
                _queues.TryRemove(url, out _);
            }
        }
        return new JsonObject();
    }

    private JsonObject ActListQueues(JsonObject data, string _)
    {
        var pfx = data["QueueNamePrefix"]?.GetValue<string>() ?? "";
        var max = data["MaxResults"]?.GetValue<int>() ?? 1000;
        lock (_lock)
        {
            var urls = new JsonArray();
            var count = 0;
            foreach (var kv in _queues.Items)
            {
                var url = kv.Key;
                var q   = kv.Value;
                if (count >= max) break;
                if (string.IsNullOrEmpty(pfx) || q.Name.StartsWith(pfx, StringComparison.Ordinal))
                {
                    urls.Add(url);
                    count++;
                }
            }
            return new JsonObject { ["QueueUrls"] = urls };
        }
    }

    private JsonObject ActGetQueueUrl(JsonObject data, string _)
    {
        var name = data["QueueName"]?.GetValue<string>() ?? "";
        lock (_lock)
        {
            if (!_queueNameToUrl.TryGetValue(name, out var url) || string.IsNullOrEmpty(url))
                throw new SqsException("QueueDoesNotExist",
                    "The specified queue does not exist.");
            return new JsonObject { ["QueueUrl"] = url };
        }
    }

    // ── SendMessage ─────────────────────────────────────────────────────────────

    private JsonObject ActSendMessage(JsonObject data, string qurl)
    {
        var url = data["QueueUrl"]?.GetValue<string>() ?? qurl;
        lock (_lock)
        {
            var q = GetQueue(url);
            var bodyText = data["MessageBody"]?.GetValue<string>() ?? "";
            if (string.IsNullOrEmpty(bodyText))
                throw new SqsException("MissingParameter",
                    "The request must contain the parameter MessageBody.");

            var delay = data["DelaySeconds"] != null
                ? int.Parse(data["DelaySeconds"]!.GetValue<string>()
                    .TrimStart().TrimEnd())
                : int.Parse(q.Attributes.GetValueOrDefault("DelaySeconds", "0"));

            var msgAttrs   = data["MessageAttributes"]?.AsObject() ?? new JsonObject();
            var groupId    = data["MessageGroupId"]?.GetValue<string>();
            var dedupId    = data["MessageDeduplicationId"]?.GetValue<string>();
            string? seqNum = null;

            if (q.IsFifo)
            {
                if (string.IsNullOrEmpty(groupId))
                    throw new SqsException("MissingParameter",
                        "The request must contain the parameter MessageGroupId.");

                if (string.IsNullOrEmpty(dedupId))
                {
                    if (q.Attributes.GetValueOrDefault("ContentBasedDeduplication") == "true")
                        dedupId = ComputeSha256Hex(bodyText);
                    else
                        throw new SqsException("InvalidParameterValue",
                            "The queue should either have ContentBasedDeduplication " +
                            "enabled or MessageDeduplicationId provided explicitly.");
                }

                PruneDedup(q);
                if (q.DedupCache.TryGetValue(dedupId, out var cached))
                {
                    // Return the existing MessageId and SequenceNumber, but compute
                    // MD5 against the current request body so the SDK validation passes
                    // (the real SQS does the same — the duplicate body may differ).
                    var dupMd5 = ComputeMd5Hex(bodyText);
                    var dup = new JsonObject
                    {
                        ["MessageId"]        = cached.Id,
                        ["MD5OfMessageBody"] = dupMd5,
                    };
                    if (cached.Md5Attrs is not null)
                        dup["MD5OfMessageAttributes"] = cached.Md5Attrs;
                    if (cached.SequenceNumber is not null)
                        dup["SequenceNumber"] = cached.SequenceNumber;
                    return dup;
                }

                q.FifoSeq++;
                seqNum = q.FifoSeq.ToString().PadLeft(20, '0');
                delay  = 0;
            }

            var now   = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            var mid   = Guid.NewGuid().ToString();
            var md5b  = ComputeMd5Hex(bodyText);
            var md5a  = ComputeMd5MsgAttrs(msgAttrs);

            var msg = new SqsMessage
            {
                Id                 = mid,
                Body               = bodyText,
                Md5Body            = md5b,
                Md5Attrs           = md5a,
                SentAtMs           = now,
                VisibleAtMs        = now + delay * 1000L,
                MessageAttributes  = msgAttrs,
                SystemAttributes   = new Dictionary<string, string>(StringComparer.Ordinal)
                {
                    ["SenderId"]      = AccountContext.GetAccountId(),
                    ["SentTimestamp"] = now.ToString(),
                },
                GroupId            = groupId,
                DedupId            = dedupId,
                SequenceNumber     = seqNum,
            };
            q.Messages.Add(msg);

            if (q.IsFifo && dedupId is not null)
            {
                q.DedupCache[dedupId] = new SqsDedupEntry
                {
                    Id             = mid,
                    Md5Body        = md5b,
                    Md5Attrs       = md5a,
                    SequenceNumber = seqNum,
                    ExpireAtMs     = now + DedupWindowSeconds * 1000L,
                };
            }

            var result = new JsonObject
            {
                ["MessageId"]        = mid,
                ["MD5OfMessageBody"] = md5b,
            };
            if (md5a is not null)
                result["MD5OfMessageAttributes"] = md5a;
            if (seqNum is not null)
                result["SequenceNumber"] = seqNum;
            return result;
        }
    }

    // ── ReceiveMessage (async — long polling) ───────────────────────────────────

    private async Task<JsonObject> ActReceiveMessageAsync(JsonObject data, string qurl)
    {
        var url = data["QueueUrl"]?.GetValue<string>() ?? qurl;

        var maxN = Math.Min(data["MaxNumberOfMessages"]?.GetValue<int>() ?? 1, 10);
        int vis;
        int wait;
        lock (_lock)
        {
            var q = GetQueue(url);
            vis  = data["VisibilityTimeout"] != null
                ? data["VisibilityTimeout"]!.GetValue<int>()
                : int.Parse(q.Attributes.GetValueOrDefault("VisibilityTimeout", "30"));
            wait = data["WaitTimeSeconds"] != null
                ? data["WaitTimeSeconds"]!.GetValue<int>()
                : int.Parse(q.Attributes.GetValueOrDefault("ReceiveMessageWaitTimeSeconds", "0"));
        }

        var attrNames    = (data["AttributeNames"] ?? data["MessageSystemAttributeNames"])?.AsArray()
            .Select(n => n?.GetValue<string>() ?? "").ToList() ?? [];
        var msgAttrNames = data["MessageAttributeNames"]?.AsArray()
            .Select(n => n?.GetValue<string>() ?? "").ToList() ?? [];

        var deadline = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() + wait * 1000L;
        List<SqsMessage> msgs = [];

        while (true)
        {
            lock (_lock)
            {
                var q = GetQueue(url);
                DlqSweepWithQueues(q);
                msgs = CollectMessages(q, maxN, vis);
            }
            if (msgs.Count > 0 || DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() >= deadline)
                break;

            var remaining = deadline - DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            await Task.Delay((int)Math.Clamp(Math.Min(100, remaining), 10, 100));
        }

        var out_ = new JsonArray();
        foreach (var m in msgs)
        {
            var entry = new JsonObject
            {
                ["MessageId"]     = m.Id,
                ["ReceiptHandle"] = m.ReceiptHandle!,
                ["MD5OfBody"]     = m.Md5Body,
                ["Body"]          = m.Body,
            };

            var sa = BuildSysAttrs(m, attrNames);
            if (sa.Count > 0)
                entry["Attributes"] = sa;

            var fa = FilterMsgAttrs(m.MessageAttributes, msgAttrNames);
            if (fa.Count > 0)
            {
                entry["MessageAttributes"] = fa;
                if (m.Md5Attrs is not null)
                    entry["MD5OfMessageAttributes"] = m.Md5Attrs;
            }

            out_.Add(entry);
        }

        return out_.Count > 0
            ? new JsonObject { ["Messages"] = out_ }
            : new JsonObject();
    }

    // ── DeleteMessage ───────────────────────────────────────────────────────────

    private JsonObject ActDeleteMessage(JsonObject data, string qurl)
    {
        var url = data["QueueUrl"]?.GetValue<string>() ?? qurl;
        var rh  = data["ReceiptHandle"]?.GetValue<string>() ?? "";
        if (string.IsNullOrEmpty(rh))
            throw new SqsException("MissingParameter",
                "The request must contain the parameter ReceiptHandle.");
        lock (_lock)
        {
            var q = GetQueue(url);
            q.Messages.RemoveAll(m =>
            {
                if (m.ReceiptHandle is null || m.ReceiptHandle != rh) return false;
                if (q.IsFifo && m.DedupId is not null)
                    q.DedupCache.Remove(m.DedupId);
                return true;
            });
        }
        return new JsonObject();
    }

    // ── ChangeMessageVisibility ─────────────────────────────────────────────────

    private JsonObject ActChangeVisibility(JsonObject data, string qurl)
    {
        var url = data["QueueUrl"]?.GetValue<string>() ?? qurl;
        var rh  = data["ReceiptHandle"]?.GetValue<string>() ?? "";
        var vt  = data["VisibilityTimeout"]?.GetValue<int>() ?? 30;
        lock (_lock)
        {
            var q   = GetQueue(url);
            var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            foreach (var m in q.Messages)
            {
                if (m.ReceiptHandle is not null && m.ReceiptHandle == rh)
                {
                    m.VisibleAtMs = now + vt * 1000L;
                    break;
                }
            }
        }
        return new JsonObject();
    }

    // ── ChangeMessageVisibilityBatch ────────────────────────────────────────────

    private JsonObject ActChangeVisibilityBatch(JsonObject data, string qurl)
    {
        var url     = data["QueueUrl"]?.GetValue<string>() ?? qurl;
        var entries = data["Entries"]?.AsArray() ?? new JsonArray();
        var ok      = new JsonArray();
        var fail    = new JsonArray();

        lock (_lock)
        {
            var q   = GetQueue(url);
            var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

            foreach (var e in entries)
            {
                if (e is null) continue;
                var eid  = e["Id"]?.GetValue<string>() ?? "";
                var rh   = e["ReceiptHandle"]?.GetValue<string>() ?? "";
                var vt   = e["VisibilityTimeout"]?.GetValue<int>() ?? 30;
                var found = false;

                foreach (var m in q.Messages)
                {
                    if (m.ReceiptHandle is not null && m.ReceiptHandle == rh)
                    {
                        m.VisibleAtMs = now + vt * 1000L;
                        found = true;
                        break;
                    }
                }

                if (found)
                    ok.Add(new JsonObject { ["Id"] = eid });
                else
                    fail.Add(new JsonObject
                    {
                        ["Id"]          = eid,
                        ["Code"]        = "ReceiptHandleIsInvalid",
                        ["Message"]     = "The input receipt handle is invalid.",
                        ["SenderFault"] = true,
                    });
            }
        }

        return new JsonObject { ["Successful"] = ok, ["Failed"] = fail };
    }

    // ── GetQueueAttributes ──────────────────────────────────────────────────────

    private JsonObject ActGetQueueAttributes(JsonObject data, string qurl)
    {
        var url = data["QueueUrl"]?.GetValue<string>() ?? qurl;
        lock (_lock)
        {
            var q = GetQueue(url);
            RefreshCounts(q);

            var names   = data["AttributeNames"]?.AsArray()
                .Select(n => n?.GetValue<string>() ?? "").ToList() ?? ["All"];
            var wantAll = names.Contains("All");

            var attrs = new JsonObject();
            foreach (var (k, v) in q.Attributes)
                if (wantAll || names.Contains(k))
                    attrs[k] = v;

            return new JsonObject { ["Attributes"] = attrs };
        }
    }

    // ── SetQueueAttributes ──────────────────────────────────────────────────────

    private JsonObject ActSetQueueAttributes(JsonObject data, string qurl)
    {
        var url   = data["QueueUrl"]?.GetValue<string>() ?? qurl;
        var attrs = data["Attributes"]?.AsObject() ?? new JsonObject();
        lock (_lock)
        {
            var q = GetQueue(url);
            foreach (var kv in attrs)
                q.Attributes[kv.Key] = kv.Value?.GetValue<string>() ?? "";
            q.Attributes["LastModifiedTimestamp"] =
                DateTimeOffset.UtcNow.ToUnixTimeSeconds().ToString();
        }
        return new JsonObject();
    }

    // ── PurgeQueue ──────────────────────────────────────────────────────────────

    private JsonObject ActPurgeQueue(JsonObject data, string qurl)
    {
        var url = data["QueueUrl"]?.GetValue<string>() ?? qurl;
        lock (_lock)
        {
            var q = GetQueue(url);
            q.Messages.Clear();
        }
        return new JsonObject();
    }

    // ── SendMessageBatch ────────────────────────────────────────────────────────

    private JsonObject ActSendMessageBatch(JsonObject data, string qurl)
    {
        var url     = data["QueueUrl"]?.GetValue<string>() ?? qurl;
        var entries = data["Entries"]?.AsArray() ?? new JsonArray();

        if (entries.Count > 10)
            throw new SqsException("TooManyEntriesInBatchRequest",
                "Too many messages in a batch request. A maximum of 10 messages are allowed.");

        var ok   = new JsonArray();
        var fail = new JsonArray();

        foreach (var e in entries)
        {
            if (e is null) continue;
            var eid = e["Id"]?.GetValue<string>() ?? "";
            try
            {
                var sub = new JsonObject
                {
                    ["QueueUrl"]               = url,
                    ["MessageBody"]            = e["MessageBody"]?.GetValue<string>() ?? "",
                    ["MessageAttributes"]      = e["MessageAttributes"]?.DeepClone(),
                    ["MessageGroupId"]         = e["MessageGroupId"]?.GetValue<string>(),
                    ["MessageDeduplicationId"] = e["MessageDeduplicationId"]?.GetValue<string>(),
                };
                if (e["DelaySeconds"] is not null)
                    sub["DelaySeconds"] = e["DelaySeconds"]!.GetValue<string>();

                var r = ActSendMessage(sub, url);
                r["Id"] = eid;
                ok.Add(r);
            }
            catch (SqsException ex)
            {
                fail.Add(new JsonObject
                {
                    ["Id"]          = eid,
                    ["Code"]        = ex.Code,
                    ["Message"]     = ex.Message,
                    ["SenderFault"] = true,
                });
            }
        }

        return new JsonObject { ["Successful"] = ok, ["Failed"] = fail };
    }

    // ── DeleteMessageBatch ──────────────────────────────────────────────────────

    private JsonObject ActDeleteMessageBatch(JsonObject data, string qurl)
    {
        var url     = data["QueueUrl"]?.GetValue<string>() ?? qurl;
        var entries = data["Entries"]?.AsArray() ?? new JsonArray();
        var ok      = new JsonArray();
        var fail    = new JsonArray();

        lock (_lock)
        {
            var q = GetQueue(url);
            foreach (var e in entries)
            {
                if (e is null) continue;
                var eid    = e["Id"]?.GetValue<string>() ?? "";
                var rh     = e["ReceiptHandle"]?.GetValue<string>() ?? "";
                var before = q.Messages.Count;

                q.Messages.RemoveAll(m =>
                {
                    if (m.ReceiptHandle is null || m.ReceiptHandle != rh) return false;
                    if (q.IsFifo && m.DedupId is not null)
                        q.DedupCache.Remove(m.DedupId);
                    return true;
                });

                if (q.Messages.Count < before)
                    ok.Add(new JsonObject { ["Id"] = eid });
                else
                    fail.Add(new JsonObject
                    {
                        ["Id"]          = eid,
                        ["Code"]        = "ReceiptHandleIsInvalid",
                        ["Message"]     = "The input receipt handle is invalid.",
                        ["SenderFault"] = true,
                    });
            }
        }

        return new JsonObject { ["Successful"] = ok, ["Failed"] = fail };
    }

    // ── Tags ────────────────────────────────────────────────────────────────────

    private JsonObject ActListQueueTags(JsonObject data, string qurl)
    {
        var url = data["QueueUrl"]?.GetValue<string>() ?? qurl;
        lock (_lock)
        {
            var q    = GetQueue(url);
            var tags = new JsonObject();
            foreach (var (k, v) in q.Tags) tags[k] = v;
            return new JsonObject { ["Tags"] = tags };
        }
    }

    private JsonObject ActTagQueue(JsonObject data, string qurl)
    {
        var url  = data["QueueUrl"]?.GetValue<string>() ?? qurl;
        var tags = data["Tags"]?.AsObject() ?? new JsonObject();
        lock (_lock)
        {
            var q = GetQueue(url);
            foreach (var kv in tags)
                q.Tags[kv.Key] = kv.Value?.GetValue<string>() ?? "";
        }
        return new JsonObject();
    }

    private JsonObject ActUntagQueue(JsonObject data, string qurl)
    {
        var url     = data["QueueUrl"]?.GetValue<string>() ?? qurl;
        var tagKeys = data["TagKeys"]?.AsArray() ?? new JsonArray();
        lock (_lock)
        {
            var q = GetQueue(url);
            foreach (var k in tagKeys)
                q.Tags.Remove(k?.GetValue<string>() ?? "");
        }
        return new JsonObject();
    }

    // ── Queue / Message helpers ─────────────────────────────────────────────────

    /// <summary>
    /// Look up a queue by URL, falling back to name-based lookup for
    /// hostname-mismatch scenarios (e.g. docker-compose).
    /// Must be called inside _lock.
    /// </summary>
    private SqsQueue GetQueue(string url)
    {
        if (_queues.TryGetValue(url, out var q)) return q;

        // Fallback: extract name from URL path
        var parts = url.TrimEnd('/').Split('/');
        if (parts.Length >= 2)
        {
            var name = parts[^1];
            if (_queueNameToUrl.TryGetValue(name, out var canonical) &&
                _queues.TryGetValue(canonical, out var q2))
                return q2;
        }

        throw new SqsException("QueueDoesNotExist",
            "The specified queue does not exist for this wsdl version.");
    }

    private static void RefreshCounts(SqsQueue q)
    {
        var now     = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        var visible = 0;
        var delayed = 0;
        var inflight = 0;
        foreach (var m in q.Messages)
        {
            if (m.VisibleAtMs <= now)         visible++;
            else if (m.ReceiveCount == 0)     delayed++;
            else                              inflight++;
        }
        q.Attributes["ApproximateNumberOfMessages"]          = visible.ToString();
        q.Attributes["ApproximateNumberOfMessagesNotVisible"] = inflight.ToString();
        q.Attributes["ApproximateNumberOfMessagesDelayed"]    = delayed.ToString();
    }

    private static List<SqsMessage> CollectMessages(SqsQueue q, int maxN, int visTimeout)
    {
        var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        return q.IsFifo
            ? CollectFifo(q, maxN, visTimeout, now)
            : CollectStandard(q, maxN, visTimeout, now);
    }

    private static List<SqsMessage> CollectStandard(SqsQueue q, int maxN, int visTimeout, long now)
    {
        var result  = new List<SqsMessage>();
        var visible = q.Messages.Where(m => m.VisibleAtMs <= now).Take(maxN).ToList();
        foreach (var m in visible)
        {
            m.ReceiptHandle  = Guid.NewGuid().ToString();
            m.VisibleAtMs    = now + visTimeout * 1000L;
            m.ReceiveCount++;
            m.FirstReceiveAtMs ??= now;
            result.Add(m);
        }
        return result;
    }

    private static List<SqsMessage> CollectFifo(SqsQueue q, int maxN, int visTimeout, long now)
    {
        var inflightGroups = q.Messages
            .Where(m => m.VisibleAtMs > now && m.ReceiveCount > 0 && m.GroupId is not null)
            .Select(m => m.GroupId!)
            .ToHashSet(StringComparer.Ordinal);

        var result = new List<SqsMessage>();
        foreach (var m in q.Messages)
        {
            if (result.Count >= maxN) break;
            if (m.VisibleAtMs > now) continue;
            if (m.GroupId is not null && inflightGroups.Contains(m.GroupId)) continue;

            m.ReceiptHandle  = Guid.NewGuid().ToString();
            m.VisibleAtMs    = now + visTimeout * 1000L;
            m.ReceiveCount++;
            m.FirstReceiveAtMs ??= now;
            result.Add(m);
        }
        return result;
    }

    private void DlqSweepWithQueues(SqsQueue q)
    {
        var rpRaw = q.Attributes.GetValueOrDefault("RedrivePolicy");
        if (string.IsNullOrEmpty(rpRaw)) return;

        JsonObject? rp;
        try { rp = JsonNode.Parse(rpRaw)?.AsObject(); }
        catch { return; }
        if (rp is null) return;

        var maxRc = rp["maxReceiveCount"]?.GetValue<int>() ?? 0;
        var arn   = rp["deadLetterTargetArn"]?.GetValue<string>() ?? "";
        if (maxRc == 0 || string.IsNullOrEmpty(arn)) return;

        SqsQueue? dlq = null;
        foreach (var kv in _queues.Items)
        {
            var candidate = kv.Value;
            if (candidate.Attributes.GetValueOrDefault("QueueArn") == arn)
            {
                dlq = candidate;
                break;
            }
        }
        if (dlq is null) return;

        var now  = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        var keep = new List<SqsMessage>();
        foreach (var m in q.Messages)
        {
            if (m.ReceiveCount >= maxRc && m.VisibleAtMs <= now)
            {
                var moved = m.Clone();
                moved.ReceiptHandle = null;
                moved.VisibleAtMs   = now;
                dlq.Messages.Add(moved);
            }
            else
            {
                keep.Add(m);
            }
        }
        q.Messages = keep;
    }

    private static void PruneDedup(SqsQueue q)
    {
        var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        var expired = q.DedupCache.Where(kv => kv.Value.ExpireAtMs <= now)
                                  .Select(kv => kv.Key).ToList();
        foreach (var k in expired)
            q.DedupCache.Remove(k);
    }

    private static JsonObject BuildSysAttrs(SqsMessage m, List<string> names)
    {
        if (names.Count == 0) return new JsonObject();
        var wantAll = names.Contains("All");
        var r = new JsonObject();
        if (wantAll || names.Contains("SenderId"))
            r["SenderId"] = m.SystemAttributes.GetValueOrDefault("SenderId", "");
        if (wantAll || names.Contains("SentTimestamp"))
            r["SentTimestamp"] = m.SystemAttributes.GetValueOrDefault("SentTimestamp", "0");
        if (wantAll || names.Contains("ApproximateReceiveCount"))
            r["ApproximateReceiveCount"] = m.ReceiveCount.ToString();
        if (wantAll || names.Contains("ApproximateFirstReceiveTimestamp"))
            r["ApproximateFirstReceiveTimestamp"] =
                m.FirstReceiveAtMs.HasValue ? m.FirstReceiveAtMs.Value.ToString() : "0";
        if (m.SequenceNumber is not null && (wantAll || names.Contains("SequenceNumber")))
            r["SequenceNumber"] = m.SequenceNumber;
        if (m.DedupId is not null && (wantAll || names.Contains("MessageDeduplicationId")))
            r["MessageDeduplicationId"] = m.DedupId;
        if (m.GroupId is not null && (wantAll || names.Contains("MessageGroupId")))
            r["MessageGroupId"] = m.GroupId;
        return r;
    }

    private static JsonObject FilterMsgAttrs(JsonObject attrs, List<string> names)
    {
        if (attrs.Count == 0 || names.Count == 0) return new JsonObject();
        if (names.Contains("All") || names.Contains(".*"))
            return (JsonObject)attrs.DeepClone();

        var out_ = new JsonObject();
        foreach (var n in names)
        {
            if (n.EndsWith(".*", StringComparison.Ordinal))
            {
                var pfx = n[..^2];
                foreach (var (k, v) in attrs)
                    if (k.StartsWith(pfx, StringComparison.Ordinal))
                        out_[k] = v?.DeepClone();
            }
            else if (attrs[n] is not null)
            {
                out_[n] = attrs[n]?.DeepClone();
            }
        }
        return out_;
    }

    // ── Helpers ─────────────────────────────────────────────────────────────────

    private static string QueueUrl(string name) =>
        $"http://{_defaultHost}:{_defaultPort}/{AccountContext.GetAccountId()}/{name}";

    private static string UrlFromPath(string path)
    {
        var parts = path.TrimStart('/').Split('/');
        return parts.Length >= 2 ? QueueUrl(parts[^1]) : "";
    }

    private static string ComputeMd5Hex(string text)
    {
        var hash = MD5.HashData(Encoding.UTF8.GetBytes(text));
        return Convert.ToHexStringLower(hash);
    }

    private static string ComputeSha256Hex(string text)
    {
        var hash = SHA256.HashData(Encoding.UTF8.GetBytes(text));
        return Convert.ToHexStringLower(hash);
    }

    /// <summary>
    /// Compute MD5 of message attributes following the AWS wire-format binary encoding.
    /// Port of ministack's _md5_msg_attrs().
    /// </summary>
    private static string? ComputeMd5MsgAttrs(JsonObject? attrs)
    {
        if (attrs is null || attrs.Count == 0) return null;

        using var ms = new MemoryStream();
        using var bw = new BinaryWriter(ms, Encoding.UTF8, leaveOpen: true);

        foreach (var name in attrs.Select(kv => kv.Key).Order(StringComparer.Ordinal))
        {
            var a        = attrs[name]?.AsObject() ?? new JsonObject();
            var dataType = (a["DataType"]?.GetValue<string>() ?? "String");
            var nameBytes = Encoding.UTF8.GetBytes(name);
            var dtBytes   = Encoding.UTF8.GetBytes(dataType);

            WriteBigEndianInt32(bw, nameBytes.Length);
            bw.Write(nameBytes);
            WriteBigEndianInt32(bw, dtBytes.Length);
            bw.Write(dtBytes);

            if (dataType.StartsWith("Binary", StringComparison.Ordinal))
            {
                bw.Write((byte)0x02);
                byte[] val;
                var bvNode = a["BinaryValue"];
                if (bvNode is not null)
                {
                    var bvStr = bvNode.GetValue<string>();
                    val = Convert.FromBase64String(bvStr);
                }
                else
                {
                    val = [];
                }
                WriteBigEndianInt32(bw, val.Length);
                bw.Write(val);
            }
            else
            {
                bw.Write((byte)0x01);
                var sv  = Encoding.UTF8.GetBytes(a["StringValue"]?.GetValue<string>() ?? "");
                WriteBigEndianInt32(bw, sv.Length);
                bw.Write(sv);
            }
        }

        bw.Flush();
        var hash = MD5.HashData(ms.ToArray());
        return Convert.ToHexStringLower(hash);
    }

    private static void WriteBigEndianInt32(BinaryWriter bw, int value)
    {
        bw.Write((byte)(value >> 24));
        bw.Write((byte)(value >> 16));
        bw.Write((byte)(value >>  8));
        bw.Write((byte)(value      ));
    }

    // ── Query-param normalisation ───────────────────────────────────────────────

    private static JsonObject NormaliseParams(string action, Dictionary<string, string> p)
    {
        var d = new JsonObject();

        // Scalar params
        foreach (var key in new[]
        {
            "QueueName", "QueueUrl", "MessageBody", "ReceiptHandle",
            "VisibilityTimeout", "DelaySeconds", "WaitTimeSeconds",
            "MaxNumberOfMessages", "MaxResults", "QueueNamePrefix",
            "MessageGroupId", "MessageDeduplicationId", "ReceiveRequestAttemptId",
        })
        {
            if (p.TryGetValue(key, out var v) && !string.IsNullOrEmpty(v))
                d[key] = v;
        }

        // Attribute.N.Name / .Value → Attributes dict
        var attrs = new JsonObject();
        for (var i = 1; p.TryGetValue($"Attribute.{i}.Name", out var an) && !string.IsNullOrEmpty(an); i++)
        {
            p.TryGetValue($"Attribute.{i}.Value", out var av);
            attrs[an] = av ?? "";
        }
        if (attrs.Count > 0) d["Attributes"] = attrs;

        // AttributeName.N → AttributeNames list
        var attrNames = new JsonArray();
        for (var i = 1; p.TryGetValue($"AttributeName.{i}", out var an) && !string.IsNullOrEmpty(an); i++)
            attrNames.Add(an);
        if (attrNames.Count > 0) d["AttributeNames"] = attrNames;

        // MessageAttributeName.N
        var manNames = new JsonArray();
        for (var i = 1; p.TryGetValue($"MessageAttributeName.{i}", out var mn) && !string.IsNullOrEmpty(mn); i++)
            manNames.Add(mn);
        if (manNames.Count > 0) d["MessageAttributeNames"] = manNames;

        // MessageAttribute.N.Name / .Value.*
        var ma = new JsonObject();
        for (var i = 1; p.TryGetValue($"MessageAttribute.{i}.Name", out var nm) && !string.IsNullOrEmpty(nm); i++)
        {
            var a = new JsonObject
            {
                ["DataType"] = p.GetValueOrDefault($"MessageAttribute.{i}.Value.DataType", "String"),
            };
            if (p.TryGetValue($"MessageAttribute.{i}.Value.StringValue", out var sv)) a["StringValue"] = sv;
            if (p.TryGetValue($"MessageAttribute.{i}.Value.BinaryValue", out var bv)) a["BinaryValue"] = bv;
            ma[nm] = a;
        }
        if (ma.Count > 0) d["MessageAttributes"] = ma;

        // Tag.N.Key / .Value
        var tags = new JsonObject();
        for (var i = 1; p.TryGetValue($"Tag.{i}.Key", out var tk) && !string.IsNullOrEmpty(tk); i++)
        {
            p.TryGetValue($"Tag.{i}.Value", out var tv);
            tags[tk] = tv ?? "";
        }
        if (tags.Count > 0) d["Tags"] = tags;

        // TagKey.N
        var tagKeys = new JsonArray();
        for (var i = 1; p.TryGetValue($"TagKey.{i}", out var tk) && !string.IsNullOrEmpty(tk); i++)
            tagKeys.Add(tk);
        if (tagKeys.Count > 0) d["TagKeys"] = tagKeys;

        // Batch entries
        if (action == "SendMessageBatch")
            d["Entries"] = ParseSendBatchEntries(p);

        if (action == "DeleteMessageBatch")
        {
            var entries = new JsonArray();
            const string pfx = "DeleteMessageBatchRequestEntry";
            for (var i = 1; p.TryGetValue($"{pfx}.{i}.Id", out var eid) && !string.IsNullOrEmpty(eid); i++)
            {
                p.TryGetValue($"{pfx}.{i}.ReceiptHandle", out var rh);
                entries.Add(new JsonObject { ["Id"] = eid, ["ReceiptHandle"] = rh });
            }
            d["Entries"] = entries;
        }

        if (action == "ChangeMessageVisibilityBatch")
        {
            var entries = new JsonArray();
            const string pfx = "ChangeMessageVisibilityBatchRequestEntry";
            for (var i = 1; p.TryGetValue($"{pfx}.{i}.Id", out var eid) && !string.IsNullOrEmpty(eid); i++)
            {
                p.TryGetValue($"{pfx}.{i}.ReceiptHandle", out var rh);
                p.TryGetValue($"{pfx}.{i}.VisibilityTimeout", out var vt);
                entries.Add(new JsonObject
                {
                    ["Id"]                = eid,
                    ["ReceiptHandle"]     = rh,
                    ["VisibilityTimeout"] = vt != null ? int.Parse(vt) : 30,
                });
            }
            d["Entries"] = entries;
        }

        return d;
    }

    private static JsonArray ParseSendBatchEntries(Dictionary<string, string> p)
    {
        var entries = new JsonArray();
        const string pfx = "SendMessageBatchRequestEntry";
        for (var i = 1; p.TryGetValue($"{pfx}.{i}.Id", out var eid) && !string.IsNullOrEmpty(eid); i++)
        {
            p.TryGetValue($"{pfx}.{i}.MessageBody", out var body);
            var e = new JsonObject
            {
                ["Id"]          = eid,
                ["MessageBody"] = body ?? "",
            };
            if (p.TryGetValue($"{pfx}.{i}.DelaySeconds", out var ds))          e["DelaySeconds"]          = ds;
            if (p.TryGetValue($"{pfx}.{i}.MessageGroupId", out var gid))       e["MessageGroupId"]        = gid;
            if (p.TryGetValue($"{pfx}.{i}.MessageDeduplicationId", out var did)) e["MessageDeduplicationId"] = did;

            var ema = new JsonObject();
            for (var j = 1; p.TryGetValue($"{pfx}.{i}.MessageAttribute.{j}.Name", out var anm) && !string.IsNullOrEmpty(anm); j++)
            {
                var a = new JsonObject
                {
                    ["DataType"] = p.GetValueOrDefault($"{pfx}.{i}.MessageAttribute.{j}.Value.DataType", "String"),
                };
                if (p.TryGetValue($"{pfx}.{i}.MessageAttribute.{j}.Value.StringValue", out var sv)) a["StringValue"] = sv;
                if (p.TryGetValue($"{pfx}.{i}.MessageAttribute.{j}.Value.BinaryValue", out var bv)) a["BinaryValue"] = bv;
                ema[anm] = a;
            }
            if (ema.Count > 0) e["MessageAttributes"] = ema;

            entries.Add(e);
        }
        return entries;
    }

    // ── XML response formatters ─────────────────────────────────────────────────

    private static ServiceResponse ToXmlResponse(string action, JsonObject result)
    {
        var inner = action switch
        {
            "CreateQueue" =>
                $"<CreateQueueResult><QueueUrl>{XmlEsc(result["QueueUrl"]?.GetValue<string>())}</QueueUrl></CreateQueueResult>",

            "DeleteQueue" => "",

            "ListQueues" => $"<ListQueuesResult>" +
                string.Concat((result["QueueUrls"]?.AsArray() ?? [])
                    .Select(u => $"<QueueUrl>{XmlEsc(u?.GetValue<string>())}</QueueUrl>"))
                + "</ListQueuesResult>",

            "GetQueueUrl" =>
                $"<GetQueueUrlResult><QueueUrl>{XmlEsc(result["QueueUrl"]?.GetValue<string>())}</QueueUrl></GetQueueUrlResult>",

            "SendMessage" => BuildSendMessageXml(result),

            "ReceiveMessage" =>
                $"<ReceiveMessageResult>{MsgsToXml(result["Messages"]?.AsArray() ?? [])}</ReceiveMessageResult>",

            "DeleteMessage"            => "",
            "ChangeMessageVisibility"  => "",

            "ChangeMessageVisibilityBatch" =>
                $"<ChangeMessageVisibilityBatchResult>" +
                BatchResultXml(result, "ChangeMessageVisibilityBatchResultEntry") +
                "</ChangeMessageVisibilityBatchResult>",

            "GetQueueAttributes" =>
                "<GetQueueAttributesResult>" +
                string.Concat((result["Attributes"]?.AsObject() ?? [])
                    .Select(kv => $"<Attribute><Name>{XmlEsc(kv.Key)}</Name><Value>{XmlEsc(kv.Value?.GetValue<string>())}</Value></Attribute>"))
                + "</GetQueueAttributesResult>",

            "SetQueueAttributes" => "",
            "PurgeQueue"         => "",

            "SendMessageBatch" =>
                "<SendMessageBatchResult>" + BuildSendBatchResultXml(result) + "</SendMessageBatchResult>",

            "DeleteMessageBatch" =>
                "<DeleteMessageBatchResult>" + BuildDeleteBatchResultXml(result) + "</DeleteMessageBatchResult>",

            "ListQueueTags" =>
                "<ListQueueTagsResult>" +
                string.Concat((result["Tags"]?.AsObject() ?? [])
                    .Select(kv => $"<Tag><Key>{XmlEsc(kv.Key)}</Key><Value>{XmlEsc(kv.Value?.GetValue<string>())}</Value></Tag>"))
                + "</ListQueueTagsResult>",

            "TagQueue"   => "",
            "UntagQueue" => "",

            _ => "",
        };

        return XmlResponse(action, inner);
    }

    private static string BuildSendMessageXml(JsonObject result)
    {
        var sb = new StringBuilder("<SendMessageResult>");
        sb.Append($"<MessageId>{result["MessageId"]?.GetValue<string>()}</MessageId>");
        sb.Append($"<MD5OfMessageBody>{result["MD5OfMessageBody"]?.GetValue<string>()}</MD5OfMessageBody>");
        if (result["MD5OfMessageAttributes"] is not null)
            sb.Append($"<MD5OfMessageAttributes>{result["MD5OfMessageAttributes"]?.GetValue<string>()}</MD5OfMessageAttributes>");
        if (result["SequenceNumber"] is not null)
            sb.Append($"<SequenceNumber>{result["SequenceNumber"]?.GetValue<string>()}</SequenceNumber>");
        sb.Append("</SendMessageResult>");
        return sb.ToString();
    }

    private static string BuildSendBatchResultXml(JsonObject result)
    {
        var sb = new StringBuilder();
        foreach (var e in result["Successful"]?.AsArray() ?? [])
        {
            if (e is null) continue;
            sb.Append("<SendMessageBatchResultEntry>");
            sb.Append($"<Id>{XmlEsc(e["Id"]?.GetValue<string>())}</Id>");
            sb.Append($"<MessageId>{e["MessageId"]?.GetValue<string>()}</MessageId>");
            sb.Append($"<MD5OfMessageBody>{e["MD5OfMessageBody"]?.GetValue<string>()}</MD5OfMessageBody>");
            if (e["MD5OfMessageAttributes"] is not null)
                sb.Append($"<MD5OfMessageAttributes>{e["MD5OfMessageAttributes"]?.GetValue<string>()}</MD5OfMessageAttributes>");
            if (e["SequenceNumber"] is not null)
                sb.Append($"<SequenceNumber>{e["SequenceNumber"]?.GetValue<string>()}</SequenceNumber>");
            sb.Append("</SendMessageBatchResultEntry>");
        }
        sb.Append(BatchErrorsXml(result["Failed"]?.AsArray() ?? []));
        return sb.ToString();
    }

    private static string BuildDeleteBatchResultXml(JsonObject result)
    {
        var sb = new StringBuilder();
        foreach (var e in result["Successful"]?.AsArray() ?? [])
        {
            if (e is null) continue;
            sb.Append($"<DeleteMessageBatchResultEntry><Id>{XmlEsc(e["Id"]?.GetValue<string>())}</Id></DeleteMessageBatchResultEntry>");
        }
        sb.Append(BatchErrorsXml(result["Failed"]?.AsArray() ?? []));
        return sb.ToString();
    }

    private static string MsgsToXml(JsonArray msgs)
    {
        var sb = new StringBuilder();
        foreach (var m in msgs)
        {
            if (m is null) continue;
            sb.Append("<Message>");
            sb.Append($"<MessageId>{m["MessageId"]?.GetValue<string>()}</MessageId>");
            sb.Append($"<ReceiptHandle>{XmlEsc(m["ReceiptHandle"]?.GetValue<string>())}</ReceiptHandle>");
            sb.Append($"<MD5OfBody>{m["MD5OfBody"]?.GetValue<string>()}</MD5OfBody>");
            sb.Append($"<Body>{XmlEsc(m["Body"]?.GetValue<string>())}</Body>");

            foreach (var kv in m["Attributes"]?.AsObject() ?? [])
                sb.Append($"<Attribute><Name>{XmlEsc(kv.Key)}</Name><Value>{XmlEsc(kv.Value?.GetValue<string>())}</Value></Attribute>");

            foreach (var kv in m["MessageAttributes"]?.AsObject() ?? [])
            {
                var av = kv.Value?.AsObject() ?? new JsonObject();
                sb.Append($"<MessageAttribute><Name>{XmlEsc(kv.Key)}</Name><Value>");
                sb.Append($"<DataType>{XmlEsc(av["DataType"]?.GetValue<string>() ?? "String")}</DataType>");
                if (av["StringValue"] is not null)
                    sb.Append($"<StringValue>{XmlEsc(av["StringValue"]?.GetValue<string>())}</StringValue>");
                if (av["BinaryValue"] is not null)
                    sb.Append($"<BinaryValue>{XmlEsc(av["BinaryValue"]?.GetValue<string>())}</BinaryValue>");
                sb.Append("</Value></MessageAttribute>");
            }

            if (m["MD5OfMessageAttributes"] is not null)
                sb.Append($"<MD5OfMessageAttributes>{m["MD5OfMessageAttributes"]?.GetValue<string>()}</MD5OfMessageAttributes>");

            sb.Append("</Message>");
        }
        return sb.ToString();
    }

    private static string BatchResultXml(JsonObject result, string entryTag)
    {
        var sb = new StringBuilder();
        foreach (var e in result["Successful"]?.AsArray() ?? [])
        {
            if (e is null) continue;
            sb.Append($"<{entryTag}><Id>{XmlEsc(e["Id"]?.GetValue<string>())}</Id></{entryTag}>");
        }
        sb.Append(BatchErrorsXml(result["Failed"]?.AsArray() ?? []));
        return sb.ToString();
    }

    private static string BatchErrorsXml(JsonArray failed)
    {
        var sb = new StringBuilder();
        foreach (var e in failed)
        {
            if (e is null) continue;
            sb.Append("<BatchResultErrorEntry>");
            sb.Append($"<Id>{XmlEsc(e["Id"]?.GetValue<string>())}</Id>");
            sb.Append($"<Code>{XmlEsc(e["Code"]?.GetValue<string>())}</Code>");
            sb.Append($"<Message>{XmlEsc(e["Message"]?.GetValue<string>())}</Message>");
            var sf = e["SenderFault"]?.GetValue<bool>() ?? true;
            sb.Append($"<SenderFault>{(sf ? "true" : "false")}</SenderFault>");
            sb.Append("</BatchResultErrorEntry>");
        }
        return sb.ToString();
    }

    private static ServiceResponse XmlResponse(string action, string inner)
    {
        var ns   = "http://queue.amazonaws.com/doc/2012-11-05/";
        var rid  = Guid.NewGuid().ToString();
        var body = Encoding.UTF8.GetBytes(
            $"<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
            $"<{action}Response xmlns=\"{ns}\">" +
            inner +
            $"<ResponseMetadata><RequestId>{rid}</RequestId></ResponseMetadata>" +
            $"</{action}Response>");
        return new ServiceResponse(200,
            new Dictionary<string, string> { ["Content-Type"] = "application/xml" },
            body);
    }

    private static ServiceResponse XmlErrorResponse(string code, string message, int status)
    {
        var senderType = status < 500 ? "Sender" : "Receiver";
        var rid        = Guid.NewGuid().ToString();
        var body = Encoding.UTF8.GetBytes(
            $"<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
            $"<ErrorResponse xmlns=\"http://queue.amazonaws.com/doc/2012-11-05/\">" +
            $"<Error><Type>{senderType}</Type><Code>{XmlEsc(code)}</Code>" +
            $"<Message>{XmlEsc(message)}</Message></Error>" +
            $"<RequestId>{rid}</RequestId>" +
            $"</ErrorResponse>");
        return new ServiceResponse(status,
            new Dictionary<string, string> { ["Content-Type"] = "application/xml" },
            body);
    }

    private static ServiceResponse JsonOkResponse(JsonObject data)
    {
        var body = Encoding.UTF8.GetBytes(data.ToJsonString());
        return new ServiceResponse(200,
            new Dictionary<string, string> { ["Content-Type"] = "application/x-amz-json-1.0" },
            body);
    }

    private static ServiceResponse JsonErrorResponse(string code, string message, int status)
    {
        var fault  = status < 500 ? "Sender" : "Receiver";
        var legacy = QueryCompatCodes.GetValueOrDefault(code, code);
        var body   = Encoding.UTF8.GetBytes(
            JsonSerializer.Serialize(new { __type = code, message }));
        return new ServiceResponse(status,
            new Dictionary<string, string>
            {
                ["Content-Type"]       = "application/x-amz-json-1.0",
                ["x-amzn-query-error"] = $"{legacy};{fault}",
            },
            body);
    }

    private static string XmlEsc(string? s) =>
        s is null ? "" : s
            .Replace("&", "&amp;")
            .Replace("<", "&lt;")
            .Replace(">", "&gt;")
            .Replace("\"", "&quot;")
            .Replace("'", "&apos;");

    // ── ESM helpers (internal — used by Lambda ESM poller in Phase 4) ───────────

    /// <summary>Receive up to maxNumber messages for ESM consumption (thread-safe).</summary>
    internal List<SqsMessage> ReceiveMessagesForEsm(string queueUrl, int maxNumber)
    {
        lock (_lock)
        {
            var q = GetQueue(queueUrl);
            DlqSweepWithQueues(q);
            return CollectMessages(q, Math.Min(maxNumber, 10),
                int.Parse(q.Attributes.GetValueOrDefault("VisibilityTimeout", "30")));
        }
    }

    /// <summary>Best-effort delete of messages received by ESM (thread-safe).</summary>
    internal void DeleteMessagesForEsm(string queueUrl, IReadOnlyCollection<string> receiptHandles)
    {
        if (receiptHandles.Count == 0) return;
        lock (_lock)
        {
            var q = GetQueue(queueUrl);
            q.Messages.RemoveAll(m =>
            {
                if (m.ReceiptHandle is null || !receiptHandles.Contains(m.ReceiptHandle)) return false;
                if (q.IsFifo && m.DedupId is not null)
                    q.DedupCache.Remove(m.DedupId);
                return true;
            });
        }
    }
}

// ── Data models ─────────────────────────────────────────────────────────────────

/// <summary>In-memory SQS queue.</summary>
internal sealed class SqsQueue
{
    public required string Name { get; set; }
    public required string Url  { get; set; }
    public bool IsFifo { get; set; }
    public Dictionary<string, string> Attributes { get; set; } = new(StringComparer.Ordinal);
    public List<SqsMessage>           Messages   { get; set; } = [];
    public Dictionary<string, string> Tags       { get; set; } = new(StringComparer.Ordinal);
    public Dictionary<string, SqsDedupEntry> DedupCache { get; set; } = new(StringComparer.Ordinal);
    public long FifoSeq { get; set; }
}

/// <summary>In-memory SQS message.</summary>
internal sealed class SqsMessage
{
    public required string Id             { get; set; }
    public required string Body           { get; set; }
    public required string Md5Body        { get; set; }
    public string?         Md5Attrs       { get; set; }
    public string?         ReceiptHandle  { get; set; }
    public long            SentAtMs       { get; set; }
    public long            VisibleAtMs    { get; set; }
    public int             ReceiveCount   { get; set; }
    public long?           FirstReceiveAtMs { get; set; }
    public JsonObject      MessageAttributes { get; set; } = new();
    public Dictionary<string, string> SystemAttributes { get; set; } = new(StringComparer.Ordinal);
    public string?         GroupId        { get; set; }
    public string?         DedupId        { get; set; }
    public string?         SequenceNumber { get; set; }

    internal SqsMessage Clone() => new()
    {
        Id               = Id,
        Body             = Body,
        Md5Body          = Md5Body,
        Md5Attrs         = Md5Attrs,
        ReceiptHandle    = ReceiptHandle,
        SentAtMs         = SentAtMs,
        VisibleAtMs      = VisibleAtMs,
        ReceiveCount     = ReceiveCount,
        FirstReceiveAtMs = FirstReceiveAtMs,
        MessageAttributes = (JsonObject)MessageAttributes.DeepClone(),
        SystemAttributes = new Dictionary<string, string>(SystemAttributes, StringComparer.Ordinal),
        GroupId          = GroupId,
        DedupId          = DedupId,
        SequenceNumber   = SequenceNumber,
    };
}

/// <summary>FIFO deduplication cache entry.</summary>
internal sealed class SqsDedupEntry
{
    public required string Id             { get; set; }
    public required string Md5Body        { get; set; }
    public string?         Md5Attrs       { get; set; }
    public string?         SequenceNumber { get; set; }
    public long            ExpireAtMs     { get; set; }
}

/// <summary>SQS domain error.</summary>
internal sealed class SqsException : Exception
{
    public string Code   { get; }
    public int    Status { get; }

    internal SqsException(string code, string message, int status = 400)
        : base(message)
    {
        Code   = code;
        Status = status;
    }
}
