using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using MicroStack.Internal;

namespace MicroStack.Services.Kinesis;

/// <summary>
/// Kinesis Data Streams service handler -- supports JSON protocol via X-Amz-Target.
///
/// Port of ministack/services/kinesis.py.
///
/// Supports: CreateStream, DeleteStream, DescribeStream, DescribeStreamSummary,
///           ListStreams, ListShards, PutRecord, PutRecords, GetShardIterator, GetRecords,
///           MergeShards, SplitShard, UpdateShardCount,
///           IncreaseStreamRetentionPeriod, DecreaseStreamRetentionPeriod,
///           AddTagsToStream, RemoveTagsFromStream, ListTagsForStream,
///           RegisterStreamConsumer, DeregisterStreamConsumer, ListStreamConsumers,
///           DescribeStreamConsumer, StartStreamEncryption, StopStreamEncryption,
///           EnableEnhancedMonitoring, DisableEnhancedMonitoring.
/// </summary>
internal sealed class KinesisServiceHandler : IServiceHandler
{
    private readonly Lock _lock = new();

    private static string Region => MicroStackOptions.Instance.Region;

    private static readonly System.Numerics.BigInteger MaxHashKey = System.Numerics.BigInteger.Pow(2, 128) - 1;
    private const int IteratorExpirySeconds = 300;

    // In-memory state
    private readonly AccountScopedDictionary<string, KinStream> _streams = new();
    private readonly AccountScopedDictionary<string, ShardIteratorState> _shardIterators = new();
    private readonly AccountScopedDictionary<string, KinConsumer> _consumers = new();
    private long _sequenceCounter;

    // -- IServiceHandler -------------------------------------------------------

    public string ServiceName => "kinesis";

    public Task<ServiceResponse> HandleAsync(ServiceRequest request)
    {
        var target = request.GetHeader("x-amz-target") ?? "";
        var action = target.Contains('.', StringComparison.Ordinal)
            ? target[(target.LastIndexOf('.') + 1)..]
            : "";

        JsonElement data;
        if (request.Body.Length > 0)
        {
            try
            {
                using var doc = JsonDocument.Parse(request.Body);
                data = doc.RootElement.Clone();
            }
            catch (JsonException)
            {
                return Task.FromResult(
                    AwsResponseHelpers.ErrorResponseJson("SerializationException", "Invalid JSON", 400));
            }
        }
        else
        {
            data = JsonDocument.Parse("{}").RootElement.Clone();
        }

        ExpireIterators();

        var response = action switch
        {
            "CreateStream" => ActCreateStream(data),
            "DeleteStream" => ActDeleteStream(data),
            "DescribeStream" => ActDescribeStream(data),
            "DescribeStreamSummary" => ActDescribeStreamSummary(data),
            "ListStreams" => ActListStreams(data),
            "ListShards" => ActListShards(data),
            "PutRecord" => ActPutRecord(data),
            "PutRecords" => ActPutRecords(data),
            "GetShardIterator" => ActGetShardIterator(data),
            "GetRecords" => ActGetRecords(data),
            "IncreaseStreamRetentionPeriod" => ActIncreaseRetention(data),
            "DecreaseStreamRetentionPeriod" => ActDecreaseRetention(data),
            "AddTagsToStream" => ActAddTags(data),
            "RemoveTagsFromStream" => ActRemoveTags(data),
            "ListTagsForStream" => ActListTags(data),
            "MergeShards" => ActMergeShards(data),
            "SplitShard" => ActSplitShard(data),
            "UpdateShardCount" => ActUpdateShardCount(data),
            "RegisterStreamConsumer" => ActRegisterConsumer(data),
            "DeregisterStreamConsumer" => ActDeregisterConsumer(data),
            "ListStreamConsumers" => ActListConsumers(data),
            "DescribeStreamConsumer" => ActDescribeStreamConsumer(data),
            "StartStreamEncryption" => ActStartStreamEncryption(data),
            "StopStreamEncryption" => ActStopStreamEncryption(data),
            "EnableEnhancedMonitoring" => ActEnableEnhancedMonitoring(data),
            "DisableEnhancedMonitoring" => ActDisableEnhancedMonitoring(data),
            _ => AwsResponseHelpers.ErrorResponseJson("InvalidAction", $"Unknown action: {action}", 400),
        };

        return Task.FromResult(response);
    }

    public void Reset()
    {
        lock (_lock)
        {
            _streams.Clear();
            _shardIterators.Clear();
            _consumers.Clear();
            _sequenceCounter = 0;
        }
    }

    public object? GetState() => null;

    public void RestoreState(object state) { }

    // -- JSON helpers ----------------------------------------------------------

    private static string? GetString(JsonElement el, string propertyName)
    {
        return el.TryGetProperty(propertyName, out var prop) && prop.ValueKind == JsonValueKind.String
            ? prop.GetString()
            : null;
    }

    private static int GetInt(JsonElement el, string propertyName, int defaultValue)
    {
        if (!el.TryGetProperty(propertyName, out var prop))
        {
            return defaultValue;
        }

        return prop.TryGetInt32(out var val) ? val : defaultValue;
    }

    private static double? GetNullableDouble(JsonElement el, string propertyName)
    {
        if (!el.TryGetProperty(propertyName, out var prop))
        {
            return null;
        }

        return prop.TryGetDouble(out var val) ? val : null;
    }

    // -- Sequence numbers ------------------------------------------------------

    private string NextSequenceNumber()
    {
        Interlocked.Increment(ref _sequenceCounter);
        var tsMillis = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        return $"{tsMillis:D20}{Interlocked.Read(ref _sequenceCounter):D10}";
    }

    // -- Shard helpers ---------------------------------------------------------

    private static (string Start, string End)[] ComputeHashRanges(int shardCount)
    {
        var rangeSize = (MaxHashKey + 1) / shardCount;
        var ranges = new (string Start, string End)[shardCount];
        for (var i = 0; i < shardCount; i++)
        {
            var start = i * rangeSize;
            var end = i < shardCount - 1 ? ((i + 1) * rangeSize - 1) : MaxHashKey;
            ranges[i] = (start.ToString(), end.ToString());
        }

        return ranges;
    }

    private Dictionary<string, KinShard> BuildShards(int shardCount, int startIndex)
    {
        var ranges = ComputeHashRanges(shardCount);
        var shards = new Dictionary<string, KinShard>(StringComparer.Ordinal);
        for (var i = 0; i < shardCount; i++)
        {
            var sid = $"shardId-{startIndex + i:D12}";
            shards[sid] = new KinShard
            {
                StartingHashKey = ranges[i].Start,
                EndingHashKey = ranges[i].End,
                StartingSequenceNumber = NextSequenceNumber(),
            };
        }

        return shards;
    }

    private Dictionary<string, KinShard> BuildShards(int shardCount)
    {
        return BuildShards(shardCount, 0);
    }

    private static System.Numerics.BigInteger PartitionKeyToHash(string partitionKey)
    {
        var hash = MD5.HashData(Encoding.UTF8.GetBytes(partitionKey));
        // MD5 returns 16 bytes; interpret as big-endian unsigned integer
        var reversed = new byte[hash.Length + 1]; // extra 0 byte to ensure positive
        for (var i = 0; i < hash.Length; i++)
        {
            reversed[hash.Length - 1 - i] = hash[i];
        }

        return new System.Numerics.BigInteger(reversed);
    }

    private static string RouteToShard(System.Numerics.BigInteger hashKeyInt, KinStream stream)
    {
        foreach (var (sid, shard) in stream.Shards)
        {
            if (System.Numerics.BigInteger.Parse(shard.StartingHashKey) <= hashKeyInt &&
                hashKeyInt <= System.Numerics.BigInteger.Parse(shard.EndingHashKey))
            {
                return sid;
            }
        }

        return stream.Shards.Keys.First();
    }

    private static void ExpireRecords(KinStream stream)
    {
        var cutoff = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() / 1000.0 - stream.RetentionPeriodHours * 3600;
        foreach (var shard in stream.Shards.Values)
        {
            shard.Records.RemoveAll(r => r.ApproximateArrivalTimestamp < cutoff);
        }
    }

    private void ExpireIterators()
    {
        lock (_lock)
        {
            var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() / 1000.0;
            var expired = new List<string>();
            foreach (var (tok, st) in _shardIterators.Items)
            {
                if (now - st.CreatedAt > IteratorExpirySeconds)
                {
                    expired.Add(tok);
                }
            }

            foreach (var tok in expired)
            {
                _shardIterators.TryRemove(tok, out _);
            }
        }
    }

    private (string? Name, KinStream? Stream) ResolveStream(JsonElement data)
    {
        var name = GetString(data, "StreamName");
        var arn = GetString(data, "StreamARN");
        if (name is not null && _streams.TryGetValue(name, out var s))
        {
            return (name, s);
        }

        if (arn is not null)
        {
            foreach (var (n, stream) in _streams.Items)
            {
                if (stream.StreamArn == arn)
                {
                    return (n, stream);
                }
            }
        }

        return (name ?? arn, null);
    }

    private static int MaxShardIndex(KinStream stream)
    {
        var max = -1;
        foreach (var sid in stream.Shards.Keys)
        {
            var parts = sid.Split('-');
            if (parts.Length > 1 && int.TryParse(parts[1], out var idx) && idx > max)
            {
                max = idx;
            }
        }

        return max;
    }

    private static string EnsureBase64(string value)
    {
        try
        {
            Convert.FromBase64String(value);
            return value;
        }
        catch
        {
            return Convert.ToBase64String(Encoding.UTF8.GetBytes(value));
        }
    }

    // -- Stream lifecycle ------------------------------------------------------

    private ServiceResponse ActCreateStream(JsonElement data)
    {
        var name = GetString(data, "StreamName");
        var shardCount = GetInt(data, "ShardCount", 1);

        if (string.IsNullOrEmpty(name))
        {
            return AwsResponseHelpers.ErrorResponseJson("ValidationException", "StreamName is required", 400);
        }

        if (shardCount < 1)
        {
            return AwsResponseHelpers.ErrorResponseJson("ValidationException", "ShardCount must be at least 1", 400);
        }

        lock (_lock)
        {
            if (_streams.ContainsKey(name))
            {
                return AwsResponseHelpers.ErrorResponseJson("ResourceInUseException", $"Stream {name} already exists", 400);
            }

            var arn = $"arn:aws:kinesis:{Region}:{AccountContext.GetAccountId()}:stream/{name}";
            var modeStr = "PROVISIONED";
            if (data.TryGetProperty("StreamModeDetails", out var modeEl))
            {
                modeStr = GetString(modeEl, "StreamMode") ?? "PROVISIONED";
            }

            _streams[name] = new KinStream
            {
                StreamName = name,
                StreamArn = arn,
                StreamStatus = "ACTIVE",
                StreamMode = modeStr,
                RetentionPeriodHours = 24,
                Shards = BuildShards(shardCount),
                CreationTimestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() / 1000.0,
                EncryptionType = "NONE",
            };

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>());
        }
    }

    private ServiceResponse ActDeleteStream(JsonElement data)
    {
        lock (_lock)
        {
            var (name, stream) = ResolveStream(data);
            if (stream is null)
            {
                return AwsResponseHelpers.ErrorResponseJson("ResourceNotFoundException", $"Stream {name} not found", 400);
            }

            // Remove iterators and consumers for this stream
            var iterToRemove = new List<string>();
            foreach (var (tok, st) in _shardIterators.Items)
            {
                if (st.StreamName == name)
                {
                    iterToRemove.Add(tok);
                }
            }

            foreach (var tok in iterToRemove)
            {
                _shardIterators.TryRemove(tok, out _);
            }

            var consToRemove = new List<string>();
            foreach (var (carn, c) in _consumers.Items)
            {
                if (c.StreamArn == stream.StreamArn)
                {
                    consToRemove.Add(carn);
                }
            }

            foreach (var carn in consToRemove)
            {
                _consumers.TryRemove(carn, out _);
            }

            _streams.TryRemove(name!, out _);
            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>());
        }
    }

    // -- Describe / List -------------------------------------------------------

    private ServiceResponse ActDescribeStream(JsonElement data)
    {
        lock (_lock)
        {
            var (name, stream) = ResolveStream(data);
            if (stream is null)
            {
                return AwsResponseHelpers.ErrorResponseJson("ResourceNotFoundException", $"Stream {name} not found", 400);
            }

            ExpireRecords(stream);

            var limit = GetInt(data, "Limit", 100);
            var exclusiveStart = GetString(data, "ExclusiveStartShardId");
            var shardIds = stream.Shards.Keys.OrderBy(x => x, StringComparer.Ordinal).ToList();

            if (exclusiveStart is not null)
            {
                shardIds = shardIds.Where(s => string.Compare(s, exclusiveStart, StringComparison.Ordinal) > 0).ToList();
            }

            var hasMore = shardIds.Count > limit;
            var page = shardIds.Take(limit).ToList();

            var desc = BuildStreamDescription(stream, page);
            desc["HasMoreShards"] = hasMore;

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>
            {
                ["StreamDescription"] = desc,
            });
        }
    }

    private ServiceResponse ActDescribeStreamSummary(JsonElement data)
    {
        lock (_lock)
        {
            var (name, stream) = ResolveStream(data);
            if (stream is null)
            {
                return AwsResponseHelpers.ErrorResponseJson("ResourceNotFoundException", $"Stream {name} not found", 400);
            }

            var consumerCount = 0;
            foreach (var (_, c) in _consumers.Items)
            {
                if (c.StreamArn == stream.StreamArn)
                {
                    consumerCount++;
                }
            }

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>
            {
                ["StreamDescriptionSummary"] = new Dictionary<string, object?>
                {
                    ["StreamName"] = stream.StreamName,
                    ["StreamARN"] = stream.StreamArn,
                    ["StreamStatus"] = stream.StreamStatus,
                    ["StreamModeDetails"] = new Dictionary<string, object?> { ["StreamMode"] = stream.StreamMode },
                    ["RetentionPeriodHours"] = stream.RetentionPeriodHours,
                    ["StreamCreationTimestamp"] = stream.CreationTimestamp,
                    ["EnhancedMonitoring"] = new List<object> { new Dictionary<string, object?> { ["ShardLevelMetrics"] = new List<object>() } },
                    ["EncryptionType"] = stream.EncryptionType,
                    ["OpenShardCount"] = stream.Shards.Count,
                    ["ConsumerCount"] = consumerCount,
                },
            });
        }
    }

    private ServiceResponse ActListStreams(JsonElement data)
    {
        lock (_lock)
        {
            var limit = GetInt(data, "Limit", 100);
            var exclusiveStart = GetString(data, "ExclusiveStartStreamName");
            var names = _streams.Items.Select(x => x.Key).OrderBy(x => x, StringComparer.Ordinal).ToList();

            if (exclusiveStart is not null)
            {
                names = names.Where(n => string.Compare(n, exclusiveStart, StringComparison.Ordinal) > 0).ToList();
            }

            var hasMore = names.Count > limit;
            var page = names.Take(limit).ToList();

            var summaries = new List<Dictionary<string, object?>>();
            foreach (var n in page)
            {
                if (_streams.TryGetValue(n, out var s))
                {
                    summaries.Add(new Dictionary<string, object?>
                    {
                        ["StreamName"] = n,
                        ["StreamARN"] = s.StreamArn,
                        ["StreamStatus"] = s.StreamStatus,
                        ["StreamModeDetails"] = new Dictionary<string, object?> { ["StreamMode"] = s.StreamMode },
                        ["StreamCreationTimestamp"] = s.CreationTimestamp,
                    });
                }
            }

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>
            {
                ["StreamNames"] = page,
                ["StreamSummaries"] = summaries,
                ["HasMoreStreams"] = hasMore,
            });
        }
    }

    private ServiceResponse ActListShards(JsonElement data)
    {
        lock (_lock)
        {
            var (name, stream) = ResolveStream(data);
            if (stream is null)
            {
                return AwsResponseHelpers.ErrorResponseJson("ResourceNotFoundException", $"Stream {name} not found", 400);
            }

            var maxResults = GetInt(data, "MaxResults", 10000);
            var nextToken = GetString(data, "NextToken");
            var exclusiveStart = GetString(data, "ExclusiveStartShardId");

            var shardIds = stream.Shards.Keys.OrderBy(x => x, StringComparer.Ordinal).ToList();

            if (exclusiveStart is not null)
            {
                shardIds = shardIds.Where(s => string.Compare(s, exclusiveStart, StringComparison.Ordinal) > 0).ToList();
            }

            if (nextToken is not null)
            {
                shardIds = shardIds.Where(s => string.Compare(s, nextToken, StringComparison.Ordinal) > 0).ToList();
            }

            var page = shardIds.Take(maxResults).ToList();
            var result = new Dictionary<string, object?>
            {
                ["Shards"] = page.Select(sid => BuildShardOutput(sid, stream.Shards[sid])).ToList(),
            };

            if (shardIds.Count > maxResults)
            {
                result["NextToken"] = page[^1];
            }

            return AwsResponseHelpers.JsonResponse(result);
        }
    }

    // -- PutRecord / PutRecords ------------------------------------------------

    private ServiceResponse ActPutRecord(JsonElement data)
    {
        lock (_lock)
        {
            var (name, stream) = ResolveStream(data);
            if (stream is null)
            {
                return AwsResponseHelpers.ErrorResponseJson("ResourceNotFoundException", $"Stream {name} not found", 400);
            }

            if (stream.StreamStatus != "ACTIVE")
            {
                return AwsResponseHelpers.ErrorResponseJson("ResourceInUseException", $"Stream {name} is {stream.StreamStatus}", 400);
            }

            ExpireRecords(stream);

            var partitionKey = GetString(data, "PartitionKey") ?? "";
            var recordData = GetString(data, "Data") ?? "";
            var explicitHash = GetString(data, "ExplicitHashKey");

            if (string.IsNullOrEmpty(partitionKey))
            {
                return AwsResponseHelpers.ErrorResponseJson("ValidationException", "PartitionKey is required", 400);
            }

            if (partitionKey.Length > 256)
            {
                return AwsResponseHelpers.ErrorResponseJson("ValidationException",
                    "1 validation error detected: Value at 'partitionKey' failed to satisfy constraint: Member must have length less than or equal to 256", 400);
            }

            if (!string.IsNullOrEmpty(recordData))
            {
                byte[] raw;
                try
                {
                    raw = Convert.FromBase64String(recordData);
                }
                catch
                {
                    raw = Encoding.UTF8.GetBytes(recordData);
                }

                if (raw.Length > 1_048_576)
                {
                    return AwsResponseHelpers.ErrorResponseJson("ValidationException",
                        "1 validation error detected: Value at 'data' failed to satisfy constraint: Member must have length less than or equal to 1048576", 400);
                }
            }

            var hashInt = explicitHash is not null
                ? System.Numerics.BigInteger.Parse(explicitHash)
                : PartitionKeyToHash(partitionKey);
            var shardId = RouteToShard(hashInt, stream);
            var seq = NextSequenceNumber();

            stream.Shards[shardId].Records.Add(new KinRecord
            {
                SequenceNumber = seq,
                ApproximateArrivalTimestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() / 1000.0,
                Data = recordData,
                PartitionKey = partitionKey,
            });

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>
            {
                ["ShardId"] = shardId,
                ["SequenceNumber"] = seq,
                ["EncryptionType"] = stream.EncryptionType,
            });
        }
    }

    private ServiceResponse ActPutRecords(JsonElement data)
    {
        lock (_lock)
        {
            var (name, stream) = ResolveStream(data);
            if (stream is null)
            {
                return AwsResponseHelpers.ErrorResponseJson("ResourceNotFoundException", $"Stream {name} not found", 400);
            }

            if (stream.StreamStatus != "ACTIVE")
            {
                return AwsResponseHelpers.ErrorResponseJson("ResourceInUseException", $"Stream {name} is {stream.StreamStatus}", 400);
            }

            ExpireRecords(stream);

            if (!data.TryGetProperty("Records", out var recordsEl) || recordsEl.ValueKind != JsonValueKind.Array)
            {
                return AwsResponseHelpers.ErrorResponseJson("ValidationException", "Records is required", 400);
            }

            var recordCount = recordsEl.GetArrayLength();
            if (recordCount > 500)
            {
                return AwsResponseHelpers.ErrorResponseJson("ValidationException",
                    "1 validation error detected: Value at 'records' failed to satisfy constraint: Member must have length less than or equal to 500", 400);
            }

            // Validate records first
            var totalSize = 0;
            foreach (var rec in recordsEl.EnumerateArray())
            {
                var pk = GetString(rec, "PartitionKey") ?? "";
                var rd = GetString(rec, "Data") ?? "";

                if (pk.Length > 256)
                {
                    return AwsResponseHelpers.ErrorResponseJson("ValidationException",
                        "1 validation error detected: Value at 'partitionKey' failed to satisfy constraint: Member must have length less than or equal to 256", 400);
                }

                byte[] raw;
                try
                {
                    raw = !string.IsNullOrEmpty(rd) ? Convert.FromBase64String(rd) : [];
                }
                catch
                {
                    raw = Encoding.UTF8.GetBytes(rd);
                }

                if (raw.Length > 1_048_576)
                {
                    return AwsResponseHelpers.ErrorResponseJson("ValidationException",
                        "1 validation error detected: Value at 'data' failed to satisfy constraint: Member must have length less than or equal to 1048576", 400);
                }

                totalSize += raw.Length + Encoding.UTF8.GetByteCount(pk);
            }

            if (totalSize > 5_242_880)
            {
                return AwsResponseHelpers.ErrorResponseJson("ValidationException",
                    "Records total payload size exceeds 5 MB limit", 400);
            }

            var results = new List<Dictionary<string, object?>>();
            foreach (var rec in recordsEl.EnumerateArray())
            {
                var pk = GetString(rec, "PartitionKey") ?? "";
                var rd = GetString(rec, "Data") ?? "";
                var eh = GetString(rec, "ExplicitHashKey");
                var hashInt = eh is not null
                    ? System.Numerics.BigInteger.Parse(eh)
                    : PartitionKeyToHash(pk);
                var sid = RouteToShard(hashInt, stream);
                var seq = NextSequenceNumber();

                stream.Shards[sid].Records.Add(new KinRecord
                {
                    SequenceNumber = seq,
                    ApproximateArrivalTimestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() / 1000.0,
                    Data = rd,
                    PartitionKey = pk,
                });

                results.Add(new Dictionary<string, object?>
                {
                    ["SequenceNumber"] = seq,
                    ["ShardId"] = sid,
                    ["EncryptionType"] = stream.EncryptionType,
                });
            }

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>
            {
                ["FailedRecordCount"] = 0,
                ["Records"] = results,
                ["EncryptionType"] = stream.EncryptionType,
            });
        }
    }

    // -- Shard iterators / GetRecords ------------------------------------------

    private ServiceResponse ActGetShardIterator(JsonElement data)
    {
        lock (_lock)
        {
            var (name, stream) = ResolveStream(data);
            if (stream is null)
            {
                return AwsResponseHelpers.ErrorResponseJson("ResourceNotFoundException", $"Stream {name} not found", 400);
            }

            var shardId = GetString(data, "ShardId") ?? "";
            if (!stream.Shards.ContainsKey(shardId))
            {
                return AwsResponseHelpers.ErrorResponseJson("ResourceNotFoundException", $"Shard {shardId} not found", 400);
            }

            ExpireRecords(stream);
            var shard = stream.Shards[shardId];
            var records = shard.Records;
            var itType = GetString(data, "ShardIteratorType") ?? "LATEST";
            var seq = GetString(data, "StartingSequenceNumber") ?? "";
            var atTs = GetNullableDouble(data, "Timestamp");

            int position;
            switch (itType)
            {
                case "TRIM_HORIZON":
                    position = 0;
                    break;
                case "LATEST":
                    position = records.Count;
                    break;
                case "AT_SEQUENCE_NUMBER":
                    position = records.FindIndex(r =>
                        string.Compare(r.SequenceNumber, seq, StringComparison.Ordinal) >= 0);
                    if (position < 0)
                    {
                        position = records.Count;
                    }

                    break;
                case "AFTER_SEQUENCE_NUMBER":
                    position = records.FindIndex(r =>
                        string.Compare(r.SequenceNumber, seq, StringComparison.Ordinal) > 0);
                    if (position < 0)
                    {
                        position = records.Count;
                    }

                    break;
                case "AT_TIMESTAMP":
                    if (atTs is null)
                    {
                        return AwsResponseHelpers.ErrorResponseJson("ValidationException", "Timestamp required for AT_TIMESTAMP", 400);
                    }

                    position = records.FindIndex(r => r.ApproximateArrivalTimestamp >= atTs.Value);
                    if (position < 0)
                    {
                        position = records.Count;
                    }

                    break;
                default:
                    return AwsResponseHelpers.ErrorResponseJson("ValidationException", $"Invalid ShardIteratorType: {itType}", 400);
            }

            var resolvedName = name ?? _streams.Items.FirstOrDefault(x => x.Value == stream).Key ?? "";
            var token = HashHelpers.NewUuid();
            _shardIterators[token] = new ShardIteratorState
            {
                StreamName = resolvedName,
                ShardId = shardId,
                Position = position,
                CreatedAt = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() / 1000.0,
            };

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>
            {
                ["ShardIterator"] = token,
            });
        }
    }

    private ServiceResponse ActGetRecords(JsonElement data)
    {
        lock (_lock)
        {
            var iterator = GetString(data, "ShardIterator") ?? "";
            var limit = Math.Min(GetInt(data, "Limit", 10000), 10000);

            if (!_shardIterators.TryRemove(iterator, out var state))
            {
                return AwsResponseHelpers.ErrorResponseJson("ExpiredIteratorException", "Iterator has expired or is invalid", 400);
            }

            var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() / 1000.0;
            if (now - state.CreatedAt > IteratorExpirySeconds)
            {
                return AwsResponseHelpers.ErrorResponseJson("ExpiredIteratorException", "Iterator has expired", 400);
            }

            if (!_streams.TryGetValue(state.StreamName, out var stream))
            {
                return AwsResponseHelpers.ErrorResponseJson("ResourceNotFoundException", "Stream not found", 400);
            }

            ExpireRecords(stream);
            if (!stream.Shards.TryGetValue(state.ShardId, out var shard))
            {
                return AwsResponseHelpers.ErrorResponseJson("ResourceNotFoundException", "Shard not found", 400);
            }

            var pos = Math.Min(state.Position, shard.Records.Count);
            var raw = shard.Records.GetRange(pos, Math.Min(limit, shard.Records.Count - pos));
            var newPos = pos + raw.Count;

            var outRecords = raw.Select(r => new Dictionary<string, object?>
            {
                ["SequenceNumber"] = r.SequenceNumber,
                ["ApproximateArrivalTimestamp"] = r.ApproximateArrivalTimestamp,
                ["Data"] = EnsureBase64(r.Data),
                ["PartitionKey"] = r.PartitionKey,
                ["EncryptionType"] = stream.EncryptionType,
            }).ToList();

            var millisBehind = 0L;
            if (shard.Records.Count > 0 && newPos < shard.Records.Count)
            {
                millisBehind = Math.Max(0, (long)((now - shard.Records[newPos].ApproximateArrivalTimestamp) * 1000));
            }

            var nextToken = HashHelpers.NewUuid();
            _shardIterators[nextToken] = new ShardIteratorState
            {
                StreamName = state.StreamName,
                ShardId = state.ShardId,
                Position = newPos,
                CreatedAt = now,
            };

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>
            {
                ["Records"] = outRecords,
                ["NextShardIterator"] = nextToken,
                ["MillisBehindLatest"] = millisBehind,
            });
        }
    }

    // -- Retention period ------------------------------------------------------

    private ServiceResponse ActIncreaseRetention(JsonElement data)
    {
        lock (_lock)
        {
            var (name, stream) = ResolveStream(data);
            if (stream is null)
            {
                return AwsResponseHelpers.ErrorResponseJson("ResourceNotFoundException", $"Stream {name} not found", 400);
            }

            if (!data.TryGetProperty("RetentionPeriodHours", out var hoursEl) || !hoursEl.TryGetInt32(out var hours))
            {
                return AwsResponseHelpers.ErrorResponseJson("ValidationException", "RetentionPeriodHours is required", 400);
            }

            if (hours <= stream.RetentionPeriodHours)
            {
                return AwsResponseHelpers.ErrorResponseJson("ValidationException",
                    "RetentionPeriodHours must be greater than current value", 400);
            }

            if (hours > 8760)
            {
                return AwsResponseHelpers.ErrorResponseJson("ValidationException",
                    "RetentionPeriodHours cannot exceed 8760", 400);
            }

            stream.RetentionPeriodHours = hours;
            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>());
        }
    }

    private ServiceResponse ActDecreaseRetention(JsonElement data)
    {
        lock (_lock)
        {
            var (name, stream) = ResolveStream(data);
            if (stream is null)
            {
                return AwsResponseHelpers.ErrorResponseJson("ResourceNotFoundException", $"Stream {name} not found", 400);
            }

            if (!data.TryGetProperty("RetentionPeriodHours", out var hoursEl) || !hoursEl.TryGetInt32(out var hours))
            {
                return AwsResponseHelpers.ErrorResponseJson("ValidationException", "RetentionPeriodHours is required", 400);
            }

            if (hours >= stream.RetentionPeriodHours)
            {
                return AwsResponseHelpers.ErrorResponseJson("ValidationException",
                    "RetentionPeriodHours must be less than current value", 400);
            }

            if (hours < 24)
            {
                return AwsResponseHelpers.ErrorResponseJson("ValidationException",
                    "RetentionPeriodHours cannot be less than 24", 400);
            }

            stream.RetentionPeriodHours = hours;
            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>());
        }
    }

    // -- Tags ------------------------------------------------------------------

    private ServiceResponse ActAddTags(JsonElement data)
    {
        lock (_lock)
        {
            var (name, stream) = ResolveStream(data);
            if (stream is null)
            {
                return AwsResponseHelpers.ErrorResponseJson("ResourceNotFoundException", $"Stream {name} not found", 400);
            }

            if (data.TryGetProperty("Tags", out var tagsEl) && tagsEl.ValueKind == JsonValueKind.Object)
            {
                foreach (var prop in tagsEl.EnumerateObject())
                {
                    stream.Tags[prop.Name] = prop.Value.GetString() ?? "";
                }
            }

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>());
        }
    }

    private ServiceResponse ActRemoveTags(JsonElement data)
    {
        lock (_lock)
        {
            var (name, stream) = ResolveStream(data);
            if (stream is null)
            {
                return AwsResponseHelpers.ErrorResponseJson("ResourceNotFoundException", $"Stream {name} not found", 400);
            }

            if (data.TryGetProperty("TagKeys", out var keysEl) && keysEl.ValueKind == JsonValueKind.Array)
            {
                foreach (var keyEl in keysEl.EnumerateArray())
                {
                    var key = keyEl.GetString();
                    if (key is not null)
                    {
                        stream.Tags.Remove(key);
                    }
                }
            }

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>());
        }
    }

    private ServiceResponse ActListTags(JsonElement data)
    {
        lock (_lock)
        {
            var (name, stream) = ResolveStream(data);
            if (stream is null)
            {
                return AwsResponseHelpers.ErrorResponseJson("ResourceNotFoundException", $"Stream {name} not found", 400);
            }

            var limit = GetInt(data, "Limit", 50);
            var exclusiveStart = GetString(data, "ExclusiveStartTagKey");

            var items = stream.Tags.OrderBy(kv => kv.Key, StringComparer.Ordinal).ToList();
            if (exclusiveStart is not null)
            {
                items = items.Where(kv => string.Compare(kv.Key, exclusiveStart, StringComparison.Ordinal) > 0).ToList();
            }

            var page = items.Take(limit).ToList();
            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>
            {
                ["Tags"] = page.Select(kv => new Dictionary<string, object?>
                {
                    ["Key"] = kv.Key,
                    ["Value"] = kv.Value,
                }).ToList(),
                ["HasMoreTags"] = items.Count > limit,
            });
        }
    }

    // -- MergeShards / SplitShard / UpdateShardCount ---------------------------

    private ServiceResponse ActMergeShards(JsonElement data)
    {
        lock (_lock)
        {
            var (name, stream) = ResolveStream(data);
            if (stream is null)
            {
                return AwsResponseHelpers.ErrorResponseJson("ResourceNotFoundException", $"Stream {name} not found", 400);
            }

            var s1Id = GetString(data, "ShardToMerge") ?? "";
            var s2Id = GetString(data, "AdjacentShardToMerge") ?? "";

            if (!stream.Shards.ContainsKey(s1Id))
            {
                return AwsResponseHelpers.ErrorResponseJson("ResourceNotFoundException", $"Shard {s1Id} not found", 400);
            }

            if (!stream.Shards.ContainsKey(s2Id))
            {
                return AwsResponseHelpers.ErrorResponseJson("ResourceNotFoundException", $"Shard {s2Id} not found", 400);
            }

            var s1 = stream.Shards[s1Id];
            var s2 = stream.Shards[s2Id];

            var newStart = System.Numerics.BigInteger.Min(
                System.Numerics.BigInteger.Parse(s1.StartingHashKey),
                System.Numerics.BigInteger.Parse(s2.StartingHashKey));
            var newEnd = System.Numerics.BigInteger.Max(
                System.Numerics.BigInteger.Parse(s1.EndingHashKey),
                System.Numerics.BigInteger.Parse(s2.EndingHashKey));

            var newIdx = MaxShardIndex(stream) + 1;
            var newSid = $"shardId-{newIdx:D12}";
            stream.Shards[newSid] = new KinShard
            {
                StartingHashKey = newStart.ToString(),
                EndingHashKey = newEnd.ToString(),
                StartingSequenceNumber = NextSequenceNumber(),
                ParentShardId = s1Id,
                AdjacentParentShardId = s2Id,
            };

            stream.Shards.Remove(s1Id);
            stream.Shards.Remove(s2Id);

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>());
        }
    }

    private ServiceResponse ActSplitShard(JsonElement data)
    {
        lock (_lock)
        {
            var (name, stream) = ResolveStream(data);
            if (stream is null)
            {
                return AwsResponseHelpers.ErrorResponseJson("ResourceNotFoundException", $"Stream {name} not found", 400);
            }

            var shardId = GetString(data, "ShardToSplit") ?? "";
            var newHash = GetString(data, "NewStartingHashKey") ?? "";

            if (!stream.Shards.ContainsKey(shardId))
            {
                return AwsResponseHelpers.ErrorResponseJson("ResourceNotFoundException", $"Shard {shardId} not found", 400);
            }

            if (string.IsNullOrEmpty(newHash))
            {
                return AwsResponseHelpers.ErrorResponseJson("ValidationException", "NewStartingHashKey is required", 400);
            }

            var old = stream.Shards[shardId];
            var splitPt = System.Numerics.BigInteger.Parse(newHash);
            var oldStart = System.Numerics.BigInteger.Parse(old.StartingHashKey);
            var oldEnd = System.Numerics.BigInteger.Parse(old.EndingHashKey);

            if (splitPt <= oldStart || splitPt > oldEnd)
            {
                return AwsResponseHelpers.ErrorResponseJson("ValidationException",
                    "NewStartingHashKey must be within the shard range", 400);
            }

            var baseIdx = MaxShardIndex(stream) + 1;
            var c1 = $"shardId-{baseIdx:D12}";
            var c2 = $"shardId-{baseIdx + 1:D12}";

            stream.Shards[c1] = new KinShard
            {
                StartingHashKey = oldStart.ToString(),
                EndingHashKey = (splitPt - 1).ToString(),
                StartingSequenceNumber = NextSequenceNumber(),
                ParentShardId = shardId,
            };

            stream.Shards[c2] = new KinShard
            {
                StartingHashKey = splitPt.ToString(),
                EndingHashKey = oldEnd.ToString(),
                StartingSequenceNumber = NextSequenceNumber(),
                ParentShardId = shardId,
            };

            stream.Shards.Remove(shardId);
            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>());
        }
    }

    private ServiceResponse ActUpdateShardCount(JsonElement data)
    {
        lock (_lock)
        {
            var (name, stream) = ResolveStream(data);
            if (stream is null)
            {
                return AwsResponseHelpers.ErrorResponseJson("ResourceNotFoundException", $"Stream {name} not found", 400);
            }

            if (!data.TryGetProperty("TargetShardCount", out var targetEl) || !targetEl.TryGetInt32(out var target))
            {
                return AwsResponseHelpers.ErrorResponseJson("ValidationException", "TargetShardCount is required", 400);
            }

            if (target < 1)
            {
                return AwsResponseHelpers.ErrorResponseJson("ValidationException", "TargetShardCount must be >= 1", 400);
            }

            var current = stream.Shards.Count;
            stream.Shards = BuildShards(target);

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>
            {
                ["StreamName"] = stream.StreamName,
                ["CurrentShardCount"] = current,
                ["TargetShardCount"] = target,
                ["StreamARN"] = stream.StreamArn,
            });
        }
    }

    // -- Consumers -------------------------------------------------------------

    private ServiceResponse ActRegisterConsumer(JsonElement data)
    {
        var streamArn = GetString(data, "StreamARN") ?? "";
        var consumerName = GetString(data, "ConsumerName") ?? "";

        if (string.IsNullOrEmpty(streamArn) || string.IsNullOrEmpty(consumerName))
        {
            return AwsResponseHelpers.ErrorResponseJson("ValidationException",
                "StreamARN and ConsumerName are required", 400);
        }

        lock (_lock)
        {
            var streamFound = false;
            foreach (var (_, s) in _streams.Items)
            {
                if (s.StreamArn == streamArn)
                {
                    streamFound = true;
                    break;
                }
            }

            if (!streamFound)
            {
                return AwsResponseHelpers.ErrorResponseJson("ResourceNotFoundException",
                    $"Stream with ARN {streamArn} not found", 400);
            }

            foreach (var (_, c) in _consumers.Items)
            {
                if (c.StreamArn == streamArn && c.ConsumerName == consumerName)
                {
                    return AwsResponseHelpers.ErrorResponseJson("ResourceInUseException",
                        $"Consumer {consumerName} already exists", 400);
                }
            }

            var ts = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
            var consumerArn = $"{streamArn}/consumer/{consumerName}:{ts}";
            var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() / 1000.0;

            _consumers[consumerArn] = new KinConsumer
            {
                ConsumerName = consumerName,
                ConsumerArn = consumerArn,
                ConsumerStatus = "ACTIVE",
                ConsumerCreationTimestamp = now,
                StreamArn = streamArn,
            };

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>
            {
                ["Consumer"] = new Dictionary<string, object?>
                {
                    ["ConsumerName"] = consumerName,
                    ["ConsumerARN"] = consumerArn,
                    ["ConsumerStatus"] = "ACTIVE",
                    ["ConsumerCreationTimestamp"] = now,
                },
            });
        }
    }

    private ServiceResponse ActDeregisterConsumer(JsonElement data)
    {
        var consumerArn = GetString(data, "ConsumerARN");
        var streamArn = GetString(data, "StreamARN");
        var consumerName = GetString(data, "ConsumerName");

        lock (_lock)
        {
            if (consumerArn is not null)
            {
                if (!_consumers.TryRemove(consumerArn, out _))
                {
                    return AwsResponseHelpers.ErrorResponseJson("ResourceNotFoundException", "Consumer not found", 400);
                }
            }
            else if (streamArn is not null && consumerName is not null)
            {
                string? found = null;
                foreach (var (a, c) in _consumers.Items)
                {
                    if (c.StreamArn == streamArn && c.ConsumerName == consumerName)
                    {
                        found = a;
                        break;
                    }
                }

                if (found is null)
                {
                    return AwsResponseHelpers.ErrorResponseJson("ResourceNotFoundException", "Consumer not found", 400);
                }

                _consumers.TryRemove(found, out _);
            }
            else
            {
                return AwsResponseHelpers.ErrorResponseJson("ValidationException",
                    "ConsumerARN or StreamARN+ConsumerName required", 400);
            }

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>());
        }
    }

    private ServiceResponse ActListConsumers(JsonElement data)
    {
        var streamArn = GetString(data, "StreamARN") ?? "";
        if (string.IsNullOrEmpty(streamArn))
        {
            return AwsResponseHelpers.ErrorResponseJson("ValidationException", "StreamARN is required", 400);
        }

        var maxResults = GetInt(data, "MaxResults", 100);
        var nextToken = GetString(data, "NextToken");

        lock (_lock)
        {
            var items = new List<Dictionary<string, object?>>();
            foreach (var (_, c) in _consumers.Items)
            {
                if (c.StreamArn == streamArn)
                {
                    items.Add(new Dictionary<string, object?>
                    {
                        ["ConsumerName"] = c.ConsumerName,
                        ["ConsumerARN"] = c.ConsumerArn,
                        ["ConsumerStatus"] = c.ConsumerStatus,
                        ["ConsumerCreationTimestamp"] = c.ConsumerCreationTimestamp,
                    });
                }
            }

            var start = 0;
            if (nextToken is not null && int.TryParse(nextToken, out var parsedStart))
            {
                start = parsedStart;
            }

            var page = items.GetRange(start, Math.Min(maxResults, items.Count - start));
            var result = new Dictionary<string, object?> { ["Consumers"] = page };
            if (start + maxResults < items.Count)
            {
                result["NextToken"] = (start + maxResults).ToString();
            }

            return AwsResponseHelpers.JsonResponse(result);
        }
    }

    private ServiceResponse ActDescribeStreamConsumer(JsonElement data)
    {
        var consumerArn = GetString(data, "ConsumerARN");
        var streamArn = GetString(data, "StreamARN");
        var consumerName = GetString(data, "ConsumerName");

        lock (_lock)
        {
            KinConsumer? consumer = null;
            if (consumerArn is not null)
            {
                _consumers.TryGetValue(consumerArn, out consumer);
            }
            else if (streamArn is not null && consumerName is not null)
            {
                foreach (var (_, c) in _consumers.Items)
                {
                    if (c.StreamArn == streamArn && c.ConsumerName == consumerName)
                    {
                        consumer = c;
                        break;
                    }
                }
            }

            if (consumer is null)
            {
                return AwsResponseHelpers.ErrorResponseJson("ResourceNotFoundException", "Consumer not found", 400);
            }

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>
            {
                ["ConsumerDescription"] = new Dictionary<string, object?>
                {
                    ["ConsumerName"] = consumer.ConsumerName,
                    ["ConsumerARN"] = consumer.ConsumerArn,
                    ["ConsumerStatus"] = consumer.ConsumerStatus,
                    ["ConsumerCreationTimestamp"] = consumer.ConsumerCreationTimestamp,
                    ["StreamARN"] = consumer.StreamArn,
                },
            });
        }
    }

    // -- Encryption ------------------------------------------------------------

    private ServiceResponse ActStartStreamEncryption(JsonElement data)
    {
        lock (_lock)
        {
            var (name, stream) = ResolveStream(data);
            if (stream is null)
            {
                return AwsResponseHelpers.ErrorResponseJson("ResourceNotFoundException", $"Stream {name} not found", 400);
            }

            stream.EncryptionType = GetString(data, "EncryptionType") ?? "KMS";
            stream.KeyId = GetString(data, "KeyId") ?? "";
            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>());
        }
    }

    private ServiceResponse ActStopStreamEncryption(JsonElement data)
    {
        lock (_lock)
        {
            var (name, stream) = ResolveStream(data);
            if (stream is null)
            {
                return AwsResponseHelpers.ErrorResponseJson("ResourceNotFoundException", $"Stream {name} not found", 400);
            }

            stream.EncryptionType = "NONE";
            stream.KeyId = null;
            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>());
        }
    }

    // -- Enhanced monitoring ---------------------------------------------------

    private ServiceResponse ActEnableEnhancedMonitoring(JsonElement data)
    {
        lock (_lock)
        {
            var (name, stream) = ResolveStream(data);
            if (stream is null)
            {
                return AwsResponseHelpers.ErrorResponseJson("ResourceNotFoundException", $"Stream {name} not found", 400);
            }

            var desired = new List<string>();
            if (data.TryGetProperty("ShardLevelMetrics", out var metricsEl) && metricsEl.ValueKind == JsonValueKind.Array)
            {
                foreach (var m in metricsEl.EnumerateArray())
                {
                    var s = m.GetString();
                    if (s is not null)
                    {
                        desired.Add(s);
                    }
                }
            }

            var current = new List<string>(stream.ShardLevelMetrics);
            var merged = new HashSet<string>(current, StringComparer.Ordinal);
            foreach (var d in desired)
            {
                merged.Add(d);
            }

            stream.ShardLevelMetrics = [.. merged];

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>
            {
                ["StreamName"] = stream.StreamName,
                ["StreamARN"] = stream.StreamArn,
                ["CurrentShardLevelMetrics"] = current,
                ["DesiredShardLevelMetrics"] = stream.ShardLevelMetrics,
            });
        }
    }

    private ServiceResponse ActDisableEnhancedMonitoring(JsonElement data)
    {
        lock (_lock)
        {
            var (name, stream) = ResolveStream(data);
            if (stream is null)
            {
                return AwsResponseHelpers.ErrorResponseJson("ResourceNotFoundException", $"Stream {name} not found", 400);
            }

            var toDisable = new HashSet<string>(StringComparer.Ordinal);
            if (data.TryGetProperty("ShardLevelMetrics", out var metricsEl) && metricsEl.ValueKind == JsonValueKind.Array)
            {
                foreach (var m in metricsEl.EnumerateArray())
                {
                    var s = m.GetString();
                    if (s is not null)
                    {
                        toDisable.Add(s);
                    }
                }
            }

            var current = new List<string>(stream.ShardLevelMetrics);
            stream.ShardLevelMetrics = stream.ShardLevelMetrics.Where(m => !toDisable.Contains(m)).ToList();

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>
            {
                ["StreamName"] = stream.StreamName,
                ["StreamARN"] = stream.StreamArn,
                ["CurrentShardLevelMetrics"] = current,
                ["DesiredShardLevelMetrics"] = stream.ShardLevelMetrics,
            });
        }
    }

    // -- Helpers ---------------------------------------------------------------

    private static Dictionary<string, object?> BuildShardOutput(string shardId, KinShard shard)
    {
        var result = new Dictionary<string, object?>
        {
            ["ShardId"] = shardId,
            ["HashKeyRange"] = new Dictionary<string, object?>
            {
                ["StartingHashKey"] = shard.StartingHashKey,
                ["EndingHashKey"] = shard.EndingHashKey,
            },
            ["SequenceNumberRange"] = new Dictionary<string, object?>
            {
                ["StartingSequenceNumber"] = shard.StartingSequenceNumber,
            },
        };

        if (shard.ParentShardId is not null)
        {
            result["ParentShardId"] = shard.ParentShardId;
        }

        if (shard.AdjacentParentShardId is not null)
        {
            result["AdjacentParentShardId"] = shard.AdjacentParentShardId;
        }

        return result;
    }

    private static Dictionary<string, object?> BuildStreamDescription(KinStream stream, List<string>? shardIds)
    {
        shardIds ??= stream.Shards.Keys.OrderBy(x => x, StringComparer.Ordinal).ToList();
        return new Dictionary<string, object?>
        {
            ["StreamName"] = stream.StreamName,
            ["StreamARN"] = stream.StreamArn,
            ["StreamStatus"] = stream.StreamStatus,
            ["StreamModeDetails"] = new Dictionary<string, object?> { ["StreamMode"] = stream.StreamMode },
            ["RetentionPeriodHours"] = stream.RetentionPeriodHours,
            ["StreamCreationTimestamp"] = stream.CreationTimestamp,
            ["Shards"] = shardIds.Select(sid => BuildShardOutput(sid, stream.Shards[sid])).ToList(),
            ["HasMoreShards"] = false,
            ["EnhancedMonitoring"] = new List<object> { new Dictionary<string, object?> { ["ShardLevelMetrics"] = new List<object>() } },
            ["EncryptionType"] = stream.EncryptionType,
        };
    }
}

// -- Data models ---------------------------------------------------------------

internal sealed class KinStream
{
    public required string StreamName { get; set; }
    public required string StreamArn { get; set; }
    public required string StreamStatus { get; set; }
    public required string StreamMode { get; set; }
    public int RetentionPeriodHours { get; set; } = 24;
    public required Dictionary<string, KinShard> Shards { get; set; }
    public Dictionary<string, string> Tags { get; set; } = new(StringComparer.Ordinal);
    public double CreationTimestamp { get; set; }
    public string EncryptionType { get; set; } = "NONE";
    public string? KeyId { get; set; }
    public List<string> ShardLevelMetrics { get; set; } = [];
}

internal sealed class KinShard
{
    public required string StartingHashKey { get; set; }
    public required string EndingHashKey { get; set; }
    public required string StartingSequenceNumber { get; set; }
    public string? ParentShardId { get; set; }
    public string? AdjacentParentShardId { get; set; }
    public List<KinRecord> Records { get; set; } = [];
}

internal sealed class KinRecord
{
    public required string SequenceNumber { get; set; }
    public double ApproximateArrivalTimestamp { get; set; }
    public required string Data { get; set; }
    public required string PartitionKey { get; set; }
}

internal sealed class ShardIteratorState
{
    public required string StreamName { get; set; }
    public required string ShardId { get; set; }
    public int Position { get; set; }
    public double CreatedAt { get; set; }
}

internal sealed class KinConsumer
{
    public required string ConsumerName { get; set; }
    public required string ConsumerArn { get; set; }
    public required string ConsumerStatus { get; set; }
    public double ConsumerCreationTimestamp { get; set; }
    public required string StreamArn { get; set; }
}
