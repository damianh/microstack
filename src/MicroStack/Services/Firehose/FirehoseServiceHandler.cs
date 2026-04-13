using System.Text.Json;
using MicroStack.Internal;

namespace MicroStack.Services.Firehose;

/// <summary>
/// Amazon Data Firehose (formerly Kinesis Data Firehose) service handler.
/// JSON-based API via X-Amz-Target (Firehose_20150804).
///
/// Port of ministack/services/firehose.py.
///
/// Supports: CreateDeliveryStream, DeleteDeliveryStream, DescribeDeliveryStream,
///           ListDeliveryStreams, PutRecord, PutRecordBatch, UpdateDestination,
///           TagDeliveryStream, UntagDeliveryStream, ListTagsForDeliveryStream,
///           StartDeliveryStreamEncryption, StopDeliveryStreamEncryption.
/// </summary>
internal sealed class FirehoseServiceHandler : IServiceHandler
{
    private readonly Dictionary<string, FhStream> _streams = new(StringComparer.Ordinal);
    private readonly Lock _lock = new();
    private int _destCounter;

    private static string Region => MicroStackOptions.Instance.Region;

    // -- IServiceHandler -------------------------------------------------------

    public string ServiceName => "firehose";

    public Task<ServiceResponse> HandleAsync(ServiceRequest request)
    {
        var target = request.GetHeader("x-amz-target") ?? "";
        var action = target.Contains('.', StringComparison.Ordinal)
            ? target[(target.LastIndexOf('.') + 1)..]
            : "";

        if (string.IsNullOrEmpty(action))
        {
            return Task.FromResult(
                AwsResponseHelpers.ErrorResponseJson("InvalidArgumentException", "Missing X-Amz-Target header.", 400));
        }

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
                    AwsResponseHelpers.ErrorResponseJson("InvalidArgumentException", "Request body is not valid JSON.", 400));
            }
        }
        else
        {
            data = JsonDocument.Parse("{}").RootElement.Clone();
        }

        var response = action switch
        {
            "CreateDeliveryStream" => ActCreateDeliveryStream(data),
            "DeleteDeliveryStream" => ActDeleteDeliveryStream(data),
            "DescribeDeliveryStream" => ActDescribeDeliveryStream(data),
            "ListDeliveryStreams" => ActListDeliveryStreams(data),
            "PutRecord" => ActPutRecord(data),
            "PutRecordBatch" => ActPutRecordBatch(data),
            "UpdateDestination" => ActUpdateDestination(data),
            "TagDeliveryStream" => ActTagDeliveryStream(data),
            "UntagDeliveryStream" => ActUntagDeliveryStream(data),
            "ListTagsForDeliveryStream" => ActListTagsForDeliveryStream(data),
            "StartDeliveryStreamEncryption" => ActStartDeliveryStreamEncryption(data),
            "StopDeliveryStreamEncryption" => ActStopDeliveryStreamEncryption(data),
            _ => AwsResponseHelpers.ErrorResponseJson("InvalidArgumentException", $"Unknown operation: {action}", 400),
        };

        return Task.FromResult(response);
    }

    public void Reset()
    {
        lock (_lock)
        {
            _streams.Clear();
            _destCounter = 0;
        }
    }

    public JsonElement? GetState() => null;

    public void RestoreState(JsonElement state) { }

    // -- Helpers ---------------------------------------------------------------

    private static string StreamArn(string name)
        => $"arn:aws:firehose:{Region}:{AccountContext.GetAccountId()}:deliverystream/{name}";

    private string NextDestId()
    {
        _destCounter++;
        return $"destinationId-{_destCounter:D12}";
    }

    private static ServiceResponse NotFound(string name)
        => AwsResponseHelpers.ErrorResponseJson(
            "ResourceNotFoundException",
            $"Firehose {name} under account {AccountContext.GetAccountId()} not found.",
            400);

    private static ServiceResponse InUse(string name)
        => AwsResponseHelpers.ErrorResponseJson(
            "ResourceInUseException",
            $"Firehose {name} is not in the ACTIVE state.",
            400);

    private static ServiceResponse Invalid(string msg)
        => AwsResponseHelpers.ErrorResponseJson("InvalidArgumentException", msg, 400);

    private static string GenerateRecordId()
    {
        var ts = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        var uid = HashHelpers.NewUuid().Replace("-", "", StringComparison.Ordinal);
        return $"{ts:D20}{uid}";
    }

    // -- JSON property access helpers ------------------------------------------

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

    private static Dictionary<string, string> ParseTags(JsonElement data)
    {
        var tags = new Dictionary<string, string>(StringComparer.Ordinal);
        if (data.TryGetProperty("Tags", out var tagsEl) && tagsEl.ValueKind == JsonValueKind.Array)
        {
            foreach (var tagEl in tagsEl.EnumerateArray())
            {
                var key = GetString(tagEl, "Key");
                var value = GetString(tagEl, "Value") ?? "";
                if (key is not null)
                {
                    tags[key] = value;
                }
            }
        }

        return tags;
    }

    // -- Destination resolution ------------------------------------------------

    private static (string? Type, JsonElement? Config) ResolveDestTypeAndConfig(JsonElement data)
    {
        foreach (var (key, dtype) in DestConfigKeys())
        {
            if (data.TryGetProperty(key, out var cfg))
            {
                return (dtype, cfg.Clone());
            }
        }

        return (null, null);
    }

    private static (string? Type, JsonElement? Config) ResolveDestUpdateConfig(JsonElement data)
    {
        foreach (var (key, dtype) in DestUpdateKeys())
        {
            if (data.TryGetProperty(key, out var cfg))
            {
                return (dtype, cfg.Clone());
            }
        }

        return (null, null);
    }

    private static IEnumerable<(string Key, string Type)> DestConfigKeys()
    {
        yield return ("ExtendedS3DestinationConfiguration", "ExtendedS3");
        yield return ("S3DestinationConfiguration", "S3");
        yield return ("HttpEndpointDestinationConfiguration", "HttpEndpoint");
        yield return ("RedshiftDestinationConfiguration", "Redshift");
        yield return ("ElasticsearchDestinationConfiguration", "Elasticsearch");
        yield return ("AmazonopensearchserviceDestinationConfiguration", "AmazonOpenSearch");
        yield return ("AmazonOpenSearchServerlessDestinationConfiguration", "AmazonOpenSearchServerless");
        yield return ("SplunkDestinationConfiguration", "Splunk");
        yield return ("SnowflakeDestinationConfiguration", "Snowflake");
        yield return ("IcebergDestinationConfiguration", "Iceberg");
    }

    private static IEnumerable<(string Key, string Type)> DestUpdateKeys()
    {
        yield return ("ExtendedS3DestinationUpdate", "ExtendedS3");
        yield return ("S3DestinationUpdate", "S3");
        yield return ("HttpEndpointDestinationUpdate", "HttpEndpoint");
        yield return ("RedshiftDestinationUpdate", "Redshift");
        yield return ("ElasticsearchDestinationUpdate", "Elasticsearch");
        yield return ("AmazonopensearchserviceDestinationUpdate", "AmazonOpenSearch");
        yield return ("AmazonOpenSearchServerlessDestinationUpdate", "AmazonOpenSearchServerless");
        yield return ("SplunkDestinationUpdate", "Splunk");
        yield return ("SnowflakeDestinationUpdate", "Snowflake");
        yield return ("IcebergDestinationUpdate", "Iceberg");
    }

    // -- Destination description builder ---------------------------------------

    private static Dictionary<string, object?> BuildDestDescription(FhDestination dest)
    {
        var result = new Dictionary<string, object?>(StringComparer.Ordinal)
        {
            ["DestinationId"] = dest.Id,
        };

        if (dest.Type is "ExtendedS3" or "S3")
        {
            var descKey = dest.Type == "ExtendedS3"
                ? "ExtendedS3DestinationDescription"
                : "S3DestinationDescription";

            var cfg = dest.Config;
            var desc = new Dictionary<string, object?>(StringComparer.Ordinal)
            {
                ["BucketARN"] = GetConfigString(cfg, "BucketARN", ""),
                ["RoleARN"] = GetConfigString(cfg, "RoleARN", ""),
                ["BufferingHints"] = GetConfigObject(cfg, "BufferingHints")
                    ?? new Dictionary<string, object?> { ["SizeInMBs"] = 5, ["IntervalInSeconds"] = 300 },
                ["CompressionFormat"] = GetConfigString(cfg, "CompressionFormat", "UNCOMPRESSED"),
                ["EncryptionConfiguration"] = GetConfigObject(cfg, "EncryptionConfiguration")
                    ?? new Dictionary<string, object?> { ["NoEncryptionConfig"] = "NoEncryption" },
                ["Prefix"] = GetConfigString(cfg, "Prefix", ""),
                ["ErrorOutputPrefix"] = GetConfigString(cfg, "ErrorOutputPrefix", ""),
                ["S3BackupMode"] = GetConfigString(cfg, "S3BackupMode", "Disabled"),
            };

            foreach (var optKey in new[]
            {
                "ProcessingConfiguration", "CloudWatchLoggingOptions",
                "DataFormatConversionConfiguration", "DynamicPartitioningConfiguration",
            })
            {
                var optVal = GetConfigObject(cfg, optKey);
                if (optVal is not null)
                {
                    desc[optKey] = optVal;
                }
            }

            result[descKey] = desc;
        }
        else if (dest.Type == "HttpEndpoint")
        {
            var cfg = dest.Config;
            result["HttpEndpointDestinationDescription"] = new Dictionary<string, object?>(StringComparer.Ordinal)
            {
                ["EndpointConfiguration"] = GetConfigObject(cfg, "EndpointConfiguration")
                    ?? new Dictionary<string, object?>(),
                ["BufferingHints"] = GetConfigObject(cfg, "BufferingHints")
                    ?? new Dictionary<string, object?> { ["SizeInMBs"] = 5, ["IntervalInSeconds"] = 300 },
                ["S3BackupMode"] = GetConfigString(cfg, "S3BackupMode", "FailedDataOnly"),
            };
        }
        else
        {
            result[$"{dest.Type}DestinationDescription"] = dest.ConfigAsObject;
        }

        return result;
    }

    private static string GetConfigString(JsonElement cfg, string key, string defaultValue)
    {
        if (cfg.ValueKind == JsonValueKind.Object && cfg.TryGetProperty(key, out var prop)
            && prop.ValueKind == JsonValueKind.String)
        {
            return prop.GetString() ?? defaultValue;
        }

        return defaultValue;
    }

    private static object? GetConfigObject(JsonElement cfg, string key)
    {
        if (cfg.ValueKind == JsonValueKind.Object && cfg.TryGetProperty(key, out var prop)
            && prop.ValueKind == JsonValueKind.Object)
        {
            return JsonElementToObject(prop);
        }

        return null;
    }

    private static object? JsonElementToObject(JsonElement element)
    {
        return element.ValueKind switch
        {
            JsonValueKind.Object => JsonElementToDict(element),
            JsonValueKind.Array => element.EnumerateArray().Select(JsonElementToObject).ToList(),
            JsonValueKind.String => element.GetString(),
            JsonValueKind.Number => element.TryGetInt64(out var l) ? l : element.GetDouble(),
            JsonValueKind.True => true,
            JsonValueKind.False => false,
            _ => null,
        };
    }

    private static Dictionary<string, object?> JsonElementToDict(JsonElement element)
    {
        var dict = new Dictionary<string, object?>(StringComparer.Ordinal);
        foreach (var prop in element.EnumerateObject())
        {
            dict[prop.Name] = JsonElementToObject(prop.Value);
        }

        return dict;
    }

    // -- Stream description builder --------------------------------------------

    private static Dictionary<string, object?> BuildStreamDescription(FhStream stream)
    {
        var desc = new Dictionary<string, object?>(StringComparer.Ordinal)
        {
            ["DeliveryStreamName"] = stream.Name,
            ["DeliveryStreamARN"] = stream.Arn,
            ["DeliveryStreamStatus"] = stream.Status,
            ["DeliveryStreamType"] = stream.Type,
            ["VersionId"] = stream.Version.ToString(),
            ["CreateTimestamp"] = stream.CreatedAt,
            ["LastUpdateTimestamp"] = stream.UpdatedAt,
            ["HasMoreDestinations"] = false,
            ["Destinations"] = stream.Destinations.ConvertAll(BuildDestDescription),
        };

        if (stream.Encryption is not null)
        {
            desc["DeliveryStreamEncryptionConfiguration"] = stream.Encryption;
        }

        if (stream.Type == "KinesisStreamAsSource" && stream.KinesisSource is not null)
        {
            desc["Source"] = new Dictionary<string, object?>(StringComparer.Ordinal)
            {
                ["KinesisStreamSourceDescription"] = stream.KinesisSource,
            };
        }

        return desc;
    }

    // -- Actions ---------------------------------------------------------------

    private ServiceResponse ActCreateDeliveryStream(JsonElement data)
    {
        var name = GetString(data, "DeliveryStreamName");
        if (string.IsNullOrEmpty(name))
        {
            return Invalid("DeliveryStreamName is required.");
        }

        lock (_lock)
        {
            if (_streams.ContainsKey(name))
            {
                return AwsResponseHelpers.ErrorResponseJson(
                    "ResourceInUseException",
                    $"Delivery stream {name} already exists.",
                    400);
            }

            if (_streams.Count >= 5000)
            {
                return AwsResponseHelpers.ErrorResponseJson(
                    "LimitExceededException",
                    "You have reached the limit on the number of delivery streams.",
                    400);
            }

            var (dtype, dcfg) = ResolveDestTypeAndConfig(data);
            var destinations = new List<FhDestination>();
            if (dtype is not null && dcfg.HasValue)
            {
                destinations.Add(new FhDestination
                {
                    Id = NextDestId(),
                    Type = dtype,
                    Config = dcfg.Value,
                    ConfigAsObject = JsonElementToObject(dcfg.Value),
                    Records = [],
                });
            }

            var streamType = GetString(data, "DeliveryStreamType") ?? "DirectPut";
            var now = TimeHelpers.NowEpoch();
            var stream = new FhStream
            {
                Name = name,
                Arn = StreamArn(name),
                Status = "ACTIVE",
                Type = streamType,
                Version = 1,
                CreatedAt = now,
                UpdatedAt = now,
                Destinations = destinations,
                Tags = ParseTags(data),
                Encryption = null,
                KinesisSource = null,
            };

            // Capture Kinesis source config
            if (streamType == "KinesisStreamAsSource"
                && data.TryGetProperty("KinesisStreamSourceConfiguration", out var ksCfg))
            {
                stream.KinesisSource = new Dictionary<string, object?>(StringComparer.Ordinal)
                {
                    ["KinesisStreamARN"] = GetString(ksCfg, "KinesisStreamARN") ?? "",
                    ["RoleARN"] = GetString(ksCfg, "RoleARN") ?? "",
                    ["DeliveryStartTimestamp"] = now,
                };
            }

            // Encryption config at create time
            if (data.TryGetProperty("DeliveryStreamEncryptionConfigurationInput", out var encInput))
            {
                var enc = new Dictionary<string, object?>(StringComparer.Ordinal)
                {
                    ["Status"] = "ENABLED",
                    ["KeyType"] = GetString(encInput, "KeyType") ?? "AWS_OWNED_CMK",
                };
                var keyArn = GetString(encInput, "KeyARN");
                if (keyArn is not null)
                {
                    enc["KeyARN"] = keyArn;
                }

                stream.Encryption = enc;
            }

            _streams[name] = stream;

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>
            {
                ["DeliveryStreamARN"] = stream.Arn,
            });
        }
    }

    private ServiceResponse ActDeleteDeliveryStream(JsonElement data)
    {
        var name = GetString(data, "DeliveryStreamName") ?? "";

        lock (_lock)
        {
            if (!_streams.TryGetValue(name, out var stream))
            {
                return NotFound(name);
            }

            if (stream.Status == "CREATING")
            {
                return InUse(name);
            }

            _streams.Remove(name);
            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>());
        }
    }

    private ServiceResponse ActDescribeDeliveryStream(JsonElement data)
    {
        var name = GetString(data, "DeliveryStreamName") ?? "";

        lock (_lock)
        {
            if (!_streams.TryGetValue(name, out var stream))
            {
                return NotFound(name);
            }

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>
            {
                ["DeliveryStreamDescription"] = BuildStreamDescription(stream),
            });
        }
    }

    private ServiceResponse ActListDeliveryStreams(JsonElement data)
    {
        var dtypeFilter = GetString(data, "DeliveryStreamType");
        var limit = Math.Min(GetInt(data, "Limit", 10), 10000);
        var start = GetString(data, "ExclusiveStartDeliveryStreamName");

        List<string> names;
        lock (_lock)
        {
            if (dtypeFilter is not null)
            {
                names = _streams
                    .Where(kvp => kvp.Value.Type == dtypeFilter)
                    .Select(kvp => kvp.Key)
                    .Order(StringComparer.Ordinal)
                    .ToList();
            }
            else
            {
                names = _streams.Keys.Order(StringComparer.Ordinal).ToList();
            }
        }

        if (start is not null)
        {
            var idx = names.IndexOf(start);
            if (idx >= 0)
            {
                names = names.GetRange(idx + 1, names.Count - idx - 1);
            }
        }

        var hasMore = names.Count > limit;
        return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>
        {
            ["DeliveryStreamNames"] = names.GetRange(0, Math.Min(limit, names.Count)),
            ["HasMoreDeliveryStreams"] = hasMore,
        });
    }

    private ServiceResponse ActPutRecord(JsonElement data)
    {
        var name = GetString(data, "DeliveryStreamName") ?? "";

        lock (_lock)
        {
            if (!_streams.TryGetValue(name, out var stream))
            {
                return NotFound(name);
            }

            if (stream.Status != "ACTIVE")
            {
                return AwsResponseHelpers.ErrorResponseJson("ServiceUnavailableException", "Service unavailable.", 503);
            }

            string rawData;
            if (data.TryGetProperty("Record", out var recordEl))
            {
                rawData = GetString(recordEl, "Data") ?? "";
            }
            else
            {
                rawData = "";
            }

            byte[] decoded;
            try
            {
                decoded = Convert.FromBase64String(rawData);
            }
            catch (FormatException)
            {
                return Invalid("Record.Data must be valid base64.");
            }

            if (decoded.Length > 1024 * 1000)
            {
                return Invalid("Record size exceeds 1,000 KiB limit.");
            }

            var recordId = GenerateRecordId();
            foreach (var dest in stream.Destinations)
            {
                dest.Records.Add(new FhRecord { Id = recordId, Data = rawData, Timestamp = TimeHelpers.NowEpoch() });
            }

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>
            {
                ["RecordId"] = recordId,
                ["Encrypted"] = false,
            });
        }
    }

    private ServiceResponse ActPutRecordBatch(JsonElement data)
    {
        var name = GetString(data, "DeliveryStreamName") ?? "";

        if (!data.TryGetProperty("Records", out var recordsEl) || recordsEl.ValueKind != JsonValueKind.Array
            || recordsEl.GetArrayLength() == 0)
        {
            return Invalid("Records must not be empty.");
        }

        if (recordsEl.GetArrayLength() > 500)
        {
            return Invalid("A maximum of 500 records can be sent per batch.");
        }

        lock (_lock)
        {
            if (!_streams.TryGetValue(name, out var stream))
            {
                return NotFound(name);
            }

            if (stream.Status != "ACTIVE")
            {
                return AwsResponseHelpers.ErrorResponseJson("ServiceUnavailableException", "Service unavailable.", 503);
            }

            var responses = new List<Dictionary<string, object?>>();
            var failed = 0;

            foreach (var rec in recordsEl.EnumerateArray())
            {
                var rawData = GetString(rec, "Data") ?? "";
                try
                {
                    var decoded = Convert.FromBase64String(rawData);
                    if (decoded.Length > 1024 * 1000)
                    {
                        throw new InvalidOperationException("Record too large");
                    }

                    var recordId = GenerateRecordId();
                    foreach (var dest in stream.Destinations)
                    {
                        dest.Records.Add(new FhRecord { Id = recordId, Data = rawData, Timestamp = TimeHelpers.NowEpoch() });
                    }

                    responses.Add(new Dictionary<string, object?>
                    {
                        ["RecordId"] = recordId,
                        ["Encrypted"] = false,
                    });
                }
                catch (Exception ex)
                {
                    failed++;
                    responses.Add(new Dictionary<string, object?>
                    {
                        ["ErrorCode"] = "ServiceUnavailableException",
                        ["ErrorMessage"] = ex.Message,
                    });
                }
            }

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>
            {
                ["FailedPutCount"] = failed,
                ["Encrypted"] = false,
                ["RequestResponses"] = responses,
            });
        }
    }

    private ServiceResponse ActUpdateDestination(JsonElement data)
    {
        var name = GetString(data, "DeliveryStreamName") ?? "";
        var destId = GetString(data, "DestinationId") ?? "";
        var versionId = GetString(data, "CurrentDeliveryStreamVersionId") ?? "";

        lock (_lock)
        {
            if (!_streams.TryGetValue(name, out var stream))
            {
                return NotFound(name);
            }

            if (stream.Version.ToString() != versionId)
            {
                return AwsResponseHelpers.ErrorResponseJson(
                    "ConcurrentModificationException",
                    "Request includes an invalid stream version ID.",
                    400);
            }

            var dest = stream.Destinations.Find(d => d.Id == destId);
            if (dest is null)
            {
                return AwsResponseHelpers.ErrorResponseJson(
                    "ResourceNotFoundException",
                    $"Destination {destId} not found in stream {name}.",
                    400);
            }

            var (dtype, dcfg) = ResolveDestUpdateConfig(data);
            if (dtype is not null && dcfg.HasValue)
            {
                if (dtype == dest.Type)
                {
                    // Same destination type — merge fields
                    var merged = JsonElementToDict(dest.Config);
                    var update = JsonElementToDict(dcfg.Value);
                    foreach (var (k, v) in update)
                    {
                        merged[k] = v;
                    }

                    // Re-serialize to JsonElement for storage
                    var mergedBytes = DictionaryObjectJsonConverter.SerializeObject(merged);
                    using var mergedDoc = JsonDocument.Parse(mergedBytes);
                    dest.Config = mergedDoc.RootElement.Clone();
                    dest.ConfigAsObject = merged;
                }
                else
                {
                    // Destination type change — full replacement
                    dest.Type = dtype;
                    dest.Config = dcfg.Value;
                    dest.ConfigAsObject = JsonElementToObject(dcfg.Value);
                }
            }

            stream.Version++;
            stream.UpdatedAt = TimeHelpers.NowEpoch();

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>());
        }
    }

    private ServiceResponse ActTagDeliveryStream(JsonElement data)
    {
        var name = GetString(data, "DeliveryStreamName") ?? "";
        var newTags = ParseTags(data);
        if (newTags.Count == 0)
        {
            return Invalid("Tags must not be empty.");
        }

        lock (_lock)
        {
            if (!_streams.TryGetValue(name, out var stream))
            {
                return NotFound(name);
            }

            if (stream.Tags.Count + newTags.Count > 50)
            {
                return AwsResponseHelpers.ErrorResponseJson(
                    "LimitExceededException",
                    "A delivery stream cannot have more than 50 tags.",
                    400);
            }

            foreach (var (k, v) in newTags)
            {
                stream.Tags[k] = v;
            }

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>());
        }
    }

    private ServiceResponse ActUntagDeliveryStream(JsonElement data)
    {
        var name = GetString(data, "DeliveryStreamName") ?? "";
        var keys = new List<string>();
        if (data.TryGetProperty("TagKeys", out var keysEl) && keysEl.ValueKind == JsonValueKind.Array)
        {
            foreach (var item in keysEl.EnumerateArray())
            {
                var s = item.GetString();
                if (s is not null)
                {
                    keys.Add(s);
                }
            }
        }

        if (keys.Count == 0)
        {
            return Invalid("TagKeys must not be empty.");
        }

        lock (_lock)
        {
            if (!_streams.TryGetValue(name, out var stream))
            {
                return NotFound(name);
            }

            foreach (var k in keys)
            {
                stream.Tags.Remove(k);
            }

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>());
        }
    }

    private ServiceResponse ActListTagsForDeliveryStream(JsonElement data)
    {
        var name = GetString(data, "DeliveryStreamName") ?? "";
        var limit = Math.Min(GetInt(data, "Limit", 50), 50);
        var start = GetString(data, "ExclusiveStartTagKey");

        lock (_lock)
        {
            if (!_streams.TryGetValue(name, out var stream))
            {
                return NotFound(name);
            }

            var allTags = stream.Tags
                .OrderBy(kvp => kvp.Key, StringComparer.Ordinal)
                .Select(kvp => new Dictionary<string, object?>
                {
                    ["Key"] = kvp.Key,
                    ["Value"] = kvp.Value,
                })
                .ToList();

            if (start is not null)
            {
                var idx = allTags.FindIndex(t => (string?)t["Key"] == start);
                if (idx >= 0)
                {
                    allTags = allTags.GetRange(idx + 1, allTags.Count - idx - 1);
                }
            }

            var hasMore = allTags.Count > limit;
            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>
            {
                ["Tags"] = allTags.GetRange(0, Math.Min(limit, allTags.Count)),
                ["HasMoreTags"] = hasMore,
            });
        }
    }

    private ServiceResponse ActStartDeliveryStreamEncryption(JsonElement data)
    {
        var name = GetString(data, "DeliveryStreamName") ?? "";

        lock (_lock)
        {
            if (!_streams.TryGetValue(name, out var stream))
            {
                return NotFound(name);
            }

            if (stream.Status != "ACTIVE")
            {
                return InUse(name);
            }

            var enc = new Dictionary<string, object?>(StringComparer.Ordinal)
            {
                ["Status"] = "ENABLED",
                ["KeyType"] = "AWS_OWNED_CMK",
            };

            if (data.TryGetProperty("DeliveryStreamEncryptionConfigurationInput", out var encInput))
            {
                var keyType = GetString(encInput, "KeyType");
                if (keyType is not null)
                {
                    enc["KeyType"] = keyType;
                }

                var keyArn = GetString(encInput, "KeyARN");
                if (keyArn is not null)
                {
                    enc["KeyARN"] = keyArn;
                }
            }

            stream.Encryption = enc;
            stream.UpdatedAt = TimeHelpers.NowEpoch();
            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>());
        }
    }

    private ServiceResponse ActStopDeliveryStreamEncryption(JsonElement data)
    {
        var name = GetString(data, "DeliveryStreamName") ?? "";

        lock (_lock)
        {
            if (!_streams.TryGetValue(name, out var stream))
            {
                return NotFound(name);
            }

            if (stream.Status != "ACTIVE")
            {
                return InUse(name);
            }

            stream.Encryption = new Dictionary<string, object?>(StringComparer.Ordinal)
            {
                ["Status"] = "DISABLED",
            };
            stream.UpdatedAt = TimeHelpers.NowEpoch();
            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>());
        }
    }

    // -- Data models -----------------------------------------------------------

    private sealed class FhStream
    {
        internal required string Name { get; init; }
        internal required string Arn { get; init; }
        internal required string Status { get; set; }
        internal required string Type { get; init; }
        internal required int Version { get; set; }
        internal required double CreatedAt { get; init; }
        internal required double UpdatedAt { get; set; }
        internal required List<FhDestination> Destinations { get; init; }
        internal required Dictionary<string, string> Tags { get; init; }
        internal Dictionary<string, object?>? Encryption { get; set; }
        internal Dictionary<string, object?>? KinesisSource { get; set; }
    }

    private sealed class FhDestination
    {
        internal required string Id { get; init; }
        internal required string Type { get; set; }
        internal required JsonElement Config { get; set; }
        internal required object? ConfigAsObject { get; set; }
        internal required List<FhRecord> Records { get; init; }
    }

    private sealed class FhRecord
    {
        internal required string Id { get; init; }
        internal required string Data { get; init; }
        internal required double Timestamp { get; init; }
    }
}
