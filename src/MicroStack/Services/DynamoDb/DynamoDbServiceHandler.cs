using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using MicroStack.Internal;

namespace MicroStack.Services.DynamoDb;

/// <summary>
/// DynamoDB service handler — JSON protocol only (X-Amz-Target: DynamoDB_20120810.*).
///
/// Port of ministack/services/dynamodb.py.
/// </summary>
internal sealed class DynamoDbServiceHandler : IServiceHandler
{
    // ── State ────────────────────────────────────────────────────────────────────

    private readonly AccountScopedDictionary<string, DdbTable> _tables = new();
    private readonly AccountScopedDictionary<string, List<JsonObject>> _tags = new();
    private readonly AccountScopedDictionary<string, DdbTtlSetting> _ttlSettings = new();
    private readonly AccountScopedDictionary<string, bool> _pitrSettings = new();
    // Stream records: table_name → list of stream event records
    private readonly AccountScopedDictionary<string, List<JsonObject>> _streamRecords = new();
    private long _streamSeqCounter;
    private readonly Lock _lock = new();

    private static string _region => MicroStackOptions.Instance.Region;

    // ── IServiceHandler ──────────────────────────────────────────────────────────

    public string ServiceName => "dynamodb";

    public Task<ServiceResponse> HandleAsync(ServiceRequest request)
    {
        var target = request.GetHeader("x-amz-target") ?? "";
        var action = target.Contains('.') ? target[(target.LastIndexOf('.') + 1)..] : "";

        JsonObject data;
        try
        {
            data = request.Body.Length > 0
                ? JsonNode.Parse(request.Body)?.AsObject() ?? new JsonObject()
                : new JsonObject();
        }
        catch
        {
            return Task.FromResult(JsonError("SerializationException", "Invalid JSON", 400));
        }

        try
        {
            var result = action switch
            {
                "CreateTable"               => ActCreateTable(data),
                "DeleteTable"               => ActDeleteTable(data),
                "DescribeTable"             => ActDescribeTable(data),
                "ListTables"                => ActListTables(data),
                "UpdateTable"               => ActUpdateTable(data),
                "PutItem"                   => ActPutItem(data),
                "GetItem"                   => ActGetItem(data),
                "DeleteItem"                => ActDeleteItem(data),
                "UpdateItem"                => ActUpdateItem(data),
                "Query"                     => ActQuery(data),
                "Scan"                      => ActScan(data),
                "BatchWriteItem"            => ActBatchWriteItem(data),
                "BatchGetItem"              => ActBatchGetItem(data),
                "TransactWriteItems"        => ActTransactWriteItems(data),
                "TransactGetItems"          => ActTransactGetItems(data),
                "DescribeTimeToLive"        => ActDescribeTtl(data),
                "UpdateTimeToLive"          => ActUpdateTtl(data),
                "DescribeContinuousBackups" => ActDescribeContinuousBackups(data),
                "UpdateContinuousBackups"   => ActUpdateContinuousBackups(data),
                "DescribeEndpoints"         => ActDescribeEndpoints(data),
                "TagResource"               => ActTagResource(data),
                "UntagResource"             => ActUntagResource(data),
                "ListTagsOfResource"        => ActListTags(data),
                _ => throw new DdbException("UnknownOperationException", $"Unknown operation: {action}", 400),
            };
            return Task.FromResult(JsonOk(result));
        }
        catch (DdbException ex)
        {
            if (ex.RawBody is not null)
            {
                var rawBytes = Encoding.UTF8.GetBytes(ex.RawBody);
                return Task.FromResult(new ServiceResponse(ex.Status,
                    new Dictionary<string, string> { ["Content-Type"] = "application/x-amz-json-1.0" },
                    rawBytes));
            }
            return Task.FromResult(JsonError(ex.Code, ex.Message, ex.Status));
        }
    }

    public void Reset()
    {
        lock (_lock)
        {
            _tables.Clear();
            _tags.Clear();
            _ttlSettings.Clear();
            _pitrSettings.Clear();
            _streamRecords.Clear();
        }
    }

    public JsonElement? GetState() => null;   // persistence not implemented in Phase 1

    public void RestoreState(JsonElement state) { }  // persistence not implemented in Phase 1

    // ── Internal stream record access (used by Lambda ESM) ──────────────────────

    /// <summary>Dequeue stream records for a table (called by Lambda ESM poller).</summary>
    internal List<JsonObject> DrainStreamRecords(string tableName, int max)
    {
        lock (_lock)
        {
            if (!_streamRecords.TryGetValue(tableName, out var records) || records.Count == 0)
                return [];
            var batch = records.Take(max).ToList();
            records.RemoveRange(0, batch.Count);
            return batch;
        }
    }

    // ── Table operations ─────────────────────────────────────────────────────────

    private JsonObject ActCreateTable(JsonObject data)
    {
        var name = data["TableName"]?.GetValue<string>() ?? "";
        if (string.IsNullOrEmpty(name))
            throw new DdbException("ValidationException", "TableName is required", 400);

        lock (_lock)
        {
            if (_tables.TryGetValue(name, out _))
                throw new DdbException("ResourceInUseException", $"Table already exists: {name}", 400);

            var keySchema = data["KeySchema"]?.AsArray() ?? [];
            var attrDefs  = data["AttributeDefinitions"]?.AsArray() ?? [];
            string? pkName = null, skName = null;
            foreach (var ks in keySchema)
            {
                if (ks?["KeyType"]?.GetValue<string>() == "HASH")
                    pkName = ks["AttributeName"]?.GetValue<string>();
                else if (ks?["KeyType"]?.GetValue<string>() == "RANGE")
                    skName = ks["AttributeName"]?.GetValue<string>();
            }

            var gsis = DeepCloneArray(data["GlobalSecondaryIndexes"]?.AsArray() ?? []);
            var lsis = DeepCloneArray(data["LocalSecondaryIndexes"]?.AsArray() ?? []);
            var accountId = AccountContext.GetAccountId();

            foreach (var gsi in gsis.OfType<JsonObject>())
            {
                gsi["IndexStatus"]           ??= "ACTIVE";
                gsi["ProvisionedThroughput"] ??= DefaultThroughput();
                gsi["IndexArn"]              = $"arn:aws:dynamodb:{_region}:{accountId}:table/{name}/index/{gsi["IndexName"]?.GetValue<string>()}";
                gsi["IndexSizeBytes"]        = 0;
                gsi["ItemCount"]             = 0;
            }
            foreach (var lsi in lsis.OfType<JsonObject>())
            {
                lsi["IndexArn"]      = $"arn:aws:dynamodb:{_region}:{accountId}:table/{name}/index/{lsi["IndexName"]?.GetValue<string>()}";
                lsi["IndexSizeBytes"] = 0;
                lsi["ItemCount"]     = 0;
            }

            var table = new DdbTable
            {
                TableName          = name,
                KeySchema          = (JsonArray)keySchema.DeepClone(),
                AttributeDefinitions = (JsonArray)attrDefs.DeepClone(),
                PkName             = pkName,
                SkName             = skName,
                TableArn           = $"arn:aws:dynamodb:{_region}:{accountId}:table/{name}",
                TableId            = Guid.NewGuid().ToString(),
                ProvisionedThroughput = (JsonObject)(data["ProvisionedThroughput"]?.DeepClone() ?? DefaultThroughput()),
                BillingModeSummary = new JsonObject { ["BillingMode"] = data["BillingMode"]?.GetValue<string>() ?? "PROVISIONED" },
                StreamSpecification = (JsonObject?)data["StreamSpecification"]?.DeepClone(),
                SseDescription     = (JsonObject?)data["SSESpecification"]?.DeepClone(),
                Gsis               = gsis,
                Lsis               = lsis,
            };

            // Apply any extra attrs from Attributes at creation
            var creationAttrs = data["Attributes"]?.AsObject();
            if (creationAttrs is not null)
                foreach (var kv in creationAttrs)
                    if (!table.Attributes.ContainsKey(kv.Key))
                        table.Attributes[kv.Key] = kv.Value?.GetValue<string>() ?? "";

            _tables[name] = table;
            return new JsonObject { ["TableDescription"] = TableDescription(table) };
        }
    }

    private JsonObject ActDeleteTable(JsonObject data)
    {
        var name = data["TableName"]?.GetValue<string>() ?? "";
        lock (_lock)
        {
            if (!_tables.TryGetValue(name, out var table))
                throw new DdbException("ResourceNotFoundException",
                    $"Requested resource not found: Table: {name} not found", 400);

            var desc = TableDescription(table);
            desc["TableStatus"] = "DELETING";
            _tables.TryRemove(name, out _);
            _tags.TryRemove(table.TableArn, out _);
            _ttlSettings.TryRemove(name, out _);
            _pitrSettings.TryRemove(name, out _);
            return new JsonObject { ["TableDescription"] = desc };
        }
    }

    private JsonObject ActDescribeTable(JsonObject data)
    {
        var name = data["TableName"]?.GetValue<string>() ?? "";
        lock (_lock)
        {
            var table = GetTable(name);
            return new JsonObject { ["Table"] = TableDescription(table) };
        }
    }

    private JsonObject ActListTables(JsonObject data)
    {
        var limit = data["Limit"]?.GetValue<int>() ?? 100;
        var start = data["ExclusiveStartTableName"]?.GetValue<string>() ?? "";
        lock (_lock)
        {
            var names = _tables.Items.Select(kv => kv.Key)
                .OrderBy(n => n, StringComparer.Ordinal)
                .Where(n => string.IsNullOrEmpty(start) || string.Compare(n, start, StringComparison.Ordinal) > 0)
                .Take(limit + 1)
                .ToList();

            var result = new JsonObject();
            var arr    = new JsonArray();
            var hasMore = names.Count > limit;
            var page   = hasMore ? names[..limit] : names;
            foreach (var n in page) ((IList<JsonNode?>)arr).Add(JsonValue.Create(n));
            result["TableNames"] = arr;
            if (hasMore)
                result["LastEvaluatedTableName"] = page[^1];
            return result;
        }
    }

    private JsonObject ActUpdateTable(JsonObject data)
    {
        var name = data["TableName"]?.GetValue<string>() ?? "";
        lock (_lock)
        {
            var table = GetTable(name);

            if (data["ProvisionedThroughput"] is not null)
                table.ProvisionedThroughput = (JsonObject)data["ProvisionedThroughput"]!.DeepClone();
            if (data["BillingMode"] is not null)
                table.BillingModeSummary = new JsonObject { ["BillingMode"] = data["BillingMode"]!.GetValue<string>() };
            if (data["AttributeDefinitions"] is not null)
                table.AttributeDefinitions = (JsonArray)data["AttributeDefinitions"]!.DeepClone();
            if (data["StreamSpecification"] is not null)
                table.StreamSpecification = (JsonObject?)data["StreamSpecification"]?.DeepClone();

            foreach (var upd in data["GlobalSecondaryIndexUpdates"]?.AsArray() ?? [])
            {
                if (upd is null) continue;
                if (upd["Create"] is JsonObject create)
                {
                    var gsi = (JsonObject)create.DeepClone();
                    gsi["IndexStatus"]           ??= "ACTIVE";
                    gsi["ProvisionedThroughput"] ??= DefaultThroughput();
                    gsi["IndexArn"]              = $"arn:aws:dynamodb:{_region}:{AccountContext.GetAccountId()}:table/{name}/index/{gsi["IndexName"]?.GetValue<string>()}";
                    gsi["IndexSizeBytes"]        = 0;
                    gsi["ItemCount"]             = 0;
                    table.Gsis.Add((JsonNode)gsi);
                }
                else if (upd["Delete"] is JsonObject del)
                {
                    var idx = del["IndexName"]?.GetValue<string>() ?? "";
                    for (var i = table.Gsis.Count - 1; i >= 0; i--)
                        if (table.Gsis[i] is JsonObject g && g["IndexName"]?.GetValue<string>() == idx)
                            table.Gsis.RemoveAt(i);
                }
                else if (upd["Update"] is JsonObject updGsi)
                {
                    var idx = updGsi["IndexName"]?.GetValue<string>() ?? "";
                    foreach (var g in table.Gsis.OfType<JsonObject>())
                        if (g["IndexName"]?.GetValue<string>() == idx && updGsi["ProvisionedThroughput"] is not null)
                            g["ProvisionedThroughput"] = updGsi["ProvisionedThroughput"]!.DeepClone();
                }
            }

            return new JsonObject { ["TableDescription"] = TableDescription(table) };
        }
    }

    private JsonObject TableDescription(DdbTable t)
    {
        var desc = new JsonObject
        {
            ["TableName"]           = t.TableName,
            ["KeySchema"]           = (JsonArray)t.KeySchema.DeepClone(),
            ["AttributeDefinitions"]= (JsonArray)t.AttributeDefinitions.DeepClone(),
            ["TableStatus"]         = t.TableStatus,
            ["CreationDateTime"]    = t.CreationDateTime,
            ["ItemCount"]           = t.ItemCount,
            ["TableSizeBytes"]      = t.TableSizeBytes,
            ["TableArn"]            = t.TableArn,
            ["TableId"]             = t.TableId,
            ["ProvisionedThroughput"] = (JsonObject)t.ProvisionedThroughput.DeepClone(),
        };
        if (t.BillingModeSummary is not null)
            desc["BillingModeSummary"] = t.BillingModeSummary.DeepClone();
        if (t.Gsis.Count > 0)
            desc["GlobalSecondaryIndexes"] = (JsonArray)t.Gsis.DeepClone();
        if (t.Lsis.Count > 0)
            desc["LocalSecondaryIndexes"] = (JsonArray)t.Lsis.DeepClone();
        if (t.StreamSpecification is not null)
        {
            desc["StreamSpecification"] = t.StreamSpecification.DeepClone();
            var iso = DateTimeOffset.UtcNow.ToString("o");
            desc["LatestStreamLabel"] = iso;
            desc["LatestStreamArn"]   = $"{t.TableArn}/stream/{iso}";
        }
        if (t.SseDescription is not null)
            desc["SSEDescription"] = t.SseDescription.DeepClone();
        desc["WarmThroughput"] = new JsonObject
        {
            ["ReadUnitsPerSecond"]  = 0,
            ["WriteUnitsPerSecond"] = 0,
            ["Status"]              = "ACTIVE",
        };
        return desc;
    }

    // ── Item operations ──────────────────────────────────────────────────────────

    private JsonObject ActPutItem(JsonObject data)
    {
        var name = data["TableName"]?.GetValue<string>() ?? "";
        lock (_lock)
        {
            var table = GetTable(name);
            var item  = data["Item"]?.AsObject() ?? new JsonObject();
            var (pkVal, skVal) = ResolveKeys(table, item, allowExtra: true);

            var oldItem = GetItemInternal(table, pkVal, skVal);

            var condExpr = data["ConditionExpression"]?.GetValue<string>() ?? "";
            if (!string.IsNullOrEmpty(condExpr))
            {
                var eav = data["ExpressionAttributeValues"]?.AsObject() ?? new JsonObject();
                var ean = data["ExpressionAttributeNames"]?.AsObject() ?? new JsonObject();
                if (!EvaluateCondition(condExpr, oldItem ?? new JsonObject(), eav, ean))
                    throw new DdbException("ConditionalCheckFailedException", "The conditional request failed", 400);
            }

            SetItemInternal(table, pkVal, skVal, (JsonObject)item.DeepClone());
            UpdateCounts(table);
            EmitStreamEvent(name, table, oldItem is null ? "INSERT" : "MODIFY", oldItem, item);

            var result = new JsonObject();
            if (data["ReturnValues"]?.GetValue<string>() == "ALL_OLD" && oldItem is not null)
                result["Attributes"] = oldItem.DeepClone();
            AddConsumedCapacity(result, data, name, write: true, table);
            return result;
        }
    }

    private JsonObject ActGetItem(JsonObject data)
    {
        var name = data["TableName"]?.GetValue<string>() ?? "";
        lock (_lock)
        {
            var table = GetTable(name);
            var key   = data["Key"]?.AsObject() ?? new JsonObject();
            var (pkVal, skVal) = ResolveKeysStrict(table, key);
            var item  = GetItemInternal(table, pkVal, skVal);

            var result = new JsonObject();
            if (item is not null)
                result["Item"] = ApplyProjection(item, data);
            AddConsumedCapacity(result, data, name, write: false, table);
            return result;
        }
    }

    private JsonObject ActDeleteItem(JsonObject data)
    {
        var name = data["TableName"]?.GetValue<string>() ?? "";
        lock (_lock)
        {
            var table = GetTable(name);
            var key   = data["Key"]?.AsObject() ?? new JsonObject();
            var (pkVal, skVal) = ResolveKeysStrict(table, key);
            var oldItem = GetItemInternal(table, pkVal, skVal);

            var condExpr = data["ConditionExpression"]?.GetValue<string>() ?? "";
            if (!string.IsNullOrEmpty(condExpr))
            {
                var eav = data["ExpressionAttributeValues"]?.AsObject() ?? new JsonObject();
                var ean = data["ExpressionAttributeNames"]?.AsObject() ?? new JsonObject();
                if (!EvaluateCondition(condExpr, oldItem ?? new JsonObject(), eav, ean))
                    throw new DdbException("ConditionalCheckFailedException", "The conditional request failed", 400);
            }

            if (oldItem is not null)
            {
                RemoveItemInternal(table, pkVal, skVal);
                EmitStreamEvent(name, table, "REMOVE", oldItem, null);
            }
            UpdateCounts(table);

            var result = new JsonObject();
            if (data["ReturnValues"]?.GetValue<string>() == "ALL_OLD" && oldItem is not null)
                result["Attributes"] = oldItem.DeepClone();
            AddConsumedCapacity(result, data, name, write: true, table);
            return result;
        }
    }

    private JsonObject ActUpdateItem(JsonObject data)
    {
        var name = data["TableName"]?.GetValue<string>() ?? "";
        lock (_lock)
        {
            var table = GetTable(name);
            var key   = data["Key"]?.AsObject() ?? new JsonObject();
            var (pkVal, skVal) = ResolveKeysStrict(table, key);

            var existing = GetItemInternal(table, pkVal, skVal);
            var oldItem  = existing is null ? null : (JsonObject)existing.DeepClone();
            var item     = existing is null
                ? (JsonObject)((JsonNode)key).DeepClone()
                : (JsonObject)existing.DeepClone();

            var condExpr = data["ConditionExpression"]?.GetValue<string>() ?? "";
            if (!string.IsNullOrEmpty(condExpr))
            {
                var eav = data["ExpressionAttributeValues"]?.AsObject() ?? new JsonObject();
                var ean = data["ExpressionAttributeNames"]?.AsObject() ?? new JsonObject();
                if (!EvaluateCondition(condExpr, existing ?? new JsonObject(), eav, ean))
                    throw new DdbException("ConditionalCheckFailedException", "The conditional request failed", 400);
            }

            var updateExpr = data["UpdateExpression"]?.GetValue<string>() ?? "";
            if (!string.IsNullOrEmpty(updateExpr))
            {
                var eav = data["ExpressionAttributeValues"]?.AsObject() ?? new JsonObject();
                var ean = data["ExpressionAttributeNames"]?.AsObject() ?? new JsonObject();
                item = ApplyUpdateExpression(item, updateExpr, eav, ean);
            }

            SetItemInternal(table, pkVal, skVal, item);
            UpdateCounts(table);
            EmitStreamEvent(name, table, oldItem is null ? "INSERT" : "MODIFY", oldItem, item);

            var result = new JsonObject();
            var rv = data["ReturnValues"]?.GetValue<string>() ?? "NONE";
            if (rv == "ALL_NEW")
                result["Attributes"] = (JsonObject)item.DeepClone();
            else if (rv == "ALL_OLD" && oldItem is not null)
                result["Attributes"] = oldItem;
            else if (rv == "UPDATED_OLD" && oldItem is not null)
                result["Attributes"] = DiffAttributes(oldItem, item, returnOld: true);
            else if (rv == "UPDATED_NEW")
                result["Attributes"] = DiffAttributes(oldItem ?? new JsonObject(), item, returnOld: false);
            AddConsumedCapacity(result, data, name, write: true, table);
            return result;
        }
    }

    // ── Query ────────────────────────────────────────────────────────────────────

    private JsonObject ActQuery(JsonObject data)
    {
        var name = data["TableName"]?.GetValue<string>() ?? "";
        lock (_lock)
        {
            var table      = GetTable(name);
            var eav        = data["ExpressionAttributeValues"]?.AsObject() ?? new JsonObject();
            var ean        = data["ExpressionAttributeNames"]?.AsObject() ?? new JsonObject();
            var keyCond    = data["KeyConditionExpression"]?.GetValue<string>() ?? "";
            var filterExpr = data["FilterExpression"]?.GetValue<string>() ?? "";

            // Legacy KeyConditions support: convert to KeyConditionExpression + EAV
            var legacyKeyConditions = data["KeyConditions"]?.AsObject();
            if (string.IsNullOrEmpty(keyCond) && legacyKeyConditions is not null)
            {
                (keyCond, eav) = ConvertKeyConditionsToExpression(legacyKeyConditions);
            }
            var limit      = data["Limit"]?.GetValue<int>();
            var scanFwd    = data["ScanIndexForward"]?.GetValue<bool>() ?? true;
            var esk        = data["ExclusiveStartKey"]?.AsObject();
            var indexName  = data["IndexName"]?.GetValue<string>();
            var select     = data["Select"]?.GetValue<string>() ?? "ALL_ATTRIBUTES";

            var (pkName, skName, _) = ResolveIndexKeys(table, indexName);
            var pkVal = ExtractPkFromCondition(keyCond, eav, ean, pkName ?? "");
            if (pkVal is null)
                throw new DdbException("ValidationException",
                    "Query condition missed key schema element", 400);

            // Gather candidates
            List<JsonObject> candidates;
            if (indexName is not null)
            {
                candidates = [];
                foreach (var pkBucket in table.Items.Values)
                    foreach (var it in pkBucket.Values)
                        if (pkName is not null && it.ContainsKey(pkName) &&
                            ExtractKeyVal(it[pkName]) == pkVal)
                            candidates.Add(it);
            }
            else
            {
                candidates = table.Items.TryGetValue(pkVal, out var bucket)
                    ? [..bucket.Values]
                    : [];
            }

            // Sort by sort key
            if (skName is not null)
            {
                var skType = GetAttrType(table, skName);
                candidates.Sort((a, b) =>
                {
                    var av = SortKeyValue(a.TryGetPropertyValue(skName, out var av2) ? av2 : null, skType);
                    var bv = SortKeyValue(b.TryGetPropertyValue(skName, out var bv2) ? bv2 : null, skType);
                    var cmp = av is string sa && bv is string sb
                        ? string.Compare(sa, sb, StringComparison.Ordinal)
                        : Comparer<decimal>.Default.Compare(
                            av is decimal da ? da : 0m,
                            bv is decimal db ? db : 0m);
                    return scanFwd ? cmp : -cmp;
                });
            }

            // Apply key condition filter (beyond PK equality already done above)
            if (!string.IsNullOrEmpty(keyCond))
                candidates = [..candidates.Where(it => EvaluateCondition(keyCond, it, eav, ean))];

            // Exclusive start key
            if (esk is not null)
                candidates = ApplyExclusiveStartKey(candidates, esk, pkName, skName, scanFwd);

            // Limit
            var hasMore = false;
            if (limit is not null && candidates.Count > limit)
            {
                hasMore    = true;
                candidates = candidates[..limit.Value];
            }

            var scannedCount = candidates.Count;

            // Filter expression
            List<JsonObject> filtered;
            var queryFilter = data["QueryFilter"]?.AsObject();
            if (queryFilter is not null && string.IsNullOrEmpty(filterExpr))
                filtered = [..candidates.Where(it => EvaluateLegacyFilter(it, queryFilter))];
            else if (!string.IsNullOrEmpty(filterExpr))
                filtered = [..candidates.Where(it => EvaluateCondition(filterExpr, it, eav, ean))];
            else
                filtered = candidates;

            JsonObject result;
            if (select == "COUNT")
            {
                result = new JsonObject { ["Count"] = filtered.Count, ["ScannedCount"] = scannedCount };
            }
            else
            {
                var arr = new JsonArray();
                foreach (var it in filtered) arr.Add(ApplyProjection(it, data));
                result = new JsonObject { ["Items"] = arr, ["Count"] = filtered.Count, ["ScannedCount"] = scannedCount };
            }

            if (hasMore && candidates.Count > 0)
            {
                var lek = BuildKey(candidates[^1], table.PkName, table.SkName);
                if (indexName is not null)
                {
                    var ik = BuildKey(candidates[^1], pkName, skName);
                    foreach (var kv in ik) if (!lek.ContainsKey(kv.Key)) lek[kv.Key] = kv.Value?.DeepClone();
                }
                result["LastEvaluatedKey"] = lek;
            }

            AddConsumedCapacity(result, data, name, write: false, table);
            return result;
        }
    }

    // ── Scan ─────────────────────────────────────────────────────────────────────

    private JsonObject ActScan(JsonObject data)
    {
        var name = data["TableName"]?.GetValue<string>() ?? "";
        lock (_lock)
        {
            var table      = GetTable(name);
            var filterExpr = data["FilterExpression"]?.GetValue<string>() ?? "";
            var eav        = data["ExpressionAttributeValues"]?.AsObject() ?? new JsonObject();
            var ean        = data["ExpressionAttributeNames"]?.AsObject() ?? new JsonObject();
            var limit      = data["Limit"]?.GetValue<int>();
            var esk        = data["ExclusiveStartKey"]?.AsObject();
            var indexName  = data["IndexName"]?.GetValue<string>();
            var select     = data["Select"]?.GetValue<string>() ?? "ALL_ATTRIBUTES";

            List<JsonObject> allItems = [];
            foreach (var pk in table.Items.Keys.OrderBy(k => k, StringComparer.Ordinal))
                foreach (var sk in table.Items[pk].Keys.OrderBy(k => k, StringComparer.Ordinal))
                    allItems.Add(table.Items[pk][sk]);

            if (indexName is not null)
            {
                var (pkIdx, _, isGsi) = ResolveIndexKeys(table, indexName);
                if (isGsi && pkIdx is not null)
                    allItems = [..allItems.Where(it => it.ContainsKey(pkIdx))];
            }

            if (esk is not null)
                allItems = ApplyExclusiveStartKeyScan(allItems, esk, table);

            var hasMore = false;
            if (limit is not null && allItems.Count > limit)
            {
                hasMore  = true;
                allItems = allItems[..limit.Value];
            }

            var scannedCount = allItems.Count;

            var scanFilter = data["ScanFilter"]?.AsObject() ?? data["QueryFilter"]?.AsObject();
            List<JsonObject> filtered;
            if (scanFilter is not null && string.IsNullOrEmpty(filterExpr))
                filtered = [..allItems.Where(it => EvaluateLegacyFilter(it, scanFilter))];
            else if (!string.IsNullOrEmpty(filterExpr))
                filtered = [..allItems.Where(it => EvaluateCondition(filterExpr, it, eav, ean))];
            else
                filtered = allItems;

            JsonObject result;
            if (select == "COUNT")
            {
                result = new JsonObject { ["Count"] = filtered.Count, ["ScannedCount"] = scannedCount };
            }
            else
            {
                var arr = new JsonArray();
                foreach (var it in filtered) arr.Add(ApplyProjection(it, data));
                result = new JsonObject { ["Items"] = arr, ["Count"] = filtered.Count, ["ScannedCount"] = scannedCount };
            }

            if (hasMore && allItems.Count > 0)
                result["LastEvaluatedKey"] = BuildKey(allItems[^1], table.PkName, table.SkName);

            AddConsumedCapacity(result, data, name, write: false, table);
            return result;
        }
    }

    // ── Batch operations ─────────────────────────────────────────────────────────

    private JsonObject ActBatchWriteItem(JsonObject data)
    {
        var requestItems = data["RequestItems"]?.AsObject() ?? new JsonObject();
        var unprocessed  = new JsonObject();

        lock (_lock)
        {
            foreach (var kv in requestItems)
            {
                var tableName = kv.Key;
                if (!_tables.TryGetValue(tableName, out var table))
                {
                    unprocessed[tableName] = kv.Value?.DeepClone();
                    continue;
                }

                foreach (var req in kv.Value?.AsArray() ?? [])
                {
                    if (req is null) continue;
                    if (req["PutRequest"] is JsonObject put)
                    {
                        var item = put["Item"]?.AsObject() ?? new JsonObject();
                        var (pkVal, skVal) = ResolveKeys(table, item, allowExtra: true);
                        var oldItem = GetItemInternal(table, pkVal, skVal);
                        SetItemInternal(table, pkVal, skVal, (JsonObject)item.DeepClone());
                        EmitStreamEvent(tableName, table, oldItem is null ? "INSERT" : "MODIFY", oldItem, item);
                    }
                    else if (req["DeleteRequest"] is JsonObject del)
                    {
                        var key = del["Key"]?.AsObject() ?? new JsonObject();
                        var (pkVal, skVal) = ResolveKeysStrict(table, key);
                        var oldItem = GetItemInternal(table, pkVal, skVal);
                        RemoveItemInternal(table, pkVal, skVal);
                        if (oldItem is not null)
                            EmitStreamEvent(tableName, table, "REMOVE", oldItem, null);
                    }
                }
                UpdateCounts(table);
            }
        }

        var result = new JsonObject { ["UnprocessedItems"] = unprocessed };
        var rc = data["ReturnConsumedCapacity"]?.GetValue<string>() ?? "NONE";
        if (rc != "NONE")
        {
            var consumed = new JsonArray();
            lock (_lock)
            {
                foreach (var kv in requestItems)
                {
                    if (!_tables.TryGetValue(kv.Key, out var t)) continue;
                    var gsiCount = t.Gsis.Count;
                    var units    = ((kv.Value?.AsArray()?.Count ?? 0) * (1.0 + gsiCount));
                    var entry    = new JsonObject { ["TableName"] = kv.Key, ["CapacityUnits"] = units };
                    if (rc == "INDEXES" && gsiCount > 0)
                    {
                        var gsiCap = new JsonObject();
                        foreach (var g in t.Gsis.OfType<JsonObject>())
                        {
                            var gsiName = g["IndexName"]?.GetValue<string>() ?? "";
                            gsiCap[gsiName] = new JsonObject { ["CapacityUnits"] = (double)(kv.Value?.AsArray()?.Count ?? 0) };
                        }
                        entry["GlobalSecondaryIndexes"] = gsiCap;
                    }
                    consumed.Add((JsonNode)entry);
                }
            }
            result["ConsumedCapacity"] = consumed;
        }
        return result;
    }

    private JsonObject ActBatchGetItem(JsonObject data)
    {
        var requestItems = data["RequestItems"]?.AsObject() ?? new JsonObject();
        var responses    = new JsonObject();
        var unprocessed  = new JsonObject();

        lock (_lock)
        {
            foreach (var kv in requestItems)
            {
                var tableName = kv.Key;
                if (!_tables.TryGetValue(tableName, out var table))
                {
                    unprocessed[tableName] = kv.Value?.DeepClone();
                    continue;
                }

                var config = kv.Value?.AsObject() ?? new JsonObject();
                var proj   = config["ProjectionExpression"]?.GetValue<string>();
                var cfgEan = config["ExpressionAttributeNames"]?.AsObject() ?? new JsonObject();
                var arr    = new JsonArray();

                foreach (var keyNode in config["Keys"]?.AsArray() ?? [])
                {
                    var key = keyNode?.AsObject() ?? new JsonObject();
                    var (pkVal, skVal) = ResolveKeysStrict(table, key);
                    var item = GetItemInternal(table, pkVal, skVal);
                    if (item is not null)
                    {
                        var projected = proj is not null
                            ? ProjectItem(item, proj, cfgEan)
                            : (JsonObject)item.DeepClone();
                        arr.Add((JsonNode)projected);
                    }
                }
                responses[tableName] = arr;
            }
        }

        return new JsonObject { ["Responses"] = responses, ["UnprocessedKeys"] = unprocessed };
    }

    // ── Transact operations ───────────────────────────────────────────────────────

    private JsonObject ActTransactWriteItems(JsonObject data)
    {
        var items = data["TransactItems"]?.AsArray() ?? [];
        lock (_lock)
        {
            // Phase 1: condition checks
            for (var idx = 0; idx < items.Count; idx++)
            {
                var transact = items[idx];
                if (transact is null) continue;
                var (opType, op) = ExtractTransactOp(transact.AsObject());
                if (op is null) continue;

                var tableName = op["TableName"]?.GetValue<string>() ?? "";
                var t = GetTable(tableName);
                var cond = op["ConditionExpression"]?.GetValue<string>() ?? "";
                if (!string.IsNullOrEmpty(cond))
                {
                    JsonObject? existing;
                    if (opType == "Put")
                    {
                        var itm = op["Item"]?.AsObject() ?? new JsonObject();
                        var (pk2, sk2) = ResolveKeys(t, itm, allowExtra: true);
                        existing = GetItemInternal(t, pk2, sk2);
                    }
                    else
                    {
                        var k = op["Key"]?.AsObject() ?? new JsonObject();
                        var (pk2, sk2) = ResolveKeysStrict(t, k);
                        existing = GetItemInternal(t, pk2, sk2);
                    }
                    var eav = op["ExpressionAttributeValues"]?.AsObject() ?? new JsonObject();
                    var ean = op["ExpressionAttributeNames"]?.AsObject() ?? new JsonObject();
                    if (!EvaluateCondition(cond, existing ?? new JsonObject(), eav, ean))
                        ThrowTransactCanceled(items.Count, idx, "ConditionalCheckFailed");
                }
            }

            // Phase 2: apply
            foreach (var transact in items)
            {
                if (transact is null) continue;
                var (opType, op) = ExtractTransactOp(transact.AsObject());
                if (op is null || opType == "ConditionCheck") continue;

                var tableName = op["TableName"]?.GetValue<string>() ?? "";
                if (!_tables.TryGetValue(tableName, out var t)) continue;

                var eav = op["ExpressionAttributeValues"]?.AsObject() ?? new JsonObject();
                var ean = op["ExpressionAttributeNames"]?.AsObject() ?? new JsonObject();

                if (opType == "Put")
                {
                    var item = op["Item"]?.AsObject() ?? new JsonObject();
                    var (pkVal, skVal) = ResolveKeys(t, item, allowExtra: true);
                    var oldItem = GetItemInternal(t, pkVal, skVal);
                    SetItemInternal(t, pkVal, skVal, (JsonObject)item.DeepClone());
                    EmitStreamEvent(tableName, t, oldItem is null ? "INSERT" : "MODIFY", oldItem, item);
                }
                else if (opType == "Delete")
                {
                    var key = op["Key"]?.AsObject() ?? new JsonObject();
                    var (pkVal, skVal) = ResolveKeysStrict(t, key);
                    var oldItem = GetItemInternal(t, pkVal, skVal);
                    RemoveItemInternal(t, pkVal, skVal);
                    if (oldItem is not null)
                        EmitStreamEvent(tableName, t, "REMOVE", oldItem, null);
                }
                else if (opType == "Update")
                {
                    var key = op["Key"]?.AsObject() ?? new JsonObject();
                    var (pkVal, skVal) = ResolveKeysStrict(t, key);
                    var existing = GetItemInternal(t, pkVal, skVal);
                    var oldItem  = existing is null ? null : (JsonObject)existing.DeepClone();
                    var item     = existing is null
                        ? (JsonObject)((JsonNode)key).DeepClone()
                        : (JsonObject)existing.DeepClone();
                    var ue = op["UpdateExpression"]?.GetValue<string>() ?? "";
                    if (!string.IsNullOrEmpty(ue))
                        item = ApplyUpdateExpression(item, ue, eav, ean);
                    SetItemInternal(t, pkVal, skVal, item);
                    EmitStreamEvent(tableName, t, oldItem is null ? "INSERT" : "MODIFY", oldItem, item);
                }
                UpdateCounts(t);
            }
        }
        return new JsonObject();
    }

    private JsonObject ActTransactGetItems(JsonObject data)
    {
        var items = data["TransactItems"]?.AsArray() ?? [];
        lock (_lock)
        {
            var responses = new JsonArray();
            foreach (var transact in items)
            {
                var getOp = transact?["Get"]?.AsObject() ?? new JsonObject();
                var tableName = getOp["TableName"]?.GetValue<string>() ?? "";
                if (!_tables.TryGetValue(tableName, out var t))
                {
                    responses.Add((JsonNode)new JsonObject());
                    continue;
                }
                var key = getOp["Key"]?.AsObject() ?? new JsonObject();
                var (pkVal, skVal) = ResolveKeysStrict(t, key);
                var item = GetItemInternal(t, pkVal, skVal);
                if (item is not null)
                {
                    var proj = getOp["ProjectionExpression"]?.GetValue<string>();
                    var ean  = getOp["ExpressionAttributeNames"]?.AsObject() ?? new JsonObject();
                    var projected = proj is not null ? ProjectItem(item, proj, ean) : (JsonObject)item.DeepClone();
                    responses.Add((JsonNode)new JsonObject { ["Item"] = projected });
                }
                else
                {
                    responses.Add((JsonNode)new JsonObject());
                }
            }
            return new JsonObject { ["Responses"] = responses };
        }
    }

    private static (string opType, JsonObject? op) ExtractTransactOp(JsonObject transact)
    {
        foreach (var opType in new[] { "ConditionCheck", "Put", "Delete", "Update" })
            if (transact[opType] is JsonObject op) return (opType, op);
        return ("", null);
    }

    private static void ThrowTransactCanceled(int total, int failedIdx, string reason)
    {
        var reasons = new JsonArray();
        for (var i = 0; i < total; i++)
            reasons.Add((JsonNode)(i == failedIdx ? new JsonObject { ["Code"] = reason, ["Message"] = "The conditional request failed" } : new JsonObject { ["Code"] = "None" }));

        var codes = string.Join(", ", Enumerable.Range(0, total)
            .Select(i => i == failedIdx ? reason : "None"));

        var body = new JsonObject
        {
            ["__type"]             = "TransactionCanceledException",
            ["message"]            = $"Transaction cancelled, please refer cancellation reasons for specific reasons [{codes}]",
            ["CancellationReasons"] = reasons,
        };
        var rawJson = body.ToJsonString();
        throw new DdbException("TransactionCanceledException",
            $"Transaction cancelled [{codes}]", 400, rawJson);
    }

    // ── TTL ──────────────────────────────────────────────────────────────────────

    private JsonObject ActDescribeTtl(JsonObject data)
    {
        var name = data["TableName"]?.GetValue<string>() ?? "";
        lock (_lock)
        {
            GetTable(name); // validate exists
            _ttlSettings.TryGetValue(name, out var setting);
            var desc = new JsonObject
            {
                ["TimeToLiveStatus"] = setting?.Status ?? "DISABLED",
            };
            if (setting?.AttributeName is not null)
                desc["AttributeName"] = setting.AttributeName;
            return new JsonObject { ["TimeToLiveDescription"] = desc };
        }
    }

    private JsonObject ActUpdateTtl(JsonObject data)
    {
        var name = data["TableName"]?.GetValue<string>() ?? "";
        lock (_lock)
        {
            GetTable(name);
            var spec = data["TimeToLiveSpecification"]?.AsObject() ?? new JsonObject();
            var enabled = spec["Enabled"]?.GetValue<bool>() ?? false;
            _ttlSettings[name] = new DdbTtlSetting
            {
                Status        = enabled ? "ENABLED" : "DISABLED",
                AttributeName = spec["AttributeName"]?.GetValue<string>() ?? "",
            };
            return new JsonObject { ["TimeToLiveSpecification"] = spec.DeepClone() };
        }
    }

    // ── Continuous backups ────────────────────────────────────────────────────────

    private JsonObject ActDescribeContinuousBackups(JsonObject data)
    {
        var name = data["TableName"]?.GetValue<string>() ?? "";
        lock (_lock)
        {
            GetTable(name);
            _pitrSettings.TryGetValue(name, out var pitrEnabled);
            return new JsonObject
            {
                ["ContinuousBackupsDescription"] = new JsonObject
                {
                    ["ContinuousBackupsStatus"] = "ENABLED",
                    ["PointInTimeRecoveryDescription"] = new JsonObject
                    {
                        ["PointInTimeRecoveryStatus"]    = pitrEnabled ? "ENABLED" : "DISABLED",
                        ["EarliestRestorableDateTime"]   = 0,
                        ["LatestRestorableDateTime"]     = 0,
                    },
                },
            };
        }
    }

    private JsonObject ActUpdateContinuousBackups(JsonObject data)
    {
        var name = data["TableName"]?.GetValue<string>() ?? "";
        lock (_lock)
        {
            GetTable(name);
            var spec    = data["PointInTimeRecoverySpecification"]?.AsObject() ?? new JsonObject();
            var enabled = spec["PointInTimeRecoveryEnabled"]?.GetValue<bool>() ?? false;
            _pitrSettings[name] = enabled;
            return new JsonObject
            {
                ["ContinuousBackupsDescription"] = new JsonObject
                {
                    ["ContinuousBackupsStatus"] = "ENABLED",
                    ["PointInTimeRecoveryDescription"] = new JsonObject
                    {
                        ["PointInTimeRecoveryStatus"] = enabled ? "ENABLED" : "DISABLED",
                    },
                },
            };
        }
    }

    // ── Endpoints ─────────────────────────────────────────────────────────────────

    private static JsonObject ActDescribeEndpoints(JsonObject _)
        => new()
        {
            ["Endpoints"] = new JsonArray
            {
                (JsonNode)new JsonObject { ["Address"] = "dynamodb.us-east-1.amazonaws.com", ["CachePeriodInMinutes"] = 1440 },
            },
        };

    // ── Tags ──────────────────────────────────────────────────────────────────────

    private JsonObject ActTagResource(JsonObject data)
    {
        var arn  = data["ResourceArn"]?.GetValue<string>() ?? "";
        var tags = data["Tags"]?.AsArray() ?? [];
        lock (_lock)
        {
            if (!_tags.TryGetValue(arn, out var existing))
                existing = [];
            var keyMap = existing.Select((t, i) => (Key: t["Key"]?.GetValue<string>() ?? "", i))
                .ToDictionary(x => x.Key, x => x.i);

            foreach (var tag in tags.OfType<JsonObject>())
            {
                var k = tag["Key"]?.GetValue<string>() ?? "";
                if (keyMap.TryGetValue(k, out var i))
                    existing[i] = tag;
                else
                    existing.Add(tag);
            }
            _tags[arn] = existing;
        }
        return new JsonObject();
    }

    private JsonObject ActUntagResource(JsonObject data)
    {
        var arn  = data["ResourceArn"]?.GetValue<string>() ?? "";
        var keys = data["TagKeys"]?.AsArray().Select(k => k?.GetValue<string>() ?? "").ToHashSet() ?? [];
        lock (_lock)
        {
            if (_tags.TryGetValue(arn, out var existing))
                _tags[arn] = [..existing.Where(t => !keys.Contains(t["Key"]?.GetValue<string>() ?? ""))];
        }
        return new JsonObject();
    }

    private JsonObject ActListTags(JsonObject data)
    {
        var arn = data["ResourceArn"]?.GetValue<string>() ?? "";
        lock (_lock)
        {
            _tags.TryGetValue(arn, out var existing);
            var arr = new JsonArray();
            foreach (var t in existing ?? []) arr.Add(t.DeepClone());
            return new JsonObject { ["Tags"] = arr };
        }
    }

    // ── Item storage helpers ──────────────────────────────────────────────────────

    private static JsonObject? GetItemInternal(DdbTable table, string pkVal, string skVal)
    {
        if (!table.Items.TryGetValue(pkVal, out var bucket)) return null;
        bucket.TryGetValue(skVal, out var item);
        return item;
    }

    private static void SetItemInternal(DdbTable table, string pkVal, string skVal, JsonObject item)
    {
        if (!table.Items.TryGetValue(pkVal, out var bucket))
        {
            bucket = new Dictionary<string, JsonObject>(StringComparer.Ordinal);
            table.Items[pkVal] = bucket;
        }
        bucket[skVal] = item;
    }

    private static void RemoveItemInternal(DdbTable table, string pkVal, string skVal)
    {
        if (table.Items.TryGetValue(pkVal, out var bucket))
        {
            bucket.Remove(skVal);
            if (bucket.Count == 0)
                table.Items.Remove(pkVal);
        }
    }

    private static void UpdateCounts(DdbTable table)
    {
        var count = table.Items.Values.Sum(b => b.Count);
        table.ItemCount    = count;
        table.TableSizeBytes = count * 200L;
    }

    // ── Key resolution ────────────────────────────────────────────────────────────

    private DdbTable GetTable(string name)
    {
        if (!_tables.TryGetValue(name, out var table))
            throw new DdbException("ResourceNotFoundException",
                $"Requested resource not found: Table: {name} not found", 400);
        return table;
    }

    private static (string pkVal, string skVal) ResolveKeys(
        DdbTable table, JsonObject attrs, bool allowExtra)
    {
        var pkVal = ExtractKeyVal(attrs.TryGetPropertyValue(table.PkName ?? "", out var pk) ? pk : null);
        var skVal = table.SkName is not null
            ? ExtractKeyVal(attrs.TryGetPropertyValue(table.SkName, out var sk) ? sk : null)
            : "__no_sort__";
        return (pkVal, skVal);
    }

    private static (string pkVal, string skVal) ResolveKeysStrict(DdbTable table, JsonObject key)
    {
        const string SchemaError = "The provided key element does not match the schema";

        // Ensure no extra key attributes are provided
        var validKeyNames = new HashSet<string>(StringComparer.Ordinal);
        if (table.PkName is not null) validKeyNames.Add(table.PkName);
        if (table.SkName is not null) validKeyNames.Add(table.SkName);
        foreach (var kv in key)
            if (!validKeyNames.Contains(kv.Key))
                throw new DdbException("ValidationException", SchemaError, 400);

        // Validate and extract PK
        if (!key.TryGetPropertyValue(table.PkName ?? "", out var pkNode) || pkNode is null)
            throw new DdbException("ValidationException", SchemaError, 400);
        var pkType = GetKeyAttrType(table, table.PkName ?? "");
        if (!NodeMatchesType(pkNode, pkType))
            throw new DdbException("ValidationException", SchemaError, 400);
        var pkVal = ExtractKeyVal(pkNode);

        // Validate and extract SK (required if table has SK)
        string skVal;
        if (table.SkName is not null)
        {
            if (!key.TryGetPropertyValue(table.SkName, out var skNode) || skNode is null)
                throw new DdbException("ValidationException", SchemaError, 400);
            var skType = GetKeyAttrType(table, table.SkName);
            if (!NodeMatchesType(skNode, skType))
                throw new DdbException("ValidationException", SchemaError, 400);
            skVal = ExtractKeyVal(skNode);
        }
        else
        {
            skVal = "__no_sort__";
        }

        return (pkVal, skVal);
    }

    private static string GetKeyAttrType(DdbTable table, string attrName)
    {
        foreach (var ad in table.AttributeDefinitions.OfType<JsonObject>())
            if (ad["AttributeName"]?.GetValue<string>() == attrName)
                return ad["AttributeType"]?.GetValue<string>() ?? "S";
        return "S";
    }

    private static bool NodeMatchesType(JsonNode node, string expectedType)
    {
        if (node is not JsonObject obj) return false;
        return expectedType switch
        {
            "S" => obj.ContainsKey("S"),
            "N" => obj.ContainsKey("N"),
            "B" => obj.ContainsKey("B"),
            _   => true,
        };
    }

    private static string ExtractKeyVal(JsonNode? attr)
    {
        if (attr is null) return "";
        if (attr is JsonObject obj)
        {
            if (obj.TryGetPropertyValue("S", out var s)) return s?.GetValue<string>() ?? "";
            if (obj.TryGetPropertyValue("N", out var n)) return n?.GetValue<string>() ?? "";
            if (obj.TryGetPropertyValue("B", out var b)) return b?.GetValue<string>() ?? "";
        }
        return attr.ToString();
    }

    private static (string? pkName, string? skName, bool isGsi) ResolveIndexKeys(
        DdbTable table, string? indexName)
    {
        if (indexName is null) return (table.PkName, table.SkName, false);

        foreach (var gsi in table.Gsis.OfType<JsonObject>())
        {
            if (gsi["IndexName"]?.GetValue<string>() != indexName) continue;
            string? pk = null, sk = null;
            foreach (var ks in gsi["KeySchema"]?.AsArray() ?? [])
            {
                if (ks?["KeyType"]?.GetValue<string>() == "HASH")  pk = ks["AttributeName"]?.GetValue<string>();
                if (ks?["KeyType"]?.GetValue<string>() == "RANGE") sk = ks["AttributeName"]?.GetValue<string>();
            }
            return (pk, sk, true);
        }

        foreach (var lsi in table.Lsis.OfType<JsonObject>())
        {
            if (lsi["IndexName"]?.GetValue<string>() != indexName) continue;
            string? pk = null, sk = null;
            foreach (var ks in lsi["KeySchema"]?.AsArray() ?? [])
            {
                if (ks?["KeyType"]?.GetValue<string>() == "HASH")  pk = ks["AttributeName"]?.GetValue<string>();
                if (ks?["KeyType"]?.GetValue<string>() == "RANGE") sk = ks["AttributeName"]?.GetValue<string>();
            }
            return (pk, sk, false);
        }

        return (table.PkName, table.SkName, false);
    }

    private static string GetAttrType(DdbTable table, string attrName)
    {
        foreach (var ad in table.AttributeDefinitions.OfType<JsonObject>())
            if (ad["AttributeName"]?.GetValue<string>() == attrName)
                return ad["AttributeType"]?.GetValue<string>() ?? "S";
        return "S";
    }

    private static object SortKeyValue(JsonNode? attr, string skType)
    {
        if (attr is null) return skType == "N" ? (object)0m : "";
        var val = ExtractKeyVal(attr);
        if (skType == "N")
        {
            if (decimal.TryParse(val, out var d)) return d;
            return 0m;
        }
        return val;
    }

    private static string? ExtractPkFromCondition(
        string condition, JsonObject eav, JsonObject ean, string pkName)
    {
        if (string.IsNullOrEmpty(condition)) return null;

        var pkRefs = new List<string> { pkName };
        foreach (var kv in ean)
            if (kv.Value?.GetValue<string>() == pkName)
                pkRefs.Add(kv.Key);

        foreach (var refName in pkRefs)
        {
            // ref = :val
            var m = System.Text.RegularExpressions.Regex.Match(
                condition,
                $@"(?:^|[\s(]){System.Text.RegularExpressions.Regex.Escape(refName)}\s*=\s*(:\w+)");
            if (m.Success && eav.TryGetPropertyValue(m.Groups[1].Value, out var v1))
                return ExtractKeyVal(v1);

            // :val = ref
            var m2 = System.Text.RegularExpressions.Regex.Match(
                condition,
                $@"(:\w+)\s*=\s*{System.Text.RegularExpressions.Regex.Escape(refName)}(?:$|[\s)])");
            if (m2.Success && eav.TryGetPropertyValue(m2.Groups[1].Value, out var v2))
                return ExtractKeyVal(v2);
        }
        return null;
    }

    /// <summary>
    /// Converts legacy <c>KeyConditions</c> format to a <c>KeyConditionExpression</c> string
    /// plus <c>ExpressionAttributeValues</c>. The legacy format is:
    /// <c>{ "attrName": { "ComparisonOperator": "EQ", "AttributeValueList": [{"S":"val"}] } }</c>
    /// </summary>
    private static (string keyCond, JsonObject eav) ConvertKeyConditionsToExpression(
        JsonObject keyConditions)
    {
        var parts = new List<string>();
        var eav = new JsonObject();
        var idx = 0;

        foreach (var kv in keyConditions)
        {
            var attrName = kv.Key;
            var condition = kv.Value?.AsObject() ?? new JsonObject();
            var op = condition["ComparisonOperator"]?.GetValue<string>() ?? "";
            var attrVals = condition["AttributeValueList"]?.AsArray() ?? [];

            var placeholder = $":kcv{idx++}";

            switch (op)
            {
                case "EQ":
                    parts.Add($"{attrName} = {placeholder}");
                    if (attrVals.Count > 0) eav[placeholder] = attrVals[0]?.DeepClone();
                    break;
                case "LE":
                    parts.Add($"{attrName} <= {placeholder}");
                    if (attrVals.Count > 0) eav[placeholder] = attrVals[0]?.DeepClone();
                    break;
                case "LT":
                    parts.Add($"{attrName} < {placeholder}");
                    if (attrVals.Count > 0) eav[placeholder] = attrVals[0]?.DeepClone();
                    break;
                case "GE":
                    parts.Add($"{attrName} >= {placeholder}");
                    if (attrVals.Count > 0) eav[placeholder] = attrVals[0]?.DeepClone();
                    break;
                case "GT":
                    parts.Add($"{attrName} > {placeholder}");
                    if (attrVals.Count > 0) eav[placeholder] = attrVals[0]?.DeepClone();
                    break;
                case "BEGINS_WITH":
                    parts.Add($"begins_with({attrName}, {placeholder})");
                    if (attrVals.Count > 0) eav[placeholder] = attrVals[0]?.DeepClone();
                    break;
                case "BETWEEN":
                    var placeholder2 = $":kcv{idx++}";
                    parts.Add($"{attrName} BETWEEN {placeholder} AND {placeholder2}");
                    if (attrVals.Count > 0) eav[placeholder] = attrVals[0]?.DeepClone();
                    if (attrVals.Count > 1) eav[placeholder2] = attrVals[1]?.DeepClone();
                    break;
            }
        }

        return (string.Join(" AND ", parts), eav);
    }

    private static JsonObject BuildKey(JsonObject item, string? pkName, string? skName)
    {
        var key = new JsonObject();
        if (pkName is not null && item.TryGetPropertyValue(pkName, out var pk))
            key[pkName] = pk?.DeepClone();
        if (skName is not null && item.TryGetPropertyValue(skName, out var sk))
            key[skName] = sk?.DeepClone();
        return key;
    }

    // ── Pagination helpers ────────────────────────────────────────────────────────

    private static List<JsonObject> ApplyExclusiveStartKey(
        List<JsonObject> candidates, JsonObject esk, string? pkName, string? skName, bool scanFwd)
    {
        if (candidates.Count == 0) return candidates;

        if (skName is null || !esk.ContainsKey(skName))
        {
            var startPk = ExtractKeyVal(esk.TryGetPropertyValue(pkName ?? "", out var p) ? p : null);
            var found = false;
            var result = new List<JsonObject>();
            foreach (var item in candidates)
            {
                if (found)
                    result.Add(item);
                else if (pkName is not null && item.TryGetPropertyValue(pkName, out var ipk) &&
                    ExtractKeyVal(ipk) == startPk)
                    found = true;
            }
            return result;
        }

        var startSk = esk.TryGetPropertyValue(skName, out var sv) ? sv : null;
        return [..candidates.Where(item =>
        {
            if (!item.TryGetPropertyValue(skName, out var sk)) return false;
            return scanFwd
                ? CompareDdb(sk, ">", startSk)
                : CompareDdb(sk, "<", startSk);
        })];
    }

    private static List<JsonObject> ApplyExclusiveStartKeyScan(
        List<JsonObject> allItems, JsonObject esk, DdbTable table)
    {
        var pkName  = table.PkName ?? "";
        var skName  = table.SkName;
        var startPk = ExtractKeyVal(esk.TryGetPropertyValue(pkName, out var pk) ? pk : null);
        var startSk = skName is not null && esk.TryGetPropertyValue(skName, out var sk)
            ? ExtractKeyVal(sk) : "";

        return [..allItems.Where(item =>
        {
            var itemPk = item.TryGetPropertyValue(pkName, out var ipk) ? ExtractKeyVal(ipk) : "";
            var itemSk = skName is not null && item.TryGetPropertyValue(skName, out var isk)
                ? ExtractKeyVal(isk) : "";
            return string.CompareOrdinal(itemPk + "\0" + itemSk, startPk + "\0" + startSk) > 0;
        })];
    }

    // ── Projection ────────────────────────────────────────────────────────────────

    private static JsonNode ApplyProjection(JsonObject item, JsonObject data)
    {
        var proj = data["ProjectionExpression"]?.GetValue<string>();
        var ean  = data["ExpressionAttributeNames"]?.AsObject() ?? new JsonObject();
        if (proj is null) return item.DeepClone();
        return ProjectItem(item, proj, ean);
    }

    private static JsonObject ProjectItem(JsonObject item, string projExpr, JsonObject attrNames)
    {
        var attrs  = projExpr.Split(',');
        var result = new JsonObject();
        foreach (var attr in attrs)
        {
            var a       = attr.Trim();
            var first   = a.Split('.', '[')[0];
            var resolved = first.StartsWith('#') && attrNames.TryGetPropertyValue(first, out var v)
                ? v?.GetValue<string>() ?? first
                : first;
            if (item.TryGetPropertyValue(resolved, out var val))
                result[resolved] = val?.DeepClone();
        }
        return result;
    }

    // ── Expression evaluation ─────────────────────────────────────────────────────

    private static bool EvaluateCondition(
        string expr, JsonObject item, JsonObject eav, JsonObject ean)
    {
        if (string.IsNullOrWhiteSpace(expr)) return true;
        try
        {
            var tokens = Tokenize(expr);
            var eval   = new ExprEval(tokens, item, eav, ean);
            return eval.Evaluate();
        }
        catch
        {
            return true; // lenient fallback
        }
    }

    private static bool EvaluateLegacyFilter(JsonObject item, JsonObject scanFilter)
    {
        foreach (var kv in scanFilter)
        {
            var condition = kv.Value?.AsObject() ?? new JsonObject();
            var op        = condition["ComparisonOperator"]?.GetValue<string>() ?? "";
            var attrVals  = condition["AttributeValueList"]?.AsArray() ?? [];
            item.TryGetPropertyValue(kv.Key, out var itemVal);

            if (op == "EQ")
            {
                if (itemVal is null || !DdbEquals(itemVal, attrVals[0])) return false;
            }
            else if (op == "NE")
            {
                if (itemVal is not null && DdbEquals(itemVal, attrVals[0])) return false;
            }
            else if (op == "NOT_NULL")
            {
                if (itemVal is null) return false;
            }
            else if (op == "NULL")
            {
                if (itemVal is not null) return false;
            }
            else if (op == "CONTAINS")
            {
                var val    = itemVal is not null ? ExtractKeyVal(itemVal) : "";
                var target = attrVals.Count > 0 ? ExtractKeyVal(attrVals[0]) : "";
                if (!val.Contains(target, StringComparison.Ordinal)) return false;
            }
            else if (op == "BEGINS_WITH")
            {
                var val    = itemVal is not null ? ExtractKeyVal(itemVal) : "";
                var target = attrVals.Count > 0 ? ExtractKeyVal(attrVals[0]) : "";
                if (!val.StartsWith(target, StringComparison.Ordinal)) return false;
            }
        }
        return true;
    }

    // ── Update expression ─────────────────────────────────────────────────────────

    private static JsonObject ApplyUpdateExpression(
        JsonObject item, string expr, JsonObject eav, JsonObject ean)
    {
        item = (JsonObject)item.DeepClone();
        var tokens  = Tokenize(expr);
        var clauses = new Dictionary<string, List<Token>>(StringComparer.Ordinal);
        string? current = null;
        var currentTokens = new List<Token>();

        foreach (var tok in tokens)
        {
            if (tok.Type == TokenType.Ident &&
                tok.Value.ToUpperInvariant() is "SET" or "REMOVE" or "ADD" or "DELETE")
            {
                if (current is not null) clauses[current] = currentTokens;
                current       = tok.Value.ToUpperInvariant();
                currentTokens = [];
            }
            else if (tok.Type != TokenType.Eof)
            {
                currentTokens.Add(tok);
            }
        }
        if (current is not null) clauses[current] = currentTokens;

        if (clauses.TryGetValue("SET",    out var setToks))    ApplySet(item, setToks, eav, ean);
        if (clauses.TryGetValue("REMOVE", out var remToks))    ApplyRemove(item, remToks, ean);
        if (clauses.TryGetValue("ADD",    out var addToks))    ApplyAdd(item, addToks, eav, ean);
        if (clauses.TryGetValue("DELETE", out var delToks))    ApplyDelete(item, delToks, eav, ean);
        return item;
    }

    private static void ApplySet(JsonObject item, List<Token> tokens, JsonObject eav, JsonObject ean)
    {
        foreach (var assignment in SplitByComma(tokens))
        {
            var eqIdx = assignment.FindIndex(t => t.Type == TokenType.Eq);
            if (eqIdx < 0) continue;
            var path  = ParsePathFromTokens(assignment[..eqIdx], ean);
            var value = EvalSetValue(assignment[(eqIdx + 1)..], item, eav, ean);
            if (path.Count > 0 && value is not null)
                SetAtPath(item, path, value);
        }
    }

    private static JsonNode? EvalSetValue(
        List<Token> tokens, JsonObject item, JsonObject eav, JsonObject ean)
    {
        if (tokens.Count == 0) return null;

        // Check for +/- at top level
        var depth = 0;
        for (var i = 0; i < tokens.Count; i++)
        {
            if (tokens[i].Type == TokenType.LParen) depth++;
            else if (tokens[i].Type == TokenType.RParen) depth--;
            else if (depth == 0 && i > 0 &&
                     tokens[i].Type is TokenType.Plus or TokenType.Minus)
            {
                var left  = EvalSetValue(tokens[..i], item, eav, ean);
                var right = EvalSetValue(tokens[(i + 1)..], item, eav, ean);
                if (left is JsonObject lo && right is JsonObject ro &&
                    lo.TryGetPropertyValue("N", out var lnv) &&
                    ro.TryGetPropertyValue("N", out var rnv))
                {
                    var lv = decimal.Parse(lnv?.GetValue<string>() ?? "0");
                    var rv = decimal.Parse(rnv?.GetValue<string>() ?? "0");
                    return new JsonObject { ["N"] = tokens[i].Type == TokenType.Plus
                        ? (lv + rv).ToString()
                        : (lv - rv).ToString() };
                }
                return left;
            }
        }

        // Function call?
        if (tokens.Count >= 2 && tokens[0].Type == TokenType.Ident &&
            tokens[1].Type == TokenType.LParen)
        {
            var fn = tokens[0].Value.ToLowerInvariant();
            var end = FindMatchingParen(tokens, 1);
            if (end is not null)
            {
                var inner = tokens[2..end.Value];
                if (fn == "if_not_exists")
                {
                    var parts = SplitByComma(inner);
                    if (parts.Count == 2)
                    {
                        var path = ParsePathFromTokens(parts[0], ean);
                        var existing = GetAtPath(item, path);
                        return existing ?? EvalSetValue(parts[1], item, eav, ean);
                    }
                }
                else if (fn == "list_append")
                {
                    var parts = SplitByComma(inner);
                    if (parts.Count == 2)
                    {
                        var a  = EvalSetValue(parts[0], item, eav, ean);
                        var b  = EvalSetValue(parts[1], item, eav, ean);
                        var al = a is JsonObject ao && ao.TryGetPropertyValue("L", out var alv)
                            ? alv?.AsArray() ?? [] : [];
                        var bl = b is JsonObject bo && bo.TryGetPropertyValue("L", out var blv)
                            ? blv?.AsArray() ?? [] : [];
                        var result = new JsonArray();
                        foreach (var e in al) result.Add(e?.DeepClone());
                        foreach (var e in bl) result.Add(e?.DeepClone());
                        return new JsonObject { ["L"] = result };
                    }
                }
            }
        }

        if (tokens.Count == 1 && tokens[0].Type == TokenType.ValueRef)
        {
            eav.TryGetPropertyValue(tokens[0].Value, out var v);
            return v?.DeepClone();
        }

        var pathP = ParsePathFromTokens(tokens, ean);
        if (pathP.Count > 0)
        {
            var existing = GetAtPath(item, pathP);
            if (existing is not null) return existing;
        }

        return null;
    }

    private static void ApplyRemove(JsonObject item, List<Token> tokens, JsonObject ean)
    {
        foreach (var pathTokens in SplitByComma(tokens))
        {
            var path = ParsePathFromTokens(pathTokens, ean);
            if (path.Count > 0) RemoveAtPath(item, path);
        }
    }

    private static void ApplyAdd(JsonObject item, List<Token> tokens, JsonObject eav, JsonObject ean)
    {
        foreach (var part in SplitByComma(tokens))
        {
            var valIdx = -1;
            for (var i = part.Count - 1; i >= 0; i--)
                if (part[i].Type == TokenType.ValueRef) { valIdx = i; break; }
            if (valIdx < 0) continue;

            var path   = ParsePathFromTokens(part[..valIdx], ean);
            if (!eav.TryGetPropertyValue(part[valIdx].Value, out var addValNode)) continue;
            if (path.Count == 0 || addValNode is null) continue;

            var addVal  = addValNode.AsObject();
            var existing = GetAtPath(item, path);

            if (addVal.TryGetPropertyValue("N", out var nv))
            {
                var inc = decimal.Parse(nv?.GetValue<string>() ?? "0");
                var cur = existing is JsonObject eo && eo.TryGetPropertyValue("N", out var cv)
                    ? decimal.Parse(cv?.GetValue<string>() ?? "0") : 0m;
                SetAtPath(item, path, new JsonObject { ["N"] = (cur + inc).ToString() });
            }
            else foreach (var setType in new[] { "SS", "NS", "BS" })
            {
                if (!addVal.TryGetPropertyValue(setType, out var addSet)) continue;
                var cur = existing is JsonObject exo && exo.TryGetPropertyValue(setType, out var cs)
                    ? cs?.AsArray()?.Select(e => e?.GetValue<string>() ?? "").ToHashSet() ?? []
                    : [];
                cur.UnionWith(addSet?.AsArray()?.Select(e => e?.GetValue<string>() ?? "") ?? []);
                SetAtPath(item, path, new JsonObject { [setType] = ArrayFromStrings(cur.OrderBy(s => s)) });
                break;
            }
        }
    }

    private static void ApplyDelete(JsonObject item, List<Token> tokens, JsonObject eav, JsonObject ean)
    {
        foreach (var part in SplitByComma(tokens))
        {
            var valIdx = -1;
            for (var i = part.Count - 1; i >= 0; i--)
                if (part[i].Type == TokenType.ValueRef) { valIdx = i; break; }
            if (valIdx < 0) continue;

            var path   = ParsePathFromTokens(part[..valIdx], ean);
            if (!eav.TryGetPropertyValue(part[valIdx].Value, out var delValNode)) continue;
            if (path.Count == 0 || delValNode is null) continue;

            var delVal   = delValNode.AsObject();
            var existing = GetAtPath(item, path);
            if (existing is null) continue;

            foreach (var setType in new[] { "SS", "NS", "BS" })
            {
                if (!delVal.TryGetPropertyValue(setType, out var delSet)) continue;
                if (existing is not JsonObject exo || !exo.TryGetPropertyValue(setType, out var cs)) break;
                var delSet2 = delSet?.AsArray()?.Select(e => e?.GetValue<string>() ?? "").ToHashSet() ?? [];
                var remaining = cs?.AsArray()?.Select(e => e?.GetValue<string>() ?? "")
                    .Where(s => !delSet2.Contains(s)).ToList() ?? [];
                if (remaining.Count > 0)
                    SetAtPath(item, path, new JsonObject { [setType] = ArrayFromStrings(remaining) });
                else
                    RemoveAtPath(item, path);
                break;
            }
        }
    }

    // ── Path operations ───────────────────────────────────────────────────────────

    private static JsonNode? GetAtPath(JsonObject item, List<object> path)
    {
        if (path.Count == 0) return null;
        JsonNode? current = null;
        if (!item.TryGetPropertyValue(path[0].ToString()!, out current)) return null;

        for (var i = 1; i < path.Count; i++)
        {
            if (current is null) return null;
            if (path[i] is int idx)
            {
                if (current is JsonObject lo && lo.TryGetPropertyValue("L", out var lst) &&
                    lst is JsonArray la && idx >= 0 && idx < la.Count)
                    current = la[idx];
                else
                    return null;
            }
            else
            {
                var key = path[i].ToString()!;
                if (current is JsonObject mo && mo.TryGetPropertyValue("M", out var map) &&
                    map is JsonObject mo2 && mo2.TryGetPropertyValue(key, out var v))
                    current = v;
                else
                    return null;
            }
        }
        return current;
    }

    private static void SetAtPath(JsonObject item, List<object> path, JsonNode value)
    {
        if (path.Count == 0) return;
        if (path.Count == 1)
        {
            var part = path[0];
            if (part is int idx)
            {
                if (item.TryGetPropertyValue("L", out var lst) && lst is JsonArray la)
                {
                    while (la.Count <= idx) la.Add((JsonNode)new JsonObject { ["NULL"] = true });
                    la[idx] = value.DeepClone();
                }
            }
            else
            {
                if (item.TryGetPropertyValue("M", out var m) && m is JsonObject mo)
                    mo[part.ToString()!] = value.DeepClone();
                else
                    item[part.ToString()!] = value.DeepClone();
            }
            return;
        }

        var first = path[0];
        var rest  = path[1..];

        if (first is int fi)
        {
            if (item.TryGetPropertyValue("L", out var lst) && lst is JsonArray la)
            {
                while (la.Count <= fi) la.Add((JsonNode)new JsonObject { ["NULL"] = true });
                if (la[fi] is not JsonObject child)
                {
                    child = rest[0] is string ? new JsonObject { ["M"] = new JsonObject() } : new JsonObject { ["L"] = new JsonArray() };
                    la[fi] = child;
                }
                SetAtPath(child, rest, value);
            }
        }
        else
        {
            JsonObject container;
            var key = first.ToString()!;
            if (item.TryGetPropertyValue("M", out var mp) && mp is JsonObject mo)
                container = mo;
            else
                container = item;

            if (!container.TryGetPropertyValue(key, out var child) || child is not JsonObject childObj)
            {
                childObj = rest[0] is int
                    ? new JsonObject { ["L"] = new JsonArray() }
                    : new JsonObject { ["M"] = new JsonObject() };
                container[key] = childObj;
            }
            SetAtPath(childObj, rest, value);
        }
    }

    private static void RemoveAtPath(JsonObject item, List<object> path)
    {
        if (path.Count == 0) return;
        if (path.Count == 1)
        {
            var part = path[0];
            if (part is int idx)
            {
                if (item.TryGetPropertyValue("L", out var lst) && lst is JsonArray la &&
                    idx >= 0 && idx < la.Count)
                    la.RemoveAt(idx);
            }
            else
            {
                if (item.TryGetPropertyValue("M", out var m) && m is JsonObject mo)
                    mo.Remove(part.ToString()!);
                else
                    item.Remove(part.ToString()!);
            }
            return;
        }

        var first = path[0];
        var rest  = path[1..];

        if (first is int fi)
        {
            if (item.TryGetPropertyValue("L", out var lst) && lst is JsonArray la &&
                fi >= 0 && fi < la.Count && la[fi] is JsonObject child)
                RemoveAtPath(child, rest);
        }
        else
        {
            var key = first.ToString()!;
            if (item.TryGetPropertyValue("M", out var mp) && mp is JsonObject mo)
            {
                if (mo.TryGetPropertyValue(key, out var child) && child is JsonObject childObj)
                    RemoveAtPath(childObj, rest);
            }
            else if (item.TryGetPropertyValue(key, out var child2) && child2 is JsonObject childObj2)
            {
                RemoveAtPath(childObj2, rest);
            }
        }
    }

    // ── DynamoDB value comparison ─────────────────────────────────────────────────

    private static bool CompareDdb(JsonNode? left, string op, JsonNode? right)
    {
        if (left is null || right is null)
        {
            if (op == "=")  return left is null && right is null;
            if (op == "<>") return !(left is null && right is null);
            return false;
        }

        var (lt, lv) = DdbComparable(left);
        var (rt, rv) = DdbComparable(right);

        if (lt != rt) return op == "<>";
        if (op is "<" or ">" or "<=" or ">=" && lt is not ("S" or "N" or "B")) return false;

        try
        {
            return (op, lv, rv) switch
            {
                ("=",  string ls, string rs) => ls == rs,
                ("=",  decimal ld, decimal rd) => ld == rd,
                ("=",  bool lb, bool rb) => lb == rb,
                ("<>", string ls, string rs) => ls != rs,
                ("<>", decimal ld, decimal rd) => ld != rd,
                ("<>", bool lb, bool rb) => lb != rb,
                ("<",  string ls, string rs) => string.CompareOrdinal(ls, rs) < 0,
                ("<",  decimal ld, decimal rd) => ld < rd,
                (">",  string ls, string rs) => string.CompareOrdinal(ls, rs) > 0,
                (">",  decimal ld, decimal rd) => ld > rd,
                ("<=", string ls, string rs) => string.CompareOrdinal(ls, rs) <= 0,
                ("<=", decimal ld, decimal rd) => ld <= rd,
                (">=", string ls, string rs) => string.CompareOrdinal(ls, rs) >= 0,
                (">=", decimal ld, decimal rd) => ld >= rd,
                _ => false,
            };
        }
        catch { return false; }
    }

    private static (string type, object? value) DdbComparable(JsonNode? val)
    {
        if (val is JsonObject obj)
        {
            if (obj.TryGetPropertyValue("S", out var s))
                return ("S", s?.GetValue<string>());
            if (obj.TryGetPropertyValue("N", out var n))
            {
                if (decimal.TryParse(n?.GetValue<string>(), out var d)) return ("N", d);
                return ("N", 0m);
            }
            if (obj.TryGetPropertyValue("B", out var b)) return ("B", b?.GetValue<string>());
            if (obj.TryGetPropertyValue("BOOL", out var bo)) return ("BOOL", bo?.GetValue<bool>());
            if (obj.TryGetPropertyValue("NULL", out _)) return ("NULL", null);
        }
        return ("UNKNOWN", null);
    }

    private static bool DdbEquals(JsonNode? a, JsonNode? b)
    {
        if (a is null && b is null) return true;
        if (a is null || b is null) return false;
        var (ta, va) = DdbComparable(a);
        var (tb, vb) = DdbComparable(b);
        return ta == tb && Equals(va, vb);
    }

    private static string DdbType(JsonNode? val)
    {
        if (val is JsonObject obj)
            foreach (var t in new[] { "S", "N", "B", "SS", "NS", "BS", "BOOL", "NULL", "L", "M" })
                if (obj.ContainsKey(t)) return t;
        return "";
    }

    private static int DdbSize(JsonNode? val)
    {
        if (val is JsonObject obj)
        {
            if (obj.TryGetPropertyValue("S",  out var s)) return s?.GetValue<string>()?.Length ?? 0;
            if (obj.TryGetPropertyValue("B",  out var b)) return b?.GetValue<string>()?.Length ?? 0;
            foreach (var t in new[] { "SS", "NS", "BS", "L" })
                if (obj.TryGetPropertyValue(t, out var arr)) return arr?.AsArray()?.Count ?? 0;
            if (obj.TryGetPropertyValue("M", out var m)) return m?.AsObject()?.Count ?? 0;
        }
        return 0;
    }

    // ── Consumed capacity ─────────────────────────────────────────────────────────

    private static void AddConsumedCapacity(
        JsonObject result, JsonObject data, string tableName, bool write, DdbTable table)
    {
        var rc = data["ReturnConsumedCapacity"]?.GetValue<string>() ?? "NONE";
        if (rc == "NONE") return;

        var gsiCount = write ? table.Gsis.Count : 0;
        var units    = 1.0 + gsiCount;
        var cap      = new JsonObject { ["TableName"] = tableName, ["CapacityUnits"] = units };
        if (rc == "INDEXES")
        {
            cap["Table"] = new JsonObject { ["CapacityUnits"] = 1.0 };
            if (write && gsiCount > 0)
            {
                var gsiCap = new JsonObject();
                foreach (var g in table.Gsis.OfType<JsonObject>())
                    gsiCap[g["IndexName"]?.GetValue<string>() ?? ""] = new JsonObject { ["CapacityUnits"] = 1.0 };
                cap["GlobalSecondaryIndexes"] = gsiCap;
            }
        }
        result["ConsumedCapacity"] = cap;
    }

    // ── Diff helpers ──────────────────────────────────────────────────────────────

    private static JsonObject DiffAttributes(JsonObject oldItem, JsonObject newItem, bool returnOld)
    {
        var result  = new JsonObject();
        var allKeys = new HashSet<string>(oldItem.Select(kv => kv.Key), StringComparer.Ordinal);
        allKeys.UnionWith(newItem.Select(kv => kv.Key));

        foreach (var k in allKeys)
        {
            var ov = oldItem.TryGetPropertyValue(k, out var ovn) ? ovn : null;
            var nv = newItem.TryGetPropertyValue(k, out var nvn) ? nvn : null;
            if (JsonNode.DeepEquals(ov, nv)) continue;
            if (returnOld && ov is not null) result[k] = ov.DeepClone();
            else if (!returnOld && nv is not null) result[k] = nv.DeepClone();
        }
        return result;
    }

    // ── Stream events ─────────────────────────────────────────────────────────────

    private void EmitStreamEvent(
        string tableName, DdbTable table, string eventName,
        JsonObject? oldItem, JsonObject? newItem)
    {
        var spec = table.StreamSpecification;
        if (spec is null || spec["StreamEnabled"]?.GetValue<bool>() != true) return;

        var viewType = spec["StreamViewType"]?.GetValue<string>() ?? "NEW_AND_OLD_IMAGES";
        var seq      = NextStreamSeq();
        var keys     = new JsonObject();
        var refItem  = newItem ?? oldItem ?? new JsonObject();

        if (table.PkName is not null && refItem.TryGetPropertyValue(table.PkName, out var pk))
            keys[table.PkName] = pk?.DeepClone();
        if (table.SkName is not null && refItem.TryGetPropertyValue(table.SkName, out var sk))
            keys[table.SkName] = sk?.DeepClone();

        var dynamo = new JsonObject
        {
            ["ApproximateCreationDateTime"] = DateTimeOffset.UtcNow.ToUnixTimeSeconds(),
            ["Keys"]                        = keys,
            ["SequenceNumber"]              = seq,
            ["SizeBytes"]                   = 0,
            ["StreamViewType"]              = viewType,
        };

        if (viewType is "NEW_AND_OLD_IMAGES" or "OLD_IMAGE" && oldItem is not null)
            dynamo["OldImage"] = (JsonObject)oldItem.DeepClone();
        if (viewType is "NEW_AND_OLD_IMAGES" or "NEW_IMAGE" && newItem is not null)
            dynamo["NewImage"] = (JsonObject)newItem.DeepClone();

        var record = new JsonObject
        {
            ["eventID"]      = Guid.NewGuid().ToString(),
            ["eventName"]    = eventName,
            ["eventVersion"] = "1.1",
            ["eventSource"]  = "aws:dynamodb",
            ["awsRegion"]    = _region,
            ["dynamodb"]     = dynamo,
            ["eventSourceARN"] = $"{table.TableArn}/stream/{DateTimeOffset.UtcNow:o}",
        };

        if (!_streamRecords.TryGetValue(tableName, out var records))
            records = [];
        records.Add(record);
        _streamRecords[tableName] = records;
    }

    private string NextStreamSeq()
    {
        var counter = Interlocked.Increment(ref _streamSeqCounter);
        return $"{DateTimeOffset.UtcNow.ToUnixTimeMilliseconds():D20}{counter:D10}";
    }

    // ── Response helpers ──────────────────────────────────────────────────────────

    private static ServiceResponse JsonOk(JsonObject data)
    {
        var body = Encoding.UTF8.GetBytes(data.ToJsonString());
        return new ServiceResponse(200,
            new Dictionary<string, string> { ["Content-Type"] = "application/x-amz-json-1.0" },
            body);
    }

    private static ServiceResponse JsonError(string code, string message, int status)
    {
        var body = Encoding.UTF8.GetBytes(
            JsonSerializer.Serialize(new AwsJsonError(code, message), MicroStackJsonContext.Default.AwsJsonError));
        return new ServiceResponse(status,
            new Dictionary<string, string> { ["Content-Type"] = "application/x-amz-json-1.0" },
            body);
    }

    // ── Static helpers ────────────────────────────────────────────────────────────

    private static JsonObject DefaultThroughput()
        => new() { ["ReadCapacityUnits"] = 5, ["WriteCapacityUnits"] = 5 };

    private static JsonArray DeepCloneArray(JsonArray src)
    {
        var arr = new JsonArray();
        foreach (var item in src) arr.Add(item?.DeepClone());
        return arr;
    }

    private static JsonArray ArrayFromStrings(IEnumerable<string> strings)
    {
        var arr = new JsonArray();
        foreach (var s in strings) ((IList<JsonNode?>)arr).Add(JsonValue.Create(s));
        return arr;
    }

    // ── Tokenizer ─────────────────────────────────────────────────────────────────

    private enum TokenType
    {
        LParen, RParen, LBracket, RBracket, Comma, Dot,
        Plus, Minus, Eq, Ne, Lt, Gt, Le, Ge,
        ValueRef, NameRef, Number, Ident, Eof,
    }

    private sealed class Token
    {
        public TokenType Type;
        public string    Value;
        public Token(TokenType t, string v) { Type = t; Value = v; }
    }

    private static List<Token> Tokenize(string expr)
    {
        var tokens = new List<Token>();
        var i = 0;
        var n = expr.Length;

        while (i < n)
        {
            var c = expr[i];
            if (char.IsWhiteSpace(c)) { i++; continue; }

            if (c == '(') { tokens.Add(new Token(TokenType.LParen, "(")); i++; }
            else if (c == ')') { tokens.Add(new Token(TokenType.RParen, ")")); i++; }
            else if (c == '[') { tokens.Add(new Token(TokenType.LBracket, "[")); i++; }
            else if (c == ']') { tokens.Add(new Token(TokenType.RBracket, "]")); i++; }
            else if (c == ',') { tokens.Add(new Token(TokenType.Comma, ",")); i++; }
            else if (c == '.') { tokens.Add(new Token(TokenType.Dot, ".")); i++; }
            else if (c == '+') { tokens.Add(new Token(TokenType.Plus, "+")); i++; }
            else if (c == '-') { tokens.Add(new Token(TokenType.Minus, "-")); i++; }
            else if (c == '=') { tokens.Add(new Token(TokenType.Eq, "=")); i++; }
            else if (c == '<')
            {
                if (i + 1 < n && expr[i + 1] == '>') { tokens.Add(new Token(TokenType.Ne, "<>")); i += 2; }
                else if (i + 1 < n && expr[i + 1] == '=') { tokens.Add(new Token(TokenType.Le, "<=")); i += 2; }
                else { tokens.Add(new Token(TokenType.Lt, "<")); i++; }
            }
            else if (c == '>')
            {
                if (i + 1 < n && expr[i + 1] == '=') { tokens.Add(new Token(TokenType.Ge, ">=")); i += 2; }
                else { tokens.Add(new Token(TokenType.Gt, ">")); i++; }
            }
            else if (c == ':')
            {
                var j = i + 1;
                while (j < n && (char.IsLetterOrDigit(expr[j]) || expr[j] == '_')) j++;
                tokens.Add(new Token(TokenType.ValueRef, expr[i..j]));
                i = j;
            }
            else if (c == '#')
            {
                var j = i + 1;
                while (j < n && (char.IsLetterOrDigit(expr[j]) || expr[j] == '_')) j++;
                tokens.Add(new Token(TokenType.NameRef, expr[i..j]));
                i = j;
            }
            else if (char.IsDigit(c))
            {
                var j = i;
                while (j < n && (char.IsDigit(expr[j]) || expr[j] == '.')) j++;
                tokens.Add(new Token(TokenType.Number, expr[i..j]));
                i = j;
            }
            else if (char.IsLetter(c) || c == '_')
            {
                var j = i;
                while (j < n && (char.IsLetterOrDigit(expr[j]) || expr[j] == '_')) j++;
                tokens.Add(new Token(TokenType.Ident, expr[i..j]));
                i = j;
            }
            else { i++; }
        }

        tokens.Add(new Token(TokenType.Eof, ""));
        return tokens;
    }

    // ── Token helpers ─────────────────────────────────────────────────────────────

    private static List<List<Token>> SplitByComma(List<Token> tokens)
    {
        var parts   = new List<List<Token>>();
        var current = new List<Token>();
        var depth   = 0;
        foreach (var tok in tokens)
        {
            if (tok.Type == TokenType.LParen) { depth++; current.Add(tok); }
            else if (tok.Type == TokenType.RParen) { depth--; current.Add(tok); }
            else if (tok.Type == TokenType.Comma && depth == 0)
            {
                if (current.Count > 0) parts.Add(current);
                current = [];
            }
            else current.Add(tok);
        }
        if (current.Count > 0) parts.Add(current);
        return parts;
    }

    private static int? FindMatchingParen(List<Token> tokens, int start)
    {
        var depth = 0;
        for (var i = start; i < tokens.Count; i++)
        {
            if (tokens[i].Type == TokenType.LParen) depth++;
            else if (tokens[i].Type == TokenType.RParen)
            {
                depth--;
                if (depth == 0) return i;
            }
        }
        return null;
    }

    private static List<object> ParsePathFromTokens(List<Token> tokens, JsonObject attrNames)
    {
        var parts = new List<object>();
        var i = 0;
        while (i < tokens.Count)
        {
            var tok = tokens[i];
            if (tok.Type == TokenType.NameRef)
                parts.Add(attrNames.TryGetPropertyValue(tok.Value, out var v)
                    ? v?.GetValue<string>() ?? tok.Value
                    : (object)tok.Value);
            else if (tok.Type == TokenType.Ident) parts.Add((object)tok.Value);
            else if (tok.Type == TokenType.LBracket)
            {
                i++;
                if (i < tokens.Count && tokens[i].Type == TokenType.Number)
                {
                    parts.Add(int.Parse(tokens[i].Value));
                    i++;
                }
                // skip RBRACKET
            }
            else if (tok.Type is not (TokenType.Dot or TokenType.RBracket)) break;
            i++;
        }
        return parts;
    }

    // ── Expression evaluator ──────────────────────────────────────────────────────

    private sealed class ExprEval
    {
        private readonly List<Token> _tokens;
        private int _pos;
        private readonly JsonObject _item;
        private readonly JsonObject _av;
        private readonly JsonObject _an;

        public ExprEval(List<Token> tokens, JsonObject item, JsonObject av, JsonObject an)
        {
            _tokens = tokens; _item = item; _av = av; _an = an;
        }

        private Token Peek(int offset = 0)
        {
            var p = _pos + offset;
            return p < _tokens.Count ? _tokens[p] : new Token(TokenType.Eof, "");
        }

        private Token Advance() => _tokens[_pos++];

        private bool IsKw(string kw) =>
            Peek().Type == TokenType.Ident &&
            string.Equals(Peek().Value, kw, StringComparison.OrdinalIgnoreCase);

        public bool Evaluate() => OrExpr();

        private bool OrExpr()
        {
            var left = AndExpr();
            while (IsKw("OR")) { Advance(); left |= AndExpr(); }
            return left;
        }

        private bool AndExpr()
        {
            var left = NotExpr();
            while (IsKw("AND")) { Advance(); left &= NotExpr(); }
            return left;
        }

        private bool NotExpr()
        {
            if (!IsKw("NOT")) return Primary();
            Advance();
            return !NotExpr();
        }

        private bool Primary()
        {
            if (Peek().Type == TokenType.LParen)
            {
                Advance();
                var r = OrExpr();
                Advance(); // RParen
                return r;
            }

            if (Peek().Type == TokenType.Ident)
            {
                var fn = Peek().Value.ToLowerInvariant();
                if (fn == "attribute_exists"     && Peek(1).Type == TokenType.LParen) return FnAttrExists(true);
                if (fn == "attribute_not_exists" && Peek(1).Type == TokenType.LParen) return FnAttrExists(false);
                if (fn == "attribute_type"       && Peek(1).Type == TokenType.LParen) return FnAttrType();
                if (fn == "begins_with"          && Peek(1).Type == TokenType.LParen) return FnBeginsWith();
                if (fn == "contains"             && Peek(1).Type == TokenType.LParen) return FnContains();
            }

            var left = Operand();
            var tok  = Peek();

            if (tok.Type is TokenType.Eq or TokenType.Ne or
                            TokenType.Lt or TokenType.Gt or
                            TokenType.Le or TokenType.Ge)
            {
                var op    = Advance().Value;
                var right = Operand();
                return CompareDdb(left, op, right);
            }

            if (IsKw("BETWEEN"))
            {
                Advance();
                var low = Operand();
                if (IsKw("AND")) Advance();
                var high = Operand();
                return CompareDdb(low, "<=", left) && CompareDdb(left, "<=", high);
            }

            if (IsKw("IN"))
            {
                Advance();
                Advance(); // LPAREN
                var values = new List<JsonNode?> { Operand() };
                while (Peek().Type == TokenType.Comma) { Advance(); values.Add(Operand()); }
                Advance(); // RPAREN
                return values.Any(v => CompareDdb(left, "=", v));
            }

            return left is not null;
        }

        private JsonNode? Operand()
        {
            var tok = Peek();
            if (tok.Type == TokenType.Ident &&
                tok.Value.Equals("size", StringComparison.OrdinalIgnoreCase) &&
                Peek(1).Type == TokenType.LParen)
                return FnSizeNode();

            if (tok.Type == TokenType.ValueRef)
            {
                Advance();
                return _av.TryGetPropertyValue(tok.Value, out var v) ? v : null;
            }

            var path = ParsePath();
            return GetAtPath(_item, path);
        }

        private List<object> ParsePath()
        {
            var parts = new List<object>();
            var tok   = Peek();
            if (tok.Type == TokenType.NameRef)
            {
                Advance();
                parts.Add(_an.TryGetPropertyValue(tok.Value, out var v)
                    ? v?.GetValue<string>() ?? tok.Value
                    : (object)tok.Value);
            }
            else if (tok.Type == TokenType.Ident) { Advance(); parts.Add((object)tok.Value); }
            else return parts;

            while (true)
            {
                if (Peek().Type == TokenType.Dot)
                {
                    Advance();
                    var t = Peek();
                    if (t.Type == TokenType.NameRef)
                    {
                        Advance();
                        parts.Add(_an.TryGetPropertyValue(t.Value, out var v)
                            ? v?.GetValue<string>() ?? t.Value : (object)t.Value);
                    }
                    else if (t.Type == TokenType.Ident) { Advance(); parts.Add((object)t.Value); }
                    else break;
                }
                else if (Peek().Type == TokenType.LBracket)
                {
                    Advance();
                    if (Peek().Type == TokenType.Number)
                        parts.Add(int.Parse(Advance().Value));
                    Advance(); // RBRACKET
                }
                else break;
            }
            return parts;
        }

        private bool FnAttrExists(bool shouldExist)
        {
            Advance(); Advance(); // fn + LPAREN
            var path   = ParsePath();
            Advance(); // RPAREN
            var exists = GetAtPath(_item, path) is not null;
            return shouldExist ? exists : !exists;
        }

        private bool FnAttrType()
        {
            Advance(); Advance(); // fn + LPAREN
            var path = ParsePath();
            Advance(); // COMMA
            var typeVal = Operand();
            Advance(); // RPAREN
            var attr    = GetAtPath(_item, path);
            if (attr is null || typeVal is null) return false;
            var expected = typeVal is JsonObject to && to.TryGetPropertyValue("S", out var ts)
                ? ts?.GetValue<string>() : "";
            return DdbType(attr) == expected;
        }

        private bool FnBeginsWith()
        {
            Advance(); Advance();
            var path   = ParsePath();
            Advance(); // COMMA
            var substr = Operand();
            Advance(); // RPAREN
            var attr   = GetAtPath(_item, path);
            if (attr is null || substr is null) return false;
            if (attr is JsonObject ao && ao.TryGetPropertyValue("S", out var as_) &&
                substr is JsonObject so && so.TryGetPropertyValue("S", out var ss))
                return as_?.GetValue<string>()?.StartsWith(ss?.GetValue<string>() ?? "", StringComparison.Ordinal) ?? false;
            return false;
        }

        private bool FnContains()
        {
            Advance(); Advance();
            var path = ParsePath();
            Advance(); // COMMA
            var val  = Operand();
            Advance(); // RPAREN
            var attr = GetAtPath(_item, path);
            if (attr is null || val is null) return false;

            if (attr is JsonObject ao && val is JsonObject vo)
            {
                if (ao.TryGetPropertyValue("S", out var as_) && vo.TryGetPropertyValue("S", out var vs))
                    return as_?.GetValue<string>()?.Contains(vs?.GetValue<string>() ?? "", StringComparison.Ordinal) ?? false;
                if (ao.TryGetPropertyValue("SS", out var ass) && vo.TryGetPropertyValue("S", out var vs2))
                    return ass?.AsArray()?.Any(e => e?.GetValue<string>() == vs2?.GetValue<string>()) ?? false;
                if (ao.TryGetPropertyValue("NS", out var ans) && vo.TryGetPropertyValue("N", out var vn))
                    return ans?.AsArray()?.Any(e => e?.GetValue<string>() == vn?.GetValue<string>()) ?? false;
                if (ao.TryGetPropertyValue("L", out var al))
                    return al?.AsArray()?.Any(e => DdbEquals(e, val)) ?? false;
            }
            return false;
        }

        private JsonNode? FnSizeNode()
        {
            Advance(); Advance();
            var path = ParsePath();
            Advance(); // RPAREN
            var attr = GetAtPath(_item, path);
            return attr is null ? null : new JsonObject { ["N"] = DdbSize(attr).ToString() };
        }
    }
}

// ── Data models ──────────────────────────────────────────────────────────────────

/// <summary>In-memory DynamoDB table.</summary>
internal sealed class DdbTable
{
    public required string TableName { get; set; }
    public JsonArray       KeySchema          { get; set; } = [];
    public JsonArray       AttributeDefinitions { get; set; } = [];
    public string?         PkName             { get; set; }
    public string?         SkName             { get; set; }
    /// <summary>Items stored as pk → sk → item.</summary>
    public Dictionary<string, Dictionary<string, JsonObject>> Items { get; set; } = new(StringComparer.Ordinal);
    public string          TableStatus        { get; set; } = "ACTIVE";
    public double          CreationDateTime   { get; set; } = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
    public int             ItemCount          { get; set; }
    public long            TableSizeBytes     { get; set; }
    public required string TableArn           { get; set; }
    public required string TableId            { get; set; }
    public JsonObject      ProvisionedThroughput { get; set; } = new() { ["ReadCapacityUnits"] = 5, ["WriteCapacityUnits"] = 5 };
    public JsonObject?     BillingModeSummary { get; set; }
    public JsonObject?     StreamSpecification { get; set; }
    public JsonObject?     SseDescription     { get; set; }
    public JsonArray       Gsis               { get; set; } = [];
    public JsonArray       Lsis               { get; set; } = [];
    public Dictionary<string, string> Attributes { get; set; } = new(StringComparer.Ordinal);
}

/// <summary>TTL configuration for a table.</summary>
internal sealed class DdbTtlSetting
{
    public required string Status        { get; set; }
    public required string AttributeName { get; set; }
}

/// <summary>DynamoDB domain exception.</summary>
internal sealed class DdbException : Exception
{
    public string  Code    { get; }
    public int     Status  { get; }
    /// <summary>When set, the raw JSON is sent as the response body verbatim (bypasses the default error serialization).</summary>
    public string? RawBody { get; }

    internal DdbException(string code, string message, int status = 400)
        : base(message)
    {
        Code   = code;
        Status = status;
    }

    internal DdbException(string code, string message, int status, string rawBody)
        : base(message)
    {
        Code    = code;
        Status  = status;
        RawBody = rawBody;
    }
}
