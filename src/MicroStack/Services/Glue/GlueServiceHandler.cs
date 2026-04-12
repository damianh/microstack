using System.Text.Json;
using MicroStack.Internal;

namespace MicroStack.Services.Glue;

/// <summary>
/// Glue service handler -- supports JSON protocol via X-Amz-Target (AWSGlue).
///
/// Port of ministack/services/glue.py.
///
/// Supports: CreateDatabase, GetDatabase, GetDatabases, DeleteDatabase, UpdateDatabase,
///           CreateTable, GetTable, GetTables, DeleteTable, UpdateTable, BatchDeleteTable,
///           CreatePartition, GetPartition, GetPartitions, DeletePartition,
///           BatchCreatePartition, BatchGetPartition,
///           CreateCrawler, GetCrawler, GetCrawlers, DeleteCrawler, UpdateCrawler,
///           StartCrawler, StopCrawler, GetCrawlerMetrics,
///           CreateJob, GetJob, GetJobs, DeleteJob, UpdateJob,
///           StartJobRun, GetJobRun, GetJobRuns, BatchStopJobRun,
///           CreateRegistry, GetRegistry, ListRegistries, DeleteRegistry,
///           CreateSchema, GetSchema, ListSchemas, DeleteSchema,
///           RegisterSchemaVersion, GetSchemaVersion, ListSchemaVersions,
///           TagResource, UntagResource, GetTags.
/// </summary>
internal sealed class GlueServiceHandler : IServiceHandler
{
    private readonly Lock _lock = new();

    private static string Region => MicroStackOptions.Instance.Region;

    // In-memory state
    private readonly AccountScopedDictionary<string, Dictionary<string, object?>> _databases = new();
    private readonly AccountScopedDictionary<string, Dictionary<string, object?>> _tables = new(); // "db/table" -> table
    private readonly AccountScopedDictionary<string, List<Dictionary<string, object?>>> _partitions = new(); // "db/table" -> [part...]
    private readonly AccountScopedDictionary<string, Dictionary<string, object?>> _crawlers = new();
    private readonly AccountScopedDictionary<string, Dictionary<string, object?>> _jobs = new();
    private readonly AccountScopedDictionary<string, List<Dictionary<string, object?>>> _jobRuns = new(); // jobName -> [run...]
    private readonly AccountScopedDictionary<string, Dictionary<string, string>> _tags = new(); // arn -> {key:val}
    private readonly AccountScopedDictionary<string, Dictionary<string, object?>> _registries = new();
    private readonly AccountScopedDictionary<string, Dictionary<string, object?>> _schemas = new(); // "registry/schema" -> schema
    private readonly AccountScopedDictionary<string, List<Dictionary<string, object?>>> _schemaVersions = new(); // "registry/schema" -> [ver...]

    // -- IServiceHandler -------------------------------------------------------

    public string ServiceName => "glue";

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

        var response = action switch
        {
            // Databases
            "CreateDatabase" => ActCreateDatabase(data),
            "GetDatabase" => ActGetDatabase(data),
            "GetDatabases" => ActGetDatabases(data),
            "DeleteDatabase" => ActDeleteDatabase(data),
            "UpdateDatabase" => ActUpdateDatabase(data),
            // Tables
            "CreateTable" => ActCreateTable(data),
            "GetTable" => ActGetTable(data),
            "GetTables" => ActGetTables(data),
            "DeleteTable" => ActDeleteTable(data),
            "UpdateTable" => ActUpdateTable(data),
            "BatchDeleteTable" => ActBatchDeleteTable(data),
            "BatchGetTable" => ActBatchGetTable(data),
            // Partitions
            "CreatePartition" => ActCreatePartition(data),
            "GetPartition" => ActGetPartition(data),
            "GetPartitions" => ActGetPartitions(data),
            "DeletePartition" => ActDeletePartition(data),
            "BatchCreatePartition" => ActBatchCreatePartition(data),
            "BatchDeletePartition" => ActBatchDeletePartition(data),
            "UpdatePartition" => ActUpdatePartition(data),
            // Crawlers
            "CreateCrawler" => ActCreateCrawler(data),
            "GetCrawler" => ActGetCrawler(data),
            "GetCrawlers" => ActGetCrawlers(data),
            "DeleteCrawler" => ActDeleteCrawler(data),
            "UpdateCrawler" => ActUpdateCrawler(data),
            "StartCrawler" => ActStartCrawler(data),
            "StopCrawler" => ActStopCrawler(data),
            "GetCrawlerMetrics" => ActGetCrawlerMetrics(data),
            // Jobs
            "CreateJob" => ActCreateJob(data),
            "GetJob" => ActGetJob(data),
            "GetJobs" => ActGetJobs(data),
            "DeleteJob" => ActDeleteJob(data),
            "UpdateJob" => ActUpdateJob(data),
            "StartJobRun" => ActStartJobRun(data),
            "GetJobRun" => ActGetJobRun(data),
            "GetJobRuns" => ActGetJobRuns(data),
            "BatchStopJobRun" => ActBatchStopJobRun(data),
            // Registries
            "CreateRegistry" => ActCreateRegistry(data),
            "GetRegistry" => ActGetRegistry(data),
            "ListRegistries" => ActListRegistries(data),
            "DeleteRegistry" => ActDeleteRegistry(data),
            // Schemas
            "CreateSchema" => ActCreateSchema(data),
            "GetSchema" => ActGetSchema(data),
            "ListSchemas" => ActListSchemas(data),
            "DeleteSchema" => ActDeleteSchema(data),
            "RegisterSchemaVersion" => ActRegisterSchemaVersion(data),
            "GetSchemaVersion" => ActGetSchemaVersion(data),
            "ListSchemaVersions" => ActListSchemaVersions(data),
            // Tags
            "TagResource" => ActTagResource(data),
            "UntagResource" => ActUntagResource(data),
            "GetTags" => ActGetTags(data),
            _ => AwsResponseHelpers.ErrorResponseJson("InvalidAction", $"Unknown Glue action: {action}", 400),
        };

        return Task.FromResult(response);
    }

    public void Reset()
    {
        lock (_lock)
        {
            _databases.Clear();
            _tables.Clear();
            _partitions.Clear();
            _crawlers.Clear();
            _jobs.Clear();
            _jobRuns.Clear();
            _tags.Clear();
            _registries.Clear();
            _schemas.Clear();
            _schemaVersions.Clear();
        }
    }

    public object? GetState() => null;

    public void RestoreState(object state) { }

    // -- JSON helpers ----------------------------------------------------------

    private static string? GetString(JsonElement el, string prop)
    {
        return el.TryGetProperty(prop, out var p) && p.ValueKind == JsonValueKind.String
            ? p.GetString()
            : null;
    }

    private static int GetInt(JsonElement el, string prop, int def)
    {
        if (!el.TryGetProperty(prop, out var p))
        {
            return def;
        }

        return p.TryGetInt32(out var val) ? val : def;
    }

    private static Dictionary<string, object?> ElementToDict(JsonElement el)
    {
        var dict = new Dictionary<string, object?>(StringComparer.Ordinal);
        if (el.ValueKind != JsonValueKind.Object)
        {
            return dict;
        }

        foreach (var prop in el.EnumerateObject())
        {
            dict[prop.Name] = prop.Value.ValueKind switch
            {
                JsonValueKind.String => prop.Value.GetString(),
                JsonValueKind.Number => prop.Value.GetDouble(),
                JsonValueKind.True => true,
                JsonValueKind.False => false,
                JsonValueKind.Null => null,
                JsonValueKind.Array => ElementToArray(prop.Value),
                JsonValueKind.Object => ElementToDict(prop.Value),
                _ => prop.Value.GetRawText(),
            };
        }

        return dict;
    }

    private static List<object?> ElementToArray(JsonElement el)
    {
        var list = new List<object?>();
        foreach (var item in el.EnumerateArray())
        {
            list.Add(item.ValueKind switch
            {
                JsonValueKind.String => item.GetString(),
                JsonValueKind.Number => item.GetDouble(),
                JsonValueKind.True => true,
                JsonValueKind.False => false,
                JsonValueKind.Null => null,
                JsonValueKind.Array => ElementToArray(item),
                JsonValueKind.Object => ElementToDict(item),
                _ => item.GetRawText(),
            });
        }

        return list;
    }

    private static List<string> GetStringList(JsonElement el, string prop)
    {
        var list = new List<string>();
        if (el.TryGetProperty(prop, out var arr) && arr.ValueKind == JsonValueKind.Array)
        {
            foreach (var item in arr.EnumerateArray())
            {
                var s = item.GetString();
                if (s is not null)
                {
                    list.Add(s);
                }
            }
        }

        return list;
    }

    private static Dictionary<string, string> GetStringMap(JsonElement el, string prop)
    {
        var map = new Dictionary<string, string>(StringComparer.Ordinal);
        if (el.TryGetProperty(prop, out var obj) && obj.ValueKind == JsonValueKind.Object)
        {
            foreach (var p in obj.EnumerateObject())
            {
                map[p.Name] = p.Value.GetString() ?? "";
            }
        }

        return map;
    }

    private static bool ValuesEqual(object? a, object? b)
    {
        if (a is List<object?> la && b is List<object?> lb)
        {
            if (la.Count != lb.Count)
            {
                return false;
            }

            for (var i = 0; i < la.Count; i++)
            {
                if (!Equals(la[i], lb[i]))
                {
                    return false;
                }
            }

            return true;
        }

        return Equals(a, b);
    }

    private string Arn(string resourceType, string name)
    {
        return $"arn:aws:glue:{Region}:{AccountContext.GetAccountId()}:{resourceType}/{name}";
    }

    // -- Databases -------------------------------------------------------------

    private ServiceResponse ActCreateDatabase(JsonElement data)
    {
        lock (_lock)
        {
            var dbInput = data.TryGetProperty("DatabaseInput", out var di)
                ? di
                : JsonDocument.Parse("{}").RootElement;
            var name = GetString(dbInput, "Name");
            if (string.IsNullOrEmpty(name))
            {
                return AwsResponseHelpers.ErrorResponseJson("InvalidInputException", "DatabaseInput.Name is required", 400);
            }

            if (_databases.ContainsKey(name))
            {
                return AwsResponseHelpers.ErrorResponseJson("AlreadyExistsException", $"Database {name} already exists", 400);
            }

            _databases[name] = new Dictionary<string, object?>(StringComparer.Ordinal)
            {
                ["Name"] = name,
                ["Description"] = GetString(dbInput, "Description") ?? "",
                ["LocationUri"] = GetString(dbInput, "LocationUri") ?? "",
                ["Parameters"] = dbInput.TryGetProperty("Parameters", out var pEl) ? ElementToDict(pEl) : new Dictionary<string, object?>(),
                ["CreateTime"] = TimeHelpers.NowEpoch(),
                ["CatalogId"] = AccountContext.GetAccountId(),
            };

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>());
        }
    }

    private ServiceResponse ActGetDatabase(JsonElement data)
    {
        lock (_lock)
        {
            var name = GetString(data, "Name");
            if (name is null || !_databases.TryGetValue(name, out var db))
            {
                return AwsResponseHelpers.ErrorResponseJson("EntityNotFoundException", $"Database {name} not found", 400);
            }

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?> { ["Database"] = db });
        }
    }

    private ServiceResponse ActGetDatabases(JsonElement data)
    {
        lock (_lock)
        {
            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>
            {
                ["DatabaseList"] = _databases.Values.ToList(),
            });
        }
    }

    private ServiceResponse ActDeleteDatabase(JsonElement data)
    {
        lock (_lock)
        {
            var name = GetString(data, "Name");
            if (name is null || !_databases.ContainsKey(name))
            {
                return AwsResponseHelpers.ErrorResponseJson("EntityNotFoundException", $"Database {name} not found", 400);
            }

            _databases.TryRemove(name, out _);

            // Clean up tables, partitions under this database
            var keysToRemove = _tables.Keys.Where(k => k.StartsWith($"{name}/", StringComparison.Ordinal)).ToList();
            foreach (var k in keysToRemove)
            {
                _tables.TryRemove(k, out _);
                _partitions.TryRemove(k, out _);
            }

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>());
        }
    }

    private ServiceResponse ActUpdateDatabase(JsonElement data)
    {
        lock (_lock)
        {
            var name = GetString(data, "Name");
            if (name is null || !_databases.TryGetValue(name, out var db))
            {
                return AwsResponseHelpers.ErrorResponseJson("EntityNotFoundException", $"Database {name} not found", 400);
            }

            var dbInput = data.TryGetProperty("DatabaseInput", out var di) ? di : JsonDocument.Parse("{}").RootElement;
            foreach (var prop in new[] { "Description", "LocationUri" })
            {
                var val = GetString(dbInput, prop);
                if (val is not null)
                {
                    db[prop] = val;
                }
            }

            if (dbInput.TryGetProperty("Parameters", out var pEl))
            {
                db["Parameters"] = ElementToDict(pEl);
            }

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>());
        }
    }

    // -- Tables ----------------------------------------------------------------

    private ServiceResponse ActCreateTable(JsonElement data)
    {
        lock (_lock)
        {
            var dbName = GetString(data, "DatabaseName") ?? "";
            var tableInput = data.TryGetProperty("TableInput", out var ti) ? ti : JsonDocument.Parse("{}").RootElement;
            var name = GetString(tableInput, "Name") ?? "";
            var key = $"{dbName}/{name}";

            if (_tables.ContainsKey(key))
            {
                return AwsResponseHelpers.ErrorResponseJson("AlreadyExistsException", $"Table {name} already exists", 400);
            }

            var now = TimeHelpers.NowEpoch();
            _tables[key] = new Dictionary<string, object?>(StringComparer.Ordinal)
            {
                ["Name"] = name,
                ["DatabaseName"] = dbName,
                ["Description"] = GetString(tableInput, "Description") ?? "",
                ["Owner"] = GetString(tableInput, "Owner") ?? "",
                ["CreateTime"] = now,
                ["UpdateTime"] = now,
                ["LastAccessTime"] = now,
                ["StorageDescriptor"] = tableInput.TryGetProperty("StorageDescriptor", out var sd) ? ElementToDict(sd) : new Dictionary<string, object?>(),
                ["PartitionKeys"] = tableInput.TryGetProperty("PartitionKeys", out var pk) ? ElementToArray(pk) : new List<object?>(),
                ["TableType"] = GetString(tableInput, "TableType") ?? "EXTERNAL_TABLE",
                ["Parameters"] = tableInput.TryGetProperty("Parameters", out var paramsEl) ? ElementToDict(paramsEl) : new Dictionary<string, object?>(),
                ["IsRegisteredWithLakeFormation"] = false,
                ["CatalogId"] = AccountContext.GetAccountId(),
            };

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>());
        }
    }

    private ServiceResponse ActGetTable(JsonElement data)
    {
        lock (_lock)
        {
            var dbName = GetString(data, "DatabaseName") ?? "";
            var name = GetString(data, "Name") ?? "";
            var key = $"{dbName}/{name}";

            if (!_tables.TryGetValue(key, out var table))
            {
                return AwsResponseHelpers.ErrorResponseJson("EntityNotFoundException", $"Table {name} not found in {dbName}", 400);
            }

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?> { ["Table"] = table });
        }
    }

    private ServiceResponse ActGetTables(JsonElement data)
    {
        lock (_lock)
        {
            var dbName = GetString(data, "DatabaseName") ?? "";
            var tables = _tables.Items
                .Where(kv => kv.Key.StartsWith($"{dbName}/", StringComparison.Ordinal))
                .Select(kv => kv.Value)
                .ToList();

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?> { ["TableList"] = tables });
        }
    }

    private ServiceResponse ActDeleteTable(JsonElement data)
    {
        lock (_lock)
        {
            var dbName = GetString(data, "DatabaseName") ?? "";
            var name = GetString(data, "Name") ?? "";
            var key = $"{dbName}/{name}";
            _tables.TryRemove(key, out _);
            _partitions.TryRemove(key, out _);
            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>());
        }
    }

    private ServiceResponse ActUpdateTable(JsonElement data)
    {
        lock (_lock)
        {
            var dbName = GetString(data, "DatabaseName") ?? "";
            var tableInput = data.TryGetProperty("TableInput", out var ti) ? ti : JsonDocument.Parse("{}").RootElement;
            var name = GetString(tableInput, "Name") ?? "";
            var key = $"{dbName}/{name}";

            if (!_tables.TryGetValue(key, out var table))
            {
                return AwsResponseHelpers.ErrorResponseJson("EntityNotFoundException", $"Table {name} not found", 400);
            }

            foreach (var prop in new[] { "Description", "Owner", "TableType" })
            {
                var val = GetString(tableInput, prop);
                if (val is not null)
                {
                    table[prop] = val;
                }
            }

            if (tableInput.TryGetProperty("StorageDescriptor", out var sd))
            {
                table["StorageDescriptor"] = ElementToDict(sd);
            }

            if (tableInput.TryGetProperty("PartitionKeys", out var pk))
            {
                table["PartitionKeys"] = ElementToArray(pk);
            }

            if (tableInput.TryGetProperty("Parameters", out var paramsEl))
            {
                table["Parameters"] = ElementToDict(paramsEl);
            }

            table["UpdateTime"] = TimeHelpers.NowEpoch();
            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>());
        }
    }

    private ServiceResponse ActBatchDeleteTable(JsonElement data)
    {
        lock (_lock)
        {
            var dbName = GetString(data, "DatabaseName") ?? "";
            var names = GetStringList(data, "TablesToDelete");
            var errors = new List<Dictionary<string, object?>>();

            foreach (var name in names)
            {
                var key = $"{dbName}/{name}";
                if (!_tables.ContainsKey(key))
                {
                    errors.Add(new Dictionary<string, object?>
                    {
                        ["TableName"] = name,
                        ["ErrorDetail"] = new Dictionary<string, object?>
                        {
                            ["ErrorCode"] = "EntityNotFoundException",
                            ["ErrorMessage"] = "Table not found",
                        },
                    });
                }
                else
                {
                    _tables.TryRemove(key, out _);
                    _partitions.TryRemove(key, out _);
                }
            }

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?> { ["Errors"] = errors });
        }
    }

    private ServiceResponse ActBatchGetTable(JsonElement data)
    {
        lock (_lock)
        {
            var dbName = GetString(data, "DatabaseName") ?? "";
            var names = GetStringList(data, "TablesToGet");
            var tables = new List<Dictionary<string, object?>>();
            var errors = new List<Dictionary<string, object?>>();

            foreach (var name in names)
            {
                var key = $"{dbName}/{name}";
                if (_tables.TryGetValue(key, out var table))
                {
                    tables.Add(table);
                }
                else
                {
                    errors.Add(new Dictionary<string, object?>
                    {
                        ["TableName"] = name,
                        ["ErrorDetail"] = new Dictionary<string, object?>
                        {
                            ["ErrorCode"] = "EntityNotFoundException",
                            ["ErrorMessage"] = "Table not found",
                        },
                    });
                }
            }

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>
            {
                ["Tables"] = tables,
                ["Errors"] = errors,
            });
        }
    }

    // -- Partitions ------------------------------------------------------------

    private ServiceResponse ActCreatePartition(JsonElement data)
    {
        lock (_lock)
        {
            var dbName = GetString(data, "DatabaseName") ?? "";
            var tableName = GetString(data, "TableName") ?? "";
            var key = $"{dbName}/{tableName}";

            var partInput = data.TryGetProperty("PartitionInput", out var pi)
                ? ElementToDict(pi)
                : new Dictionary<string, object?>();

            if (!_partitions.TryGetValue(key, out var partitions))
            {
                partitions = [];
                _partitions[key] = partitions;
            }

            var values = partInput.TryGetValue("Values", out var vObj) ? vObj : null;
            foreach (var existing in partitions)
            {
                if (existing.TryGetValue("Values", out var existingValues) && ValuesEqual(existingValues, values))
                {
                    return AwsResponseHelpers.ErrorResponseJson("AlreadyExistsException",
                        $"Partition with values {values} already exists", 400);
                }
            }

            partInput["DatabaseName"] = dbName;
            partInput["TableName"] = tableName;
            partInput["CreationTime"] = TimeHelpers.NowEpoch();
            partInput["LastAccessTime"] = TimeHelpers.NowEpoch();
            partInput["CatalogId"] = AccountContext.GetAccountId();
            partitions.Add(partInput);

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>());
        }
    }

    private ServiceResponse ActGetPartition(JsonElement data)
    {
        lock (_lock)
        {
            var dbName = GetString(data, "DatabaseName") ?? "";
            var tableName = GetString(data, "TableName") ?? "";
            var key = $"{dbName}/{tableName}";
            var reqValues = data.TryGetProperty("PartitionValues", out var pv) ? ElementToArray(pv) : new List<object?>();

            if (_partitions.TryGetValue(key, out var partitions))
            {
                foreach (var p in partitions)
                {
                    if (p.TryGetValue("Values", out var existingValues) && ValuesEqual(existingValues, reqValues))
                    {
                        return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?> { ["Partition"] = p });
                    }
                }
            }

            return AwsResponseHelpers.ErrorResponseJson("EntityNotFoundException", "Partition not found", 400);
        }
    }

    private ServiceResponse ActGetPartitions(JsonElement data)
    {
        lock (_lock)
        {
            var dbName = GetString(data, "DatabaseName") ?? "";
            var tableName = GetString(data, "TableName") ?? "";
            var key = $"{dbName}/{tableName}";
            var partitions = _partitions.TryGetValue(key, out var ps) ? ps : [];
            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?> { ["Partitions"] = partitions });
        }
    }

    private ServiceResponse ActDeletePartition(JsonElement data)
    {
        lock (_lock)
        {
            var dbName = GetString(data, "DatabaseName") ?? "";
            var tableName = GetString(data, "TableName") ?? "";
            var key = $"{dbName}/{tableName}";
            var reqValues = data.TryGetProperty("PartitionValues", out var pv) ? ElementToArray(pv) : new List<object?>();

            if (_partitions.TryGetValue(key, out var partitions))
            {
                _partitions[key] = partitions
                    .Where(p => !(p.TryGetValue("Values", out var v) && ValuesEqual(v, reqValues)))
                    .ToList();
            }

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>());
        }
    }

    private ServiceResponse ActBatchCreatePartition(JsonElement data)
    {
        lock (_lock)
        {
            var dbName = GetString(data, "DatabaseName") ?? "";
            var tableName = GetString(data, "TableName") ?? "";
            var key = $"{dbName}/{tableName}";

            if (!_partitions.TryGetValue(key, out var partitions))
            {
                partitions = [];
                _partitions[key] = partitions;
            }

            var errors = new List<Dictionary<string, object?>>();

            if (data.TryGetProperty("PartitionInputList", out var listEl) && listEl.ValueKind == JsonValueKind.Array)
            {
                foreach (var piEl in listEl.EnumerateArray())
                {
                    var pi = ElementToDict(piEl);
                    var values = pi.TryGetValue("Values", out var vObj) ? vObj : null;
                    var dupe = partitions.Any(p =>
                        p.TryGetValue("Values", out var ev) && ValuesEqual(ev, values));

                    if (dupe)
                    {
                        errors.Add(new Dictionary<string, object?>
                        {
                            ["PartitionValues"] = values,
                            ["ErrorDetail"] = new Dictionary<string, object?>
                            {
                                ["ErrorCode"] = "AlreadyExistsException",
                                ["ErrorMessage"] = "Partition already exists",
                            },
                        });
                    }
                    else
                    {
                        pi["DatabaseName"] = dbName;
                        pi["TableName"] = tableName;
                        pi["CreationTime"] = TimeHelpers.NowEpoch();
                        pi["CatalogId"] = AccountContext.GetAccountId();
                        partitions.Add(pi);
                    }
                }
            }

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?> { ["Errors"] = errors });
        }
    }

    private ServiceResponse ActBatchDeletePartition(JsonElement data)
    {
        lock (_lock)
        {
            var dbName = GetString(data, "DatabaseName") ?? "";
            var tableName = GetString(data, "TableName") ?? "";
            var key = $"{dbName}/{tableName}";
            var errors = new List<Dictionary<string, object?>>();

            if (data.TryGetProperty("PartitionsToDelete", out var listEl) && listEl.ValueKind == JsonValueKind.Array)
            {
                foreach (var entry in listEl.EnumerateArray())
                {
                    var reqValues = entry.TryGetProperty("Values", out var vEl) ? ElementToArray(vEl) : new List<object?>();

                    if (_partitions.TryGetValue(key, out var partitions))
                    {
                        var before = partitions.Count;
                        _partitions[key] = partitions
                            .Where(p => !(p.TryGetValue("Values", out var v) && ValuesEqual(v, reqValues)))
                            .ToList();
                        if (_partitions[key].Count == before)
                        {
                            errors.Add(new Dictionary<string, object?>
                            {
                                ["PartitionValues"] = reqValues,
                                ["ErrorDetail"] = new Dictionary<string, object?>
                                {
                                    ["ErrorCode"] = "EntityNotFoundException",
                                    ["ErrorMessage"] = "Partition not found",
                                },
                            });
                        }
                    }
                }
            }

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?> { ["Errors"] = errors });
        }
    }

    private ServiceResponse ActUpdatePartition(JsonElement data)
    {
        lock (_lock)
        {
            var dbName = GetString(data, "DatabaseName") ?? "";
            var tableName = GetString(data, "TableName") ?? "";
            var key = $"{dbName}/{tableName}";
            var reqValues = data.TryGetProperty("PartitionValueList", out var pvl) ? ElementToArray(pvl) : new List<object?>();
            var partInput = data.TryGetProperty("PartitionInput", out var pi) ? ElementToDict(pi) : new Dictionary<string, object?>();

            if (_partitions.TryGetValue(key, out var partitions))
            {
                for (var i = 0; i < partitions.Count; i++)
                {
                    if (partitions[i].TryGetValue("Values", out var v) && ValuesEqual(v, reqValues))
                    {
                        // Update in place with new partition input fields
                        foreach (var (pk, pv) in partInput)
                        {
                            partitions[i][pk] = pv;
                        }

                        return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>());
                    }
                }
            }

            return AwsResponseHelpers.ErrorResponseJson("EntityNotFoundException", "Partition not found", 400);
        }
    }

    // -- Crawlers --------------------------------------------------------------

    private ServiceResponse ActCreateCrawler(JsonElement data)
    {
        lock (_lock)
        {
            var name = GetString(data, "Name") ?? "";
            if (_crawlers.ContainsKey(name))
            {
                return AwsResponseHelpers.ErrorResponseJson("AlreadyExistsException", $"Crawler {name} already exists", 400);
            }

            var schedule = GetString(data, "Schedule") ?? "";
            var scheduleObj = !string.IsNullOrEmpty(schedule)
                ? new Dictionary<string, object?> { ["ScheduleExpression"] = schedule }
                : new Dictionary<string, object?>();

            _crawlers[name] = new Dictionary<string, object?>(StringComparer.Ordinal)
            {
                ["Name"] = name,
                ["Role"] = GetString(data, "Role") ?? "",
                ["DatabaseName"] = GetString(data, "DatabaseName") ?? "",
                ["Description"] = GetString(data, "Description") ?? "",
                ["Targets"] = data.TryGetProperty("Targets", out var t) ? ElementToDict(t) : new Dictionary<string, object?>(),
                ["Schedule"] = scheduleObj,
                ["Classifiers"] = data.TryGetProperty("Classifiers", out var cls) ? ElementToArray(cls) : new List<object?>(),
                ["TablePrefix"] = GetString(data, "TablePrefix") ?? "",
                ["SchemaChangePolicy"] = data.TryGetProperty("SchemaChangePolicy", out var scp) ? ElementToDict(scp) : new Dictionary<string, object?>(),
                ["State"] = "READY",
                ["CrawlElapsedTime"] = 0.0,
                ["CreationTime"] = TimeHelpers.NowEpoch(),
                ["LastUpdated"] = TimeHelpers.NowEpoch(),
                ["LastCrawl"] = null,
                ["Version"] = 1.0,
            };

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>());
        }
    }

    private ServiceResponse ActGetCrawler(JsonElement data)
    {
        lock (_lock)
        {
            var name = GetString(data, "Name") ?? "";
            if (!_crawlers.TryGetValue(name, out var crawler))
            {
                return AwsResponseHelpers.ErrorResponseJson("EntityNotFoundException", $"Crawler {name} not found", 400);
            }

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?> { ["Crawler"] = crawler });
        }
    }

    private ServiceResponse ActGetCrawlers(JsonElement data)
    {
        lock (_lock)
        {
            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>
            {
                ["Crawlers"] = _crawlers.Values.ToList(),
            });
        }
    }

    private ServiceResponse ActDeleteCrawler(JsonElement data)
    {
        lock (_lock)
        {
            var name = GetString(data, "Name") ?? "";
            if (!_crawlers.ContainsKey(name))
            {
                return AwsResponseHelpers.ErrorResponseJson("EntityNotFoundException", $"Crawler {name} not found", 400);
            }

            _crawlers.TryRemove(name, out _);
            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>());
        }
    }

    private ServiceResponse ActUpdateCrawler(JsonElement data)
    {
        lock (_lock)
        {
            var name = GetString(data, "Name") ?? "";
            if (!_crawlers.TryGetValue(name, out var crawler))
            {
                return AwsResponseHelpers.ErrorResponseJson("EntityNotFoundException", $"Crawler {name} not found", 400);
            }

            foreach (var prop in new[] { "Role", "DatabaseName", "Description", "TablePrefix" })
            {
                var val = GetString(data, prop);
                if (val is not null)
                {
                    crawler[prop] = val;
                }
            }

            if (data.TryGetProperty("Targets", out var t))
            {
                crawler["Targets"] = ElementToDict(t);
            }

            if (data.TryGetProperty("Schedule", out var schedEl))
            {
                if (schedEl.ValueKind == JsonValueKind.String)
                {
                    var sched = schedEl.GetString() ?? "";
                    crawler["Schedule"] = new Dictionary<string, object?> { ["ScheduleExpression"] = sched };
                }
                else
                {
                    crawler["Schedule"] = ElementToDict(schedEl);
                }
            }

            crawler["LastUpdated"] = TimeHelpers.NowEpoch();
            crawler["Version"] = (crawler.TryGetValue("Version", out var vObj) && vObj is double v ? v : 1.0) + 1;

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>());
        }
    }

    private ServiceResponse ActStartCrawler(JsonElement data)
    {
        lock (_lock)
        {
            var name = GetString(data, "Name") ?? "";
            if (!_crawlers.TryGetValue(name, out var crawler))
            {
                return AwsResponseHelpers.ErrorResponseJson("EntityNotFoundException", $"Crawler {name} not found", 400);
            }

            if (string.Equals(crawler["State"] as string, "RUNNING", StringComparison.Ordinal))
            {
                return AwsResponseHelpers.ErrorResponseJson("CrawlerRunningException",
                    $"Crawler {name} is already running", 400);
            }

            crawler["State"] = "RUNNING";
            crawler["CrawlElapsedTime"] = 0.0;

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>());
        }
    }

    private ServiceResponse ActStopCrawler(JsonElement data)
    {
        lock (_lock)
        {
            var name = GetString(data, "Name") ?? "";
            if (!_crawlers.TryGetValue(name, out var crawler))
            {
                return AwsResponseHelpers.ErrorResponseJson("EntityNotFoundException", $"Crawler {name} not found", 400);
            }

            if (!string.Equals(crawler["State"] as string, "RUNNING", StringComparison.Ordinal))
            {
                return AwsResponseHelpers.ErrorResponseJson("CrawlerNotRunningException",
                    $"Crawler {name} is not running", 400);
            }

            crawler["State"] = "READY";
            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>());
        }
    }

    private ServiceResponse ActGetCrawlerMetrics(JsonElement data)
    {
        lock (_lock)
        {
            var crawlerNames = GetStringList(data, "CrawlerNameList");
            if (crawlerNames.Count == 0)
            {
                crawlerNames = _crawlers.Keys.ToList();
            }

            var metrics = new List<Dictionary<string, object?>>();
            foreach (var name in crawlerNames)
            {
                if (_crawlers.TryGetValue(name, out var crawler))
                {
                    var elapsed = crawler.TryGetValue("CrawlElapsedTime", out var e) && e is double d ? d : 0.0;
                    metrics.Add(new Dictionary<string, object?>
                    {
                        ["CrawlerName"] = name,
                        ["TimeLeftSeconds"] = 0.0,
                        ["StillEstimating"] = false,
                        ["LastRuntimeSeconds"] = elapsed / 1000.0,
                        ["MedianRuntimeSeconds"] = elapsed / 1000.0,
                        ["TablesCreated"] = 0.0,
                        ["TablesUpdated"] = 0.0,
                        ["TablesDeleted"] = 0.0,
                    });
                }
            }

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?> { ["CrawlerMetricsList"] = metrics });
        }
    }

    // -- Jobs ------------------------------------------------------------------

    private ServiceResponse ActCreateJob(JsonElement data)
    {
        lock (_lock)
        {
            var name = GetString(data, "Name");
            if (string.IsNullOrEmpty(name))
            {
                return AwsResponseHelpers.ErrorResponseJson("InvalidInputException", "Name is required", 400);
            }

            if (_jobs.ContainsKey(name))
            {
                return AwsResponseHelpers.ErrorResponseJson("AlreadyExistsException", $"Job {name} already exists", 400);
            }

            var now = TimeHelpers.NowEpoch();
            _jobs[name] = new Dictionary<string, object?>(StringComparer.Ordinal)
            {
                ["Name"] = name,
                ["Description"] = GetString(data, "Description") ?? "",
                ["Role"] = GetString(data, "Role") ?? "",
                ["Command"] = data.TryGetProperty("Command", out var cmd) ? ElementToDict(cmd) : new Dictionary<string, object?>(),
                ["DefaultArguments"] = data.TryGetProperty("DefaultArguments", out var da) ? ElementToDict(da) : new Dictionary<string, object?>(),
                ["MaxRetries"] = GetInt(data, "MaxRetries", 0),
                ["Timeout"] = GetInt(data, "Timeout", 2880),
                ["GlueVersion"] = GetString(data, "GlueVersion") ?? "3.0",
                ["NumberOfWorkers"] = GetInt(data, "NumberOfWorkers", 2),
                ["WorkerType"] = GetString(data, "WorkerType") ?? "G.1X",
                ["Tags"] = data.TryGetProperty("Tags", out var tags) ? ElementToDict(tags) : new Dictionary<string, object?>(),
                ["CreatedOn"] = now,
                ["LastModifiedOn"] = now,
            };
            _jobRuns[name] = [];

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?> { ["Name"] = name });
        }
    }

    private ServiceResponse ActGetJob(JsonElement data)
    {
        lock (_lock)
        {
            var name = GetString(data, "JobName") ?? "";
            if (!_jobs.TryGetValue(name, out var job))
            {
                return AwsResponseHelpers.ErrorResponseJson("EntityNotFoundException", $"Job {name} not found", 400);
            }

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?> { ["Job"] = job });
        }
    }

    private ServiceResponse ActGetJobs(JsonElement data)
    {
        lock (_lock)
        {
            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>
            {
                ["Jobs"] = _jobs.Values.ToList(),
            });
        }
    }

    private ServiceResponse ActDeleteJob(JsonElement data)
    {
        lock (_lock)
        {
            var name = GetString(data, "JobName") ?? "";
            _jobs.TryRemove(name, out _);
            _jobRuns.TryRemove(name, out _);
            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?> { ["JobName"] = name });
        }
    }

    private ServiceResponse ActUpdateJob(JsonElement data)
    {
        lock (_lock)
        {
            var name = GetString(data, "JobName") ?? "";
            if (!_jobs.TryGetValue(name, out var job))
            {
                return AwsResponseHelpers.ErrorResponseJson("EntityNotFoundException", $"Job {name} not found", 400);
            }

            if (data.TryGetProperty("JobUpdate", out var jobUpdate))
            {
                foreach (var prop in new[] { "Description", "Role", "GlueVersion", "WorkerType" })
                {
                    var val = GetString(jobUpdate, prop);
                    if (val is not null)
                    {
                        job[prop] = val;
                    }
                }

                if (jobUpdate.TryGetProperty("Command", out var cmd))
                {
                    job["Command"] = ElementToDict(cmd);
                }

                if (jobUpdate.TryGetProperty("DefaultArguments", out var da))
                {
                    job["DefaultArguments"] = ElementToDict(da);
                }

                if (jobUpdate.TryGetProperty("MaxRetries", out var mr) && mr.TryGetInt32(out var mrVal))
                {
                    job["MaxRetries"] = mrVal;
                }

                if (jobUpdate.TryGetProperty("Timeout", out var to) && to.TryGetInt32(out var toVal))
                {
                    job["Timeout"] = toVal;
                }

                if (jobUpdate.TryGetProperty("NumberOfWorkers", out var nw) && nw.TryGetInt32(out var nwVal))
                {
                    job["NumberOfWorkers"] = nwVal;
                }
            }

            job["LastModifiedOn"] = TimeHelpers.NowEpoch();
            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?> { ["JobName"] = name });
        }
    }

    private ServiceResponse ActStartJobRun(JsonElement data)
    {
        lock (_lock)
        {
            var jobName = GetString(data, "JobName") ?? "";
            if (!_jobs.TryGetValue(jobName, out var job))
            {
                return AwsResponseHelpers.ErrorResponseJson("EntityNotFoundException", $"Job {jobName} not found", 400);
            }

            var runId = HashHelpers.NewUuid();
            var now = TimeHelpers.NowEpoch();

            var run = new Dictionary<string, object?>(StringComparer.Ordinal)
            {
                ["Id"] = runId,
                ["JobName"] = jobName,
                ["StartedOn"] = now,
                ["LastModifiedOn"] = now,
                ["CompletedOn"] = null,
                ["JobRunState"] = "SUCCEEDED",
                ["Arguments"] = data.TryGetProperty("Arguments", out var args) ? ElementToDict(args) : new Dictionary<string, object?>(),
                ["ErrorMessage"] = "",
                ["ExecutionTime"] = 0.0,
                ["Timeout"] = job.TryGetValue("Timeout", out var t) ? t : 2880,
                ["NumberOfWorkers"] = job.TryGetValue("NumberOfWorkers", out var nw) ? nw : 2,
                ["GlueVersion"] = job.TryGetValue("GlueVersion", out var gv) ? gv : "3.0",
                ["Attempt"] = 0.0,
            };

            if (!_jobRuns.TryGetValue(jobName, out var runs))
            {
                runs = [];
                _jobRuns[jobName] = runs;
            }

            runs.Add(run);
            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?> { ["JobRunId"] = runId });
        }
    }

    private ServiceResponse ActGetJobRun(JsonElement data)
    {
        lock (_lock)
        {
            var jobName = GetString(data, "JobName") ?? "";
            var runId = GetString(data, "RunId") ?? "";

            if (_jobRuns.TryGetValue(jobName, out var runs))
            {
                foreach (var run in runs)
                {
                    if (string.Equals(run.TryGetValue("Id", out var id) ? id as string : null, runId, StringComparison.Ordinal))
                    {
                        return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?> { ["JobRun"] = run });
                    }
                }
            }

            return AwsResponseHelpers.ErrorResponseJson("EntityNotFoundException", $"Job run {runId} not found", 400);
        }
    }

    private ServiceResponse ActGetJobRuns(JsonElement data)
    {
        lock (_lock)
        {
            var jobName = GetString(data, "JobName") ?? "";
            var runs = _jobRuns.TryGetValue(jobName, out var r) ? r : [];
            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?> { ["JobRuns"] = runs });
        }
    }

    private ServiceResponse ActBatchStopJobRun(JsonElement data)
    {
        lock (_lock)
        {
            var jobName = GetString(data, "JobName") ?? "";
            var runIds = GetStringList(data, "JobRunIds");
            var errors = new List<Dictionary<string, object?>>();
            var successful = new List<Dictionary<string, object?>>();

            foreach (var runId in runIds)
            {
                var found = false;
                if (_jobRuns.TryGetValue(jobName, out var runs))
                {
                    foreach (var run in runs)
                    {
                        if (string.Equals(run.TryGetValue("Id", out var id) ? id as string : null, runId, StringComparison.Ordinal))
                        {
                            var state = run.TryGetValue("JobRunState", out var s) ? s as string : "";
                            if (state is "STARTING" or "RUNNING")
                            {
                                run["JobRunState"] = "STOPPED";
                                run["CompletedOn"] = TimeHelpers.NowEpoch();
                                run["LastModifiedOn"] = TimeHelpers.NowEpoch();
                                successful.Add(new Dictionary<string, object?> { ["JobName"] = jobName, ["JobRunId"] = runId });
                            }
                            else
                            {
                                errors.Add(new Dictionary<string, object?>
                                {
                                    ["JobName"] = jobName,
                                    ["JobRunId"] = runId,
                                    ["ErrorDetail"] = new Dictionary<string, object?>
                                    {
                                        ["ErrorCode"] = "InvalidInputException",
                                        ["ErrorMessage"] = $"Run {runId} is in state {state}",
                                    },
                                });
                            }

                            found = true;
                            break;
                        }
                    }
                }

                if (!found)
                {
                    errors.Add(new Dictionary<string, object?>
                    {
                        ["JobName"] = jobName,
                        ["JobRunId"] = runId,
                        ["ErrorDetail"] = new Dictionary<string, object?>
                        {
                            ["ErrorCode"] = "EntityNotFoundException",
                            ["ErrorMessage"] = "Run not found",
                        },
                    });
                }
            }

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>
            {
                ["SuccessfulSubmissions"] = successful,
                ["Errors"] = errors,
            });
        }
    }

    // -- Registries ------------------------------------------------------------

    private ServiceResponse ActCreateRegistry(JsonElement data)
    {
        lock (_lock)
        {
            var name = GetString(data, "RegistryName") ?? "";
            if (string.IsNullOrEmpty(name))
            {
                return AwsResponseHelpers.ErrorResponseJson("InvalidInputException", "RegistryName is required", 400);
            }

            if (_registries.ContainsKey(name))
            {
                return AwsResponseHelpers.ErrorResponseJson("AlreadyExistsException", $"Registry {name} already exists", 400);
            }

            var arn = Arn("registry", name);
            _registries[name] = new Dictionary<string, object?>(StringComparer.Ordinal)
            {
                ["RegistryName"] = name,
                ["RegistryArn"] = arn,
                ["Description"] = GetString(data, "Description") ?? "",
                ["Status"] = "AVAILABLE",
                ["CreatedTime"] = TimeHelpers.NowIso(),
                ["UpdatedTime"] = TimeHelpers.NowIso(),
            };

            return AwsResponseHelpers.JsonResponse(_registries[name]);
        }
    }

    private ServiceResponse ActGetRegistry(JsonElement data)
    {
        lock (_lock)
        {
            var registryId = data.TryGetProperty("RegistryId", out var ri) ? ri : data;
            var name = GetString(registryId, "RegistryName") ?? "";

            if (!_registries.TryGetValue(name, out var registry))
            {
                return AwsResponseHelpers.ErrorResponseJson("EntityNotFoundException", $"Registry {name} not found", 400);
            }

            return AwsResponseHelpers.JsonResponse(registry);
        }
    }

    private ServiceResponse ActListRegistries(JsonElement data)
    {
        lock (_lock)
        {
            var registries = _registries.Values.ToList();
            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?> { ["Registries"] = registries });
        }
    }

    private ServiceResponse ActDeleteRegistry(JsonElement data)
    {
        lock (_lock)
        {
            var registryId = data.TryGetProperty("RegistryId", out var ri) ? ri : data;
            var name = GetString(registryId, "RegistryName") ?? "";

            if (!_registries.ContainsKey(name))
            {
                return AwsResponseHelpers.ErrorResponseJson("EntityNotFoundException", $"Registry {name} not found", 400);
            }

            _registries.TryRemove(name, out _);

            // Clean up schemas belonging to this registry
            var schemaKeys = _schemas.Keys.Where(k => k.StartsWith($"{name}/", StringComparison.Ordinal)).ToList();
            foreach (var sk in schemaKeys)
            {
                _schemas.TryRemove(sk, out _);
                _schemaVersions.TryRemove(sk, out _);
            }

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?> { ["RegistryName"] = name, ["Status"] = "DELETING" });
        }
    }

    // -- Schemas ---------------------------------------------------------------

    private ServiceResponse ActCreateSchema(JsonElement data)
    {
        lock (_lock)
        {
            var registryId = data.TryGetProperty("RegistryId", out var ri) ? ri : data;
            var registryName = GetString(registryId, "RegistryName") ?? "default-registry";
            var schemaName = GetString(data, "SchemaName") ?? "";
            var key = $"{registryName}/{schemaName}";

            if (_schemas.ContainsKey(key))
            {
                return AwsResponseHelpers.ErrorResponseJson("AlreadyExistsException", $"Schema {schemaName} already exists", 400);
            }

            var arn = Arn("schema", $"{registryName}/{schemaName}");
            _schemas[key] = new Dictionary<string, object?>(StringComparer.Ordinal)
            {
                ["SchemaName"] = schemaName,
                ["SchemaArn"] = arn,
                ["RegistryName"] = registryName,
                ["DataFormat"] = GetString(data, "DataFormat") ?? "AVRO",
                ["Compatibility"] = GetString(data, "Compatibility") ?? "NONE",
                ["Description"] = GetString(data, "Description") ?? "",
                ["SchemaStatus"] = "AVAILABLE",
                ["LatestSchemaVersion"] = 1.0,
                ["NextSchemaVersion"] = 2.0,
                ["SchemaCheckpoint"] = 1.0,
                ["CreatedTime"] = TimeHelpers.NowIso(),
                ["UpdatedTime"] = TimeHelpers.NowIso(),
            };

            var definition = GetString(data, "SchemaDefinition") ?? "";
            _schemaVersions[key] =
            [
                new Dictionary<string, object?>(StringComparer.Ordinal)
                {
                    ["SchemaVersionId"] = HashHelpers.NewUuid(),
                    ["VersionNumber"] = 1.0,
                    ["Status"] = "AVAILABLE",
                    ["SchemaDefinition"] = definition,
                    ["CreatedTime"] = TimeHelpers.NowIso(),
                },
            ];

            return AwsResponseHelpers.JsonResponse(_schemas[key]);
        }
    }

    private ServiceResponse ActGetSchema(JsonElement data)
    {
        lock (_lock)
        {
            var schemaId = data.TryGetProperty("SchemaId", out var si) ? si : data;
            var registryName = GetString(schemaId, "RegistryName") ?? "default-registry";
            var schemaName = GetString(schemaId, "SchemaName") ?? "";
            var key = $"{registryName}/{schemaName}";

            if (!_schemas.TryGetValue(key, out var schema))
            {
                return AwsResponseHelpers.ErrorResponseJson("EntityNotFoundException", $"Schema {schemaName} not found", 400);
            }

            return AwsResponseHelpers.JsonResponse(schema);
        }
    }

    private ServiceResponse ActListSchemas(JsonElement data)
    {
        lock (_lock)
        {
            var registryId = data.TryGetProperty("RegistryId", out var ri) ? ri : data;
            var registryName = GetString(registryId, "RegistryName");

            var schemas = registryName is not null
                ? _schemas.Items
                    .Where(kv => kv.Key.StartsWith($"{registryName}/", StringComparison.Ordinal))
                    .Select(kv => kv.Value)
                    .ToList()
                : _schemas.Values.ToList();

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?> { ["Schemas"] = schemas });
        }
    }

    private ServiceResponse ActDeleteSchema(JsonElement data)
    {
        lock (_lock)
        {
            var schemaId = data.TryGetProperty("SchemaId", out var si) ? si : data;
            var registryName = GetString(schemaId, "RegistryName") ?? "default-registry";
            var schemaName = GetString(schemaId, "SchemaName") ?? "";
            var key = $"{registryName}/{schemaName}";

            if (!_schemas.ContainsKey(key))
            {
                return AwsResponseHelpers.ErrorResponseJson("EntityNotFoundException", $"Schema {schemaName} not found", 400);
            }

            _schemas.TryRemove(key, out _);
            _schemaVersions.TryRemove(key, out _);

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?> { ["SchemaName"] = schemaName, ["Status"] = "DELETING" });
        }
    }

    private ServiceResponse ActRegisterSchemaVersion(JsonElement data)
    {
        lock (_lock)
        {
            var schemaId = data.TryGetProperty("SchemaId", out var si) ? si : data;
            var registryName = GetString(schemaId, "RegistryName") ?? "default-registry";
            var schemaName = GetString(schemaId, "SchemaName") ?? "";
            var key = $"{registryName}/{schemaName}";

            if (!_schemas.TryGetValue(key, out var schema))
            {
                return AwsResponseHelpers.ErrorResponseJson("EntityNotFoundException", $"Schema {schemaName} not found", 400);
            }

            if (!_schemaVersions.TryGetValue(key, out var versions))
            {
                versions = [];
                _schemaVersions[key] = versions;
            }

            var versionNumber = versions.Count + 1;
            var versionId = HashHelpers.NewUuid();
            var definition = GetString(data, "SchemaDefinition") ?? "";

            versions.Add(new Dictionary<string, object?>(StringComparer.Ordinal)
            {
                ["SchemaVersionId"] = versionId,
                ["VersionNumber"] = (double)versionNumber,
                ["Status"] = "AVAILABLE",
                ["SchemaDefinition"] = definition,
                ["CreatedTime"] = TimeHelpers.NowIso(),
            });

            schema["LatestSchemaVersion"] = (double)versionNumber;
            schema["NextSchemaVersion"] = (double)(versionNumber + 1);
            schema["UpdatedTime"] = TimeHelpers.NowIso();

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>
            {
                ["SchemaVersionId"] = versionId,
                ["VersionNumber"] = (double)versionNumber,
                ["Status"] = "AVAILABLE",
            });
        }
    }

    private ServiceResponse ActGetSchemaVersion(JsonElement data)
    {
        lock (_lock)
        {
            var schemaId = data.TryGetProperty("SchemaId", out var si) ? si : data;
            var registryName = GetString(schemaId, "RegistryName") ?? "default-registry";
            var schemaName = GetString(schemaId, "SchemaName") ?? "";
            var key = $"{registryName}/{schemaName}";

            var schemaVersionId = GetString(data, "SchemaVersionId");
            var schemaVersionNumber = data.TryGetProperty("SchemaVersionNumber", out var svn)
                ? svn.TryGetProperty("VersionNumber", out var vn) && vn.TryGetInt64(out var vnVal) ? (int)vnVal
                    : svn.TryGetProperty("LatestVersion", out var lv) && lv.ValueKind == JsonValueKind.True ? -1
                    : 0
                : 0;

            if (schemaVersionId is not null)
            {
                // Search all schemas for this version ID
                foreach (var kv in _schemaVersions.Items)
                {
                    foreach (var ver in kv.Value)
                    {
                        if (string.Equals(ver.TryGetValue("SchemaVersionId", out var vid) ? vid as string : null,
                            schemaVersionId, StringComparison.Ordinal))
                        {
                            return AwsResponseHelpers.JsonResponse(ver);
                        }
                    }
                }

                return AwsResponseHelpers.ErrorResponseJson("EntityNotFoundException", "Schema version not found", 400);
            }

            if (!_schemaVersions.TryGetValue(key, out var versions) || versions.Count == 0)
            {
                return AwsResponseHelpers.ErrorResponseJson("EntityNotFoundException", "Schema version not found", 400);
            }

            if (schemaVersionNumber == -1)
            {
                return AwsResponseHelpers.JsonResponse(versions[^1]);
            }

            if (schemaVersionNumber > 0 && schemaVersionNumber <= versions.Count)
            {
                return AwsResponseHelpers.JsonResponse(versions[schemaVersionNumber - 1]);
            }

            return AwsResponseHelpers.JsonResponse(versions[^1]);
        }
    }

    private ServiceResponse ActListSchemaVersions(JsonElement data)
    {
        lock (_lock)
        {
            var schemaId = data.TryGetProperty("SchemaId", out var si) ? si : data;
            var registryName = GetString(schemaId, "RegistryName") ?? "default-registry";
            var schemaName = GetString(schemaId, "SchemaName") ?? "";
            var key = $"{registryName}/{schemaName}";

            var versions = _schemaVersions.TryGetValue(key, out var vs) ? vs : [];
            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?> { ["Schemas"] = versions });
        }
    }

    // -- Tags ------------------------------------------------------------------

    private ServiceResponse ActTagResource(JsonElement data)
    {
        lock (_lock)
        {
            var arn = GetString(data, "ResourceArn") ?? "";
            if (!_tags.TryGetValue(arn, out var tagMap))
            {
                tagMap = new Dictionary<string, string>(StringComparer.Ordinal);
                _tags[arn] = tagMap;
            }

            var tagsToAdd = GetStringMap(data, "TagsToAdd");
            foreach (var (k, v) in tagsToAdd)
            {
                tagMap[k] = v;
            }

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>());
        }
    }

    private ServiceResponse ActUntagResource(JsonElement data)
    {
        lock (_lock)
        {
            var arn = GetString(data, "ResourceArn") ?? "";
            if (_tags.TryGetValue(arn, out var tagMap))
            {
                var keysToRemove = GetStringList(data, "TagsToRemove");
                foreach (var k in keysToRemove)
                {
                    tagMap.Remove(k);
                }
            }

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>());
        }
    }

    private ServiceResponse ActGetTags(JsonElement data)
    {
        lock (_lock)
        {
            var arn = GetString(data, "ResourceArn") ?? "";
            var tagMap = _tags.TryGetValue(arn, out var tm) ? tm : new Dictionary<string, string>();
            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?> { ["Tags"] = tagMap });
        }
    }
}
