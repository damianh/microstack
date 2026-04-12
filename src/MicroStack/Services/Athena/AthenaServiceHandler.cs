using System.Text.Json;
using MicroStack.Internal;

namespace MicroStack.Services.Athena;

/// <summary>
/// Athena service handler -- supports JSON protocol via X-Amz-Target (AmazonAthena).
///
/// Port of ministack/services/athena.py.
///
/// Supports: CreateWorkGroup, GetWorkGroup, ListWorkGroups, DeleteWorkGroup, UpdateWorkGroup,
///           StartQueryExecution, GetQueryExecution, ListQueryExecutions, StopQueryExecution,
///           GetQueryResults, GetNamedQuery, CreateNamedQuery, DeleteNamedQuery, ListNamedQueries,
///           BatchGetNamedQuery, BatchGetQueryExecution,
///           CreateDataCatalog, GetDataCatalog, ListDataCatalogs, DeleteDataCatalog, UpdateDataCatalog,
///           CreatePreparedStatement, GetPreparedStatement, DeletePreparedStatement, ListPreparedStatements,
///           ListTableMetadata, GetTableMetadata, ListDatabases, GetDatabase,
///           TagResource, UntagResource, ListTagsForResource.
/// </summary>
internal sealed class AthenaServiceHandler : IServiceHandler
{
    private readonly Lock _lock = new();

    private static string Region => MicroStackOptions.Instance.Region;

    // In-memory state
    private readonly AccountScopedDictionary<string, Dictionary<string, object?>> _executions = new();
    private Dictionary<string, Dictionary<string, object?>> _workgroups = CreateDefaultWorkgroups();
    private readonly AccountScopedDictionary<string, Dictionary<string, object?>> _namedQueries = new();
    private Dictionary<string, Dictionary<string, object?>> _dataCatalogs = CreateDefaultCatalogs();
    private readonly AccountScopedDictionary<string, Dictionary<string, object?>> _preparedStatements = new(); // "wg/name" -> stmt
    private readonly AccountScopedDictionary<string, Dictionary<string, string>> _tags = new(); // arn -> {key:val}

    private static Dictionary<string, Dictionary<string, object?>> CreateDefaultWorkgroups()
    {
        return new Dictionary<string, Dictionary<string, object?>>(StringComparer.Ordinal)
        {
            ["primary"] = new Dictionary<string, object?>(StringComparer.Ordinal)
            {
                ["Name"] = "primary",
                ["State"] = "ENABLED",
                ["Description"] = "Primary workgroup",
                ["CreationTime"] = TimeHelpers.NowEpoch(),
                ["Configuration"] = new Dictionary<string, object?>(StringComparer.Ordinal)
                {
                    ["ResultConfiguration"] = new Dictionary<string, object?>(StringComparer.Ordinal)
                    {
                        ["OutputLocation"] = "s3://athena-results/",
                    },
                },
            },
        };
    }

    private static Dictionary<string, Dictionary<string, object?>> CreateDefaultCatalogs()
    {
        return new Dictionary<string, Dictionary<string, object?>>(StringComparer.Ordinal)
        {
            ["AwsDataCatalog"] = new Dictionary<string, object?>(StringComparer.Ordinal)
            {
                ["Name"] = "AwsDataCatalog",
                ["Description"] = "AWS Glue Data Catalog",
                ["Type"] = "GLUE",
                ["Parameters"] = new Dictionary<string, object?>(),
            },
        };
    }

    // -- IServiceHandler -------------------------------------------------------

    public string ServiceName => "athena";

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
            // Query Execution
            "StartQueryExecution" => ActStartQueryExecution(data),
            "GetQueryExecution" => ActGetQueryExecution(data),
            "GetQueryResults" => ActGetQueryResults(data),
            "StopQueryExecution" => ActStopQueryExecution(data),
            "ListQueryExecutions" => ActListQueryExecutions(data),
            "BatchGetQueryExecution" => ActBatchGetQueryExecution(data),
            // WorkGroups
            "CreateWorkGroup" => ActCreateWorkGroup(data),
            "GetWorkGroup" => ActGetWorkGroup(data),
            "ListWorkGroups" => ActListWorkGroups(data),
            "DeleteWorkGroup" => ActDeleteWorkGroup(data),
            "UpdateWorkGroup" => ActUpdateWorkGroup(data),
            // Named Queries
            "CreateNamedQuery" => ActCreateNamedQuery(data),
            "GetNamedQuery" => ActGetNamedQuery(data),
            "DeleteNamedQuery" => ActDeleteNamedQuery(data),
            "ListNamedQueries" => ActListNamedQueries(data),
            "BatchGetNamedQuery" => ActBatchGetNamedQuery(data),
            // Data Catalogs
            "CreateDataCatalog" => ActCreateDataCatalog(data),
            "GetDataCatalog" => ActGetDataCatalog(data),
            "ListDataCatalogs" => ActListDataCatalogs(data),
            "DeleteDataCatalog" => ActDeleteDataCatalog(data),
            "UpdateDataCatalog" => ActUpdateDataCatalog(data),
            // Prepared Statements
            "CreatePreparedStatement" => ActCreatePreparedStatement(data),
            "GetPreparedStatement" => ActGetPreparedStatement(data),
            "DeletePreparedStatement" => ActDeletePreparedStatement(data),
            "ListPreparedStatements" => ActListPreparedStatements(data),
            // Table / Database metadata (stubs)
            "GetTableMetadata" => ActGetTableMetadata(data),
            "ListTableMetadata" => ActListTableMetadata(data),
            "GetDatabase" => ActGetDatabase(data),
            "ListDatabases" => ActListDatabases(data),
            // Tags
            "TagResource" => ActTagResource(data),
            "UntagResource" => ActUntagResource(data),
            "ListTagsForResource" => ActListTagsForResource(data),
            _ => AwsResponseHelpers.ErrorResponseJson("InvalidAction", $"Unknown Athena action: {action}", 400),
        };

        return Task.FromResult(response);
    }

    public void Reset()
    {
        lock (_lock)
        {
            _executions.Clear();
            _namedQueries.Clear();
            _preparedStatements.Clear();
            _tags.Clear();
            _workgroups = CreateDefaultWorkgroups();
            _dataCatalogs = CreateDefaultCatalogs();
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

    private string ArnWorkgroup(string name)
    {
        return $"arn:aws:athena:{Region}:{AccountContext.GetAccountId()}:workgroup/{name}";
    }

    private string ArnDataCatalog(string name)
    {
        return $"arn:aws:athena:{Region}:{AccountContext.GetAccountId()}:datacatalog/{name}";
    }

    // -- Query Execution -------------------------------------------------------

    private ServiceResponse ActStartQueryExecution(JsonElement data)
    {
        lock (_lock)
        {
            var query = GetString(data, "QueryString") ?? "";
            var queryId = HashHelpers.NewUuid();
            var workgroup = GetString(data, "WorkGroup") ?? "primary";

            var outputLocation = "s3://athena-results/";
            if (data.TryGetProperty("ResultConfiguration", out var rc))
            {
                outputLocation = GetString(rc, "OutputLocation") ?? outputLocation;
            }
            else if (_workgroups.TryGetValue(workgroup, out var wg)
                     && wg.TryGetValue("Configuration", out var cfgObj) && cfgObj is Dictionary<string, object?> cfg
                     && cfg.TryGetValue("ResultConfiguration", out var rcObj) && rcObj is Dictionary<string, object?> rcDict
                     && rcDict.TryGetValue("OutputLocation", out var olObj) && olObj is string ol)
            {
                outputLocation = ol;
            }

            var db = "default";
            var catalog = "AwsDataCatalog";
            if (data.TryGetProperty("QueryExecutionContext", out var ctx))
            {
                db = GetString(ctx, "Database") ?? db;
                catalog = GetString(ctx, "Catalog") ?? catalog;
            }

            var statementType = DetectStatementType(query);
            var now = TimeHelpers.NowEpoch();

            // Mock query results
            var mockResults = MockQueryResults(query);
            var resultColumns = mockResults.TryGetValue("columns", out var cObj) && cObj is List<object?> cols
                ? cols.Cast<string>().ToList()
                : new List<string>();
            var resultRows = mockResults.TryGetValue("rows", out var rObj) && rObj is List<object?> rows
                ? rows
                : new List<object?>();
            var resultColumnTypes = mockResults.TryGetValue("column_types", out var ctObj) && ctObj is List<object?> cts
                ? cts.Cast<string>().ToList()
                : resultColumns.ConvertAll(_ => "varchar");

            var execution = new Dictionary<string, object?>(StringComparer.Ordinal)
            {
                ["QueryExecutionId"] = queryId,
                ["Query"] = query,
                ["StatementType"] = statementType,
                ["ResultConfiguration"] = new Dictionary<string, object?> { ["OutputLocation"] = $"{outputLocation}{queryId}.csv" },
                ["QueryExecutionContext"] = new Dictionary<string, object?> { ["Database"] = db, ["Catalog"] = catalog },
                ["Status"] = new Dictionary<string, object?>
                {
                    ["State"] = "SUCCEEDED",
                    ["SubmissionDateTime"] = now,
                    ["CompletionDateTime"] = now,
                    ["StateChangeReason"] = "",
                },
                ["Statistics"] = new Dictionary<string, object?>
                {
                    ["EngineExecutionTimeInMillis"] = 50.0,
                    ["DataScannedInBytes"] = 0.0,
                    ["TotalExecutionTimeInMillis"] = 100.0,
                    ["QueryQueueTimeInMillis"] = 0.0,
                    ["QueryPlanningTimeInMillis"] = 10.0,
                    ["ServiceProcessingTimeInMillis"] = 30.0,
                },
                ["WorkGroup"] = workgroup,
                ["EngineVersion"] = new Dictionary<string, object?>
                {
                    ["SelectedEngineVersion"] = "Athena engine version 3",
                    ["EffectiveEngineVersion"] = "Athena engine version 3",
                },
                // Internal state for results
                ["_resultColumns"] = resultColumns,
                ["_resultRows"] = resultRows,
                ["_resultColumnTypes"] = resultColumnTypes,
            };

            _executions[queryId] = execution;
            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?> { ["QueryExecutionId"] = queryId });
        }
    }

    private ServiceResponse ActGetQueryExecution(JsonElement data)
    {
        lock (_lock)
        {
            var queryId = GetString(data, "QueryExecutionId") ?? "";
            if (!_executions.TryGetValue(queryId, out var execution))
            {
                return AwsResponseHelpers.ErrorResponseJson("InvalidRequestException", $"Query {queryId} not found", 400);
            }

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?> { ["QueryExecution"] = ExecutionOut(execution) });
        }
    }

    private ServiceResponse ActGetQueryResults(JsonElement data)
    {
        lock (_lock)
        {
            var queryId = GetString(data, "QueryExecutionId") ?? "";
            var maxResults = GetInt(data, "MaxResults", 1000);
            var nextToken = GetString(data, "NextToken");

            if (!_executions.TryGetValue(queryId, out var execution))
            {
                return AwsResponseHelpers.ErrorResponseJson("InvalidRequestException", $"Query {queryId} not found", 400);
            }

            var statusDict = execution.TryGetValue("Status", out var sObj) && sObj is Dictionary<string, object?> sd ? sd : new Dictionary<string, object?>();
            var state = statusDict.TryGetValue("State", out var stObj) && stObj is string st ? st : "";

            if (state == "FAILED")
            {
                var reason = statusDict.TryGetValue("StateChangeReason", out var srObj) && srObj is string sr ? sr : "Unknown";
                return AwsResponseHelpers.ErrorResponseJson("InvalidRequestException", $"Query has failed: {reason}", 400);
            }

            if (state != "SUCCEEDED")
            {
                return AwsResponseHelpers.ErrorResponseJson("InvalidRequestException", $"Query is in state {state}", 400);
            }

            var columns = execution.TryGetValue("_resultColumns", out var colObj) && colObj is List<string> c ? c : [];
            var allRows = execution.TryGetValue("_resultRows", out var rowObj) && rowObj is List<object?> r ? r : [];
            var columnTypes = execution.TryGetValue("_resultColumnTypes", out var ctObj) && ctObj is List<string> ct ? ct : columns.ConvertAll(_ => "varchar");

            var startIdx = 0;
            if (nextToken is not null && int.TryParse(nextToken, out var parsedIdx))
            {
                startIdx = parsedIdx;
            }

            var pageRows = allRows.Skip(startIdx).Take(maxResults).ToList();

            var resultRows = new List<Dictionary<string, object?>>();

            // Header row
            resultRows.Add(new Dictionary<string, object?>
            {
                ["Data"] = columns.ConvertAll<object?>(col => new Dictionary<string, object?> { ["VarCharValue"] = col }),
            });

            // Data rows
            foreach (var row in pageRows)
            {
                if (row is List<object?> rowVals)
                {
                    resultRows.Add(new Dictionary<string, object?>
                    {
                        ["Data"] = rowVals.ConvertAll<object?>(v => new Dictionary<string, object?> { ["VarCharValue"] = v?.ToString() ?? "" }),
                    });
                }
            }

            var columnInfo = new List<Dictionary<string, object?>>();
            for (var i = 0; i < columns.Count; i++)
            {
                var ctype = i < columnTypes.Count ? columnTypes[i] : "varchar";
                var (precision, scale) = TypePrecisionScale(ctype);
                columnInfo.Add(new Dictionary<string, object?>
                {
                    ["CatalogName"] = "hive",
                    ["SchemaName"] = "",
                    ["TableName"] = "",
                    ["Name"] = columns[i],
                    ["Label"] = columns[i],
                    ["Type"] = ctype,
                    ["Precision"] = precision,
                    ["Scale"] = scale,
                    ["Nullable"] = "UNKNOWN",
                    ["CaseSensitive"] = ctype == "varchar",
                });
            }

            var response = new Dictionary<string, object?>
            {
                ["ResultSet"] = new Dictionary<string, object?>
                {
                    ["Rows"] = resultRows,
                    ["ResultSetMetadata"] = new Dictionary<string, object?> { ["ColumnInfo"] = columnInfo },
                },
                ["UpdateCount"] = 0.0,
            };

            var endIdx = startIdx + maxResults;
            if (endIdx < allRows.Count)
            {
                response["NextToken"] = endIdx.ToString();
            }

            return AwsResponseHelpers.JsonResponse(response);
        }
    }

    private ServiceResponse ActStopQueryExecution(JsonElement data)
    {
        lock (_lock)
        {
            var queryId = GetString(data, "QueryExecutionId") ?? "";
            if (_executions.TryGetValue(queryId, out var execution))
            {
                if (execution.TryGetValue("Status", out var sObj) && sObj is Dictionary<string, object?> sd)
                {
                    var state = sd.TryGetValue("State", out var stObj) && stObj is string st ? st : "";
                    if (state is "QUEUED" or "RUNNING")
                    {
                        sd["State"] = "CANCELLED";
                        sd["StateChangeReason"] = "Query was cancelled by user";
                        sd["CompletionDateTime"] = TimeHelpers.NowEpoch();
                    }
                }
            }

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>());
        }
    }

    private ServiceResponse ActListQueryExecutions(JsonElement data)
    {
        lock (_lock)
        {
            var workgroup = GetString(data, "WorkGroup") ?? "primary";
            var ids = _executions.Items
                .Where(kv => kv.Value.TryGetValue("WorkGroup", out var wg) && string.Equals(wg as string, workgroup, StringComparison.Ordinal))
                .Select(kv => (object?)kv.Key)
                .ToList();

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?> { ["QueryExecutionIds"] = ids });
        }
    }

    private ServiceResponse ActBatchGetQueryExecution(JsonElement data)
    {
        lock (_lock)
        {
            var ids = GetStringList(data, "QueryExecutionIds");
            var execs = new List<Dictionary<string, object?>>();
            var unprocessed = new List<Dictionary<string, object?>>();

            foreach (var id in ids)
            {
                if (_executions.TryGetValue(id, out var execution))
                {
                    execs.Add(ExecutionOut(execution));
                }
                else
                {
                    unprocessed.Add(new Dictionary<string, object?>
                    {
                        ["QueryExecutionId"] = id,
                        ["ErrorCode"] = "INTERNAL_FAILURE",
                        ["ErrorMessage"] = "Not found",
                    });
                }
            }

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>
            {
                ["QueryExecutions"] = execs,
                ["UnprocessedQueryExecutionIds"] = unprocessed,
            });
        }
    }

    // -- WorkGroups ------------------------------------------------------------

    private ServiceResponse ActCreateWorkGroup(JsonElement data)
    {
        lock (_lock)
        {
            var name = GetString(data, "Name") ?? "";
            if (_workgroups.ContainsKey(name))
            {
                return AwsResponseHelpers.ErrorResponseJson("InvalidRequestException", $"WorkGroup {name} already exists", 400);
            }

            _workgroups[name] = new Dictionary<string, object?>(StringComparer.Ordinal)
            {
                ["Name"] = name,
                ["State"] = "ENABLED",
                ["Description"] = GetString(data, "Description") ?? "",
                ["CreationTime"] = TimeHelpers.NowEpoch(),
                ["Configuration"] = data.TryGetProperty("Configuration", out var cfg) ? ElementToDict(cfg) : new Dictionary<string, object?>(),
            };

            if (data.TryGetProperty("Tags", out var tagsEl) && tagsEl.ValueKind == JsonValueKind.Array)
            {
                var arn = ArnWorkgroup(name);
                var tagMap = new Dictionary<string, string>(StringComparer.Ordinal);
                foreach (var tagEl in tagsEl.EnumerateArray())
                {
                    var k = GetString(tagEl, "Key");
                    var v = GetString(tagEl, "Value") ?? "";
                    if (k is not null)
                    {
                        tagMap[k] = v;
                    }
                }

                if (tagMap.Count > 0)
                {
                    _tags[arn] = tagMap;
                }
            }

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>());
        }
    }

    private ServiceResponse ActGetWorkGroup(JsonElement data)
    {
        lock (_lock)
        {
            var name = GetString(data, "WorkGroup") ?? "";
            if (!_workgroups.TryGetValue(name, out var wg))
            {
                return AwsResponseHelpers.ErrorResponseJson("InvalidRequestException", $"WorkGroup {name} not found", 400);
            }

            var result = new Dictionary<string, object?>(wg, StringComparer.Ordinal);
            if (!result.ContainsKey("WorkGroupConfiguration"))
            {
                result["WorkGroupConfiguration"] = result.TryGetValue("Configuration", out var cfg) ? cfg : new Dictionary<string, object?>();
            }

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?> { ["WorkGroup"] = result });
        }
    }

    private ServiceResponse ActListWorkGroups(JsonElement data)
    {
        lock (_lock)
        {
            var wgs = _workgroups.Values.Select(wg => new Dictionary<string, object?>
            {
                ["Name"] = wg.TryGetValue("Name", out var n) ? n : "",
                ["State"] = wg.TryGetValue("State", out var s) ? s : "ENABLED",
                ["Description"] = wg.TryGetValue("Description", out var d) ? d : "",
                ["CreationTime"] = wg.TryGetValue("CreationTime", out var ct) ? ct : 0.0,
            }).ToList();

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?> { ["WorkGroups"] = wgs });
        }
    }

    private ServiceResponse ActDeleteWorkGroup(JsonElement data)
    {
        lock (_lock)
        {
            var name = GetString(data, "WorkGroup") ?? "";
            if (string.Equals(name, "primary", StringComparison.Ordinal))
            {
                return AwsResponseHelpers.ErrorResponseJson("InvalidRequestException", "Cannot delete primary workgroup", 400);
            }

            _workgroups.Remove(name);
            _tags.TryRemove(ArnWorkgroup(name), out _);
            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>());
        }
    }

    private ServiceResponse ActUpdateWorkGroup(JsonElement data)
    {
        lock (_lock)
        {
            var name = GetString(data, "WorkGroup") ?? "";
            if (!_workgroups.TryGetValue(name, out var wg))
            {
                return AwsResponseHelpers.ErrorResponseJson("InvalidRequestException", $"WorkGroup {name} not found", 400);
            }

            if (data.TryGetProperty("ConfigurationUpdates", out var updates))
            {
                if (!wg.TryGetValue("Configuration", out var cfgObj) || cfgObj is not Dictionary<string, object?> config)
                {
                    config = new Dictionary<string, object?>(StringComparer.Ordinal);
                    wg["Configuration"] = config;
                }

                if (updates.TryGetProperty("ResultConfigurationUpdates", out var rcu))
                {
                    if (!config.TryGetValue("ResultConfiguration", out var rcObj) || rcObj is not Dictionary<string, object?> rc)
                    {
                        rc = new Dictionary<string, object?>(StringComparer.Ordinal);
                        config["ResultConfiguration"] = rc;
                    }

                    var ol = GetString(rcu, "OutputLocation");
                    if (ol is not null)
                    {
                        rc["OutputLocation"] = ol;
                    }

                    if (rcu.TryGetProperty("EncryptionConfiguration", out var enc))
                    {
                        rc["EncryptionConfiguration"] = ElementToDict(enc);
                    }

                    if (rcu.TryGetProperty("RemoveOutputLocation", out var rol) && rol.ValueKind == JsonValueKind.True)
                    {
                        rc.Remove("OutputLocation");
                    }

                    if (rcu.TryGetProperty("RemoveEncryptionConfiguration", out var rec) && rec.ValueKind == JsonValueKind.True)
                    {
                        rc.Remove("EncryptionConfiguration");
                    }
                }
            }

            var desc = GetString(data, "Description");
            if (desc is not null)
            {
                wg["Description"] = desc;
            }

            var state = GetString(data, "State");
            if (state is not null)
            {
                wg["State"] = state;
            }

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>());
        }
    }

    // -- Named Queries ---------------------------------------------------------

    private ServiceResponse ActCreateNamedQuery(JsonElement data)
    {
        lock (_lock)
        {
            var queryId = HashHelpers.NewUuid();
            _namedQueries[queryId] = new Dictionary<string, object?>(StringComparer.Ordinal)
            {
                ["NamedQueryId"] = queryId,
                ["Name"] = GetString(data, "Name") ?? "",
                ["Description"] = GetString(data, "Description") ?? "",
                ["Database"] = GetString(data, "Database") ?? "default",
                ["QueryString"] = GetString(data, "QueryString") ?? "",
                ["WorkGroup"] = GetString(data, "WorkGroup") ?? "primary",
            };

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?> { ["NamedQueryId"] = queryId });
        }
    }

    private ServiceResponse ActGetNamedQuery(JsonElement data)
    {
        lock (_lock)
        {
            var queryId = GetString(data, "NamedQueryId") ?? "";
            if (!_namedQueries.TryGetValue(queryId, out var nq))
            {
                return AwsResponseHelpers.ErrorResponseJson("InvalidRequestException", $"Named query {queryId} not found", 400);
            }

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?> { ["NamedQuery"] = nq });
        }
    }

    private ServiceResponse ActDeleteNamedQuery(JsonElement data)
    {
        lock (_lock)
        {
            var queryId = GetString(data, "NamedQueryId") ?? "";
            _namedQueries.TryRemove(queryId, out _);
            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>());
        }
    }

    private ServiceResponse ActListNamedQueries(JsonElement data)
    {
        lock (_lock)
        {
            var workgroup = GetString(data, "WorkGroup") ?? "primary";
            var ids = _namedQueries.Items
                .Where(kv => kv.Value.TryGetValue("WorkGroup", out var wg) && string.Equals(wg as string, workgroup, StringComparison.Ordinal))
                .Select(kv => (object?)kv.Key)
                .ToList();

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?> { ["NamedQueryIds"] = ids });
        }
    }

    private ServiceResponse ActBatchGetNamedQuery(JsonElement data)
    {
        lock (_lock)
        {
            var ids = GetStringList(data, "NamedQueryIds");
            var queries = new List<Dictionary<string, object?>>();
            var unprocessed = new List<Dictionary<string, object?>>();

            foreach (var id in ids)
            {
                if (_namedQueries.TryGetValue(id, out var nq))
                {
                    queries.Add(nq);
                }
                else
                {
                    unprocessed.Add(new Dictionary<string, object?>
                    {
                        ["NamedQueryId"] = id,
                        ["ErrorCode"] = "INTERNAL_FAILURE",
                        ["ErrorMessage"] = "Not found",
                    });
                }
            }

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>
            {
                ["NamedQueries"] = queries,
                ["UnprocessedNamedQueryIds"] = unprocessed,
            });
        }
    }

    // -- Data Catalogs ---------------------------------------------------------

    private ServiceResponse ActCreateDataCatalog(JsonElement data)
    {
        lock (_lock)
        {
            var name = GetString(data, "Name");
            if (string.IsNullOrEmpty(name))
            {
                return AwsResponseHelpers.ErrorResponseJson("InvalidRequestException", "Name is required", 400);
            }

            if (_dataCatalogs.ContainsKey(name))
            {
                return AwsResponseHelpers.ErrorResponseJson("InvalidRequestException", $"Data catalog {name} already exists", 400);
            }

            var catalogType = GetString(data, "Type") ?? "HIVE";
            if (catalogType is not ("HIVE" or "LAMBDA" or "GLUE"))
            {
                return AwsResponseHelpers.ErrorResponseJson("InvalidRequestException", $"Invalid catalog type: {catalogType}", 400);
            }

            _dataCatalogs[name] = new Dictionary<string, object?>(StringComparer.Ordinal)
            {
                ["Name"] = name,
                ["Description"] = GetString(data, "Description") ?? "",
                ["Type"] = catalogType,
                ["Parameters"] = data.TryGetProperty("Parameters", out var pEl) ? ElementToDict(pEl) : new Dictionary<string, object?>(),
            };

            if (data.TryGetProperty("Tags", out var tagsEl) && tagsEl.ValueKind == JsonValueKind.Array)
            {
                var arn = ArnDataCatalog(name);
                var tagMap = new Dictionary<string, string>(StringComparer.Ordinal);
                foreach (var tagEl in tagsEl.EnumerateArray())
                {
                    var k = GetString(tagEl, "Key");
                    var v = GetString(tagEl, "Value") ?? "";
                    if (k is not null)
                    {
                        tagMap[k] = v;
                    }
                }

                if (tagMap.Count > 0)
                {
                    _tags[arn] = tagMap;
                }
            }

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>());
        }
    }

    private ServiceResponse ActGetDataCatalog(JsonElement data)
    {
        lock (_lock)
        {
            var name = GetString(data, "Name") ?? "";
            if (!_dataCatalogs.TryGetValue(name, out var catalog))
            {
                return AwsResponseHelpers.ErrorResponseJson("InvalidRequestException", $"Data catalog {name} not found", 400);
            }

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?> { ["DataCatalog"] = catalog });
        }
    }

    private ServiceResponse ActListDataCatalogs(JsonElement data)
    {
        lock (_lock)
        {
            var summaries = _dataCatalogs.Values.Select(c => new Dictionary<string, object?>
            {
                ["CatalogName"] = c.TryGetValue("Name", out var n) ? n : "",
                ["Type"] = c.TryGetValue("Type", out var t) ? t : "",
            }).ToList();

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?> { ["DataCatalogsSummary"] = summaries });
        }
    }

    private ServiceResponse ActDeleteDataCatalog(JsonElement data)
    {
        lock (_lock)
        {
            var name = GetString(data, "Name") ?? "";
            if (string.Equals(name, "AwsDataCatalog", StringComparison.Ordinal))
            {
                return AwsResponseHelpers.ErrorResponseJson("InvalidRequestException", "Cannot delete the default AWS data catalog", 400);
            }

            if (!_dataCatalogs.ContainsKey(name))
            {
                return AwsResponseHelpers.ErrorResponseJson("InvalidRequestException", $"Data catalog {name} not found", 400);
            }

            _dataCatalogs.Remove(name);
            _tags.TryRemove(ArnDataCatalog(name), out _);
            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>());
        }
    }

    private ServiceResponse ActUpdateDataCatalog(JsonElement data)
    {
        lock (_lock)
        {
            var name = GetString(data, "Name") ?? "";
            if (!_dataCatalogs.TryGetValue(name, out var catalog))
            {
                return AwsResponseHelpers.ErrorResponseJson("InvalidRequestException", $"Data catalog {name} not found", 400);
            }

            var desc = GetString(data, "Description");
            if (desc is not null)
            {
                catalog["Description"] = desc;
            }

            var type = GetString(data, "Type");
            if (type is not null)
            {
                catalog["Type"] = type;
            }

            if (data.TryGetProperty("Parameters", out var pEl))
            {
                catalog["Parameters"] = ElementToDict(pEl);
            }

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>());
        }
    }

    // -- Prepared Statements ---------------------------------------------------

    private ServiceResponse ActCreatePreparedStatement(JsonElement data)
    {
        lock (_lock)
        {
            var name = GetString(data, "StatementName");
            var workgroup = GetString(data, "WorkGroup") ?? "primary";

            if (string.IsNullOrEmpty(name))
            {
                return AwsResponseHelpers.ErrorResponseJson("InvalidRequestException", "StatementName is required", 400);
            }

            var key = $"{workgroup}/{name}";
            if (_preparedStatements.ContainsKey(key))
            {
                return AwsResponseHelpers.ErrorResponseJson("InvalidRequestException",
                    $"Prepared statement {name} already exists in {workgroup}", 400);
            }

            _preparedStatements[key] = new Dictionary<string, object?>(StringComparer.Ordinal)
            {
                ["StatementName"] = name,
                ["WorkGroupName"] = workgroup,
                ["QueryStatement"] = GetString(data, "QueryStatement") ?? "",
                ["Description"] = GetString(data, "Description") ?? "",
                ["LastModifiedTime"] = TimeHelpers.NowEpoch(),
            };

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>());
        }
    }

    private ServiceResponse ActGetPreparedStatement(JsonElement data)
    {
        lock (_lock)
        {
            var name = GetString(data, "StatementName") ?? "";
            var workgroup = GetString(data, "WorkGroup") ?? GetString(data, "WorkGroupName") ?? "primary";
            var key = $"{workgroup}/{name}";

            if (!_preparedStatements.TryGetValue(key, out var stmt))
            {
                return AwsResponseHelpers.ErrorResponseJson("ResourceNotFoundException",
                    $"Prepared statement {name} not found in {workgroup}", 400);
            }

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?> { ["PreparedStatement"] = stmt });
        }
    }

    private ServiceResponse ActDeletePreparedStatement(JsonElement data)
    {
        lock (_lock)
        {
            var name = GetString(data, "StatementName") ?? "";
            var workgroup = GetString(data, "WorkGroup") ?? GetString(data, "WorkGroupName") ?? "primary";
            var key = $"{workgroup}/{name}";

            if (!_preparedStatements.ContainsKey(key))
            {
                return AwsResponseHelpers.ErrorResponseJson("ResourceNotFoundException",
                    $"Prepared statement {name} not found", 400);
            }

            _preparedStatements.TryRemove(key, out _);
            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>());
        }
    }

    private ServiceResponse ActListPreparedStatements(JsonElement data)
    {
        lock (_lock)
        {
            var workgroup = GetString(data, "WorkGroup") ?? GetString(data, "WorkGroupName") ?? "primary";
            var stmts = _preparedStatements.Items
                .Where(kv => kv.Value.TryGetValue("WorkGroupName", out var wg) &&
                             string.Equals(wg as string, workgroup, StringComparison.Ordinal))
                .Select(kv => new Dictionary<string, object?>
                {
                    ["StatementName"] = kv.Value.TryGetValue("StatementName", out var sn) ? sn : "",
                    ["LastModifiedTime"] = kv.Value.TryGetValue("LastModifiedTime", out var lm) ? lm : 0.0,
                })
                .ToList();

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?> { ["PreparedStatements"] = stmts });
        }
    }

    // -- Table / Database metadata (stubs) -------------------------------------

    private ServiceResponse ActGetTableMetadata(JsonElement data)
    {
        lock (_lock)
        {
            var table = GetString(data, "TableName") ?? "";
            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>
            {
                ["TableMetadata"] = new Dictionary<string, object?>
                {
                    ["Name"] = table,
                    ["CreateTime"] = TimeHelpers.NowEpoch(),
                    ["LastAccessTime"] = TimeHelpers.NowEpoch(),
                    ["TableType"] = "EXTERNAL_TABLE",
                    ["Columns"] = new List<object?>(),
                    ["PartitionKeys"] = new List<object?>(),
                    ["Parameters"] = new Dictionary<string, object?> { ["classification"] = "csv" },
                },
            });
        }
    }

    private static ServiceResponse ActListTableMetadata(JsonElement data)
    {
        return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?> { ["TableMetadataList"] = new List<object?>() });
    }

    private static ServiceResponse ActGetDatabase(JsonElement data)
    {
        var dbName = GetString(data, "DatabaseName") ?? "";
        return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>
        {
            ["Database"] = new Dictionary<string, object?>
            {
                ["Name"] = dbName,
                ["Description"] = "",
                ["Parameters"] = new Dictionary<string, object?>(),
            },
        });
    }

    private static ServiceResponse ActListDatabases(JsonElement data)
    {
        return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?> { ["DatabaseList"] = new List<object?>() });
    }

    // -- Tags ------------------------------------------------------------------

    private ServiceResponse ActTagResource(JsonElement data)
    {
        lock (_lock)
        {
            var arn = GetString(data, "ResourceARN") ?? "";
            if (!_tags.TryGetValue(arn, out var tagMap))
            {
                tagMap = new Dictionary<string, string>(StringComparer.Ordinal);
                _tags[arn] = tagMap;
            }

            if (data.TryGetProperty("Tags", out var tagsEl) && tagsEl.ValueKind == JsonValueKind.Array)
            {
                foreach (var tagEl in tagsEl.EnumerateArray())
                {
                    var k = GetString(tagEl, "Key");
                    var v = GetString(tagEl, "Value") ?? "";
                    if (k is not null)
                    {
                        tagMap[k] = v;
                    }
                }
            }

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>());
        }
    }

    private ServiceResponse ActUntagResource(JsonElement data)
    {
        lock (_lock)
        {
            var arn = GetString(data, "ResourceARN") ?? "";
            if (_tags.TryGetValue(arn, out var tagMap))
            {
                var keys = GetStringList(data, "TagKeys");
                foreach (var k in keys)
                {
                    tagMap.Remove(k);
                }
            }

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>());
        }
    }

    private ServiceResponse ActListTagsForResource(JsonElement data)
    {
        lock (_lock)
        {
            var arn = GetString(data, "ResourceARN") ?? "";
            var tagMap = _tags.TryGetValue(arn, out var tm) ? tm : new Dictionary<string, string>();
            var tags = tagMap.Select(kv => (object?)new Dictionary<string, object?> { ["Key"] = kv.Key, ["Value"] = kv.Value }).ToList();
            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?> { ["Tags"] = tags });
        }
    }

    // -- Helpers ---------------------------------------------------------------

    private static Dictionary<string, object?> ExecutionOut(Dictionary<string, object?> ex)
    {
        return ex.Where(kv => !kv.Key.StartsWith('_'))
            .ToDictionary(kv => kv.Key, kv => kv.Value, StringComparer.Ordinal);
    }

    private static string DetectStatementType(string query)
    {
        var q = query.TrimStart().ToUpperInvariant();
        if (q.StartsWith("SELECT", StringComparison.Ordinal) || q.StartsWith("WITH", StringComparison.Ordinal))
        {
            return "DML";
        }

        if (q.StartsWith("CREATE", StringComparison.Ordinal) || q.StartsWith("DROP", StringComparison.Ordinal) || q.StartsWith("ALTER", StringComparison.Ordinal))
        {
            return "DDL";
        }

        if (q.StartsWith("INSERT", StringComparison.Ordinal) || q.StartsWith("DELETE", StringComparison.Ordinal) ||
            q.StartsWith("UPDATE", StringComparison.Ordinal) || q.StartsWith("MERGE", StringComparison.Ordinal))
        {
            return "DML";
        }

        return "UTILITY";
    }

    private static Dictionary<string, object?> MockQueryResults(string query)
    {
        var queryUpper = query.Trim().ToUpperInvariant();
        if (queryUpper.StartsWith("SELECT", StringComparison.Ordinal))
        {
            // Try to parse "SELECT <num> AS <alias>, ..." pattern
            var aliasPattern = System.Text.RegularExpressions.Regex.Matches(
                query.Trim(),
                @"(?:(\d+(?:\.\d+)?)|'([^']*)')\s+AS\s+(\w+)",
                System.Text.RegularExpressions.RegexOptions.IgnoreCase);

            if (aliasPattern.Count > 0)
            {
                var cols = new List<object?>();
                var types = new List<object?>();
                var vals = new List<object?>();
                foreach (System.Text.RegularExpressions.Match m in aliasPattern)
                {
                    cols.Add(m.Groups[3].Value);
                    types.Add(!string.IsNullOrEmpty(m.Groups[1].Value) ? "integer" : "varchar");
                    vals.Add(!string.IsNullOrEmpty(m.Groups[1].Value) ? m.Groups[1].Value : m.Groups[2].Value);
                }

                return new Dictionary<string, object?>
                {
                    ["columns"] = cols,
                    ["column_types"] = types,
                    ["rows"] = new List<object?> { vals },
                };
            }

            // Try to parse "SELECT '<value>'" pattern
            var singleMatch = System.Text.RegularExpressions.Regex.Match(
                query.Trim(),
                @"SELECT\s+'([^']*)'",
                System.Text.RegularExpressions.RegexOptions.IgnoreCase);
            if (singleMatch.Success)
            {
                var val = singleMatch.Groups[1].Value;
                return new Dictionary<string, object?>
                {
                    ["columns"] = new List<object?> { val },
                    ["column_types"] = new List<object?> { "varchar" },
                    ["rows"] = new List<object?> { new List<object?> { val } },
                };
            }

            return new Dictionary<string, object?>
            {
                ["columns"] = new List<object?> { "result" },
                ["column_types"] = new List<object?> { "varchar" },
                ["rows"] = new List<object?> { new List<object?> { "mock_value" } },
            };
        }

        return new Dictionary<string, object?>
        {
            ["columns"] = new List<object?>(),
            ["column_types"] = new List<object?>(),
            ["rows"] = new List<object?>(),
        };
    }

    private static (int Precision, int Scale) TypePrecisionScale(string athenaType)
    {
        return athenaType switch
        {
            "integer" or "int" => (10, 0),
            "bigint" => (19, 0),
            "smallint" => (5, 0),
            "tinyint" => (3, 0),
            "double" => (17, 0),
            "float" => (7, 0),
            "boolean" => (0, 0),
            "decimal" => (38, 0),
            _ => (0, 0),
        };
    }
}
