using System.Text;
using System.Text.Json;
using MicroStack.Internal;

namespace MicroStack.Services.CloudFormation;

// Provisioners — resource create/delete handlers for each AWS resource type.
internal sealed partial class CloudFormationServiceHandler
{
    // ── S3 ──────────────────────────────────────────────────────────────────

    private (string, Dictionary<string, object?>) ProvisionS3Bucket(string logicalId, Dictionary<string, object?> props, string stackName)
    {
        var name = GetString(props.GetValueOrDefault("BucketName"));
        if (string.IsNullOrEmpty(name))
            name = PhysicalName(stackName, logicalId, lowercase: true, maxLen: 63);

        var request = BuildFormRequest("s3", "CreateBucket", []);
        // Direct S3 creation — use PUT /{bucket}
        request = new ServiceRequest("PUT", $"/{name}", EmptyHeaders, [], EmptyQuery);
        CallHandlerGetResponse("s3", request);

        var attrs = new Dictionary<string, object?>(StringComparer.Ordinal)
        {
            ["Arn"] = $"arn:aws:s3:::{name}",
            ["DomainName"] = $"{name}.s3.amazonaws.com",
            ["RegionalDomainName"] = $"{name}.s3.{Region}.amazonaws.com",
            ["WebsiteURL"] = $"http://{name}.s3-website-{Region}.amazonaws.com",
        };
        return (name, attrs);
    }

    private (string, Dictionary<string, object?>) ProvisionS3BucketPolicy(string logicalId, Dictionary<string, object?> props, string stackName)
    {
        var bucket = GetString(props.GetValueOrDefault("Bucket"));
        if (!string.IsNullOrEmpty(bucket) && props.TryGetValue("PolicyDocument", out var policyObj) && policyObj is not null)
        {
            var policyJson = policyObj is string ps ? ps : JsonSerializer.Serialize(policyObj);
            var queryParams = new Dictionary<string, string[]>(StringComparer.Ordinal)
            {
                ["policy"] = [""],
            };
            var request = new ServiceRequest("PUT", $"/{bucket}", EmptyHeaders,
                Encoding.UTF8.GetBytes(policyJson), queryParams);
            CallHandlerGetResponse("s3", request);
        }
        return ($"{bucket}-policy", new Dictionary<string, object?>());
    }

    // ── SQS ─────────────────────────────────────────────────────────────────

    private (string, Dictionary<string, object?>) ProvisionSqsQueue(string logicalId, Dictionary<string, object?> props, string stackName)
    {
        var name = GetString(props.GetValueOrDefault("QueueName"));
        if (string.IsNullOrEmpty(name))
            name = PhysicalName(stackName, logicalId, maxLen: 80);

        var formData = $"Action=CreateQueue&QueueName={Uri.EscapeDataString(name)}";
        var idx = 1;
        var propsToAttrs = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["VisibilityTimeout"] = "VisibilityTimeout",
            ["MaximumMessageSize"] = "MaximumMessageSize",
            ["MessageRetentionPeriod"] = "MessageRetentionPeriod",
            ["DelaySeconds"] = "DelaySeconds",
            ["ReceiveMessageWaitTimeSeconds"] = "ReceiveMessageWaitTimeSeconds",
        };

        foreach (var (propName, attrName) in propsToAttrs)
        {
            if (props.TryGetValue(propName, out var val) && val is not null)
            {
                formData += $"&Attribute.{idx}.Name={attrName}&Attribute.{idx}.Value={Uri.EscapeDataString(GetString(val))}";
                idx++;
            }
        }

        if (name.EndsWith(".fifo", StringComparison.Ordinal))
        {
            formData += $"&Attribute.{idx}.Name=FifoQueue&Attribute.{idx}.Value=true";
            idx++;
        }

        var request = new ServiceRequest("POST", "/", SqsHeaders,
            Encoding.UTF8.GetBytes(formData), EmptyQuery);
        var response = CallHandlerGetResponse("sqs", request);
        var responseBody = Encoding.UTF8.GetString(response.Body);

        // Parse QueueUrl from XML response
        var queueUrl = ExtractXmlValue(responseBody, "QueueUrl");
        var accountId = AccountContext.GetAccountId();
        var arn = $"arn:aws:sqs:{Region}:{accountId}:{name}";

        var attrs = new Dictionary<string, object?>(StringComparer.Ordinal)
        {
            ["Arn"] = arn, ["QueueName"] = name, ["QueueUrl"] = queueUrl,
        };
        return (queueUrl, attrs);
    }

    // ── SNS ─────────────────────────────────────────────────────────────────

    private (string, Dictionary<string, object?>) ProvisionSnsTopic(string logicalId, Dictionary<string, object?> props, string stackName)
    {
        var name = GetString(props.GetValueOrDefault("TopicName"));
        if (string.IsNullOrEmpty(name))
            name = PhysicalName(stackName, logicalId, maxLen: 256);

        var formData = $"Action=CreateTopic&Name={Uri.EscapeDataString(name)}";
        var request = new ServiceRequest("POST", "/", SnsHeaders,
            Encoding.UTF8.GetBytes(formData), EmptyQuery);
        var response = CallHandlerGetResponse("sns", request);
        var responseBody = Encoding.UTF8.GetString(response.Body);
        var arn = ExtractXmlValue(responseBody, "TopicArn");

        // Handle subscriptions
        var subs = props.GetValueOrDefault("Subscription");
        if (subs is List<object?> subList)
        {
            foreach (var subObj in subList)
            {
                var sub = GetDict(subObj);
                var protocol = GetString(sub.GetValueOrDefault("Protocol"));
                var endpoint = GetString(sub.GetValueOrDefault("Endpoint"));
                var subForm = $"Action=Subscribe&TopicArn={Uri.EscapeDataString(arn)}&Protocol={Uri.EscapeDataString(protocol)}&Endpoint={Uri.EscapeDataString(endpoint)}";
                var subReq = new ServiceRequest("POST", "/", SnsHeaders,
                    Encoding.UTF8.GetBytes(subForm), EmptyQuery);
                CallHandlerGetResponse("sns", subReq);
            }
        }

        return (arn, new Dictionary<string, object?>(StringComparer.Ordinal)
        {
            ["TopicArn"] = arn, ["TopicName"] = name,
        });
    }

    private (string, Dictionary<string, object?>) ProvisionSnsSubscription(string logicalId, Dictionary<string, object?> props, string stackName)
    {
        var topicArn = GetString(props.GetValueOrDefault("TopicArn"));
        var protocol = GetString(props.GetValueOrDefault("Protocol"));
        var endpoint = GetString(props.GetValueOrDefault("Endpoint"));

        var formData = $"Action=Subscribe&TopicArn={Uri.EscapeDataString(topicArn)}&Protocol={Uri.EscapeDataString(protocol)}&Endpoint={Uri.EscapeDataString(endpoint)}";
        var request = new ServiceRequest("POST", "/", SnsHeaders,
            Encoding.UTF8.GetBytes(formData), EmptyQuery);
        var response = CallHandlerGetResponse("sns", request);
        var responseBody = Encoding.UTF8.GetString(response.Body);
        var subArn = ExtractXmlValue(responseBody, "SubscriptionArn");
        return (subArn, new Dictionary<string, object?>(StringComparer.Ordinal)
        {
            ["SubscriptionArn"] = subArn,
        });
    }

    // ── DynamoDB ────────────────────────────────────────────────────────────

    private (string, Dictionary<string, object?>) ProvisionDynamoDbTable(string logicalId, Dictionary<string, object?> props, string stackName)
    {
        var name = GetString(props.GetValueOrDefault("TableName"));
        if (string.IsNullOrEmpty(name))
            name = PhysicalName(stackName, logicalId, maxLen: 255);

        // Build DynamoDB JSON request
        var createReq = new Dictionary<string, object?>(StringComparer.Ordinal)
        {
            ["TableName"] = name,
            ["KeySchema"] = props.GetValueOrDefault("KeySchema"),
            ["AttributeDefinitions"] = props.GetValueOrDefault("AttributeDefinitions"),
            ["BillingMode"] = GetString(props.GetValueOrDefault("BillingMode", "PROVISIONED")),
        };
        if (props.TryGetValue("ProvisionedThroughput", out var pt) && pt is not null)
            createReq["ProvisionedThroughput"] = pt;
        if (props.TryGetValue("GlobalSecondaryIndexes", out var gsi) && gsi is not null)
            createReq["GlobalSecondaryIndexes"] = gsi;
        if (props.TryGetValue("LocalSecondaryIndexes", out var lsi) && lsi is not null)
            createReq["LocalSecondaryIndexes"] = lsi;
        if (props.TryGetValue("StreamSpecification", out var ss) && ss is not null)
            createReq["StreamSpecification"] = ss;

        var jsonBody = JsonSerializer.Serialize(createReq);
        var headers = new Dictionary<string, string>
        {
            ["Content-Type"] = "application/x-amz-json-1.0",
            ["X-Amz-Target"] = "DynamoDB_20120810.CreateTable",
        };
        var request = new ServiceRequest("POST", "/", headers,
            Encoding.UTF8.GetBytes(jsonBody), EmptyQuery);
        var response = CallHandlerGetResponse("dynamodb", request);
        var responseBody = Encoding.UTF8.GetString(response.Body);

        var arn = $"arn:aws:dynamodb:{Region}:{AccountContext.GetAccountId()}:table/{name}";

        // Try to get stream ARN from response
        try
        {
            using var doc = JsonDocument.Parse(responseBody);
            if (doc.RootElement.TryGetProperty("TableDescription", out var td)
                && td.TryGetProperty("LatestStreamArn", out var streamArn))
            {
                return (name, new Dictionary<string, object?>(StringComparer.Ordinal)
                {
                    ["Arn"] = arn, ["StreamArn"] = streamArn.GetString(),
                });
            }
        }
        catch { /* ignore */ }

        return (name, new Dictionary<string, object?>(StringComparer.Ordinal) { ["Arn"] = arn });
    }

    // ── Lambda ──────────────────────────────────────────────────────────────

    private (string, Dictionary<string, object?>) ProvisionLambdaFunction(string logicalId, Dictionary<string, object?> props, string stackName)
    {
        var name = GetString(props.GetValueOrDefault("FunctionName"));
        if (string.IsNullOrEmpty(name))
            name = PhysicalName(stackName, logicalId, maxLen: 64);

        var code = GetDict(props.GetValueOrDefault("Code"));
        var runtime = GetString(props.GetValueOrDefault("Runtime", "python3.9"));
        var handler = GetString(props.GetValueOrDefault("Handler", "index.handler"));
        var role = GetString(props.GetValueOrDefault("Role", $"arn:aws:iam::{AccountContext.GetAccountId()}:role/dummy-role"));

        // The Lambda handler expects Code.ZipFile to be base64-encoded.
        // CloudFormation templates provide raw source code, so encode it here.
        var codeForRequest = new Dictionary<string, object?>(code, StringComparer.Ordinal);
        if (codeForRequest.TryGetValue("ZipFile", out var zipFileObj))
        {
            var sourceCode = GetString(zipFileObj);
            if (!string.IsNullOrEmpty(sourceCode))
            {
                codeForRequest["ZipFile"] = Convert.ToBase64String(Encoding.UTF8.GetBytes(sourceCode));
            }
        }

        var createReq = new Dictionary<string, object?>(StringComparer.Ordinal)
        {
            ["FunctionName"] = name,
            ["Runtime"] = runtime,
            ["Handler"] = handler,
            ["Role"] = role,
            ["Code"] = codeForRequest,
        };
        if (props.TryGetValue("Timeout", out var timeout)) createReq["Timeout"] = timeout;
        if (props.TryGetValue("MemorySize", out var mem)) createReq["MemorySize"] = mem;
        if (props.TryGetValue("Environment", out var env)) createReq["Environment"] = env;
        if (props.TryGetValue("Description", out var desc)) createReq["Description"] = desc;
        if (props.TryGetValue("Layers", out var layers)) createReq["Layers"] = layers;

        var jsonBody = JsonSerializer.Serialize(createReq);
        var headers = new Dictionary<string, string>
        {
            ["Content-Type"] = "application/json",
        };
        var request = new ServiceRequest("POST", "/2015-03-31/functions", headers,
            Encoding.UTF8.GetBytes(jsonBody), EmptyQuery);
        CallHandlerGetResponse("lambda", request);

        var arn = $"arn:aws:lambda:{Region}:{AccountContext.GetAccountId()}:function:{name}";
        return (name, new Dictionary<string, object?>(StringComparer.Ordinal) { ["Arn"] = arn });
    }

    // ── IAM Role ────────────────────────────────────────────────────────────

    private (string, Dictionary<string, object?>) ProvisionIamRole(string logicalId, Dictionary<string, object?> props, string stackName)
    {
        var name = GetString(props.GetValueOrDefault("RoleName"));
        if (string.IsNullOrEmpty(name))
            name = PhysicalName(stackName, logicalId, maxLen: 64);

        var assumeDoc = props.GetValueOrDefault("AssumeRolePolicyDocument");
        var assumeDocStr = assumeDoc is string s ? s : JsonSerializer.Serialize(assumeDoc);

        var formData = $"Action=CreateRole&RoleName={Uri.EscapeDataString(name)}&AssumeRolePolicyDocument={Uri.EscapeDataString(assumeDocStr)}";
        if (props.TryGetValue("Path", out var path))
            formData += $"&Path={Uri.EscapeDataString(GetString(path))}";

        var request = new ServiceRequest("POST", "/", IamHeaders,
            Encoding.UTF8.GetBytes(formData), EmptyQuery);
        var response = CallHandlerGetResponse("iam", request);
        var responseBody = Encoding.UTF8.GetString(response.Body);

        var arn = ExtractXmlValue(responseBody, "Arn");
        if (string.IsNullOrEmpty(arn))
            arn = $"arn:aws:iam::{AccountContext.GetAccountId()}:role/{name}";
        var roleId = ExtractXmlValue(responseBody, "RoleId");

        // Attach managed policies
        if (props.TryGetValue("ManagedPolicyArns", out var mpObj) && mpObj is List<object?> mpList)
        {
            foreach (var mp in mpList)
            {
                var policyArn = GetString(mp);
                var attachForm = $"Action=AttachRolePolicy&RoleName={Uri.EscapeDataString(name)}&PolicyArn={Uri.EscapeDataString(policyArn)}";
                var attachReq = new ServiceRequest("POST", "/", IamHeaders,
                    Encoding.UTF8.GetBytes(attachForm), EmptyQuery);
                CallHandlerGetResponse("iam", attachReq);
            }
        }

        // Inline policies
        if (props.TryGetValue("Policies", out var polObj) && polObj is List<object?> polList)
        {
            foreach (var pol in polList)
            {
                var polDict = GetDict(pol);
                var polName = GetString(polDict.GetValueOrDefault("PolicyName"));
                var polDoc = polDict.GetValueOrDefault("PolicyDocument");
                var polDocStr = polDoc is string ps ? ps : JsonSerializer.Serialize(polDoc);
                var putForm = $"Action=PutRolePolicy&RoleName={Uri.EscapeDataString(name)}&PolicyName={Uri.EscapeDataString(polName)}&PolicyDocument={Uri.EscapeDataString(polDocStr)}";
                var putReq = new ServiceRequest("POST", "/", IamHeaders,
                    Encoding.UTF8.GetBytes(putForm), EmptyQuery);
                CallHandlerGetResponse("iam", putReq);
            }
        }

        return (name, new Dictionary<string, object?>(StringComparer.Ordinal) { ["Arn"] = arn, ["RoleId"] = roleId });
    }

    private (string, Dictionary<string, object?>) ProvisionIamPolicy(string logicalId, Dictionary<string, object?> props, string stackName)
    {
        var name = GetString(props.GetValueOrDefault("PolicyName"));
        if (string.IsNullOrEmpty(name)) name = PhysicalName(stackName, logicalId, maxLen: 128);
        var polDoc = props.GetValueOrDefault("PolicyDocument");
        var polDocStr = polDoc is string ps ? ps : JsonSerializer.Serialize(polDoc);

        var formData = $"Action=CreatePolicy&PolicyName={Uri.EscapeDataString(name)}&PolicyDocument={Uri.EscapeDataString(polDocStr)}";
        var request = new ServiceRequest("POST", "/", IamHeaders,
            Encoding.UTF8.GetBytes(formData), EmptyQuery);
        var response = CallHandlerGetResponse("iam", request);
        var responseBody = Encoding.UTF8.GetString(response.Body);
        var arn = ExtractXmlValue(responseBody, "Arn");
        if (string.IsNullOrEmpty(arn))
            arn = $"arn:aws:iam::{AccountContext.GetAccountId()}:policy/{name}";
        return (arn, new Dictionary<string, object?>(StringComparer.Ordinal) { ["PolicyArn"] = arn });
    }

    private (string, Dictionary<string, object?>) ProvisionIamInstanceProfile(string logicalId, Dictionary<string, object?> props, string stackName)
    {
        var name = GetString(props.GetValueOrDefault("InstanceProfileName"));
        if (string.IsNullOrEmpty(name)) name = PhysicalName(stackName, logicalId, maxLen: 128);
        var arn = $"arn:aws:iam::{AccountContext.GetAccountId()}:instance-profile/{name}";

        var formData = $"Action=CreateInstanceProfile&InstanceProfileName={Uri.EscapeDataString(name)}";
        var request = new ServiceRequest("POST", "/", IamHeaders,
            Encoding.UTF8.GetBytes(formData), EmptyQuery);
        CallHandlerGetResponse("iam", request);

        if (props.TryGetValue("Roles", out var rolesObj) && rolesObj is List<object?> roles)
        {
            foreach (var rObj in roles)
            {
                var roleName = GetString(rObj);
                var addForm = $"Action=AddRoleToInstanceProfile&InstanceProfileName={Uri.EscapeDataString(name)}&RoleName={Uri.EscapeDataString(roleName)}";
                var addReq = new ServiceRequest("POST", "/", IamHeaders,
                    Encoding.UTF8.GetBytes(addForm), EmptyQuery);
                CallHandlerGetResponse("iam", addReq);
            }
        }

        return (arn, new Dictionary<string, object?>(StringComparer.Ordinal) { ["Arn"] = arn });
    }

    private (string, Dictionary<string, object?>) ProvisionIamManagedPolicy(string logicalId, Dictionary<string, object?> props, string stackName)
    {
        var name = GetString(props.GetValueOrDefault("ManagedPolicyName"));
        if (string.IsNullOrEmpty(name)) name = $"{stackName}-{logicalId}";
        var arn = $"arn:aws:iam::{AccountContext.GetAccountId()}:policy/{name}";
        var polDoc = props.GetValueOrDefault("PolicyDocument");
        var polDocStr = polDoc is string ps ? ps : JsonSerializer.Serialize(polDoc);

        var formData = $"Action=CreatePolicy&PolicyName={Uri.EscapeDataString(name)}&PolicyDocument={Uri.EscapeDataString(polDocStr)}";
        var request = new ServiceRequest("POST", "/", IamHeaders,
            Encoding.UTF8.GetBytes(formData), EmptyQuery);
        CallHandlerGetResponse("iam", request);

        return (arn, new Dictionary<string, object?>(StringComparer.Ordinal) { ["Arn"] = arn });
    }

    // ── SSM ─────────────────────────────────────────────────────────────────

    private (string, Dictionary<string, object?>) ProvisionSsmParameter(string logicalId, Dictionary<string, object?> props, string stackName)
    {
        var name = GetString(props.GetValueOrDefault("Name"));
        if (string.IsNullOrEmpty(name)) name = $"/{stackName}/{logicalId}";
        var ptype = GetString(props.GetValueOrDefault("Type", "String"));
        var value = GetString(props.GetValueOrDefault("Value"));

        var formData = $"Action=PutParameter&Name={Uri.EscapeDataString(name)}&Type={Uri.EscapeDataString(ptype)}&Value={Uri.EscapeDataString(value)}&Overwrite=true";
        var headers = new Dictionary<string, string>
        {
            ["Content-Type"] = "application/x-amz-json-1.1",
            ["X-Amz-Target"] = "AmazonSSM.PutParameter",
        };
        var jsonBody = JsonSerializer.Serialize(new Dictionary<string, object?>
        {
            ["Name"] = name, ["Type"] = ptype, ["Value"] = value, ["Overwrite"] = true,
        });
        var request = new ServiceRequest("POST", "/", headers,
            Encoding.UTF8.GetBytes(jsonBody), EmptyQuery);
        CallHandlerGetResponse("ssm", request);

        return (name, new Dictionary<string, object?>(StringComparer.Ordinal)
        {
            ["Type"] = ptype, ["Value"] = value,
        });
    }

    // ── CloudWatch Logs ─────────────────────────────────────────────────────

    private (string, Dictionary<string, object?>) ProvisionLogGroup(string logicalId, Dictionary<string, object?> props, string stackName)
    {
        var name = GetString(props.GetValueOrDefault("LogGroupName"));
        if (string.IsNullOrEmpty(name)) name = $"/aws/cloudformation/{stackName}/{logicalId}";
        var arn = $"arn:aws:logs:{Region}:{AccountContext.GetAccountId()}:log-group:{name}:*";

        var jsonBody = JsonSerializer.Serialize(new Dictionary<string, object?> { ["logGroupName"] = name });
        var headers = new Dictionary<string, string>
        {
            ["Content-Type"] = "application/x-amz-json-1.1",
            ["X-Amz-Target"] = "Logs_20140328.CreateLogGroup",
        };
        var request = new ServiceRequest("POST", "/", headers,
            Encoding.UTF8.GetBytes(jsonBody), EmptyQuery);
        CallHandlerGetResponse("logs", request);

        return (name, new Dictionary<string, object?>(StringComparer.Ordinal) { ["Arn"] = arn });
    }

    // ── EventBridge ─────────────────────────────────────────────────────────

    private (string, Dictionary<string, object?>) ProvisionEventBridgeRule(string logicalId, Dictionary<string, object?> props, string stackName)
    {
        var name = GetString(props.GetValueOrDefault("Name"));
        if (string.IsNullOrEmpty(name)) name = PhysicalName(stackName, logicalId, maxLen: 64);
        var bus = GetString(props.GetValueOrDefault("EventBusName", "default"));
        var arn = $"arn:aws:events:{Region}:{AccountContext.GetAccountId()}:rule/{bus}/{name}";

        var putReq = new Dictionary<string, object?>(StringComparer.Ordinal)
        {
            ["Name"] = name, ["EventBusName"] = bus,
            ["State"] = GetString(props.GetValueOrDefault("State", "ENABLED")),
        };
        if (props.TryGetValue("ScheduleExpression", out var se)) putReq["ScheduleExpression"] = se;
        if (props.TryGetValue("EventPattern", out var ep))
            putReq["EventPattern"] = ep is string eps ? eps : JsonSerializer.Serialize(ep);
        if (props.TryGetValue("Description", out var desc)) putReq["Description"] = desc;

        var jsonBody = JsonSerializer.Serialize(putReq);
        var headers = new Dictionary<string, string>
        {
            ["Content-Type"] = "application/x-amz-json-1.1",
            ["X-Amz-Target"] = "AWSEvents.PutRule",
        };
        var request = new ServiceRequest("POST", "/", headers,
            Encoding.UTF8.GetBytes(jsonBody), EmptyQuery);
        CallHandlerGetResponse("events", request);

        return (name, new Dictionary<string, object?>(StringComparer.Ordinal) { ["Arn"] = arn });
    }

    // ── Kinesis ─────────────────────────────────────────────────────────────

    private (string, Dictionary<string, object?>) ProvisionKinesisStream(string logicalId, Dictionary<string, object?> props, string stackName)
    {
        var name = GetString(props.GetValueOrDefault("Name"));
        if (string.IsNullOrEmpty(name)) name = PhysicalName(stackName, logicalId, lowercase: true, maxLen: 128);
        var shardCount = 1;
        if (props.TryGetValue("ShardCount", out var sc))
            shardCount = int.Parse(GetString(sc));
        if (shardCount < 1) shardCount = 1;

        var createReq = new Dictionary<string, object?>
        {
            ["StreamName"] = name, ["ShardCount"] = shardCount,
        };
        var jsonBody = JsonSerializer.Serialize(createReq);
        var headers = new Dictionary<string, string>
        {
            ["Content-Type"] = "application/x-amz-json-1.1",
            ["X-Amz-Target"] = "Kinesis_20131202.CreateStream",
        };
        var request = new ServiceRequest("POST", "/", headers,
            Encoding.UTF8.GetBytes(jsonBody), EmptyQuery);
        CallHandlerGetResponse("kinesis", request);

        var arn = $"arn:aws:kinesis:{Region}:{AccountContext.GetAccountId()}:stream/{name}";
        return (name, new Dictionary<string, object?>(StringComparer.Ordinal)
        {
            ["Arn"] = arn, ["StreamId"] = Guid.NewGuid().ToString(),
        });
    }

    // ── Lambda Permission / Version / ESM / Alias ───────────────────────────

    private (string, Dictionary<string, object?>) ProvisionLambdaPermission(string logicalId, Dictionary<string, object?> props, string stackName)
    {
        var funcName = GetString(props.GetValueOrDefault("FunctionName"));
        if (funcName.StartsWith("arn:", StringComparison.Ordinal))
            funcName = funcName.Split(':')[^1];
        var sid = GetString(props.GetValueOrDefault("Id"));
        if (string.IsNullOrEmpty(sid)) sid = logicalId;

        var addReq = new Dictionary<string, object?>
        {
            ["StatementId"] = sid,
            ["Action"] = GetString(props.GetValueOrDefault("Action", "lambda:InvokeFunction")),
            ["Principal"] = GetString(props.GetValueOrDefault("Principal", "*")),
            ["FunctionName"] = funcName,
        };
        if (props.TryGetValue("SourceArn", out var sa)) addReq["SourceArn"] = sa;

        var jsonBody = JsonSerializer.Serialize(addReq);
        var headers = new Dictionary<string, string> { ["Content-Type"] = "application/json" };
        var request = new ServiceRequest("POST", $"/2015-03-31/functions/{Uri.EscapeDataString(funcName)}/policy",
            headers, Encoding.UTF8.GetBytes(jsonBody), EmptyQuery);
        try { CallHandlerGetResponse("lambda", request); } catch { /* ignore */ }

        return ($"{stackName}-{logicalId}-{Guid.NewGuid().ToString()[..8]}", new Dictionary<string, object?>());
    }

    private (string, Dictionary<string, object?>) ProvisionLambdaVersion(string logicalId, Dictionary<string, object?> props, string stackName)
    {
        var funcName = GetString(props.GetValueOrDefault("FunctionName"));
        if (funcName.StartsWith("arn:", StringComparison.Ordinal))
            funcName = funcName.Split(':')[^1];

        var jsonBody = JsonSerializer.Serialize(new Dictionary<string, object?> { ["FunctionName"] = funcName });
        var headers = new Dictionary<string, string> { ["Content-Type"] = "application/json" };
        var request = new ServiceRequest("POST", $"/2015-03-31/functions/{Uri.EscapeDataString(funcName)}/versions",
            headers, Encoding.UTF8.GetBytes(jsonBody), EmptyQuery);
        var response = CallHandlerGetResponse("lambda", request);
        var responseBody = Encoding.UTF8.GetString(response.Body);

        var verArn = $"arn:aws:lambda:{Region}:{AccountContext.GetAccountId()}:function:{funcName}";
        var version = "1";
        try
        {
            using var doc = JsonDocument.Parse(responseBody);
            if (doc.RootElement.TryGetProperty("Version", out var v))
                version = v.GetString() ?? "1";
        }
        catch { /* ignore */ }

        return (verArn, new Dictionary<string, object?>(StringComparer.Ordinal) { ["Version"] = version });
    }

    private (string, Dictionary<string, object?>) ProvisionLambdaEsm(string logicalId, Dictionary<string, object?> props, string stackName)
    {
        var funcName = GetString(props.GetValueOrDefault("FunctionName"));
        var esmId = Guid.NewGuid().ToString();
        return (esmId, new Dictionary<string, object?>(StringComparer.Ordinal) { ["UUID"] = esmId });
    }

    private (string, Dictionary<string, object?>) ProvisionLambdaAlias(string logicalId, Dictionary<string, object?> props, string stackName)
    {
        var funcName = GetString(props.GetValueOrDefault("FunctionName"));
        if (funcName.StartsWith("arn:", StringComparison.Ordinal))
            funcName = funcName.Split(':')[^1];
        var aliasName = GetString(props.GetValueOrDefault("Name"));
        var arn = $"arn:aws:lambda:{Region}:{AccountContext.GetAccountId()}:function:{funcName}:{aliasName}";
        return (arn, new Dictionary<string, object?>(StringComparer.Ordinal) { ["AliasArn"] = arn });
    }

    // ── Queue/Topic Policy ──────────────────────────────────────────────────

    private (string, Dictionary<string, object?>) ProvisionSqsQueuePolicy(string logicalId, Dictionary<string, object?> props, string stackName)
    {
        return ($"{stackName}-{logicalId}-{Guid.NewGuid().ToString()[..8]}", new Dictionary<string, object?>());
    }

    private (string, Dictionary<string, object?>) ProvisionSnsTopicPolicy(string logicalId, Dictionary<string, object?> props, string stackName)
    {
        return ($"{stackName}-{logicalId}-{Guid.NewGuid().ToString()[..8]}", new Dictionary<string, object?>());
    }

    // ── SecretsManager ──────────────────────────────────────────────────────

    private (string, Dictionary<string, object?>) ProvisionSecret(string logicalId, Dictionary<string, object?> props, string stackName)
    {
        var name = GetString(props.GetValueOrDefault("Name"));
        if (string.IsNullOrEmpty(name)) name = PhysicalName(stackName, logicalId);
        var secretString = GetString(props.GetValueOrDefault("SecretString"));
        var arn = $"arn:aws:secretsmanager:{Region}:{AccountContext.GetAccountId()}:secret:{name}-{Guid.NewGuid().ToString()[..6]}";

        var createReq = new Dictionary<string, object?> { ["Name"] = name };

        if (props.TryGetValue("GenerateSecretString", out var genObj) && genObj is not null && string.IsNullOrEmpty(secretString))
        {
            var gen = GetDict(genObj);
            var length = 32;
            if (gen.TryGetValue("PasswordLength", out var pl)) length = int.Parse(GetString(pl));
            var chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789!@#$%^&*";
            var excludeChars = GetString(gen.GetValueOrDefault("ExcludeCharacters"));
            if (!string.IsNullOrEmpty(excludeChars))
                chars = new string(chars.Where(c => !excludeChars.Contains(c)).ToArray());
            var generated = new string(Enumerable.Range(0, length).Select(_ => chars[Random.Shared.Next(chars.Length)]).ToArray());

            var template = GetString(gen.GetValueOrDefault("SecretStringTemplate"));
            var genKey = GetString(gen.GetValueOrDefault("GenerateStringKey", "password"));
            if (!string.IsNullOrEmpty(template))
            {
                try
                {
                    var obj = JsonSerializer.Deserialize<Dictionary<string, object?>>(template) ?? new();
                    obj[genKey] = generated;
                    secretString = JsonSerializer.Serialize(obj);
                }
                catch
                {
                    secretString = generated;
                }
            }
            else
            {
                secretString = generated;
            }
        }

        createReq["SecretString"] = secretString;

        var jsonBody = JsonSerializer.Serialize(createReq);
        var headers = new Dictionary<string, string>
        {
            ["Content-Type"] = "application/x-amz-json-1.1",
            ["X-Amz-Target"] = "secretsmanager.CreateSecret",
        };
        var request = new ServiceRequest("POST", "/", headers,
            Encoding.UTF8.GetBytes(jsonBody), EmptyQuery);
        CallHandlerGetResponse("secretsmanager", request);

        return (name, new Dictionary<string, object?>(StringComparer.Ordinal) { ["Arn"] = arn });
    }

    // ── KMS ─────────────────────────────────────────────────────────────────

    private (string, Dictionary<string, object?>) ProvisionKmsKey(string logicalId, Dictionary<string, object?> props, string stackName)
    {
        var keyId = Guid.NewGuid().ToString();
        var arn = $"arn:aws:kms:{Region}:{AccountContext.GetAccountId()}:key/{keyId}";

        var createReq = new Dictionary<string, object?>();
        if (props.TryGetValue("Description", out var desc)) createReq["Description"] = desc;
        if (props.TryGetValue("KeyUsage", out var ku)) createReq["KeyUsage"] = ku;

        var jsonBody = JsonSerializer.Serialize(createReq);
        var headers = new Dictionary<string, string>
        {
            ["Content-Type"] = "application/x-amz-json-1.1",
            ["X-Amz-Target"] = "TrentService.CreateKey",
        };
        var request = new ServiceRequest("POST", "/", headers,
            Encoding.UTF8.GetBytes(jsonBody), EmptyQuery);
        try { CallHandlerGetResponse("kms", request); } catch { /* ignore */ }

        return (keyId, new Dictionary<string, object?>(StringComparer.Ordinal) { ["Arn"] = arn, ["KeyId"] = keyId });
    }

    private (string, Dictionary<string, object?>) ProvisionKmsAlias(string logicalId, Dictionary<string, object?> props, string stackName)
    {
        var aliasName = GetString(props.GetValueOrDefault("AliasName", $"alias/{stackName}-{logicalId}"));
        var targetKey = GetString(props.GetValueOrDefault("TargetKeyId"));

        var createReq = new Dictionary<string, object?> { ["AliasName"] = aliasName, ["TargetKeyId"] = targetKey };
        var jsonBody = JsonSerializer.Serialize(createReq);
        var headers = new Dictionary<string, string>
        {
            ["Content-Type"] = "application/x-amz-json-1.1",
            ["X-Amz-Target"] = "TrentService.CreateAlias",
        };
        var request = new ServiceRequest("POST", "/", headers,
            Encoding.UTF8.GetBytes(jsonBody), EmptyQuery);
        try { CallHandlerGetResponse("kms", request); } catch { /* ignore */ }

        return (aliasName, new Dictionary<string, object?>());
    }

    // ── EC2 ─────────────────────────────────────────────────────────────────

    private (string, Dictionary<string, object?>) ProvisionEc2Vpc(string logicalId, Dictionary<string, object?> props, string stackName)
    {
        var cidr = GetString(props.GetValueOrDefault("CidrBlock", "10.0.0.0/16"));
        var formData = $"Action=CreateVpc&CidrBlock={Uri.EscapeDataString(cidr)}";
        var request = new ServiceRequest("POST", "/", Ec2Headers,
            Encoding.UTF8.GetBytes(formData), EmptyQuery);
        var response = CallHandlerGetResponse("ec2", request);
        var body = Encoding.UTF8.GetString(response.Body);
        var vpcId = ExtractXmlValue(body, "vpcId");
        return (vpcId, new Dictionary<string, object?>(StringComparer.Ordinal)
        {
            ["VpcId"] = vpcId,
            ["DefaultSecurityGroup"] = ExtractXmlValue(body, "defaultSecurityGroupId"),
            ["DefaultNetworkAcl"] = ExtractXmlValue(body, "defaultNetworkAclId"),
        });
    }

    private (string, Dictionary<string, object?>) ProvisionEc2Subnet(string logicalId, Dictionary<string, object?> props, string stackName)
    {
        var vpcId = GetString(props.GetValueOrDefault("VpcId"));
        var cidr = GetString(props.GetValueOrDefault("CidrBlock", "10.0.1.0/24"));
        var az = GetString(props.GetValueOrDefault("AvailabilityZone", $"{Region}a"));
        var formData = $"Action=CreateSubnet&VpcId={Uri.EscapeDataString(vpcId)}&CidrBlock={Uri.EscapeDataString(cidr)}&AvailabilityZone={Uri.EscapeDataString(az)}";
        var request = new ServiceRequest("POST", "/", Ec2Headers,
            Encoding.UTF8.GetBytes(formData), EmptyQuery);
        var response = CallHandlerGetResponse("ec2", request);
        var body = Encoding.UTF8.GetString(response.Body);
        var subnetId = ExtractXmlValue(body, "subnetId");
        return (subnetId, new Dictionary<string, object?>(StringComparer.Ordinal)
        {
            ["SubnetId"] = subnetId, ["AvailabilityZone"] = az,
        });
    }

    private (string, Dictionary<string, object?>) ProvisionEc2SecurityGroup(string logicalId, Dictionary<string, object?> props, string stackName)
    {
        var groupName = GetString(props.GetValueOrDefault("GroupName", $"{stackName}-{logicalId}"));
        var desc = GetString(props.GetValueOrDefault("GroupDescription", groupName));
        var vpcId = GetString(props.GetValueOrDefault("VpcId"));
        var formData = $"Action=CreateSecurityGroup&GroupName={Uri.EscapeDataString(groupName)}&GroupDescription={Uri.EscapeDataString(desc)}";
        if (!string.IsNullOrEmpty(vpcId)) formData += $"&VpcId={Uri.EscapeDataString(vpcId)}";
        var request = new ServiceRequest("POST", "/", Ec2Headers,
            Encoding.UTF8.GetBytes(formData), EmptyQuery);
        var response = CallHandlerGetResponse("ec2", request);
        var body = Encoding.UTF8.GetString(response.Body);
        var sgId = ExtractXmlValue(body, "groupId");
        var arn = $"arn:aws:ec2:{Region}:{AccountContext.GetAccountId()}:security-group/{sgId}";
        return (sgId, new Dictionary<string, object?>(StringComparer.Ordinal)
        {
            ["GroupId"] = sgId, ["VpcId"] = vpcId, ["Arn"] = arn,
        });
    }

    private (string, Dictionary<string, object?>) ProvisionEc2InternetGateway(string logicalId, Dictionary<string, object?> props, string stackName)
    {
        var formData = "Action=CreateInternetGateway";
        var request = new ServiceRequest("POST", "/", Ec2Headers,
            Encoding.UTF8.GetBytes(formData), EmptyQuery);
        var response = CallHandlerGetResponse("ec2", request);
        var body = Encoding.UTF8.GetString(response.Body);
        var igwId = ExtractXmlValue(body, "internetGatewayId");
        return (igwId, new Dictionary<string, object?>(StringComparer.Ordinal) { ["InternetGatewayId"] = igwId });
    }

    private (string, Dictionary<string, object?>) ProvisionEc2VpcGwAttachment(string logicalId, Dictionary<string, object?> props, string stackName)
    {
        var vpcId = GetString(props.GetValueOrDefault("VpcId"));
        var igwId = GetString(props.GetValueOrDefault("InternetGatewayId"));
        var formData = $"Action=AttachInternetGateway&InternetGatewayId={Uri.EscapeDataString(igwId)}&VpcId={Uri.EscapeDataString(vpcId)}";
        var request = new ServiceRequest("POST", "/", Ec2Headers,
            Encoding.UTF8.GetBytes(formData), EmptyQuery);
        CallHandlerGetResponse("ec2", request);
        return ($"{igwId}|{vpcId}", new Dictionary<string, object?>());
    }

    private (string, Dictionary<string, object?>) ProvisionEc2RouteTable(string logicalId, Dictionary<string, object?> props, string stackName)
    {
        var vpcId = GetString(props.GetValueOrDefault("VpcId"));
        var formData = $"Action=CreateRouteTable&VpcId={Uri.EscapeDataString(vpcId)}";
        var request = new ServiceRequest("POST", "/", Ec2Headers,
            Encoding.UTF8.GetBytes(formData), EmptyQuery);
        var response = CallHandlerGetResponse("ec2", request);
        var body = Encoding.UTF8.GetString(response.Body);
        var rtbId = ExtractXmlValue(body, "routeTableId");
        return (rtbId, new Dictionary<string, object?>(StringComparer.Ordinal) { ["RouteTableId"] = rtbId });
    }

    private (string, Dictionary<string, object?>) ProvisionEc2Route(string logicalId, Dictionary<string, object?> props, string stackName)
    {
        var rtbId = GetString(props.GetValueOrDefault("RouteTableId"));
        var dest = GetString(props.GetValueOrDefault("DestinationCidrBlock", "0.0.0.0/0"));
        var formData = $"Action=CreateRoute&RouteTableId={Uri.EscapeDataString(rtbId)}&DestinationCidrBlock={Uri.EscapeDataString(dest)}";
        if (props.TryGetValue("GatewayId", out var gw)) formData += $"&GatewayId={Uri.EscapeDataString(GetString(gw))}";
        if (props.TryGetValue("NatGatewayId", out var nat)) formData += $"&NatGatewayId={Uri.EscapeDataString(GetString(nat))}";
        var request = new ServiceRequest("POST", "/", Ec2Headers,
            Encoding.UTF8.GetBytes(formData), EmptyQuery);
        CallHandlerGetResponse("ec2", request);
        return ($"{rtbId}|{dest}", new Dictionary<string, object?>());
    }

    private (string, Dictionary<string, object?>) ProvisionEc2SubnetRtbAssoc(string logicalId, Dictionary<string, object?> props, string stackName)
    {
        var rtbId = GetString(props.GetValueOrDefault("RouteTableId"));
        var subnetId = GetString(props.GetValueOrDefault("SubnetId"));
        var formData = $"Action=AssociateRouteTable&RouteTableId={Uri.EscapeDataString(rtbId)}&SubnetId={Uri.EscapeDataString(subnetId)}";
        var request = new ServiceRequest("POST", "/", Ec2Headers,
            Encoding.UTF8.GetBytes(formData), EmptyQuery);
        var response = CallHandlerGetResponse("ec2", request);
        var body = Encoding.UTF8.GetString(response.Body);
        var assocId = ExtractXmlValue(body, "associationId");
        if (string.IsNullOrEmpty(assocId)) assocId = $"rtbassoc-{Guid.NewGuid().ToString("N")[..17]}";
        return (assocId, new Dictionary<string, object?>());
    }

    private (string, Dictionary<string, object?>) ProvisionEc2LaunchTemplate(string logicalId, Dictionary<string, object?> props, string stackName)
    {
        var name = GetString(props.GetValueOrDefault("LaunchTemplateName"));
        if (string.IsNullOrEmpty(name)) name = PhysicalName(stackName, logicalId);

        var ltData = GetDict(props.GetValueOrDefault("LaunchTemplateData"));
        // Build form data for EC2 CreateLaunchTemplate
        var formData = $"Action=CreateLaunchTemplate&LaunchTemplateName={Uri.EscapeDataString(name)}";
        if (ltData.TryGetValue("InstanceType", out var it))
            formData += $"&LaunchTemplateData.InstanceType={Uri.EscapeDataString(GetString(it))}";
        if (ltData.TryGetValue("ImageId", out var img))
            formData += $"&LaunchTemplateData.ImageId={Uri.EscapeDataString(GetString(img))}";
        var request = new ServiceRequest("POST", "/", Ec2Headers,
            Encoding.UTF8.GetBytes(formData), EmptyQuery);
        var response = CallHandlerGetResponse("ec2", request);
        var body = Encoding.UTF8.GetString(response.Body);
        var ltId = ExtractXmlValue(body, "launchTemplateId");

        return (ltId, new Dictionary<string, object?>(StringComparer.Ordinal)
        {
            ["LaunchTemplateId"] = ltId, ["LaunchTemplateName"] = name,
            ["DefaultVersionNumber"] = "1", ["LatestVersionNumber"] = "1",
        });
    }

    private void DeleteEc2Resource(string resourceType, string physicalId, Dictionary<string, object?> props)
    {
        var action = resourceType switch
        {
            "AWS::EC2::VPC" => $"Action=DeleteVpc&VpcId={Uri.EscapeDataString(physicalId)}",
            "AWS::EC2::Subnet" => $"Action=DeleteSubnet&SubnetId={Uri.EscapeDataString(physicalId)}",
            "AWS::EC2::SecurityGroup" => $"Action=DeleteSecurityGroup&GroupId={Uri.EscapeDataString(physicalId)}",
            "AWS::EC2::InternetGateway" => $"Action=DeleteInternetGateway&InternetGatewayId={Uri.EscapeDataString(physicalId)}",
            "AWS::EC2::RouteTable" => $"Action=DeleteRouteTable&RouteTableId={Uri.EscapeDataString(physicalId)}",
            "AWS::EC2::LaunchTemplate" => $"Action=DeleteLaunchTemplate&LaunchTemplateId={Uri.EscapeDataString(physicalId)}",
            _ => null,
        };
        if (action is null) return;
        var request = new ServiceRequest("POST", "/", Ec2Headers,
            Encoding.UTF8.GetBytes(action), EmptyQuery);
        CallHandlerGetResponse("ec2", request);
    }

    // ── ECR ─────────────────────────────────────────────────────────────────

    private (string, Dictionary<string, object?>) ProvisionEcrRepository(string logicalId, Dictionary<string, object?> props, string stackName)
    {
        var name = GetString(props.GetValueOrDefault("RepositoryName", $"{stackName}-{logicalId}".ToLowerInvariant()));
        var arn = $"arn:aws:ecr:{Region}:{AccountContext.GetAccountId()}:repository/{name}";
        var uri = $"{AccountContext.GetAccountId()}.dkr.ecr.{Region}.amazonaws.com/{name}";

        var jsonBody = JsonSerializer.Serialize(new Dictionary<string, object?> { ["repositoryName"] = name });
        var headers = new Dictionary<string, string>
        {
            ["Content-Type"] = "application/x-amz-json-1.1",
            ["X-Amz-Target"] = "AmazonEC2ContainerRegistry_V20150921.CreateRepository",
        };
        var request = new ServiceRequest("POST", "/", headers,
            Encoding.UTF8.GetBytes(jsonBody), EmptyQuery);
        CallHandlerGetResponse("ecr", request);

        return (name, new Dictionary<string, object?>(StringComparer.Ordinal) { ["Arn"] = arn, ["RepositoryUri"] = uri });
    }

    // ── ECS ─────────────────────────────────────────────────────────────────

    private (string, Dictionary<string, object?>) ProvisionEcsCluster(string logicalId, Dictionary<string, object?> props, string stackName)
    {
        var name = GetString(props.GetValueOrDefault("ClusterName", $"{stackName}-{logicalId}"));
        var arn = $"arn:aws:ecs:{Region}:{AccountContext.GetAccountId()}:cluster/{name}";

        var jsonBody = JsonSerializer.Serialize(new Dictionary<string, object?> { ["clusterName"] = name });
        var headers = new Dictionary<string, string>
        {
            ["Content-Type"] = "application/x-amz-json-1.1",
            ["X-Amz-Target"] = "AmazonEC2ContainerServiceV20141113.CreateCluster",
        };
        var request = new ServiceRequest("POST", "/", headers,
            Encoding.UTF8.GetBytes(jsonBody), EmptyQuery);
        CallHandlerGetResponse("ecs", request);

        return (name, new Dictionary<string, object?>(StringComparer.Ordinal) { ["Arn"] = arn, ["ClusterName"] = name });
    }

    private (string, Dictionary<string, object?>) ProvisionEcsTaskDefinition(string logicalId, Dictionary<string, object?> props, string stackName)
    {
        var family = GetString(props.GetValueOrDefault("Family", $"{stackName}-{logicalId}"));
        var arn = $"arn:aws:ecs:{Region}:{AccountContext.GetAccountId()}:task-definition/{family}:1";
        return (arn, new Dictionary<string, object?>(StringComparer.Ordinal) { ["TaskDefinitionArn"] = arn });
    }

    private (string, Dictionary<string, object?>) ProvisionEcsService(string logicalId, Dictionary<string, object?> props, string stackName)
    {
        var name = GetString(props.GetValueOrDefault("ServiceName", $"{stackName}-{logicalId}"));
        var cluster = GetString(props.GetValueOrDefault("Cluster", "default"));
        var arn = $"arn:aws:ecs:{Region}:{AccountContext.GetAccountId()}:service/{cluster}/{name}";
        return (arn, new Dictionary<string, object?>(StringComparer.Ordinal) { ["ServiceArn"] = arn, ["Name"] = name });
    }

    // ── ELBv2 ───────────────────────────────────────────────────────────────

    private (string, Dictionary<string, object?>) ProvisionElbv2LoadBalancer(string logicalId, Dictionary<string, object?> props, string stackName)
    {
        var name = GetString(props.GetValueOrDefault("Name"));
        if (string.IsNullOrEmpty(name)) name = PhysicalName(stackName, logicalId, lowercase: true, maxLen: 32);

        var formData = $"Action=CreateLoadBalancer&Name={Uri.EscapeDataString(name)}";
        if (props.TryGetValue("Type", out var typeObj)) formData += $"&Type={Uri.EscapeDataString(GetString(typeObj))}";
        if (props.TryGetValue("Scheme", out var scheme)) formData += $"&Scheme={Uri.EscapeDataString(GetString(scheme))}";

        // Subnets
        if (props.TryGetValue("Subnets", out var subnets) && subnets is List<object?> subList)
        {
            for (var i = 0; i < subList.Count; i++)
                formData += $"&Subnets.member.{i + 1}={Uri.EscapeDataString(GetString(subList[i]))}";
        }
        // SecurityGroups
        if (props.TryGetValue("SecurityGroups", out var sgs) && sgs is List<object?> sgList)
        {
            for (var i = 0; i < sgList.Count; i++)
                formData += $"&SecurityGroups.member.{i + 1}={Uri.EscapeDataString(GetString(sgList[i]))}";
        }

        var request = new ServiceRequest("POST", "/", ElbHeaders,
            Encoding.UTF8.GetBytes(formData), EmptyQuery);
        var response = CallHandlerGetResponse("elasticloadbalancing", request);
        var body = Encoding.UTF8.GetString(response.Body);

        var arn = ExtractXmlValue(body, "LoadBalancerArn");
        var dnsName = ExtractXmlValue(body, "DNSName");
        var lbId = arn.Split('/').LastOrDefault() ?? "";
        var fullName = $"app/{name}/{lbId}";

        return (arn, new Dictionary<string, object?>(StringComparer.Ordinal)
        {
            ["Arn"] = arn, ["LoadBalancerArn"] = arn, ["LoadBalancerName"] = name,
            ["DNSName"] = dnsName, ["LoadBalancerFullName"] = fullName,
            ["CanonicalHostedZoneID"] = "Z35SXDOTRQ7X7K",
        });
    }

    private (string, Dictionary<string, object?>) ProvisionElbv2Listener(string logicalId, Dictionary<string, object?> props, string stackName)
    {
        var lbArn = GetString(props.GetValueOrDefault("LoadBalancerArn"));
        var port = GetString(props.GetValueOrDefault("Port", "80"));
        var protocol = GetString(props.GetValueOrDefault("Protocol", "HTTP"));

        var formData = $"Action=CreateListener&LoadBalancerArn={Uri.EscapeDataString(lbArn)}&Port={port}&Protocol={protocol}";

        // Default actions
        if (props.TryGetValue("DefaultActions", out var actionsObj) && actionsObj is List<object?> actionsList)
        {
            for (var i = 0; i < actionsList.Count; i++)
            {
                var action = GetDict(actionsList[i]);
                var actionType = GetString(action.GetValueOrDefault("Type", "fixed-response"));
                formData += $"&DefaultActions.member.{i + 1}.Type={Uri.EscapeDataString(actionType)}";
                if (action.TryGetValue("TargetGroupArn", out var tgArn))
                    formData += $"&DefaultActions.member.{i + 1}.TargetGroupArn={Uri.EscapeDataString(GetString(tgArn))}";
                if (action.TryGetValue("FixedResponseConfig", out var frc))
                {
                    var frDict = GetDict(frc);
                    formData += $"&DefaultActions.member.{i + 1}.FixedResponseConfig.StatusCode={Uri.EscapeDataString(GetString(frDict.GetValueOrDefault("StatusCode", "200")))}";
                    if (frDict.TryGetValue("ContentType", out var ct))
                        formData += $"&DefaultActions.member.{i + 1}.FixedResponseConfig.ContentType={Uri.EscapeDataString(GetString(ct))}";
                    if (frDict.TryGetValue("MessageBody", out var mb))
                        formData += $"&DefaultActions.member.{i + 1}.FixedResponseConfig.MessageBody={Uri.EscapeDataString(GetString(mb))}";
                }
            }
        }

        var request = new ServiceRequest("POST", "/", ElbHeaders,
            Encoding.UTF8.GetBytes(formData), EmptyQuery);
        var response = CallHandlerGetResponse("elasticloadbalancing", request);
        var body = Encoding.UTF8.GetString(response.Body);
        var listenerArn = ExtractXmlValue(body, "ListenerArn");

        return (listenerArn, new Dictionary<string, object?>(StringComparer.Ordinal)
        {
            ["ListenerArn"] = listenerArn, ["Arn"] = listenerArn,
        });
    }

    // ── Cognito ─────────────────────────────────────────────────────────────

    private (string, Dictionary<string, object?>) ProvisionCognitoUserPool(string logicalId, Dictionary<string, object?> props, string stackName)
    {
        var name = GetString(props.GetValueOrDefault("PoolName"));
        if (string.IsNullOrEmpty(name)) name = PhysicalName(stackName, logicalId, maxLen: 128);
        var pid = $"{Region}_{Guid.NewGuid().ToString("N")[..9]}";
        var arn = $"arn:aws:cognito-idp:{Region}:{AccountContext.GetAccountId()}:userpool/{pid}";
        var providerName = $"cognito-idp.{Region}.amazonaws.com/{pid}";

        var jsonBody = JsonSerializer.Serialize(new Dictionary<string, object?> { ["PoolName"] = name });
        var headers = new Dictionary<string, string>
        {
            ["Content-Type"] = "application/x-amz-json-1.1",
            ["X-Amz-Target"] = "AWSCognitoIdentityProviderService.CreateUserPool",
        };
        var request = new ServiceRequest("POST", "/", headers,
            Encoding.UTF8.GetBytes(jsonBody), EmptyQuery);
        try
        {
            var response = CallHandlerGetResponse("cognito-idp", request);
            var respBody = Encoding.UTF8.GetString(response.Body);
            using var doc = JsonDocument.Parse(respBody);
            if (doc.RootElement.TryGetProperty("UserPool", out var up) && up.TryGetProperty("Id", out var idEl))
            {
                pid = idEl.GetString() ?? pid;
                arn = $"arn:aws:cognito-idp:{Region}:{AccountContext.GetAccountId()}:userpool/{pid}";
                providerName = $"cognito-idp.{Region}.amazonaws.com/{pid}";
            }
        }
        catch { /* ignore */ }

        return (pid, new Dictionary<string, object?>(StringComparer.Ordinal)
        {
            ["Arn"] = arn, ["ProviderName"] = providerName,
        });
    }

    private (string, Dictionary<string, object?>) ProvisionCognitoUserPoolClient(string logicalId, Dictionary<string, object?> props, string stackName)
    {
        var poolId = GetString(props.GetValueOrDefault("UserPoolId"));
        var clientName = GetString(props.GetValueOrDefault("ClientName"));
        var cid = Guid.NewGuid().ToString("N")[..26];

        var jsonBody = JsonSerializer.Serialize(new Dictionary<string, object?>
        {
            ["UserPoolId"] = poolId, ["ClientName"] = clientName,
        });
        var headers = new Dictionary<string, string>
        {
            ["Content-Type"] = "application/x-amz-json-1.1",
            ["X-Amz-Target"] = "AWSCognitoIdentityProviderService.CreateUserPoolClient",
        };
        var request = new ServiceRequest("POST", "/", headers,
            Encoding.UTF8.GetBytes(jsonBody), EmptyQuery);
        try
        {
            var response = CallHandlerGetResponse("cognito-idp", request);
            var respBody = Encoding.UTF8.GetString(response.Body);
            using var doc = JsonDocument.Parse(respBody);
            if (doc.RootElement.TryGetProperty("UserPoolClient", out var upc) && upc.TryGetProperty("ClientId", out var cidEl))
                cid = cidEl.GetString() ?? cid;
        }
        catch { /* ignore */ }

        return (cid, new Dictionary<string, object?>());
    }

    private (string, Dictionary<string, object?>) ProvisionCognitoIdentityPool(string logicalId, Dictionary<string, object?> props, string stackName)
    {
        var name = GetString(props.GetValueOrDefault("IdentityPoolName"));
        if (string.IsNullOrEmpty(name)) name = PhysicalName(stackName, logicalId, maxLen: 128);
        var iid = $"{Region}:{Guid.NewGuid()}";
        return (iid, new Dictionary<string, object?>());
    }

    private (string, Dictionary<string, object?>) ProvisionCognitoUserPoolDomain(string logicalId, Dictionary<string, object?> props, string stackName)
    {
        var pid = GetString(props.GetValueOrDefault("UserPoolId"));
        var domain = GetString(props.GetValueOrDefault("Domain"));
        return ($"{pid}-domain-{domain}", new Dictionary<string, object?>());
    }

    // ── API Gateway ─────────────────────────────────────────────────────────

    private (string, Dictionary<string, object?>) ProvisionApiGwRestApi(string logicalId, Dictionary<string, object?> props, string stackName)
    {
        var name = GetString(props.GetValueOrDefault("Name"));
        if (string.IsNullOrEmpty(name)) name = PhysicalName(stackName, logicalId, maxLen: 64);
        var apiId = Guid.NewGuid().ToString("N")[..10];
        return (apiId, new Dictionary<string, object?>(StringComparer.Ordinal)
        {
            ["RootResourceId"] = Guid.NewGuid().ToString("N")[..10],
            ["Arn"] = $"arn:aws:apigateway:{Region}::/restapis/{apiId}",
        });
    }

    private (string, Dictionary<string, object?>) ProvisionApiGwResource(string logicalId, Dictionary<string, object?> props, string stackName)
    {
        var resId = Guid.NewGuid().ToString("N")[..10];
        return (resId, new Dictionary<string, object?>(StringComparer.Ordinal) { ["ResourceId"] = resId });
    }

    private (string, Dictionary<string, object?>) ProvisionApiGwMethod(string logicalId, Dictionary<string, object?> props, string stackName)
    {
        var apiId = GetString(props.GetValueOrDefault("RestApiId"));
        var resId = GetString(props.GetValueOrDefault("ResourceId"));
        var method = GetString(props.GetValueOrDefault("HttpMethod", "ANY"));
        return ($"{apiId}-{resId}-{method}", new Dictionary<string, object?>());
    }

    private (string, Dictionary<string, object?>) ProvisionApiGwDeployment(string logicalId, Dictionary<string, object?> props, string stackName)
    {
        var deployId = Guid.NewGuid().ToString("N")[..10];
        return (deployId, new Dictionary<string, object?>(StringComparer.Ordinal) { ["DeploymentId"] = deployId });
    }

    private (string, Dictionary<string, object?>) ProvisionApiGwStage(string logicalId, Dictionary<string, object?> props, string stackName)
    {
        var apiId = GetString(props.GetValueOrDefault("RestApiId"));
        var stageName = GetString(props.GetValueOrDefault("StageName"));
        return ($"{apiId}-{stageName}", new Dictionary<string, object?>(StringComparer.Ordinal)
        {
            ["StageName"] = stageName,
        });
    }

    // ── AppSync ─────────────────────────────────────────────────────────────

    private (string, Dictionary<string, object?>) ProvisionAppSyncApi(string logicalId, Dictionary<string, object?> props, string stackName)
    {
        var name = GetString(props.GetValueOrDefault("Name"));
        if (string.IsNullOrEmpty(name)) name = PhysicalName(stackName, logicalId);
        var apiId = Guid.NewGuid().ToString("N")[..8];
        var arn = $"arn:aws:appsync:{Region}:{AccountContext.GetAccountId()}:apis/{apiId}";
        var gqlUrl = $"https://{apiId}.appsync-api.{Region}.amazonaws.com/graphql";
        return (apiId, new Dictionary<string, object?>(StringComparer.Ordinal)
        {
            ["ApiId"] = apiId, ["Arn"] = arn, ["GraphQLUrl"] = gqlUrl,
        });
    }

    private (string, Dictionary<string, object?>) ProvisionAppSyncDataSource(string logicalId, Dictionary<string, object?> props, string stackName)
    {
        var apiId = GetString(props.GetValueOrDefault("ApiId"));
        var name = GetString(props.GetValueOrDefault("Name"));
        if (string.IsNullOrEmpty(name)) name = logicalId;
        var dsArn = $"arn:aws:appsync:{Region}:{AccountContext.GetAccountId()}:apis/{apiId}/datasources/{name}";
        return ($"{apiId}/{name}", new Dictionary<string, object?>(StringComparer.Ordinal)
        {
            ["Name"] = name, ["DataSourceArn"] = dsArn,
        });
    }

    private (string, Dictionary<string, object?>) ProvisionAppSyncResolver(string logicalId, Dictionary<string, object?> props, string stackName)
    {
        var apiId = GetString(props.GetValueOrDefault("ApiId"));
        var typeName = GetString(props.GetValueOrDefault("TypeName", "Query"));
        var fieldName = GetString(props.GetValueOrDefault("FieldName", logicalId));
        var resArn = $"arn:aws:appsync:{Region}:{AccountContext.GetAccountId()}:apis/{apiId}/types/{typeName}/resolvers/{fieldName}";
        return ($"{apiId}/{typeName}/{fieldName}", new Dictionary<string, object?>(StringComparer.Ordinal)
        {
            ["ResolverArn"] = resArn,
        });
    }

    private (string, Dictionary<string, object?>) ProvisionAppSyncSchema(string logicalId, Dictionary<string, object?> props, string stackName)
    {
        var apiId = GetString(props.GetValueOrDefault("ApiId"));
        return ($"{apiId}/schema", new Dictionary<string, object?>());
    }

    private (string, Dictionary<string, object?>) ProvisionAppSyncApiKey(string logicalId, Dictionary<string, object?> props, string stackName)
    {
        var apiId = GetString(props.GetValueOrDefault("ApiId"));
        var keyId = Guid.NewGuid().ToString("N")[..8];
        var arn = $"arn:aws:appsync:{Region}:{AccountContext.GetAccountId()}:apis/{apiId}/apikeys/{keyId}";
        return (keyId, new Dictionary<string, object?>(StringComparer.Ordinal)
        {
            ["ApiKey"] = keyId, ["Arn"] = arn,
        });
    }

    // ── CloudFormation internal types ────────────────────────────────────────

    private static (string, Dictionary<string, object?>) ProvisionWaitCondition(string logicalId, string stackName)
    {
        return ($"{stackName}-{logicalId}-{Guid.NewGuid().ToString()[..8]}",
            new Dictionary<string, object?>(StringComparer.Ordinal) { ["Data"] = "{}" });
    }

    private static (string, Dictionary<string, object?>) ProvisionWaitConditionHandle(string logicalId, string stackName)
    {
        var pid = $"{stackName}-{logicalId}-{Guid.NewGuid().ToString()[..8]}";
        return (pid, new Dictionary<string, object?>(StringComparer.Ordinal)
        {
            ["Ref"] = $"https://cloudformation-waitcondition-{Region}.s3.amazonaws.com/{pid}",
        });
    }

    // ── Delete request builders ─────────────────────────────────────────────

    private static ServiceRequest BuildDeleteBucketRequest(string name) =>
        new("DELETE", $"/{name}", EmptyHeaders, [], EmptyQuery);

    private static ServiceRequest BuildDeleteQueueRequest(string url) =>
        new("POST", "/", SqsHeaders,
            Encoding.UTF8.GetBytes($"Action=DeleteQueue&QueueUrl={Uri.EscapeDataString(url)}"), EmptyQuery);

    private static ServiceRequest BuildDeleteTopicRequest(string arn) =>
        new("POST", "/", SnsHeaders,
            Encoding.UTF8.GetBytes($"Action=DeleteTopic&TopicArn={Uri.EscapeDataString(arn)}"), EmptyQuery);

    private static ServiceRequest BuildDeleteTableRequest(string name)
    {
        var json = JsonSerializer.Serialize(new Dictionary<string, object?> { ["TableName"] = name });
        return new("POST", "/", new Dictionary<string, string>
        {
            ["Content-Type"] = "application/x-amz-json-1.0",
            ["X-Amz-Target"] = "DynamoDB_20120810.DeleteTable",
        }, Encoding.UTF8.GetBytes(json), EmptyQuery);
    }

    private static ServiceRequest BuildDeleteFunctionRequest(string name) =>
        new("DELETE", $"/2015-03-31/functions/{Uri.EscapeDataString(name)}",
            EmptyHeaders, [], EmptyQuery);

    private static ServiceRequest BuildDeleteRoleRequest(string name) =>
        new("POST", "/", IamHeaders,
            Encoding.UTF8.GetBytes($"Action=DeleteRole&RoleName={Uri.EscapeDataString(name)}"), EmptyQuery);

    private static ServiceRequest BuildDeleteParameterRequest(string name)
    {
        var json = JsonSerializer.Serialize(new Dictionary<string, object?> { ["Name"] = name });
        return new("POST", "/", new Dictionary<string, string>
        {
            ["Content-Type"] = "application/x-amz-json-1.1",
            ["X-Amz-Target"] = "AmazonSSM.DeleteParameter",
        }, Encoding.UTF8.GetBytes(json), EmptyQuery);
    }

    private static ServiceRequest BuildDeleteLogGroupRequest(string name)
    {
        var json = JsonSerializer.Serialize(new Dictionary<string, object?> { ["logGroupName"] = name });
        return new("POST", "/", new Dictionary<string, string>
        {
            ["Content-Type"] = "application/x-amz-json-1.1",
            ["X-Amz-Target"] = "Logs_20140328.DeleteLogGroup",
        }, Encoding.UTF8.GetBytes(json), EmptyQuery);
    }

    private static ServiceRequest BuildDeleteRuleRequest(string name, Dictionary<string, object?> props)
    {
        var bus = GetString(props.GetValueOrDefault("EventBusName", "default"));
        var json = JsonSerializer.Serialize(new Dictionary<string, object?> { ["Name"] = name, ["EventBusName"] = bus });
        return new("POST", "/", new Dictionary<string, string>
        {
            ["Content-Type"] = "application/x-amz-json-1.1",
            ["X-Amz-Target"] = "AWSEvents.DeleteRule",
        }, Encoding.UTF8.GetBytes(json), EmptyQuery);
    }

    private static ServiceRequest BuildDeleteStreamRequest(string name)
    {
        var json = JsonSerializer.Serialize(new Dictionary<string, object?> { ["StreamName"] = name });
        return new("POST", "/", new Dictionary<string, string>
        {
            ["Content-Type"] = "application/x-amz-json-1.1",
            ["X-Amz-Target"] = "Kinesis_20131202.DeleteStream",
        }, Encoding.UTF8.GetBytes(json), EmptyQuery);
    }

    private static ServiceRequest BuildDeleteSecretRequest(string name)
    {
        var json = JsonSerializer.Serialize(new Dictionary<string, object?>
        {
            ["SecretId"] = name, ["ForceDeleteWithoutRecovery"] = true,
        });
        return new("POST", "/", new Dictionary<string, string>
        {
            ["Content-Type"] = "application/x-amz-json-1.1",
            ["X-Amz-Target"] = "secretsmanager.DeleteSecret",
        }, Encoding.UTF8.GetBytes(json), EmptyQuery);
    }

    private static ServiceRequest BuildDeleteKmsKeyRequest(string keyId)
    {
        var json = JsonSerializer.Serialize(new Dictionary<string, object?> { ["KeyId"] = keyId });
        return new("POST", "/", new Dictionary<string, string>
        {
            ["Content-Type"] = "application/x-amz-json-1.1",
            ["X-Amz-Target"] = "TrentService.ScheduleKeyDeletion",
        }, Encoding.UTF8.GetBytes(json), EmptyQuery);
    }

    private static ServiceRequest BuildDeleteEcrRepoRequest(string name)
    {
        var json = JsonSerializer.Serialize(new Dictionary<string, object?>
        {
            ["repositoryName"] = name, ["force"] = true,
        });
        return new("POST", "/", new Dictionary<string, string>
        {
            ["Content-Type"] = "application/x-amz-json-1.1",
            ["X-Amz-Target"] = "AmazonEC2ContainerRegistry_V20150921.DeleteRepository",
        }, Encoding.UTF8.GetBytes(json), EmptyQuery);
    }

    private static ServiceRequest BuildDeleteEcsClusterRequest(string name)
    {
        var json = JsonSerializer.Serialize(new Dictionary<string, object?> { ["cluster"] = name });
        return new("POST", "/", new Dictionary<string, string>
        {
            ["Content-Type"] = "application/x-amz-json-1.1",
            ["X-Amz-Target"] = "AmazonEC2ContainerServiceV20141113.DeleteCluster",
        }, Encoding.UTF8.GetBytes(json), EmptyQuery);
    }

    private static ServiceRequest BuildDeleteLbRequest(string arn) =>
        new("POST", "/", ElbHeaders,
            Encoding.UTF8.GetBytes($"Action=DeleteLoadBalancer&LoadBalancerArn={Uri.EscapeDataString(arn)}"), EmptyQuery);

    private static ServiceRequest BuildDeleteListenerRequest(string arn) =>
        new("POST", "/", ElbHeaders,
            Encoding.UTF8.GetBytes($"Action=DeleteListener&ListenerArn={Uri.EscapeDataString(arn)}"), EmptyQuery);

    // ── Shared request building helpers ──────────────────────────────────────

    private static readonly IReadOnlyDictionary<string, string> EmptyHeaders =
        new Dictionary<string, string>();

    private static readonly IReadOnlyDictionary<string, string[]> EmptyQuery =
        new Dictionary<string, string[]>();

    private static readonly IReadOnlyDictionary<string, string> SqsHeaders =
        new Dictionary<string, string> { ["Content-Type"] = "application/x-www-form-urlencoded" };

    private static readonly IReadOnlyDictionary<string, string> SnsHeaders =
        new Dictionary<string, string> { ["Content-Type"] = "application/x-www-form-urlencoded" };

    private static readonly IReadOnlyDictionary<string, string> IamHeaders =
        new Dictionary<string, string> { ["Content-Type"] = "application/x-www-form-urlencoded" };

    private static readonly IReadOnlyDictionary<string, string> Ec2Headers =
        new Dictionary<string, string> { ["Content-Type"] = "application/x-www-form-urlencoded" };

    private static readonly IReadOnlyDictionary<string, string> ElbHeaders =
        new Dictionary<string, string> { ["Content-Type"] = "application/x-www-form-urlencoded" };

    private static ServiceRequest BuildFormRequest(string service, string action, List<(string Key, string Value)> extraParams)
    {
        var sb = new StringBuilder($"Action={action}");
        foreach (var (key, val) in extraParams)
            sb.Append($"&{key}={Uri.EscapeDataString(val)}");
        return new ServiceRequest("POST", "/",
            new Dictionary<string, string> { ["Content-Type"] = "application/x-www-form-urlencoded" },
            Encoding.UTF8.GetBytes(sb.ToString()), EmptyQuery);
    }

    private static string ExtractXmlValue(string xml, string tag)
    {
        var startTag = $"<{tag}>";
        var endTag = $"</{tag}>";
        var start = xml.IndexOf(startTag, StringComparison.Ordinal);
        if (start < 0) return "";
        start += startTag.Length;
        var end = xml.IndexOf(endTag, start, StringComparison.Ordinal);
        if (end < 0) return "";
        return xml[start..end];
    }
}
