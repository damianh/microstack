using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
using System.Xml.Linq;
using MicroStack.Internal;

namespace MicroStack.Services.S3;

/// <summary>
/// S3 service handler — REST/XML protocol.
///
/// Port of ministack/services/s3.py.
/// </summary>
internal sealed partial class S3ServiceHandler : IServiceHandler
{
    // ── Constants ────────────────────────────────────────────────────────────────

    private const string S3Ns = "http://s3.amazonaws.com/doc/2006-03-01/";
    private static readonly XNamespace Ns = S3Ns;
    private const string XmlDecl = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n";

    private static readonly string[] PreservedHeaders =
    [
        "cache-control",
        "content-disposition",
        "content-language",
        "expires",
    ];

    private static string Region => MicroStackOptions.Instance.Region;

    [GeneratedRegex(@"^[a-z0-9][a-z0-9.\-]{1,61}[a-z0-9]$")]
    private static partial Regex BucketNameRegex();

    [GeneratedRegex(@"^\d{1,3}(\.\d{1,3}){3}$")]
    private static partial Regex IpRegex();

    // ── State ────────────────────────────────────────────────────────────────────

    private readonly AccountScopedDictionary<string, S3Bucket> _buckets = new();
    private readonly AccountScopedDictionary<string, string> _bucketPolicies = new();
    private readonly AccountScopedDictionary<string, byte[]> _bucketNotifications = new();
    private readonly AccountScopedDictionary<string, Dictionary<string, string>> _bucketTags = new();
    private readonly AccountScopedDictionary<string, string> _bucketVersioning = new();
    private readonly AccountScopedDictionary<string, byte[]> _bucketEncryption = new();
    private readonly AccountScopedDictionary<string, byte[]> _bucketLifecycle = new();
    private readonly AccountScopedDictionary<string, byte[]> _bucketCors = new();
    private readonly AccountScopedDictionary<string, byte[]> _bucketAcl = new();
    private readonly AccountScopedDictionary<string, byte[]> _bucketWebsites = new();
    private readonly AccountScopedDictionary<string, byte[]> _bucketLoggingConfig = new();
    private readonly AccountScopedDictionary<string, byte[]> _bucketAccelerateConfig = new();
    private readonly AccountScopedDictionary<string, byte[]> _bucketRequestPaymentConfig = new();
    private readonly AccountScopedDictionary<(string Bucket, string Key), Dictionary<string, string>> _objectTags = new();
    private readonly AccountScopedDictionary<string, ObjectLockConfig> _bucketObjectLock = new();
    private readonly AccountScopedDictionary<string, ReplicationConfig> _bucketReplication = new();
    private readonly AccountScopedDictionary<(string Bucket, string Key), RetentionConfig> _objectRetention = new();
    private readonly AccountScopedDictionary<(string Bucket, string Key), string> _objectLegalHold = new();
    private readonly AccountScopedDictionary<string, MultipartUpload> _multipartUploads = new();

    // ── IServiceHandler ──────────────────────────────────────────────────────────

    public string ServiceName => "s3";

    public Task<ServiceResponse> HandleAsync(ServiceRequest request)
    {
        var (bucket, key) = ParseBucketKey(request.Path, request.Headers);

        var (status, respHeaders, respBody) = Dispatch(
            request.Method, bucket, key, request.Headers, request.Body, request.QueryParams);

        respHeaders.TryAdd("x-amz-request-id", HashHelpers.NewUuid());
        respHeaders.TryAdd("x-amz-id-2", Convert.ToBase64String(RandomNumberGenerator.GetBytes(48)));

        // HEAD responses must not carry a body per HTTP/1.1 spec.
        if (string.Equals(request.Method, "HEAD", StringComparison.OrdinalIgnoreCase))
        {
            respBody = [];
        }

        var response = new ServiceResponse(status, respHeaders, respBody);
        return Task.FromResult(response);
    }

    public void Reset()
    {
        _buckets.Clear();
        _bucketPolicies.Clear();
        _bucketNotifications.Clear();
        _bucketTags.Clear();
        _bucketVersioning.Clear();
        _bucketEncryption.Clear();
        _bucketLifecycle.Clear();
        _bucketCors.Clear();
        _bucketAcl.Clear();
        _bucketWebsites.Clear();
        _bucketLoggingConfig.Clear();
        _bucketAccelerateConfig.Clear();
        _bucketRequestPaymentConfig.Clear();
        _objectTags.Clear();
        _multipartUploads.Clear();
        _bucketObjectLock.Clear();
        _bucketReplication.Clear();
        _objectRetention.Clear();
        _objectLegalHold.Clear();
    }

    public JsonElement? GetState()
    {
        // Persist bucket metadata without object bodies.
        return null;
    }

    public void RestoreState(JsonElement state)
    {
        // Not implementing restore in Phase 1.
    }

    // ── Dispatch ─────────────────────────────────────────────────────────────────

    private (int Status, Dictionary<string, string> Headers, byte[] Body) Dispatch(
        string method,
        string bucket,
        string key,
        IReadOnlyDictionary<string, string> headers,
        byte[] body,
        IReadOnlyDictionary<string, string[]> queryParams)
    {
        if (method == "GET" && string.IsNullOrEmpty(bucket))
        {
            return ListBuckets();
        }

        // ── Routes with key ──
        if (!string.IsNullOrEmpty(key))
        {
            if (method == "GET")
            {
                if (queryParams.ContainsKey("uploadId"))
                    return ListParts(bucket, key, queryParams);
                if (queryParams.ContainsKey("tagging"))
                    return GetObjectTagging(bucket, key);
                if (queryParams.ContainsKey("retention"))
                    return GetObjectRetention(bucket, key);
                if (queryParams.ContainsKey("legal-hold"))
                    return GetObjectLegalHold(bucket, key);
                return GetObject(bucket, key, headers);
            }

            if (method == "PUT")
            {
                if (queryParams.ContainsKey("partNumber") && queryParams.ContainsKey("uploadId"))
                {
                    if (GetHeader(headers, "x-amz-copy-source") is not null)
                        return UploadPartCopy(bucket, key, queryParams, headers);
                    return UploadPart(bucket, key, body, queryParams, headers);
                }
                if (queryParams.ContainsKey("tagging"))
                    return PutObjectTagging(bucket, key, body);
                if (queryParams.ContainsKey("retention"))
                    return PutObjectRetention(bucket, key, body, headers);
                if (queryParams.ContainsKey("legal-hold"))
                    return PutObjectLegalHold(bucket, key, body);
                if (GetHeader(headers, "x-amz-copy-source") is not null)
                    return CopyObject(bucket, key, headers);
                return PutObject(bucket, key, body, headers);
            }

            if (method == "POST")
            {
                if (queryParams.ContainsKey("uploads"))
                    return CreateMultipartUpload(bucket, key, headers);
                if (queryParams.ContainsKey("uploadId"))
                    return CompleteMultipartUpload(bucket, key, body, queryParams);
                return Error("MethodNotAllowed",
                    "The specified method is not allowed against this resource.", 405);
            }

            if (method == "HEAD")
                return HeadObject(bucket, key);

            if (method == "DELETE")
            {
                if (queryParams.ContainsKey("uploadId"))
                    return AbortMultipartUpload(bucket, key, queryParams);
                if (queryParams.ContainsKey("tagging"))
                    return DeleteObjectTagging(bucket, key);
                return DeleteObject(bucket, key, headers);
            }

            return Error("MethodNotAllowed",
                "The specified method is not allowed against this resource.", 405);
        }

        // ── Routes without key (bucket-level) ──
        if (string.IsNullOrEmpty(bucket))
        {
            return Error("MethodNotAllowed",
                "The specified method is not allowed against this resource.", 405);
        }

        if (method == "GET")
        {
            if (queryParams.ContainsKey("uploads"))
                return ListMultipartUploads(bucket, queryParams);
            if (queryParams.ContainsKey("versions"))
                return ListObjectVersions(bucket, queryParams);
            if (queryParams.ContainsKey("list-type") && Qp(queryParams, "list-type") == "2")
                return ListObjectsV2(bucket, queryParams);
            if (queryParams.ContainsKey("location"))
                return GetBucketLocation(bucket);
            if (queryParams.ContainsKey("policy"))
                return GetBucketPolicy(bucket);
            if (queryParams.ContainsKey("versioning"))
                return GetBucketVersioning(bucket);
            if (queryParams.ContainsKey("encryption"))
                return GetBucketEncryption(bucket);
            if (queryParams.ContainsKey("logging"))
                return GetBucketLogging(bucket);
            if (queryParams.ContainsKey("notification"))
                return GetBucketNotification(bucket);
            if (queryParams.ContainsKey("tagging"))
                return GetBucketTagging(bucket);
            if (queryParams.ContainsKey("cors"))
                return GetBucketCors(bucket);
            if (queryParams.ContainsKey("acl"))
                return GetBucketAcl(bucket);
            if (queryParams.ContainsKey("lifecycle"))
                return GetBucketLifecycle(bucket);
            if (queryParams.ContainsKey("accelerate"))
                return GetBucketAccelerate(bucket);
            if (queryParams.ContainsKey("request-payment"))
                return GetBucketRequestPayment(bucket);
            if (queryParams.ContainsKey("website"))
                return GetBucketWebsite(bucket);
            if (queryParams.ContainsKey("object-lock"))
                return GetObjectLockConfiguration(bucket);
            if (queryParams.ContainsKey("replication"))
                return GetBucketReplication(bucket);
            if (queryParams.ContainsKey("ownershipControls"))
                return GetBucketOwnershipControls(bucket);
            if (queryParams.ContainsKey("publicAccessBlock"))
                return GetPublicAccessBlock(bucket);
            return ListObjectsV1(bucket, queryParams);
        }

        if (method == "PUT")
        {
            if (queryParams.ContainsKey("policy"))
                return PutBucketPolicy(bucket, body);
            if (queryParams.ContainsKey("notification"))
                return PutBucketNotification(bucket, body);
            if (queryParams.ContainsKey("tagging"))
                return PutBucketTagging(bucket, body);
            if (queryParams.ContainsKey("versioning"))
                return PutBucketVersioning(bucket, body);
            if (queryParams.ContainsKey("encryption"))
                return PutBucketEncryption(bucket, body);
            if (queryParams.ContainsKey("lifecycle"))
                return PutBucketLifecycle(bucket, body);
            if (queryParams.ContainsKey("cors"))
                return PutBucketCors(bucket, body);
            if (queryParams.ContainsKey("acl"))
                return PutBucketAcl(bucket, body);
            if (queryParams.ContainsKey("website"))
                return PutBucketWebsite(bucket, body);
            if (queryParams.ContainsKey("logging"))
                return PutBucketLogging(bucket, body);
            if (queryParams.ContainsKey("accelerate"))
                return PutBucketAccelerate(bucket, body);
            if (queryParams.ContainsKey("requestPayment"))
                return PutBucketRequestPayment(bucket, body);
            if (queryParams.ContainsKey("object-lock"))
                return PutObjectLockConfiguration(bucket, body);
            if (queryParams.ContainsKey("replication"))
                return PutBucketReplication(bucket, body);
            if (queryParams.ContainsKey("ownershipControls"))
                return PutBucketOwnershipControls(bucket, body);
            if (queryParams.ContainsKey("publicAccessBlock"))
                return PutPublicAccessBlock(bucket, body);
            return CreateBucket(bucket, body, headers);
        }

        if (method == "DELETE")
        {
            if (queryParams.ContainsKey("policy"))
                return DeleteBucketPolicy(bucket);
            if (queryParams.ContainsKey("tagging"))
                return DeleteBucketTagging(bucket);
            if (queryParams.ContainsKey("cors"))
                return DeleteBucketCors(bucket);
            if (queryParams.ContainsKey("lifecycle"))
                return DeleteBucketLifecycle(bucket);
            if (queryParams.ContainsKey("encryption"))
                return DeleteBucketEncryption(bucket);
            if (queryParams.ContainsKey("website"))
                return DeleteBucketWebsite(bucket);
            if (queryParams.ContainsKey("replication"))
                return DeleteBucketReplication(bucket);
            if (queryParams.ContainsKey("ownershipControls"))
                return DeleteBucketOwnershipControls(bucket);
            if (queryParams.ContainsKey("publicAccessBlock"))
                return DeletePublicAccessBlock(bucket);
            return DeleteBucket(bucket);
        }

        if (method == "HEAD")
            return HeadBucket(bucket);

        if (method == "POST")
        {
            if (queryParams.ContainsKey("delete"))
                return DeleteObjects(bucket, body, headers);
            return Error("MethodNotAllowed",
                "The specified method is not allowed against this resource.", 405);
        }

        return Error("MethodNotAllowed",
            "The specified method is not allowed against this resource.", 405);
    }

    // ── Bucket operations ────────────────────────────────────────────────────────

    private (int, Dictionary<string, string>, byte[]) ListBuckets()
    {
        var root = new XElement(Ns + "ListAllMyBucketsResult",
            new XElement(Ns + "Owner",
                new XElement(Ns + "ID", "owner-id"),
                new XElement(Ns + "DisplayName", "ministack")),
            new XElement(Ns + "Buckets",
                _buckets.Items
                    .OrderBy(kv => kv.Key, StringComparer.Ordinal)
                    .Select(kv => new XElement(Ns + "Bucket",
                        new XElement(Ns + "Name", kv.Key),
                        new XElement(Ns + "CreationDate", kv.Value.Created)))));
        return XmlOk(root);
    }

    private (int, Dictionary<string, string>, byte[]) CreateBucket(
        string name, byte[] body, IReadOnlyDictionary<string, string> headers)
    {
        if (!ValidateBucketName(name))
        {
            return Error("InvalidBucketName", "The specified bucket is not valid.", 400, $"/{name}");
        }
        if (_buckets.ContainsKey(name))
        {
            return (200, new Dictionary<string, string>(StringComparer.Ordinal) { ["Location"] = $"/{name}" }, []);
        }

        string? region = null;
        if (body.Length > 0)
        {
            try
            {
                var xmlRoot = XElement.Parse(Encoding.UTF8.GetString(body));
                var locEl = FindXmlTag(xmlRoot, "LocationConstraint");
                if (locEl is not null && !string.IsNullOrEmpty(locEl.Value))
                {
                    region = locEl.Value;
                }
            }
            catch
            {
                // ignore parse errors
            }
        }

        _buckets[name] = new S3Bucket
        {
            Created = TimeHelpers.NowIso(),
            Region = region,
            Objects = new Dictionary<string, S3Object>(StringComparer.Ordinal),
        };

        if (string.Equals(GetHeader(headers, "x-amz-bucket-object-lock-enabled"), "true", StringComparison.OrdinalIgnoreCase))
        {
            _bucketObjectLock[name] = new ObjectLockConfig { Enabled = true };
            _bucketVersioning[name] = "Enabled";
        }

        return (200, new Dictionary<string, string>(StringComparer.Ordinal) { ["Location"] = $"/{name}" }, []);
    }

    private (int, Dictionary<string, string>, byte[]) DeleteBucket(string name)
    {
        if (!_buckets.TryGetValue(name, out var bucket))
            return NoSuchBucket(name);
        if (bucket.Objects.Count > 0)
            return Error("BucketNotEmpty", "The bucket you tried to delete is not empty", 409, $"/{name}");

        _buckets.TryRemove(name, out _);
        _bucketPolicies.TryRemove(name, out _);
        _bucketNotifications.TryRemove(name, out _);
        _bucketTags.TryRemove(name, out _);
        _bucketVersioning.TryRemove(name, out _);
        _bucketEncryption.TryRemove(name, out _);
        _bucketLifecycle.TryRemove(name, out _);
        _bucketCors.TryRemove(name, out _);
        _bucketAcl.TryRemove(name, out _);
        _bucketWebsites.TryRemove(name, out _);
        _bucketLoggingConfig.TryRemove(name, out _);
        _bucketAccelerateConfig.TryRemove(name, out _);
        _bucketRequestPaymentConfig.TryRemove(name, out _);
        _bucketObjectLock.TryRemove(name, out _);
        _bucketReplication.TryRemove(name, out _);

        foreach (var k in _objectTags.Keys.Where(k => k.Bucket == name).ToList())
            _objectTags.TryRemove(k, out _);
        foreach (var k in _objectRetention.Keys.Where(k => k.Bucket == name).ToList())
            _objectRetention.TryRemove(k, out _);
        foreach (var k in _objectLegalHold.Keys.Where(k => k.Bucket == name).ToList())
            _objectLegalHold.TryRemove(k, out _);

        return (204, new Dictionary<string, string>(StringComparer.Ordinal), []);
    }

    private (int, Dictionary<string, string>, byte[]) HeadBucket(string name)
    {
        if (!_buckets.TryGetValue(name, out var bucket))
            return NoSuchBucket(name);
        return (200, new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["Content-Type"] = "application/xml",
            ["x-amz-bucket-region"] = bucket.Region ?? Region,
        }, []);
    }

    private (int, Dictionary<string, string>, byte[]) GetBucketLocation(string name)
    {
        if (!_buckets.TryGetValue(name, out var bucket))
            return NoSuchBucket(name);
        var root = new XElement(Ns + "LocationConstraint");
        var region = bucket.Region;
        if (!string.IsNullOrEmpty(region) && region != Region)
        {
            root.Value = region;
        }
        return XmlOk(root);
    }

    // ── Bucket sub-resources ─────────────────────────────────────────────────────

    private (int, Dictionary<string, string>, byte[]) GetBucketPolicy(string name)
    {
        if (!_buckets.ContainsKey(name)) return NoSuchBucket(name);
        if (!_bucketPolicies.TryGetValue(name, out var policy))
            return Error("NoSuchBucketPolicy", "The bucket policy does not exist", 404, $"/{name}");
        return (200, new Dictionary<string, string>(StringComparer.Ordinal)
            { ["Content-Type"] = "application/json" }, Encoding.UTF8.GetBytes(policy));
    }

    private (int, Dictionary<string, string>, byte[]) PutBucketPolicy(string name, byte[] body)
    {
        if (!_buckets.ContainsKey(name)) return NoSuchBucket(name);
        _bucketPolicies[name] = Encoding.UTF8.GetString(body);
        return (204, new Dictionary<string, string>(StringComparer.Ordinal), []);
    }

    private (int, Dictionary<string, string>, byte[]) DeleteBucketPolicy(string name)
    {
        if (!_buckets.ContainsKey(name)) return NoSuchBucket(name);
        _bucketPolicies.TryRemove(name, out _);
        return (204, new Dictionary<string, string>(StringComparer.Ordinal), []);
    }

    private (int, Dictionary<string, string>, byte[]) GetBucketVersioning(string name)
    {
        if (!_buckets.ContainsKey(name)) return NoSuchBucket(name);
        var root = new XElement(Ns + "VersioningConfiguration");
        if (_bucketVersioning.TryGetValue(name, out var status) && !string.IsNullOrEmpty(status))
        {
            root.Add(new XElement(Ns + "Status", status));
        }
        return XmlOk(root);
    }

    private (int, Dictionary<string, string>, byte[]) PutBucketVersioning(string name, byte[] body)
    {
        if (!_buckets.ContainsKey(name)) return NoSuchBucket(name);
        try
        {
            var xmlRoot = XElement.Parse(Encoding.UTF8.GetString(body));
            var statusEl = FindXmlTag(xmlRoot, "Status");
            if (statusEl is not null && !string.IsNullOrEmpty(statusEl.Value))
            {
                _bucketVersioning[name] = statusEl.Value;
            }
        }
        catch
        {
            // ignore
        }
        return (200, new Dictionary<string, string>(StringComparer.Ordinal), []);
    }

    private (int, Dictionary<string, string>, byte[]) GetBucketEncryption(string name)
    {
        if (!_buckets.ContainsKey(name)) return NoSuchBucket(name);
        if (_bucketEncryption.TryGetValue(name, out var config))
            return (200, new Dictionary<string, string>(StringComparer.Ordinal) { ["Content-Type"] = "application/xml" }, config);
        return Error("ServerSideEncryptionConfigurationNotFoundError",
            "The server side encryption configuration was not found", 404, $"/{name}");
    }

    private (int, Dictionary<string, string>, byte[]) PutBucketEncryption(string name, byte[] body)
    {
        if (!_buckets.ContainsKey(name)) return NoSuchBucket(name);
        _bucketEncryption[name] = body;
        return (200, new Dictionary<string, string>(StringComparer.Ordinal), []);
    }

    private (int, Dictionary<string, string>, byte[]) DeleteBucketEncryption(string name)
    {
        if (!_buckets.ContainsKey(name)) return NoSuchBucket(name);
        _bucketEncryption.TryRemove(name, out _);
        return (204, new Dictionary<string, string>(StringComparer.Ordinal), []);
    }

    private (int, Dictionary<string, string>, byte[]) GetBucketLifecycle(string name)
    {
        if (!_buckets.ContainsKey(name)) return NoSuchBucket(name);
        if (_bucketLifecycle.TryGetValue(name, out var config))
            return (200, new Dictionary<string, string>(StringComparer.Ordinal) { ["Content-Type"] = "application/xml" }, config);
        return Error("NoSuchLifecycleConfiguration",
            "The lifecycle configuration does not exist", 404, $"/{name}");
    }

    private (int, Dictionary<string, string>, byte[]) PutBucketLifecycle(string name, byte[] body)
    {
        if (!_buckets.ContainsKey(name)) return NoSuchBucket(name);
        _bucketLifecycle[name] = body;
        return (200, new Dictionary<string, string>(StringComparer.Ordinal), []);
    }

    private (int, Dictionary<string, string>, byte[]) DeleteBucketLifecycle(string name)
    {
        if (!_buckets.ContainsKey(name)) return NoSuchBucket(name);
        _bucketLifecycle.TryRemove(name, out _);
        return (204, new Dictionary<string, string>(StringComparer.Ordinal), []);
    }

    private (int, Dictionary<string, string>, byte[]) GetBucketCors(string name)
    {
        if (!_buckets.ContainsKey(name)) return NoSuchBucket(name);
        if (_bucketCors.TryGetValue(name, out var config))
            return (200, new Dictionary<string, string>(StringComparer.Ordinal) { ["Content-Type"] = "application/xml" }, config);
        return Error("NoSuchCORSConfiguration",
            "The CORS configuration does not exist", 404, $"/{name}");
    }

    private (int, Dictionary<string, string>, byte[]) PutBucketCors(string name, byte[] body)
    {
        if (!_buckets.ContainsKey(name)) return NoSuchBucket(name);
        _bucketCors[name] = body;
        return (200, new Dictionary<string, string>(StringComparer.Ordinal), []);
    }

    private (int, Dictionary<string, string>, byte[]) DeleteBucketCors(string name)
    {
        if (!_buckets.ContainsKey(name)) return NoSuchBucket(name);
        _bucketCors.TryRemove(name, out _);
        return (204, new Dictionary<string, string>(StringComparer.Ordinal), []);
    }

    private (int, Dictionary<string, string>, byte[]) GetBucketAcl(string name)
    {
        if (!_buckets.ContainsKey(name)) return NoSuchBucket(name);
        if (_bucketAcl.TryGetValue(name, out var stored))
            return (200, new Dictionary<string, string>(StringComparer.Ordinal) { ["Content-Type"] = "application/xml" }, stored);

        var bodyBytes = Encoding.UTF8.GetBytes(
            XmlDecl +
            $"<AccessControlPolicy xmlns=\"{S3Ns}\">" +
            "<Owner><ID>owner-id</ID><DisplayName>ministack</DisplayName></Owner>" +
            "<AccessControlList><Grant>" +
            "<Grantee xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:type=\"CanonicalUser\">" +
            "<ID>owner-id</ID><DisplayName>ministack</DisplayName></Grantee>" +
            "<Permission>FULL_CONTROL</Permission>" +
            "</Grant></AccessControlList></AccessControlPolicy>");
        return (200, new Dictionary<string, string>(StringComparer.Ordinal) { ["Content-Type"] = "application/xml" }, bodyBytes);
    }

    private (int, Dictionary<string, string>, byte[]) PutBucketAcl(string name, byte[] body)
    {
        if (!_buckets.ContainsKey(name)) return NoSuchBucket(name);
        if (body.Length > 0)
            _bucketAcl[name] = body;
        return (200, new Dictionary<string, string>(StringComparer.Ordinal), []);
    }

    private (int, Dictionary<string, string>, byte[]) GetBucketTagging(string name)
    {
        if (!_buckets.ContainsKey(name)) return NoSuchBucket(name);
        if (!_bucketTags.TryGetValue(name, out var tags) || tags.Count == 0)
            return Error("NoSuchTagSet", "The TagSet does not exist", 404, $"/{name}");

        var root = new XElement(Ns + "Tagging",
            new XElement(Ns + "TagSet",
                tags.Select(kv => new XElement(Ns + "Tag",
                    new XElement(Ns + "Key", kv.Key),
                    new XElement(Ns + "Value", kv.Value)))));
        return XmlOk(root);
    }

    private (int, Dictionary<string, string>, byte[]) PutBucketTagging(string name, byte[] body)
    {
        if (!_buckets.ContainsKey(name)) return NoSuchBucket(name);
        Dictionary<string, string> tags;
        try
        {
            tags = ParseTagsXml(body);
        }
        catch
        {
            return Error("MalformedXML", "The XML you provided was not well-formed", 400);
        }
        if (tags.Count > 50)
            return Error("BadRequest", "Object tags cannot be greater than 50", 400);
        _bucketTags[name] = tags;
        return (204, new Dictionary<string, string>(StringComparer.Ordinal), []);
    }

    private (int, Dictionary<string, string>, byte[]) DeleteBucketTagging(string name)
    {
        if (!_buckets.ContainsKey(name)) return NoSuchBucket(name);
        _bucketTags.TryRemove(name, out _);
        return (204, new Dictionary<string, string>(StringComparer.Ordinal), []);
    }

    private (int, Dictionary<string, string>, byte[]) GetBucketNotification(string name)
    {
        if (!_buckets.ContainsKey(name)) return NoSuchBucket(name);
        if (_bucketNotifications.TryGetValue(name, out var stored))
            return (200, new Dictionary<string, string>(StringComparer.Ordinal) { ["Content-Type"] = "application/xml" }, stored);
        var root = new XElement(Ns + "NotificationConfiguration");
        return XmlOk(root);
    }

    private (int, Dictionary<string, string>, byte[]) PutBucketNotification(string name, byte[] body)
    {
        if (!_buckets.ContainsKey(name)) return NoSuchBucket(name);
        _bucketNotifications[name] = body;
        return (200, new Dictionary<string, string>(StringComparer.Ordinal), []);
    }

    private (int, Dictionary<string, string>, byte[]) GetBucketLogging(string name)
    {
        if (!_buckets.ContainsKey(name)) return NoSuchBucket(name);
        if (_bucketLoggingConfig.TryGetValue(name, out var stored))
            return (200, new Dictionary<string, string>(StringComparer.Ordinal) { ["Content-Type"] = "application/xml" }, stored);
        var root = new XElement(Ns + "BucketLoggingStatus");
        return XmlOk(root);
    }

    private (int, Dictionary<string, string>, byte[]) PutBucketLogging(string name, byte[] body)
    {
        if (!_buckets.ContainsKey(name)) return NoSuchBucket(name);
        _bucketLoggingConfig[name] = body;
        return (200, new Dictionary<string, string>(StringComparer.Ordinal), []);
    }

    private (int, Dictionary<string, string>, byte[]) GetBucketAccelerate(string name)
    {
        if (!_buckets.ContainsKey(name)) return NoSuchBucket(name);
        if (_bucketAccelerateConfig.TryGetValue(name, out var stored))
            return (200, new Dictionary<string, string>(StringComparer.Ordinal) { ["Content-Type"] = "application/xml" }, stored);
        var root = new XElement(Ns + "AccelerateConfiguration");
        return XmlOk(root);
    }

    private (int, Dictionary<string, string>, byte[]) PutBucketAccelerate(string name, byte[] body)
    {
        if (!_buckets.ContainsKey(name)) return NoSuchBucket(name);
        _bucketAccelerateConfig[name] = body;
        return (200, new Dictionary<string, string>(StringComparer.Ordinal), []);
    }

    private (int, Dictionary<string, string>, byte[]) GetBucketRequestPayment(string name)
    {
        if (!_buckets.ContainsKey(name)) return NoSuchBucket(name);
        if (_bucketRequestPaymentConfig.TryGetValue(name, out var stored))
            return (200, new Dictionary<string, string>(StringComparer.Ordinal) { ["Content-Type"] = "application/xml" }, stored);
        var root = new XElement(Ns + "RequestPaymentConfiguration",
            new XElement(Ns + "Payer", "BucketOwner"));
        return XmlOk(root);
    }

    private (int, Dictionary<string, string>, byte[]) PutBucketRequestPayment(string name, byte[] body)
    {
        if (!_buckets.ContainsKey(name)) return NoSuchBucket(name);
        _bucketRequestPaymentConfig[name] = body;
        return (200, new Dictionary<string, string>(StringComparer.Ordinal), []);
    }

    private (int, Dictionary<string, string>, byte[]) GetBucketWebsite(string name)
    {
        if (!_buckets.ContainsKey(name)) return NoSuchBucket(name);
        if (_bucketWebsites.TryGetValue(name, out var stored))
            return (200, new Dictionary<string, string>(StringComparer.Ordinal) { ["Content-Type"] = "application/xml" }, stored);
        return Error("NoSuchWebsiteConfiguration",
            "The specified bucket does not have a website configuration", 404, $"/{name}");
    }

    private (int, Dictionary<string, string>, byte[]) PutBucketWebsite(string name, byte[] body)
    {
        if (!_buckets.ContainsKey(name)) return NoSuchBucket(name);
        _bucketWebsites[name] = body;
        return (200, new Dictionary<string, string>(StringComparer.Ordinal), []);
    }

    private (int, Dictionary<string, string>, byte[]) DeleteBucketWebsite(string name)
    {
        if (!_buckets.ContainsKey(name)) return NoSuchBucket(name);
        _bucketWebsites.TryRemove(name, out _);
        return (204, new Dictionary<string, string>(StringComparer.Ordinal), []);
    }

    private (int, Dictionary<string, string>, byte[]) GetBucketOwnershipControls(string name)
    {
        if (!_buckets.TryGetValue(name, out var bucket))
            return NoSuchBucket(name);
        if (bucket.OwnershipControls is not null)
            return (200, new Dictionary<string, string>(StringComparer.Ordinal) { ["Content-Type"] = "application/xml" }, bucket.OwnershipControls);
        var root = new XElement(Ns + "OwnershipControls",
            new XElement(Ns + "Rule",
                new XElement(Ns + "ObjectOwnership", "BucketOwnerEnforced")));
        return XmlOk(root);
    }

    private (int, Dictionary<string, string>, byte[]) PutBucketOwnershipControls(string name, byte[] body)
    {
        if (!_buckets.TryGetValue(name, out var bucket))
            return NoSuchBucket(name);
        bucket.OwnershipControls = body;
        return (200, new Dictionary<string, string>(StringComparer.Ordinal), []);
    }

    private (int, Dictionary<string, string>, byte[]) DeleteBucketOwnershipControls(string name)
    {
        if (!_buckets.TryGetValue(name, out var bucket))
            return NoSuchBucket(name);
        bucket.OwnershipControls = null;
        return (204, new Dictionary<string, string>(StringComparer.Ordinal), []);
    }

    private (int, Dictionary<string, string>, byte[]) GetPublicAccessBlock(string name)
    {
        if (!_buckets.TryGetValue(name, out var bucket))
            return NoSuchBucket(name);
        if (bucket.PublicAccessBlock is not null)
            return (200, new Dictionary<string, string>(StringComparer.Ordinal) { ["Content-Type"] = "application/xml" }, bucket.PublicAccessBlock);
        var root = new XElement(Ns + "PublicAccessBlockConfiguration",
            new XElement(Ns + "BlockPublicAcls", "true"),
            new XElement(Ns + "IgnorePublicAcls", "true"),
            new XElement(Ns + "BlockPublicPolicy", "true"),
            new XElement(Ns + "RestrictPublicBuckets", "true"));
        return XmlOk(root);
    }

    private (int, Dictionary<string, string>, byte[]) PutPublicAccessBlock(string name, byte[] body)
    {
        if (!_buckets.TryGetValue(name, out var bucket))
            return NoSuchBucket(name);
        bucket.PublicAccessBlock = body;
        return (200, new Dictionary<string, string>(StringComparer.Ordinal), []);
    }

    private (int, Dictionary<string, string>, byte[]) DeletePublicAccessBlock(string name)
    {
        if (!_buckets.TryGetValue(name, out var bucket))
            return NoSuchBucket(name);
        bucket.PublicAccessBlock = null;
        return (204, new Dictionary<string, string>(StringComparer.Ordinal), []);
    }

    // ── Object operations ────────────────────────────────────────────────────────

    private (int, Dictionary<string, string>, byte[]) PutObject(
        string bucketName, string key, byte[] body, IReadOnlyDictionary<string, string> headers)
    {
        if (!_buckets.TryGetValue(bucketName, out var bucket))
            return NoSuchBucket(bucketName);

        var md5Err = ValidateContentMd5(headers, body);
        if (md5Err is not null)
            return md5Err.Value;

        var obj = BuildObjectRecord(body, headers);
        bucket.Objects[key] = obj;

        // Object Lock headers on PutObject
        ApplyObjectLockFromHeaders(bucketName, key, headers);

        // x-amz-tagging header on PutObject
        var taggingHeader = GetHeader(headers, "x-amz-tagging") ?? "";
        if (!string.IsNullOrEmpty(taggingHeader))
        {
            var tags = ParseQueryString(taggingHeader);
            if (tags.Count > 10)
                return Error("BadRequest", "Object tags cannot be greater than 10", 400);
            _objectTags[(bucketName, key)] = tags;
        }

        var respHeaders = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["ETag"] = obj.ETag,
            ["Content-Length"] = "0",
        };

        if (_bucketVersioning.TryGetValue(bucketName, out var versioning) &&
            (versioning == "Enabled" || versioning == "Suspended"))
        {
            var versionId = HashHelpers.NewUuid();
            obj.VersionId = versionId;
            respHeaders["x-amz-version-id"] = versionId;
        }

        return (200, respHeaders, []);
    }

    private (int, Dictionary<string, string>, byte[]) GetObject(
        string bucketName, string key, IReadOnlyDictionary<string, string> headers)
    {
        if (!_buckets.TryGetValue(bucketName, out var bucket))
            return NoSuchBucket(bucketName);
        if (!bucket.Objects.TryGetValue(key, out var obj))
            return Error("NoSuchKey", "The specified key does not exist.", 404, $"/{bucketName}/{key}");

        var respHeaders = ObjectResponseHeaders(obj, bucketName, key);

        var rangeHeader = GetHeader(headers, "range") ?? "";
        if (!string.IsNullOrEmpty(rangeHeader))
        {
            var rng = ParseRange(rangeHeader, (int)obj.Size);
            if (rng is null)
            {
                var errRoot = new XElement("Error",
                    new XElement("Code", "InvalidRange"),
                    new XElement("Message", "The requested range is not satisfiable"),
                    new XElement("Resource", $"/{bucketName}/{key}"),
                    new XElement("RequestId", HashHelpers.NewUuid()));
                return (416, new Dictionary<string, string>(StringComparer.Ordinal)
                {
                    ["Content-Type"] = "application/xml",
                    ["Content-Range"] = $"bytes */{obj.Size}",
                }, XmlBody(errRoot));
            }
            var (start, end) = rng.Value;
            var sliceBody = obj.Body[start..(end + 1)];
            respHeaders["Content-Length"] = sliceBody.Length.ToString();
            respHeaders["Content-Range"] = $"bytes {start}-{end}/{obj.Size}";
            return (206, respHeaders, sliceBody);
        }

        return (200, respHeaders, obj.Body);
    }

    private (int, Dictionary<string, string>, byte[]) HeadObject(string bucketName, string key)
    {
        if (!_buckets.TryGetValue(bucketName, out var bucket))
            return NoSuchBucket(bucketName);
        if (!bucket.Objects.TryGetValue(key, out var obj))
            return Error("NoSuchKey", "The specified key does not exist.", 404, $"/{bucketName}/{key}");
        return (200, ObjectResponseHeaders(obj, bucketName, key), []);
    }

    private (int, Dictionary<string, string>, byte[]) DeleteObject(
        string bucketName, string key, IReadOnlyDictionary<string, string> headers)
    {
        if (!_buckets.TryGetValue(bucketName, out var bucket))
            return NoSuchBucket(bucketName);

        if (bucket.Objects.ContainsKey(key))
        {
            var lockErr = CheckObjectLock(bucketName, key, headers);
            if (lockErr is not null)
                return lockErr.Value;
        }

        bucket.Objects.Remove(key);
        _objectTags.TryRemove((bucketName, key), out _);
        _objectRetention.TryRemove((bucketName, key), out _);
        _objectLegalHold.TryRemove((bucketName, key), out _);

        if (_bucketVersioning.TryGetValue(bucketName, out var versioning) &&
            (versioning == "Enabled" || versioning == "Suspended"))
        {
            return (204, new Dictionary<string, string>(StringComparer.Ordinal)
            {
                ["x-amz-delete-marker"] = "true",
                ["x-amz-version-id"] = "null",
            }, []);
        }
        return (204, new Dictionary<string, string>(StringComparer.Ordinal), []);
    }

    private (int, Dictionary<string, string>, byte[]) CopyObject(
        string bucketName, string destKey, IReadOnlyDictionary<string, string> headers)
    {
        var source = Uri.UnescapeDataString((GetHeader(headers, "x-amz-copy-source") ?? "").TrimStart('/'));
        var srcParts = source.Split('?', 2)[0].Split('/', 2);
        if (srcParts.Length < 2)
            return Error("InvalidArgument",
                "Copy Source must mention the source bucket and key: /sourcebucket/sourcekey", 400);

        var srcBucketName = srcParts[0];
        var srcKey = srcParts[1];

        if (!_buckets.TryGetValue(srcBucketName, out var srcBucket))
            return NoSuchBucket(srcBucketName);
        if (!srcBucket.Objects.TryGetValue(srcKey, out var srcObj))
            return Error("NoSuchKey", "The specified key does not exist.", 404, $"/{srcBucketName}/{srcKey}");

        if (!_buckets.TryGetValue(bucketName, out var destBucket))
            return NoSuchBucket(bucketName);

        // Precondition: x-amz-copy-source-if-match
        var ifMatch = GetHeader(headers, "x-amz-copy-source-if-match") ?? "";
        if (!string.IsNullOrEmpty(ifMatch) && ifMatch.Trim('"') != srcObj.ETag.Trim('"'))
            return Error("PreconditionFailed",
                "At least one of the pre-conditions you specified did not hold", 412);

        // Precondition: x-amz-copy-source-if-none-match
        var ifNoneMatch = GetHeader(headers, "x-amz-copy-source-if-none-match") ?? "";
        if (!string.IsNullOrEmpty(ifNoneMatch) && ifNoneMatch.Trim('"') == srcObj.ETag.Trim('"'))
            return Error("PreconditionFailed",
                "At least one of the pre-conditions you specified did not hold", 412);

        var directive = (GetHeader(headers, "x-amz-metadata-directive") ?? "COPY").ToUpperInvariant();
        string contentType;
        string? contentEncoding;
        Dictionary<string, string> metadata;
        Dictionary<string, string> preserved;

        if (directive == "REPLACE")
        {
            metadata = ExtractUserMetadata(headers);
            contentType = GetHeader(headers, "content-type") ?? srcObj.ContentType;
            contentEncoding = GetHeader(headers, "content-encoding") ?? srcObj.ContentEncoding;
            preserved = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            foreach (var h in PreservedHeaders)
            {
                var val = GetHeader(headers, h);
                if (val is not null)
                    preserved[h] = val;
            }
        }
        else
        {
            metadata = new Dictionary<string, string>(srcObj.Metadata, StringComparer.OrdinalIgnoreCase);
            contentType = srcObj.ContentType;
            contentEncoding = srcObj.ContentEncoding;
            preserved = new Dictionary<string, string>(srcObj.PreservedHeaders, StringComparer.OrdinalIgnoreCase);
        }

        var newEtag = srcObj.ETag;
        var lastModified = TimeHelpers.NowIso();
        var destObj = new S3Object
        {
            Body = srcObj.Body,
            ContentType = contentType,
            ContentEncoding = contentEncoding,
            ETag = newEtag,
            LastModified = lastModified,
            Size = srcObj.Size,
            Metadata = metadata,
            PreservedHeaders = preserved,
        };
        destBucket.Objects[destKey] = destObj;

        // Preserve / replace tags
        var taggingDirective = (GetHeader(headers, "x-amz-tagging-directive") ?? "COPY").ToUpperInvariant();
        if (taggingDirective == "REPLACE")
        {
            var taggingHeader = GetHeader(headers, "x-amz-tagging") ?? "";
            if (!string.IsNullOrEmpty(taggingHeader))
            {
                _objectTags[(bucketName, destKey)] = ParseQueryString(taggingHeader);
            }
            else
            {
                _objectTags.TryRemove((bucketName, destKey), out _);
            }
        }
        else
        {
            if (_objectTags.TryGetValue((srcBucketName, srcKey), out var srcTags))
                _objectTags[(bucketName, destKey)] = new Dictionary<string, string>(srcTags, StringComparer.Ordinal);
            else
                _objectTags.TryRemove((bucketName, destKey), out _);
        }

        // Preserve lock / retention
        if (_objectRetention.TryGetValue((srcBucketName, srcKey), out var srcRetention))
            _objectRetention[(bucketName, destKey)] = new RetentionConfig { Mode = srcRetention.Mode, RetainUntilDate = srcRetention.RetainUntilDate };
        else
            _objectRetention.TryRemove((bucketName, destKey), out _);

        if (_objectLegalHold.TryGetValue((srcBucketName, srcKey), out var srcHold))
            _objectLegalHold[(bucketName, destKey)] = srcHold;
        else
            _objectLegalHold.TryRemove((bucketName, destKey), out _);

        var respHeaders = new Dictionary<string, string>(StringComparer.Ordinal) { ["Content-Type"] = "application/xml" };
        if (_bucketVersioning.TryGetValue(bucketName, out var versioningStatus) &&
            (versioningStatus == "Enabled" || versioningStatus == "Suspended"))
        {
            var versionId = HashHelpers.NewUuid();
            destObj.VersionId = versionId;
            respHeaders["x-amz-version-id"] = versionId;
        }

        var root = new XElement(Ns + "CopyObjectResult",
            new XElement(Ns + "LastModified", lastModified),
            new XElement(Ns + "ETag", newEtag));
        return (200, respHeaders, XmlBody(root));
    }

    // ── Object tagging ───────────────────────────────────────────────────────────

    private (int, Dictionary<string, string>, byte[]) GetObjectTagging(string bucketName, string key)
    {
        if (!_buckets.TryGetValue(bucketName, out var bucket))
            return NoSuchBucket(bucketName);
        if (!bucket.Objects.ContainsKey(key))
            return Error("NoSuchKey", "The specified key does not exist.", 404, $"/{bucketName}/{key}");

        _objectTags.TryGetValue((bucketName, key), out var tags);
        tags ??= new Dictionary<string, string>(StringComparer.Ordinal);

        var root = new XElement(Ns + "Tagging",
            new XElement(Ns + "TagSet",
                tags.Select(kv => new XElement(Ns + "Tag",
                    new XElement(Ns + "Key", kv.Key),
                    new XElement(Ns + "Value", kv.Value)))));
        return XmlOk(root);
    }

    private (int, Dictionary<string, string>, byte[]) PutObjectTagging(string bucketName, string key, byte[] body)
    {
        if (!_buckets.TryGetValue(bucketName, out var bucket))
            return NoSuchBucket(bucketName);
        if (!bucket.Objects.ContainsKey(key))
            return Error("NoSuchKey", "The specified key does not exist.", 404, $"/{bucketName}/{key}");

        Dictionary<string, string> tags;
        try
        {
            tags = ParseTagsXml(body);
        }
        catch
        {
            return Error("MalformedXML", "The XML you provided was not well-formed", 400);
        }
        if (tags.Count > 10)
            return Error("BadRequest", "Object tags cannot be greater than 10", 400);
        _objectTags[(bucketName, key)] = tags;
        return (200, new Dictionary<string, string>(StringComparer.Ordinal) { ["Content-Type"] = "application/xml" }, []);
    }

    private (int, Dictionary<string, string>, byte[]) DeleteObjectTagging(string bucketName, string key)
    {
        if (!_buckets.TryGetValue(bucketName, out var bucket))
            return NoSuchBucket(bucketName);
        if (!bucket.Objects.ContainsKey(key))
            return Error("NoSuchKey", "The specified key does not exist.", 404, $"/{bucketName}/{key}");
        _objectTags.TryRemove((bucketName, key), out _);
        return (204, new Dictionary<string, string>(StringComparer.Ordinal), []);
    }

    // ── Object Lock ──────────────────────────────────────────────────────────────

    private (int, Dictionary<string, string>, byte[]) GetObjectLockConfiguration(string bucketName)
    {
        if (!_buckets.ContainsKey(bucketName))
            return NoSuchBucket(bucketName);

        if (!_bucketObjectLock.TryGetValue(bucketName, out var lockCfg))
            return Error("ObjectLockConfigurationNotFoundError",
                "Object Lock configuration does not exist for this bucket", 404, $"/{bucketName}");

        var root = new XElement(Ns + "ObjectLockConfiguration",
            new XElement(Ns + "ObjectLockEnabled", "Enabled"));
        if (lockCfg.DefaultRetention is not null)
        {
            var retEl = new XElement(Ns + "DefaultRetention",
                new XElement(Ns + "Mode", lockCfg.DefaultRetention.Mode));
            if (lockCfg.DefaultRetention.Days.HasValue)
                retEl.Add(new XElement(Ns + "Days", lockCfg.DefaultRetention.Days.Value));
            if (lockCfg.DefaultRetention.Years.HasValue)
                retEl.Add(new XElement(Ns + "Years", lockCfg.DefaultRetention.Years.Value));
            root.Add(new XElement(Ns + "Rule", retEl));
        }
        return XmlOk(root);
    }

    private (int, Dictionary<string, string>, byte[]) PutObjectLockConfiguration(string bucketName, byte[] body)
    {
        if (!_buckets.ContainsKey(bucketName))
            return NoSuchBucket(bucketName);

        if (!_bucketVersioning.TryGetValue(bucketName, out var versioning) || versioning != "Enabled")
            return Error("InvalidBucketState",
                "Versioning must be 'Enabled' on the bucket to apply a Object Lock configuration", 409, $"/{bucketName}");

        XElement xmlRoot;
        try
        {
            xmlRoot = XElement.Parse(Encoding.UTF8.GetString(body));
        }
        catch
        {
            return Error("MalformedXML", "The XML you provided was not well-formed", 400);
        }

        var enabledEl = FindXmlTag(xmlRoot, "ObjectLockEnabled");
        if (enabledEl is null || enabledEl.Value != "Enabled")
            return Error("MalformedXML", "The XML you provided was not well-formed", 400);

        DefaultRetention? defaultRetention = null;
        var ruleEl = FindXmlTag(xmlRoot, "Rule");
        if (ruleEl is not null)
        {
            var retEl = FindXmlTag(ruleEl, "DefaultRetention");
            if (retEl is null)
                return Error("MalformedXML", "The XML you provided was not well-formed", 400);

            var modeEl = FindXmlTag(retEl, "Mode");
            var daysEl = FindXmlTag(retEl, "Days");
            var yearsEl = FindXmlTag(retEl, "Years");

            if (modeEl is null || (modeEl.Value != "GOVERNANCE" && modeEl.Value != "COMPLIANCE"))
                return Error("MalformedXML", "The XML you provided was not well-formed", 400);

            var hasDays = daysEl is not null && !string.IsNullOrEmpty(daysEl.Value);
            var hasYears = yearsEl is not null && !string.IsNullOrEmpty(yearsEl.Value);
            if ((hasDays && hasYears) || (!hasDays && !hasYears))
                return Error("MalformedXML", "The XML you provided was not well-formed", 400);

            defaultRetention = new DefaultRetention { Mode = modeEl.Value };
            try
            {
                if (hasDays) defaultRetention.Days = int.Parse(daysEl!.Value);
                if (hasYears) defaultRetention.Years = int.Parse(yearsEl!.Value);
            }
            catch
            {
                return Error("MalformedXML", "The XML you provided was not well-formed", 400);
            }
        }

        _bucketObjectLock[bucketName] = new ObjectLockConfig { Enabled = true, DefaultRetention = defaultRetention };
        return (200, new Dictionary<string, string>(StringComparer.Ordinal) { ["Content-Type"] = "application/xml" }, []);
    }

    private (int, Dictionary<string, string>, byte[]) GetObjectRetention(string bucketName, string key)
    {
        if (!_buckets.TryGetValue(bucketName, out var bucket))
            return NoSuchBucket(bucketName);
        if (!bucket.Objects.ContainsKey(key))
            return Error("NoSuchKey", "The specified key does not exist.", 404, $"/{bucketName}/{key}");

        if (!_bucketObjectLock.ContainsKey(bucketName))
            return Error("InvalidRequest", "Bucket is missing Object Lock Configuration", 400);

        if (!_objectRetention.TryGetValue((bucketName, key), out var retention))
            return Error("NoSuchObjectLockConfiguration",
                "The specified object does not have a ObjectLock configuration", 404);

        var root = new XElement(Ns + "Retention",
            new XElement(Ns + "Mode", retention.Mode),
            new XElement(Ns + "RetainUntilDate", retention.RetainUntilDate));
        return XmlOk(root);
    }

    private (int, Dictionary<string, string>, byte[]) PutObjectRetention(
        string bucketName, string key, byte[] body, IReadOnlyDictionary<string, string> headers)
    {
        if (!_buckets.TryGetValue(bucketName, out var bucket))
            return NoSuchBucket(bucketName);
        if (!bucket.Objects.ContainsKey(key))
            return Error("NoSuchKey", "The specified key does not exist.", 404, $"/{bucketName}/{key}");

        if (!_bucketObjectLock.ContainsKey(bucketName))
            return Error("InvalidRequest", "Bucket is missing Object Lock Configuration", 400);

        XElement xmlRoot;
        try
        {
            xmlRoot = XElement.Parse(Encoding.UTF8.GetString(body));
        }
        catch
        {
            return Error("MalformedXML", "The XML you provided was not well-formed", 400);
        }

        var modeEl = FindXmlTag(xmlRoot, "Mode");
        var dateEl = FindXmlTag(xmlRoot, "RetainUntilDate");

        if (modeEl is null || (modeEl.Value != "GOVERNANCE" && modeEl.Value != "COMPLIANCE"))
            return Error("MalformedXML", "The XML you provided was not well-formed", 400);
        if (dateEl is null || string.IsNullOrEmpty(dateEl.Value))
            return Error("MalformedXML", "The XML you provided was not well-formed", 400);

        var retainUntil = dateEl.Value;

        if (_objectRetention.TryGetValue((bucketName, key), out var existing))
        {
            var isReducing = string.Compare(existing.RetainUntilDate, retainUntil, StringComparison.Ordinal) > 0
                || (modeEl.Value == "GOVERNANCE" && existing.Mode == "COMPLIANCE");
            if (isReducing)
            {
                if (existing.Mode == "COMPLIANCE")
                    return Error("AccessDenied", "Access Denied because object protected by object lock.", 403);
                if (existing.Mode == "GOVERNANCE")
                {
                    var bypass = (GetHeader(headers, "x-amz-bypass-governance-retention") ?? "").ToLowerInvariant();
                    if (bypass != "true")
                        return Error("AccessDenied", "Access Denied because object protected by object lock.", 403);
                }
            }
        }

        _objectRetention[(bucketName, key)] = new RetentionConfig { Mode = modeEl.Value, RetainUntilDate = retainUntil };
        return (200, new Dictionary<string, string>(StringComparer.Ordinal) { ["Content-Type"] = "application/xml" }, []);
    }

    private (int, Dictionary<string, string>, byte[]) GetObjectLegalHold(string bucketName, string key)
    {
        if (!_buckets.TryGetValue(bucketName, out var bucket))
            return NoSuchBucket(bucketName);
        if (!bucket.Objects.ContainsKey(key))
            return Error("NoSuchKey", "The specified key does not exist.", 404, $"/{bucketName}/{key}");

        if (!_bucketObjectLock.ContainsKey(bucketName))
            return Error("InvalidRequest", "Bucket is missing Object Lock Configuration", 400);

        if (!_objectLegalHold.TryGetValue((bucketName, key), out var status))
            return Error("NoSuchObjectLockConfiguration",
                "The specified object does not have a ObjectLock configuration", 404);

        var root = new XElement(Ns + "LegalHold",
            new XElement(Ns + "Status", status));
        return XmlOk(root);
    }

    private (int, Dictionary<string, string>, byte[]) PutObjectLegalHold(string bucketName, string key, byte[] body)
    {
        if (!_buckets.TryGetValue(bucketName, out var bucket))
            return NoSuchBucket(bucketName);
        if (!bucket.Objects.ContainsKey(key))
            return Error("NoSuchKey", "The specified key does not exist.", 404, $"/{bucketName}/{key}");

        if (!_bucketObjectLock.ContainsKey(bucketName))
            return Error("InvalidRequest", "Bucket is missing Object Lock Configuration", 400);

        XElement xmlRoot;
        try
        {
            xmlRoot = XElement.Parse(Encoding.UTF8.GetString(body));
        }
        catch
        {
            return Error("MalformedXML", "The XML you provided was not well-formed", 400);
        }

        var statusEl = FindXmlTag(xmlRoot, "Status");
        if (statusEl is null || (statusEl.Value != "ON" && statusEl.Value != "OFF"))
            return Error("MalformedXML", "The XML you provided was not well-formed", 400);

        _objectLegalHold[(bucketName, key)] = statusEl.Value;
        return (200, new Dictionary<string, string>(StringComparer.Ordinal) { ["Content-Type"] = "application/xml" }, []);
    }

    // ── Replication ──────────────────────────────────────────────────────────────

    private (int, Dictionary<string, string>, byte[]) PutBucketReplication(string bucketName, byte[] body)
    {
        if (!_buckets.ContainsKey(bucketName))
            return NoSuchBucket(bucketName);

        if (!_bucketVersioning.TryGetValue(bucketName, out var versioning) || versioning != "Enabled")
            return Error("InvalidRequest",
                "Versioning must be 'Enabled' on the bucket to apply a replication configuration", 400, $"/{bucketName}");

        XElement xmlRoot;
        try
        {
            xmlRoot = XElement.Parse(Encoding.UTF8.GetString(body));
        }
        catch
        {
            return Error("MalformedXML", "The XML you provided was not well-formed", 400);
        }

        var roleEl = FindXmlTag(xmlRoot, "Role");
        var role = roleEl is not null && !string.IsNullOrEmpty(roleEl.Value) ? roleEl.Value : "";

        var rules = new List<ReplicationRule>();
        var ruleElements = xmlRoot.Elements(Ns + "Rule").ToList();
        if (ruleElements.Count == 0)
            ruleElements = xmlRoot.Elements("Rule").ToList();

        foreach (var ruleEl in ruleElements)
        {
            var rule = new ReplicationRule();
            var idEl = FindXmlTag(ruleEl, "ID");
            rule.Id = idEl is not null && !string.IsNullOrEmpty(idEl.Value)
                ? idEl.Value
                : HashHelpers.NewUuid()[..8];
            var statusEl = FindXmlTag(ruleEl, "Status");
            rule.Status = statusEl is not null && !string.IsNullOrEmpty(statusEl.Value)
                ? statusEl.Value
                : "Enabled";
            var prefixEl = FindXmlTag(ruleEl, "Prefix");
            if (prefixEl is not null)
                rule.Prefix = prefixEl.Value;
            var destEl = FindXmlTag(ruleEl, "Destination");
            if (destEl is not null)
            {
                rule.Destination = new ReplicationDestination();
                var bucketEl = FindXmlTag(destEl, "Bucket");
                if (bucketEl is not null && !string.IsNullOrEmpty(bucketEl.Value))
                {
                    rule.Destination.Bucket = bucketEl.Value;
                    var destName = bucketEl.Value.Contains(":::")
                        ? bucketEl.Value.Split(":::")[^1]
                        : bucketEl.Value;
                    if (_buckets.ContainsKey(destName))
                    {
                        if (!_bucketVersioning.TryGetValue(destName, out var destVersioning) || destVersioning != "Enabled")
                            return Error("InvalidRequest", "Destination bucket must have versioning enabled.", 400);
                    }
                }
                var scEl = FindXmlTag(destEl, "StorageClass");
                if (scEl is not null && !string.IsNullOrEmpty(scEl.Value))
                    rule.Destination.StorageClass = scEl.Value;
            }
            rules.Add(rule);
        }

        if (rules.Count == 0)
            return Error("MalformedXML", "The XML you provided was not well-formed", 400);

        _bucketReplication[bucketName] = new ReplicationConfig { Role = role, Rules = rules };
        return (200, new Dictionary<string, string>(StringComparer.Ordinal) { ["Content-Type"] = "application/xml" }, []);
    }

    private (int, Dictionary<string, string>, byte[]) GetBucketReplication(string bucketName)
    {
        if (!_buckets.ContainsKey(bucketName))
            return NoSuchBucket(bucketName);

        if (!_bucketReplication.TryGetValue(bucketName, out var repl))
            return Error("ReplicationConfigurationNotFoundError",
                "The replication configuration was not found", 404, $"/{bucketName}");

        var root = new XElement(Ns + "ReplicationConfiguration",
            new XElement(Ns + "Role", repl.Role));
        foreach (var rule in repl.Rules)
        {
            var ruleEl = new XElement(Ns + "Rule",
                new XElement(Ns + "ID", rule.Id),
                new XElement(Ns + "Status", rule.Status));
            if (rule.Prefix is not null)
                ruleEl.Add(new XElement(Ns + "Prefix", rule.Prefix));
            if (rule.Destination is not null)
            {
                var destEl = new XElement(Ns + "Destination");
                if (rule.Destination.Bucket is not null)
                    destEl.Add(new XElement(Ns + "Bucket", rule.Destination.Bucket));
                if (rule.Destination.StorageClass is not null)
                    destEl.Add(new XElement(Ns + "StorageClass", rule.Destination.StorageClass));
                ruleEl.Add(destEl);
            }
            root.Add(ruleEl);
        }
        return XmlOk(root);
    }

    private (int, Dictionary<string, string>, byte[]) DeleteBucketReplication(string bucketName)
    {
        if (!_buckets.ContainsKey(bucketName))
            return NoSuchBucket(bucketName);
        _bucketReplication.TryRemove(bucketName, out _);
        return (204, new Dictionary<string, string>(StringComparer.Ordinal), []);
    }

    // ── List objects ─────────────────────────────────────────────────────────────

    private (int, Dictionary<string, string>, byte[]) ListObjectVersions(
        string bucketName, IReadOnlyDictionary<string, string[]> queryParams)
    {
        if (!_buckets.TryGetValue(bucketName, out var bucket))
            return NoSuchBucket(bucketName);

        var prefix = Qp(queryParams, "prefix");
        var keyMarker = Qp(queryParams, "key-marker");
        var maxKeys = int.Parse(Qp(queryParams, "max-keys", "1000"));

        var root = new XElement(Ns + "ListVersionsResult",
            new XElement(Ns + "Name", bucketName),
            new XElement(Ns + "Prefix", prefix),
            new XElement(Ns + "KeyMarker", keyMarker),
            new XElement(Ns + "VersionIdMarker", Qp(queryParams, "version-id-marker")),
            new XElement(Ns + "MaxKeys", maxKeys));

        var keys = bucket.Objects.Keys
            .Where(k => k.StartsWith(prefix, StringComparison.Ordinal) &&
                        string.Compare(k, keyMarker, StringComparison.Ordinal) > 0)
            .OrderBy(k => k, StringComparer.Ordinal)
            .ToList();

        var isTruncated = keys.Count > maxKeys;
        root.Add(new XElement(Ns + "IsTruncated", isTruncated ? "true" : "false"));

        foreach (var k in keys.Take(maxKeys))
        {
            var obj = bucket.Objects[k];
            root.Add(new XElement(Ns + "Version",
                new XElement(Ns + "Key", k),
                new XElement(Ns + "VersionId", "1"),
                new XElement(Ns + "IsLatest", "true"),
                new XElement(Ns + "LastModified", obj.LastModified),
                new XElement(Ns + "ETag", obj.ETag),
                new XElement(Ns + "Size", obj.Size),
                new XElement(Ns + "StorageClass", "STANDARD"),
                new XElement(Ns + "Owner",
                    new XElement(Ns + "ID", "owner-id"),
                    new XElement(Ns + "DisplayName", "ministack"))));
        }

        return XmlOk(root);
    }

    private static (List<string> Contents, SortedSet<string> CommonPrefixes, bool IsTruncated, string NextMarker) CollectListEntries(
        Dictionary<string, S3Object> bucketObjects,
        string prefix,
        string delimiter,
        int maxKeys,
        string startAfter)
    {
        var allKeys = bucketObjects.Keys
            .Where(k => k.StartsWith(prefix, StringComparison.Ordinal) &&
                        string.Compare(k, startAfter, StringComparison.Ordinal) > 0)
            .OrderBy(k => k, StringComparer.Ordinal)
            .ToList();

        var contents = new List<string>();
        var commonPrefixes = new SortedSet<string>(StringComparer.Ordinal);
        var isTruncated = false;
        var count = 0;
        var nextMarker = "";

        var i = 0;
        while (i < allKeys.Count)
        {
            var k = allKeys[i];

            if (!string.IsNullOrEmpty(delimiter))
            {
                var suffix = k[prefix.Length..];
                var delimIdx = suffix.IndexOf(delimiter, StringComparison.Ordinal);
                if (delimIdx >= 0)
                {
                    var cp = prefix + suffix[..(delimIdx + delimiter.Length)];
                    var isNewPrefix = !commonPrefixes.Contains(cp);
                    if (isNewPrefix)
                    {
                        if (count >= maxKeys)
                        {
                            isTruncated = true;
                            break;
                        }
                        commonPrefixes.Add(cp);
                        count++;
                    }
                    nextMarker = k;
                    i++;
                    while (i < allKeys.Count && allKeys[i].StartsWith(cp, StringComparison.Ordinal))
                    {
                        nextMarker = allKeys[i];
                        i++;
                    }
                    continue;
                }
            }

            if (count >= maxKeys)
            {
                isTruncated = true;
                break;
            }
            contents.Add(k);
            count++;
            nextMarker = k;
            i++;
        }

        return (contents, commonPrefixes, isTruncated, nextMarker);
    }

    private (int, Dictionary<string, string>, byte[]) ListObjectsV1(
        string bucketName, IReadOnlyDictionary<string, string[]> queryParams)
    {
        if (!_buckets.TryGetValue(bucketName, out var bucket))
            return NoSuchBucket(bucketName);

        var prefix = Qp(queryParams, "prefix");
        var delimiter = Qp(queryParams, "delimiter");
        var maxKeys = int.Parse(Qp(queryParams, "max-keys", "1000"));
        var marker = Qp(queryParams, "marker");
        var encodingType = Qp(queryParams, "encoding-type");
        var encode = encodingType == "url";

        var (contents, commonPrefixes, isTruncated, nextMarker) = CollectListEntries(
            bucket.Objects, prefix, delimiter, maxKeys, marker);

        var root = new XElement(Ns + "ListBucketResult",
            new XElement(Ns + "Name", bucketName),
            new XElement(Ns + "Prefix", encode && !string.IsNullOrEmpty(prefix) ? Uri.EscapeDataString(prefix) : prefix),
            new XElement(Ns + "Marker", encode && !string.IsNullOrEmpty(marker) ? Uri.EscapeDataString(marker) : marker));

        if (!string.IsNullOrEmpty(delimiter))
            root.Add(new XElement(Ns + "Delimiter", encode ? Uri.EscapeDataString(delimiter) : delimiter));
        if (!string.IsNullOrEmpty(encodingType))
            root.Add(new XElement(Ns + "EncodingType", encodingType));

        root.Add(new XElement(Ns + "MaxKeys", maxKeys));
        root.Add(new XElement(Ns + "IsTruncated", isTruncated ? "true" : "false"));

        if (isTruncated && !string.IsNullOrEmpty(nextMarker) && !string.IsNullOrEmpty(delimiter))
            root.Add(new XElement(Ns + "NextMarker", encode ? Uri.EscapeDataString(nextMarker) : nextMarker));

        foreach (var k in contents)
        {
            var obj = bucket.Objects[k];
            root.Add(new XElement(Ns + "Contents",
                new XElement(Ns + "Key", encode ? Uri.EscapeDataString(k) : k),
                new XElement(Ns + "LastModified", obj.LastModified),
                new XElement(Ns + "ETag", obj.ETag),
                new XElement(Ns + "Size", obj.Size),
                new XElement(Ns + "StorageClass", "STANDARD"),
                new XElement(Ns + "Owner",
                    new XElement(Ns + "ID", "owner-id"),
                    new XElement(Ns + "DisplayName", "ministack"))));
        }

        foreach (var cp in commonPrefixes)
        {
            root.Add(new XElement(Ns + "CommonPrefixes",
                new XElement(Ns + "Prefix", encode ? Uri.EscapeDataString(cp) : cp)));
        }

        return XmlOk(root);
    }

    private (int, Dictionary<string, string>, byte[]) ListObjectsV2(
        string bucketName, IReadOnlyDictionary<string, string[]> queryParams)
    {
        if (!_buckets.TryGetValue(bucketName, out var bucket))
            return NoSuchBucket(bucketName);

        var prefix = Qp(queryParams, "prefix");
        var delimiter = Qp(queryParams, "delimiter");
        var maxKeys = int.Parse(Qp(queryParams, "max-keys", "1000"));
        var continuation = Qp(queryParams, "continuation-token");
        var startAfter = Qp(queryParams, "start-after");
        var fetchOwner = Qp(queryParams, "fetch-owner").Equals("true", StringComparison.OrdinalIgnoreCase);
        var encodingType = Qp(queryParams, "encoding-type");
        var encode = encodingType == "url";

        string effectiveStart;
        if (!string.IsNullOrEmpty(continuation))
        {
            try
            {
                effectiveStart = Encoding.UTF8.GetString(Convert.FromBase64String(continuation));
            }
            catch
            {
                effectiveStart = continuation;
            }
        }
        else
        {
            effectiveStart = startAfter;
        }

        var (contents, commonPrefixes, isTruncated, nextMarker) = CollectListEntries(
            bucket.Objects, prefix, delimiter, maxKeys, effectiveStart);

        var root = new XElement(Ns + "ListBucketResult",
            new XElement(Ns + "Name", bucketName),
            new XElement(Ns + "Prefix", encode && !string.IsNullOrEmpty(prefix) ? Uri.EscapeDataString(prefix) : prefix));

        if (!string.IsNullOrEmpty(delimiter))
            root.Add(new XElement(Ns + "Delimiter", encode ? Uri.EscapeDataString(delimiter) : delimiter));
        if (!string.IsNullOrEmpty(encodingType))
            root.Add(new XElement(Ns + "EncodingType", encodingType));

        root.Add(new XElement(Ns + "MaxKeys", maxKeys));
        root.Add(new XElement(Ns + "KeyCount", contents.Count + commonPrefixes.Count));
        root.Add(new XElement(Ns + "IsTruncated", isTruncated ? "true" : "false"));

        if (!string.IsNullOrEmpty(continuation))
            root.Add(new XElement(Ns + "ContinuationToken", continuation));
        if (!string.IsNullOrEmpty(startAfter))
            root.Add(new XElement(Ns + "StartAfter", encode ? Uri.EscapeDataString(startAfter) : startAfter));

        if (isTruncated && !string.IsNullOrEmpty(nextMarker))
        {
            var token = Convert.ToBase64String(Encoding.UTF8.GetBytes(nextMarker));
            root.Add(new XElement(Ns + "NextContinuationToken", token));
        }

        foreach (var k in contents)
        {
            var obj = bucket.Objects[k];
            var contentsEl = new XElement(Ns + "Contents",
                new XElement(Ns + "Key", encode ? Uri.EscapeDataString(k) : k),
                new XElement(Ns + "LastModified", obj.LastModified),
                new XElement(Ns + "ETag", obj.ETag),
                new XElement(Ns + "Size", obj.Size),
                new XElement(Ns + "StorageClass", "STANDARD"));
            if (fetchOwner)
            {
                contentsEl.Add(new XElement(Ns + "Owner",
                    new XElement(Ns + "ID", "owner-id"),
                    new XElement(Ns + "DisplayName", "ministack")));
            }
            root.Add(contentsEl);
        }

        foreach (var cp in commonPrefixes)
        {
            root.Add(new XElement(Ns + "CommonPrefixes",
                new XElement(Ns + "Prefix", encode ? Uri.EscapeDataString(cp) : cp)));
        }

        return XmlOk(root);
    }

    // ── Batch delete ─────────────────────────────────────────────────────────────

    private (int, Dictionary<string, string>, byte[]) DeleteObjects(
        string bucketName, byte[] body, IReadOnlyDictionary<string, string> headers)
    {
        if (!_buckets.TryGetValue(bucketName, out var bucket))
            return NoSuchBucket(bucketName);

        XElement xmlRoot;
        try
        {
            xmlRoot = XElement.Parse(Encoding.UTF8.GetString(body));
        }
        catch
        {
            return Error("MalformedXML", "The XML you provided was not well-formed", 400);
        }

        var quiet = false;
        var quietEl = FindXmlTag(xmlRoot, "Quiet");
        if (quietEl is not null && quietEl.Value.Equals("true", StringComparison.OrdinalIgnoreCase))
            quiet = true;

        var deletedKeys = new List<string>();
        var errors = new List<(string Key, string Code, string Message)>();

        var objectElements = xmlRoot.Elements(Ns + "Object").ToList();
        if (objectElements.Count == 0)
            objectElements = xmlRoot.Elements("Object").ToList();

        foreach (var objEl in objectElements)
        {
            var keyEl = FindXmlTag(objEl, "Key");
            if (keyEl is null || string.IsNullOrEmpty(keyEl.Value))
                continue;

            var k = keyEl.Value;
            if (bucket.Objects.ContainsKey(k))
            {
                var lockErr = CheckObjectLock(bucketName, k, headers);
                if (lockErr is not null)
                {
                    errors.Add((k, "AccessDenied", "Access Denied because object protected by object lock."));
                    continue;
                }
            }
            bucket.Objects.Remove(k);
            _objectTags.TryRemove((bucketName, k), out _);
            _objectRetention.TryRemove((bucketName, k), out _);
            _objectLegalHold.TryRemove((bucketName, k), out _);
            deletedKeys.Add(k);
        }

        var resp = new XElement(Ns + "DeleteResult");
        if (!quiet)
        {
            foreach (var k in deletedKeys)
            {
                resp.Add(new XElement(Ns + "Deleted",
                    new XElement(Ns + "Key", k)));
            }
        }
        foreach (var (k, code, msg) in errors)
        {
            resp.Add(new XElement(Ns + "Error",
                new XElement(Ns + "Key", k),
                new XElement(Ns + "Code", code),
                new XElement(Ns + "Message", msg)));
        }

        return XmlOk(resp);
    }

    // ── Multipart upload ─────────────────────────────────────────────────────────

    private (int, Dictionary<string, string>, byte[]) CreateMultipartUpload(
        string bucketName, string key, IReadOnlyDictionary<string, string> headers)
    {
        if (!_buckets.ContainsKey(bucketName))
            return NoSuchBucket(bucketName);

        var uploadId = HashHelpers.NewUuid();
        var contentType = GetHeader(headers, "content-type") ?? "application/octet-stream";
        var contentEncoding = GetHeader(headers, "content-encoding");
        var metadata = ExtractUserMetadata(headers);
        var preserved = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        foreach (var h in PreservedHeaders)
        {
            var val = GetHeader(headers, h);
            if (val is not null)
                preserved[h] = val;
        }

        _multipartUploads[uploadId] = new MultipartUpload
        {
            Bucket = bucketName,
            Key = key,
            Parts = new Dictionary<int, MultipartPart>(),
            Metadata = metadata,
            ContentType = contentType,
            ContentEncoding = contentEncoding,
            PreservedHeaders = preserved,
            Created = TimeHelpers.NowIso(),
        };

        var root = new XElement(Ns + "InitiateMultipartUploadResult",
            new XElement(Ns + "Bucket", bucketName),
            new XElement(Ns + "Key", key),
            new XElement(Ns + "UploadId", uploadId));
        return XmlOk(root);
    }

    private (int, Dictionary<string, string>, byte[]) UploadPart(
        string bucketName, string key, byte[] body,
        IReadOnlyDictionary<string, string[]> queryParams,
        IReadOnlyDictionary<string, string> headers)
    {
        if (!_buckets.ContainsKey(bucketName))
            return NoSuchBucket(bucketName);

        var uploadId = Qp(queryParams, "uploadId");
        var partNumberStr = Qp(queryParams, "partNumber");

        if (!_multipartUploads.TryGetValue(uploadId, out var upload))
            return Error("NoSuchUpload", "The specified multipart upload does not exist.", 404, $"/{bucketName}/{key}");

        if (upload.Bucket != bucketName || upload.Key != key)
            return Error("NoSuchUpload", "The specified multipart upload does not exist.", 404, $"/{bucketName}/{key}");

        if (!int.TryParse(partNumberStr, out var pn))
            return Error("InvalidArgument", "Part number must be an integer between 1 and 10000, inclusive.", 400);
        if (pn < 1 || pn > 10000)
            return Error("InvalidArgument", "Part number must be an integer between 1 and 10000, inclusive.", 400);

        var md5Err = ValidateContentMd5(headers, body);
        if (md5Err is not null)
            return md5Err.Value;

        var etag = $"\"{HashHelpers.Md5Hash(body)}\"";
        upload.Parts[pn] = new MultipartPart
        {
            Body = body,
            ETag = etag,
            Size = body.Length,
            LastModified = TimeHelpers.NowIso(),
        };
        return (200, new Dictionary<string, string>(StringComparer.Ordinal) { ["ETag"] = etag }, []);
    }

    private (int, Dictionary<string, string>, byte[]) UploadPartCopy(
        string bucketName, string destKey,
        IReadOnlyDictionary<string, string[]> queryParams,
        IReadOnlyDictionary<string, string> headers)
    {
        var uploadId = Qp(queryParams, "uploadId");
        var partNumber = int.Parse(Qp(queryParams, "partNumber", "1"));

        if (!_multipartUploads.TryGetValue(uploadId, out _))
            return Error("NoSuchUpload", "The specified multipart upload does not exist.", 404);

        var source = Uri.UnescapeDataString((GetHeader(headers, "x-amz-copy-source") ?? "").TrimStart('/'));
        var srcParts = source.Split('?', 2)[0].Split('/', 2);
        if (srcParts.Length < 2)
            return Error("InvalidArgument", "Copy Source must mention the source bucket and key", 400);

        var srcBucketName = srcParts[0];
        var srcKey = srcParts[1];

        if (!_buckets.TryGetValue(srcBucketName, out var srcBucket))
            return NoSuchBucket(srcBucketName);
        if (!srcBucket.Objects.TryGetValue(srcKey, out var srcObj))
            return Error("NoSuchKey", "The specified key does not exist.", 404);

        var srcBody = srcObj.Body;

        // Handle x-amz-copy-source-range
        var copyRange = GetHeader(headers, "x-amz-copy-source-range") ?? "";
        if (!string.IsNullOrEmpty(copyRange) && copyRange.StartsWith("bytes=", StringComparison.Ordinal))
        {
            var rng = copyRange[6..];
            var rangeParts = rng.Split('-', 2);
            if (rangeParts.Length == 2 &&
                int.TryParse(rangeParts[0], out var start) &&
                int.TryParse(rangeParts[1], out var end))
            {
                srcBody = srcBody[start..(end + 1)];
            }
        }

        var etag = $"\"{HashHelpers.Md5Hash(srcBody)}\"";
        _multipartUploads[uploadId].Parts[partNumber] = new MultipartPart
        {
            Body = srcBody,
            ETag = etag,
            Size = srcBody.Length,
            LastModified = TimeHelpers.NowIso(),
        };

        var root = new XElement(Ns + "CopyPartResult",
            new XElement(Ns + "ETag", etag),
            new XElement(Ns + "LastModified", TimeHelpers.NowIso()));
        return XmlOk(root);
    }

    private (int, Dictionary<string, string>, byte[]) CompleteMultipartUpload(
        string bucketName, string key, byte[] body,
        IReadOnlyDictionary<string, string[]> queryParams)
    {
        if (!_buckets.TryGetValue(bucketName, out var bucket))
            return NoSuchBucket(bucketName);

        var uploadId = Qp(queryParams, "uploadId");
        if (!_multipartUploads.TryGetValue(uploadId, out var upload))
            return Error("NoSuchUpload", "The specified multipart upload does not exist.", 404, $"/{bucketName}/{key}");
        if (upload.Bucket != bucketName || upload.Key != key)
            return Error("NoSuchUpload", "The specified multipart upload does not exist.", 404, $"/{bucketName}/{key}");

        var xmlRoot = XElement.Parse(Encoding.UTF8.GetString(body));
        var orderedParts = new List<(int PartNumber, string? ETag)>();

        foreach (var el in xmlRoot.Descendants())
        {
            var local = el.Name.LocalName;
            if (local == "Part")
            {
                string? pnText = null;
                string? etagText = null;
                foreach (var child in el.Elements())
                {
                    var childLocal = child.Name.LocalName;
                    if (childLocal == "PartNumber")
                        pnText = child.Value;
                    else if (childLocal == "ETag")
                        etagText = child.Value;
                }
                if (pnText is not null)
                    orderedParts.Add((int.Parse(pnText), etagText));
            }
        }

        orderedParts.Sort((a, b) => a.PartNumber.CompareTo(b.PartNumber));

        var md5Digests = new List<byte>();
        var combined = new List<byte>();
        foreach (var (pn, reqEtag) in orderedParts)
        {
            if (!upload.Parts.TryGetValue(pn, out var stored))
                return Error("InvalidPart", "One or more of the specified parts could not be found.", 400);
            if (reqEtag is not null && reqEtag.Trim('"') != stored.ETag.Trim('"'))
                return Error("InvalidPart",
                    $"One or more of the specified parts could not be found. The following part numbers are invalid: {pn}", 400);
            md5Digests.AddRange(MD5.HashData(stored.Body));
            combined.AddRange(stored.Body);
        }

        var finalMd5 = Convert.ToHexStringLower(MD5.HashData(md5Digests.ToArray()));
        var finalEtag = $"\"{finalMd5}-{orderedParts.Count}\"";

        var combinedBody = combined.ToArray();
        var obj = new S3Object
        {
            Body = combinedBody,
            ContentType = upload.ContentType,
            ContentEncoding = upload.ContentEncoding,
            ETag = finalEtag,
            LastModified = TimeHelpers.NowIso(),
            Size = combinedBody.Length,
            Metadata = upload.Metadata,
            PreservedHeaders = upload.PreservedHeaders,
        };
        bucket.Objects[key] = obj;

        _multipartUploads.TryRemove(uploadId, out _);

        var root = new XElement(Ns + "CompleteMultipartUploadResult",
            new XElement(Ns + "Location", $"http://s3.amazonaws.com/{bucketName}/{key}"),
            new XElement(Ns + "Bucket", bucketName),
            new XElement(Ns + "Key", key),
            new XElement(Ns + "ETag", finalEtag));
        return XmlOk(root);
    }

    private (int, Dictionary<string, string>, byte[]) AbortMultipartUpload(
        string bucketName, string key,
        IReadOnlyDictionary<string, string[]> queryParams)
    {
        if (!_buckets.ContainsKey(bucketName))
            return NoSuchBucket(bucketName);

        var uploadId = Qp(queryParams, "uploadId");
        if (!_multipartUploads.TryGetValue(uploadId, out var upload))
            return Error("NoSuchUpload", "The specified multipart upload does not exist.", 404, $"/{bucketName}/{key}");
        if (upload.Bucket != bucketName || upload.Key != key)
            return Error("NoSuchUpload", "The specified multipart upload does not exist.", 404, $"/{bucketName}/{key}");

        _multipartUploads.TryRemove(uploadId, out _);
        return (204, new Dictionary<string, string>(StringComparer.Ordinal), []);
    }

    private (int, Dictionary<string, string>, byte[]) ListMultipartUploads(
        string bucketName, IReadOnlyDictionary<string, string[]> queryParams)
    {
        if (!_buckets.ContainsKey(bucketName))
            return NoSuchBucket(bucketName);

        var prefix = Qp(queryParams, "prefix");
        var delimiter = Qp(queryParams, "delimiter");
        var maxUploads = int.Parse(Qp(queryParams, "max-uploads", "1000"));
        var keyMarker = Qp(queryParams, "key-marker");
        var uploadIdMarker = Qp(queryParams, "upload-id-marker");

        var root = new XElement(Ns + "ListMultipartUploadsResult",
            new XElement(Ns + "Bucket", bucketName),
            new XElement(Ns + "KeyMarker", keyMarker),
            new XElement(Ns + "UploadIdMarker", uploadIdMarker),
            new XElement(Ns + "MaxUploads", maxUploads));

        if (!string.IsNullOrEmpty(prefix))
            root.Add(new XElement(Ns + "Prefix", prefix));
        if (!string.IsNullOrEmpty(delimiter))
            root.Add(new XElement(Ns + "Delimiter", delimiter));

        var uploads = new List<(string Uid, MultipartUpload Upload)>();
        foreach (var kv in _multipartUploads.Items)
        {
            var uid = kv.Key;
            var upload = kv.Value;
            if (upload.Bucket != bucketName) continue;
            if (!string.IsNullOrEmpty(prefix) && !upload.Key.StartsWith(prefix, StringComparison.Ordinal)) continue;
            if (!string.IsNullOrEmpty(keyMarker) && string.Compare(upload.Key, keyMarker, StringComparison.Ordinal) < 0) continue;
            if (!string.IsNullOrEmpty(keyMarker) && upload.Key == keyMarker &&
                !string.IsNullOrEmpty(uploadIdMarker) && string.Compare(uid, uploadIdMarker, StringComparison.Ordinal) <= 0) continue;
            uploads.Add((uid, upload));
        }

        uploads.Sort((a, b) =>
        {
            var cmp = string.Compare(a.Upload.Key, b.Upload.Key, StringComparison.Ordinal);
            return cmp != 0 ? cmp : string.Compare(a.Uid, b.Uid, StringComparison.Ordinal);
        });

        var isTruncated = uploads.Count > maxUploads;
        root.Add(new XElement(Ns + "IsTruncated", isTruncated ? "true" : "false"));

        foreach (var (uid, upload) in uploads.Take(maxUploads))
        {
            root.Add(new XElement(Ns + "Upload",
                new XElement(Ns + "Key", upload.Key),
                new XElement(Ns + "UploadId", uid),
                new XElement(Ns + "Initiator",
                    new XElement(Ns + "ID", "owner-id"),
                    new XElement(Ns + "DisplayName", "ministack")),
                new XElement(Ns + "Owner",
                    new XElement(Ns + "ID", "owner-id"),
                    new XElement(Ns + "DisplayName", "ministack")),
                new XElement(Ns + "StorageClass", "STANDARD"),
                new XElement(Ns + "Initiated", upload.Created)));
        }

        if (isTruncated && uploads.Count > 0)
        {
            var last = uploads[maxUploads - 1];
            root.Add(new XElement(Ns + "NextKeyMarker", last.Upload.Key));
            root.Add(new XElement(Ns + "NextUploadIdMarker", last.Uid));
        }

        return XmlOk(root);
    }

    private (int, Dictionary<string, string>, byte[]) ListParts(
        string bucketName, string key,
        IReadOnlyDictionary<string, string[]> queryParams)
    {
        if (!_buckets.ContainsKey(bucketName))
            return NoSuchBucket(bucketName);

        var uploadId = Qp(queryParams, "uploadId");
        if (!_multipartUploads.TryGetValue(uploadId, out var upload))
            return Error("NoSuchUpload", "The specified multipart upload does not exist.", 404, $"/{bucketName}/{key}");
        if (upload.Bucket != bucketName || upload.Key != key)
            return Error("NoSuchUpload", "The specified multipart upload does not exist.", 404, $"/{bucketName}/{key}");

        var maxParts = int.Parse(Qp(queryParams, "max-parts", "1000"));
        var partMarker = int.Parse(Qp(queryParams, "part-number-marker", "0"));

        var root = new XElement(Ns + "ListPartsResult",
            new XElement(Ns + "Bucket", bucketName),
            new XElement(Ns + "Key", key),
            new XElement(Ns + "UploadId", uploadId),
            new XElement(Ns + "Initiator",
                new XElement(Ns + "ID", "owner-id"),
                new XElement(Ns + "DisplayName", "ministack")),
            new XElement(Ns + "Owner",
                new XElement(Ns + "ID", "owner-id"),
                new XElement(Ns + "DisplayName", "ministack")),
            new XElement(Ns + "StorageClass", "STANDARD"),
            new XElement(Ns + "PartNumberMarker", partMarker),
            new XElement(Ns + "MaxParts", maxParts));

        var sortedParts = upload.Parts.Keys
            .Where(pn => pn > partMarker)
            .OrderBy(pn => pn)
            .ToList();

        var isTruncated = sortedParts.Count > maxParts;
        root.Add(new XElement(Ns + "IsTruncated", isTruncated ? "true" : "false"));

        foreach (var pn in sortedParts.Take(maxParts))
        {
            var part = upload.Parts[pn];
            root.Add(new XElement(Ns + "Part",
                new XElement(Ns + "PartNumber", pn),
                new XElement(Ns + "LastModified", part.LastModified),
                new XElement(Ns + "ETag", part.ETag),
                new XElement(Ns + "Size", part.Size)));
        }

        if (isTruncated && sortedParts.Count > 0)
            root.Add(new XElement(Ns + "NextPartNumberMarker", sortedParts[maxParts - 1]));

        return XmlOk(root);
    }

    // ── Helpers ──────────────────────────────────────────────────────────────────

    private static (string Bucket, string Key) ParseBucketKey(
        string path, IReadOnlyDictionary<string, string> headers)
    {
        // Virtual-host style handled by middleware — here we only do path-style.
        var host = GetHeader(headers, "host") ?? "";
        var vhostMatch = Regex.Match(host, @"^([a-zA-Z0-9.\-_]+)\.s3[\.\-]");
        if (vhostMatch.Success)
        {
            var bucket = vhostMatch.Groups[1].Value;
            var key = path.TrimStart('/');
            return (bucket, key);
        }

        // Virtual-host without .s3 subdomain: {bucket}.localhost[:port]
        var vhostNoS3Match = Regex.Match(host, @"^([a-zA-Z0-9\-_]+)\.localhost(:\d+)?$");
        if (vhostNoS3Match.Success)
        {
            var bucket = vhostNoS3Match.Groups[1].Value;
            var key = path.TrimStart('/');
            return (bucket, key);
        }

        var trimmed = path.TrimStart('/');
        var parts = trimmed.Split('/', 2);
        var bucketName = parts.Length > 0 ? parts[0] : "";
        var keyName = parts.Length > 1 ? parts[1] : "";
        return (bucketName, keyName);
    }

    private static (int Start, int End)? ParseRange(string rangeHeader, int total)
    {
        var match = Regex.Match(rangeHeader, @"bytes=(\d*)-(\d*)");
        if (!match.Success) return null;

        var s = match.Groups[1].Value;
        var e = match.Groups[2].Value;

        if (string.IsNullOrEmpty(s) && string.IsNullOrEmpty(e)) return null;

        if (string.IsNullOrEmpty(s))
        {
            var suffix = int.Parse(e);
            if (suffix == 0) return null;
            var start = Math.Max(0, total - suffix);
            return (start, total - 1);
        }

        var startVal = int.Parse(s);
        if (startVal >= total) return null;
        var endVal = string.IsNullOrEmpty(e) ? total - 1 : int.Parse(e);
        endVal = Math.Min(endVal, total - 1);
        if (startVal > endVal) return null;
        return (startVal, endVal);
    }

    private static (int, Dictionary<string, string>, byte[])? ValidateContentMd5(
        IReadOnlyDictionary<string, string> headers, byte[] body)
    {
        var md5Header = GetHeader(headers, "content-md5") ?? "";
        if (string.IsNullOrEmpty(md5Header)) return null;

        byte[] expected;
        try
        {
            expected = Convert.FromBase64String(md5Header);
        }
        catch
        {
            return Error("InvalidDigest", "The Content-MD5 you specified is not valid.", 400);
        }

        var actual = MD5.HashData(body);
        if (!expected.SequenceEqual(actual))
            return Error("BadDigest", "The Content-MD5 you specified did not match what we received.", 400);
        return null;
    }

    private static bool ValidateBucketName(string name)
    {
        if (string.IsNullOrEmpty(name) || name.Length < 3 || name.Length > 63) return false;
        if (!BucketNameRegex().IsMatch(name)) return false;
        if (name.Contains("..")) return false;
        if (IpRegex().IsMatch(name)) return false;
        return true;
    }

    private static Dictionary<string, string> ParseTagsXml(byte[] body)
    {
        var xmlRoot = XElement.Parse(Encoding.UTF8.GetString(body));
        var tags = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (var el in xmlRoot.Descendants())
        {
            var local = el.Name.LocalName;
            if (local == "Tag")
            {
                string? keyText = null;
                string? valText = null;
                foreach (var child in el.Elements())
                {
                    var childLocal = child.Name.LocalName;
                    if (childLocal == "Key")
                        keyText = child.Value;
                    else if (childLocal == "Value")
                        valText = child.Value;
                }
                if (keyText is not null)
                    tags[keyText] = valText ?? "";
            }
        }
        return tags;
    }

    private static Dictionary<string, string> ExtractUserMetadata(IReadOnlyDictionary<string, string> headers)
    {
        var meta = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        foreach (var (k, v) in headers)
        {
            if (k.StartsWith("x-amz-meta-", StringComparison.OrdinalIgnoreCase))
                meta[k] = v;
        }
        return meta;
    }

    private static S3Object BuildObjectRecord(byte[] body, IReadOnlyDictionary<string, string> headers)
    {
        var contentType = GetHeader(headers, "content-type") ?? "application/octet-stream";
        var contentEncoding = GetHeader(headers, "content-encoding");
        var preserved = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        foreach (var h in PreservedHeaders)
        {
            var val = GetHeader(headers, h);
            if (val is not null)
                preserved[h] = val;
        }

        return new S3Object
        {
            Body = body,
            ContentType = contentType,
            ContentEncoding = contentEncoding,
            ETag = $"\"{HashHelpers.Md5Hash(body)}\"",
            LastModified = TimeHelpers.NowIso(),
            Size = body.Length,
            Metadata = ExtractUserMetadata(headers),
            PreservedHeaders = preserved,
        };
    }

    private Dictionary<string, string> ObjectResponseHeaders(S3Object obj, string bucketName, string key)
    {
        var h = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["Content-Type"] = obj.ContentType,
            ["ETag"] = obj.ETag,
            ["Last-Modified"] = TimeHelpers.IsoToRfc7231(obj.LastModified),
            ["Content-Length"] = obj.Size.ToString(),
            ["Accept-Ranges"] = "bytes",
        };
        if (!string.IsNullOrEmpty(obj.ContentEncoding))
            h["Content-Encoding"] = obj.ContentEncoding!;
        foreach (var (k, val) in obj.PreservedHeaders)
            h[k] = val;
        foreach (var (k, val) in obj.Metadata)
            h[k] = val;
        if (!string.IsNullOrEmpty(obj.VersionId))
            h["x-amz-version-id"] = obj.VersionId!;

        if (!string.IsNullOrEmpty(bucketName) && !string.IsNullOrEmpty(key))
        {
            if (_objectRetention.TryGetValue((bucketName, key), out var retention))
            {
                h["x-amz-object-lock-mode"] = retention.Mode;
                h["x-amz-object-lock-retain-until-date"] = retention.RetainUntilDate;
            }
            if (_objectLegalHold.TryGetValue((bucketName, key), out var hold))
            {
                h["x-amz-object-lock-legal-hold"] = hold;
            }
        }
        return h;
    }

    private (int, Dictionary<string, string>, byte[])? CheckObjectLock(
        string bucketName, string key, IReadOnlyDictionary<string, string> headers)
    {
        if (_objectLegalHold.TryGetValue((bucketName, key), out var hold) && hold == "ON")
            return Error("AccessDenied", "Access Denied because object protected by object lock.", 403);

        if (_objectRetention.TryGetValue((bucketName, key), out var retention))
        {
            var retainUntil = retention.RetainUntilDate;
            if (!string.IsNullOrEmpty(retainUntil) &&
                string.Compare(retainUntil, TimeHelpers.NowIso(), StringComparison.Ordinal) > 0)
            {
                if (retention.Mode == "COMPLIANCE")
                    return Error("AccessDenied", "Access Denied because object protected by object lock.", 403);
                if (retention.Mode == "GOVERNANCE")
                {
                    var bypass = (GetHeader(headers, "x-amz-bypass-governance-retention") ?? "").ToLowerInvariant();
                    if (bypass != "true")
                        return Error("AccessDenied", "Access Denied because object protected by object lock.", 403);
                }
            }
        }
        return null;
    }

    private void ApplyObjectLockFromHeaders(string bucketName, string key, IReadOnlyDictionary<string, string> headers)
    {
        var lockMode = GetHeader(headers, "x-amz-object-lock-mode") ?? "";
        var lockUntil = GetHeader(headers, "x-amz-object-lock-retain-until-date") ?? "";
        var lockLegal = GetHeader(headers, "x-amz-object-lock-legal-hold")
            ?? GetHeader(headers, "x-amz-object-lock-legal-hold-status")
            ?? "";

        if (!string.IsNullOrEmpty(lockMode) && !string.IsNullOrEmpty(lockUntil))
        {
            _objectRetention[(bucketName, key)] = new RetentionConfig
            {
                Mode = lockMode,
                RetainUntilDate = lockUntil,
            };
        }
        else if (string.IsNullOrEmpty(lockMode) && string.IsNullOrEmpty(lockUntil))
        {
            // Apply bucket default retention if no explicit retention
            if (_bucketObjectLock.TryGetValue(bucketName, out var lockCfg) && lockCfg.DefaultRetention is not null)
            {
                var dr = lockCfg.DefaultRetention;
                var now = DateTime.UtcNow;
                DateTime until;
                if (dr.Days.HasValue)
                    until = now.AddDays(dr.Days.Value);
                else if (dr.Years.HasValue)
                    until = now.AddYears(dr.Years.Value);
                else
                    goto SkipRetention;

                _objectRetention[(bucketName, key)] = new RetentionConfig
                {
                    Mode = dr.Mode,
                    RetainUntilDate = until.ToString("yyyy-MM-ddTHH:mm:ss.000") + "Z",
                };
            }
        }
        SkipRetention:

        if (lockLegal is "ON" or "OFF")
        {
            _objectLegalHold[(bucketName, key)] = lockLegal;
        }
    }

    private static XElement? FindXmlTag(XElement parent, string tagName)
    {
        var el = parent.Element(Ns + tagName);
        return el ?? parent.Element(tagName);
    }

    private static string? GetHeader(IReadOnlyDictionary<string, string> headers, string name)
    {
        foreach (var (k, v) in headers)
        {
            if (string.Equals(k, name, StringComparison.OrdinalIgnoreCase))
                return v;
        }
        return null;
    }

    private static string Qp(IReadOnlyDictionary<string, string[]> queryParams, string key)
    {
        return Qp(queryParams, key, "");
    }

    private static string Qp(IReadOnlyDictionary<string, string[]> queryParams, string key, string defaultValue)
    {
        if (queryParams.TryGetValue(key, out var vals) && vals.Length > 0)
            return vals[0];
        return defaultValue;
    }

    private static Dictionary<string, string> ParseQueryString(string qs)
    {
        var result = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (var pair in qs.Split('&', StringSplitOptions.RemoveEmptyEntries))
        {
            var eq = pair.IndexOf('=');
            if (eq < 0) continue;
            var key = Uri.UnescapeDataString(pair[..eq]);
            var val = Uri.UnescapeDataString(pair[(eq + 1)..]);
            result[key] = val;
        }
        return result;
    }

    private static (int, Dictionary<string, string>, byte[]) NoSuchBucket(string name)
    {
        return Error("NoSuchBucket", "The specified bucket does not exist", 404, $"/{name}");
    }

    private static (int, Dictionary<string, string>, byte[]) Error(
        string code, string message, int status)
    {
        return Error(code, message, status, "");
    }

    private static (int, Dictionary<string, string>, byte[]) Error(
        string code, string message, int status, string resource)
    {
        var root = new XElement("Error",
            new XElement("Code", code),
            new XElement("Message", message),
            new XElement("Resource", resource),
            new XElement("RequestId", HashHelpers.NewUuid()));
        return (status, new Dictionary<string, string>(StringComparer.Ordinal)
            { ["Content-Type"] = "application/xml" }, XmlBody(root));
    }

    private static byte[] XmlBody(XElement root)
    {
        return Encoding.UTF8.GetBytes(XmlDecl + root.ToString(SaveOptions.DisableFormatting));
    }

    private static (int, Dictionary<string, string>, byte[]) XmlOk(XElement root)
    {
        return (200, new Dictionary<string, string>(StringComparer.Ordinal)
            { ["Content-Type"] = "application/xml" }, XmlBody(root));
    }

    // ── Internal types ───────────────────────────────────────────────────────────

    private sealed class S3Bucket
    {
        internal string Created { get; init; } = "";
        internal string? Region { get; init; }
        internal Dictionary<string, S3Object> Objects { get; init; } = new(StringComparer.Ordinal);
        internal byte[]? OwnershipControls { get; set; }
        internal byte[]? PublicAccessBlock { get; set; }
    }

    private sealed class S3Object
    {
        internal byte[] Body { get; init; } = [];
        internal string ContentType { get; init; } = "application/octet-stream";
        internal string? ContentEncoding { get; init; }
        internal string ETag { get; init; } = "";
        internal string LastModified { get; init; } = "";
        internal long Size { get; init; }
        internal Dictionary<string, string> Metadata { get; init; } = new(StringComparer.OrdinalIgnoreCase);
        internal Dictionary<string, string> PreservedHeaders { get; init; } = new(StringComparer.OrdinalIgnoreCase);
        internal string? VersionId { get; set; }
    }

    private sealed class ObjectLockConfig
    {
        internal bool Enabled { get; init; }
        internal DefaultRetention? DefaultRetention { get; init; }
    }

    private sealed class DefaultRetention
    {
        internal string Mode { get; init; } = "";
        internal int? Days { get; set; }
        internal int? Years { get; set; }
    }

    private sealed class RetentionConfig
    {
        internal string Mode { get; init; } = "";
        internal string RetainUntilDate { get; init; } = "";
    }

    private sealed class ReplicationConfig
    {
        internal string Role { get; init; } = "";
        internal List<ReplicationRule> Rules { get; init; } = [];
    }

    private sealed class ReplicationRule
    {
        internal string Id { get; set; } = "";
        internal string Status { get; set; } = "Enabled";
        internal string? Prefix { get; set; }
        internal ReplicationDestination? Destination { get; set; }
    }

    private sealed class ReplicationDestination
    {
        internal string? Bucket { get; set; }
        internal string? StorageClass { get; set; }
    }

    private sealed class MultipartUpload
    {
        internal string Bucket { get; init; } = "";
        internal string Key { get; init; } = "";
        internal Dictionary<int, MultipartPart> Parts { get; init; } = [];
        internal Dictionary<string, string> Metadata { get; init; } = new(StringComparer.OrdinalIgnoreCase);
        internal string ContentType { get; init; } = "application/octet-stream";
        internal string? ContentEncoding { get; init; }
        internal Dictionary<string, string> PreservedHeaders { get; init; } = new(StringComparer.OrdinalIgnoreCase);
        internal string Created { get; init; } = "";
    }

    private sealed class MultipartPart
    {
        internal byte[] Body { get; init; } = [];
        internal string ETag { get; init; } = "";
        internal long Size { get; init; }
        internal string LastModified { get; init; } = "";
    }
}
