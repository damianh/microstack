using System.Text;
using System.Text.RegularExpressions;
using System.Xml.Linq;
using MicroStack.Internal;

namespace MicroStack.Services.CloudFront;

/// <summary>
/// CloudFront service handler -- REST/XML protocol with path-based routing under /2020-05-31/.
///
/// Port of ministack/services/cloudfront.py.
///
/// Supports:
///   Distributions: CreateDistribution, GetDistribution, GetDistributionConfig,
///                  ListDistributions, UpdateDistribution, DeleteDistribution
///   Invalidations: CreateInvalidation, ListInvalidations, GetInvalidation
///   Tags:          TagResource, ListTagsForResource, UntagResource
/// </summary>
internal sealed partial class CloudFrontServiceHandler : IServiceHandler
{
    private readonly Lock _lock = new();

    private const string CfNs = "http://cloudfront.amazonaws.com/doc/2020-05-31/";
    private static readonly XNamespace Ns = CfNs;
    private const string XmlDecl = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n";

    private static readonly char[] IdChars =
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789".ToCharArray();

    // ── Path regexes ─────────────────────────────────────────────────────────
    [GeneratedRegex(@"^/2020-05-31/distribution/?$")]
    private static partial Regex DistRegex();

    [GeneratedRegex(@"^/2020-05-31/distribution/([^/]+)/config$")]
    private static partial Regex DistCfgRegex();

    [GeneratedRegex(@"^/2020-05-31/distribution/([^/]+)/?$")]
    private static partial Regex DistIdRegex();

    [GeneratedRegex(@"^/2020-05-31/distribution/([^/]+)/invalidation/?$")]
    private static partial Regex InvRegex();

    [GeneratedRegex(@"^/2020-05-31/distribution/([^/]+)/invalidation/([^/]+)$")]
    private static partial Regex InvIdRegex();

    [GeneratedRegex(@"^/2020-05-31/tagging/?$")]
    private static partial Regex TagRegex();

    // ── State ────────────────────────────────────────────────────────────────
    private readonly AccountScopedDictionary<string, CfDistribution> _distributions = new();
    private readonly AccountScopedDictionary<string, List<CfInvalidation>> _invalidations = new();
    private readonly AccountScopedDictionary<string, List<CfTag>> _tags = new();

    // ── IServiceHandler ──────────────────────────────────────────────────────

    public string ServiceName => "cloudfront";

    public Task<ServiceResponse> HandleAsync(ServiceRequest request)
    {
        var (status, headers, body) = HandleRequest(request);
        return Task.FromResult(new ServiceResponse(status, headers, body));
    }

    public void Reset()
    {
        lock (_lock)
        {
            _distributions.Clear();
            _invalidations.Clear();
            _tags.Clear();
        }
    }

    public object? GetState() => null;

    public void RestoreState(object state) { }

    // ── Request router ──────────────────────────────────────────────────────

    private (int Status, Dictionary<string, string> Headers, byte[] Body) HandleRequest(ServiceRequest request)
    {
        var path = request.Path;
        var method = request.Method;

        var m = DistRegex().Match(path);
        if (m.Success)
        {
            if (method == "POST")
            {
                return CreateDistribution(request.Body);
            }

            if (method == "GET")
            {
                return ListDistributions();
            }
        }

        m = DistCfgRegex().Match(path);
        if (m.Success)
        {
            var distId = m.Groups[1].Value;
            if (method == "GET")
            {
                return GetDistributionConfig(distId);
            }

            if (method == "PUT")
            {
                return UpdateDistribution(distId, request.Headers, request.Body);
            }
        }

        m = DistIdRegex().Match(path);
        if (m.Success)
        {
            var distId = m.Groups[1].Value;
            if (method == "GET")
            {
                return GetDistribution(distId);
            }

            if (method == "DELETE")
            {
                return DeleteDistribution(distId, request.Headers);
            }
        }

        m = InvRegex().Match(path);
        if (m.Success)
        {
            var distId = m.Groups[1].Value;
            if (method == "POST")
            {
                return CreateInvalidation(distId, request.Body);
            }

            if (method == "GET")
            {
                return ListInvalidations(distId);
            }
        }

        m = InvIdRegex().Match(path);
        if (m.Success)
        {
            var distId = m.Groups[1].Value;
            var invId = m.Groups[2].Value;
            if (method == "GET")
            {
                return GetInvalidation(distId, invId);
            }
        }

        m = TagRegex().Match(path);
        if (m.Success)
        {
            var resource = request.GetQueryParam("Resource") ?? "";
            var operation = request.GetQueryParam("Operation") ?? "";
            if (method == "GET")
            {
                return ListTags(resource);
            }

            if (method == "POST" && operation == "Tag")
            {
                return TagResource(resource, request.Body);
            }

            if (method == "POST" && operation == "Untag")
            {
                return UntagResource(resource, request.Body);
            }
        }

        return ErrorResponse("NoSuchResource", $"No route for {method} {path}", 404);
    }

    // ── ID generators ────────────────────────────────────────────────────────

    private static string GenerateDistId()
    {
        return "E" + GenerateRandomId(13);
    }

    private static string GenerateInvId()
    {
        return "I" + GenerateRandomId(13);
    }

    private static string GenerateRandomId(int length)
    {
        return string.Create(length, (object?)null, static (span, _) =>
        {
            for (var i = 0; i < span.Length; i++)
            {
                span[i] = IdChars[Random.Shared.Next(IdChars.Length)];
            }
        });
    }

    // ── XML helpers ──────────────────────────────────────────────────────────

    private static (int, Dictionary<string, string>, byte[]) XmlResponse(
        string rootTag, Action<XElement> builder, int status = 200, Dictionary<string, string>? extraHeaders = null)
    {
        var root = new XElement(Ns + rootTag);
        builder(root);
        var body = XmlDecl + root.ToString(SaveOptions.DisableFormatting);
        var headers = new Dictionary<string, string>(StringComparer.Ordinal) { ["Content-Type"] = "text/xml" };
        if (extraHeaders is not null)
        {
            foreach (var kv in extraHeaders)
            {
                headers[kv.Key] = kv.Value;
            }
        }

        return (status, headers, Encoding.UTF8.GetBytes(body));
    }

    private static (int, Dictionary<string, string>, byte[]) ErrorResponse(string code, string message, int status)
    {
        var root = new XElement(Ns + "ErrorResponse",
            new XElement(Ns + "Error",
                new XElement(Ns + "Code", code),
                new XElement(Ns + "Message", message)),
            new XElement(Ns + "RequestId", HashHelpers.NewUuid()));
        var body = XmlDecl + root.ToString(SaveOptions.DisableFormatting);
        return (status, new Dictionary<string, string>(StringComparer.Ordinal) { ["Content-Type"] = "text/xml" }, Encoding.UTF8.GetBytes(body));
    }

    private static XDocument? ParseBody(byte[] body)
    {
        if (body.Length == 0)
        {
            return null;
        }

        try
        {
            return XDocument.Parse(Encoding.UTF8.GetString(body));
        }
        catch
        {
            return null;
        }
    }

    private static XElement? FindChild(XElement parent, string localName)
    {
        return parent.Elements().FirstOrDefault(e => e.Name.LocalName == localName);
    }

    private static string GetChildText(XElement parent, string localName, string defaultValue = "")
    {
        var child = FindChild(parent, localName);
        return child?.Value ?? defaultValue;
    }

    private static bool GetEnabled(XElement configEl)
    {
        var val = GetChildText(configEl, "Enabled", "true");
        return !string.Equals(val.Trim(), "false", StringComparison.OrdinalIgnoreCase);
    }

    private static void BuildDistributionXml(XElement parent, CfDistribution dist)
    {
        parent.Add(new XElement(Ns + "Id", dist.Id));
        parent.Add(new XElement(Ns + "ARN", dist.Arn));
        parent.Add(new XElement(Ns + "Status", dist.Status));
        parent.Add(new XElement(Ns + "LastModifiedTime", dist.LastModifiedTime));
        parent.Add(new XElement(Ns + "InProgressInvalidationBatches", "0"));
        parent.Add(new XElement(Ns + "DomainName", dist.DomainName));
        // Re-parse and embed stored config XML
        var configEl = XDocument.Parse(dist.ConfigXml).Root!;
        configEl.Name = Ns + "DistributionConfig";
        parent.Add(configEl);
    }

    private static void BuildInvalidationXml(XElement parent, CfInvalidation inv)
    {
        parent.Add(new XElement(Ns + "Id", inv.Id));
        parent.Add(new XElement(Ns + "Status", inv.Status));
        parent.Add(new XElement(Ns + "CreateTime", inv.CreateTime));
        var batch = new XElement(Ns + "InvalidationBatch");
        var pathsEl = new XElement(Ns + "Paths");
        pathsEl.Add(new XElement(Ns + "Quantity", inv.PathItems.Count.ToString()));
        var itemsEl = new XElement(Ns + "Items");
        foreach (var p in inv.PathItems)
        {
            itemsEl.Add(new XElement(Ns + "Path", p));
        }

        pathsEl.Add(itemsEl);
        batch.Add(pathsEl);
        batch.Add(new XElement(Ns + "CallerReference", inv.CallerReference));
        parent.Add(batch);
    }

    // ── Distribution handlers ───────────────────────────────────────────────

    private (int, Dictionary<string, string>, byte[]) CreateDistribution(byte[] body)
    {
        var doc = ParseBody(body);
        if (doc?.Root is null)
        {
            return ErrorResponse("MalformedXML", "The XML document is malformed.", 400);
        }

        var configEl = doc.Root;
        if (string.IsNullOrEmpty(GetChildText(configEl, "CallerReference")))
        {
            return ErrorResponse("InvalidArgument", "CallerReference is required.", 400);
        }

        if (FindChild(configEl, "Origins") is null)
        {
            return ErrorResponse("InvalidArgument", "Origins is required.", 400);
        }

        if (FindChild(configEl, "DefaultCacheBehavior") is null)
        {
            return ErrorResponse("InvalidArgument", "DefaultCacheBehavior is required.", 400);
        }

        lock (_lock)
        {
            var distId = GenerateDistId();
            var etag = HashHelpers.NewUuid();
            var now = TimeHelpers.NowIso();

            var dist = new CfDistribution
            {
                Id = distId,
                Arn = $"arn:aws:cloudfront::{AccountContext.GetAccountId()}:distribution/{distId}",
                Status = "Deployed",
                DomainName = $"{distId}.cloudfront.net",
                LastModifiedTime = now,
                ETag = etag,
                ConfigXml = configEl.ToString(SaveOptions.DisableFormatting),
                Enabled = GetEnabled(configEl),
            };

            _distributions[distId] = dist;
            _invalidations[distId] = [];

            return XmlResponse("Distribution", root => BuildDistributionXml(root, dist),
                status: 201,
                extraHeaders: new Dictionary<string, string>(StringComparer.Ordinal)
                {
                    ["ETag"] = etag,
                    ["Location"] = $"/2020-05-31/distribution/{distId}",
                });
        }
    }

    private (int, Dictionary<string, string>, byte[]) GetDistribution(string distId)
    {
        lock (_lock)
        {
            if (!_distributions.TryGetValue(distId, out var dist))
            {
                return ErrorResponse("NoSuchDistribution", "The specified distribution does not exist.", 404);
            }

            return XmlResponse("Distribution", root => BuildDistributionXml(root, dist),
                extraHeaders: new Dictionary<string, string>(StringComparer.Ordinal) { ["ETag"] = dist.ETag });
        }
    }

    private (int, Dictionary<string, string>, byte[]) GetDistributionConfig(string distId)
    {
        lock (_lock)
        {
            if (!_distributions.TryGetValue(distId, out var dist))
            {
                return ErrorResponse("NoSuchDistribution", "The specified distribution does not exist.", 404);
            }

            var configEl = XDocument.Parse(dist.ConfigXml).Root!;
            configEl.Name = Ns + "DistributionConfig";
            var bodyStr = XmlDecl + configEl.ToString(SaveOptions.DisableFormatting);
            return (200, new Dictionary<string, string>(StringComparer.Ordinal) { ["Content-Type"] = "text/xml", ["ETag"] = dist.ETag },
                Encoding.UTF8.GetBytes(bodyStr));
        }
    }

    private (int, Dictionary<string, string>, byte[]) ListDistributions()
    {
        lock (_lock)
        {
            var items = _distributions.Values.ToList();
            return XmlResponse("DistributionList", root =>
            {
                root.Add(new XElement(Ns + "Marker", ""));
                root.Add(new XElement(Ns + "MaxItems", "100"));
                root.Add(new XElement(Ns + "IsTruncated", "false"));
                root.Add(new XElement(Ns + "Quantity", items.Count.ToString()));
                if (items.Count > 0)
                {
                    var itemsEl = new XElement(Ns + "Items");
                    foreach (var dist in items)
                    {
                        var ds = new XElement(Ns + "DistributionSummary");
                        ds.Add(new XElement(Ns + "Id", dist.Id));
                        ds.Add(new XElement(Ns + "ARN", dist.Arn));
                        ds.Add(new XElement(Ns + "Status", dist.Status));
                        ds.Add(new XElement(Ns + "LastModifiedTime", dist.LastModifiedTime));
                        ds.Add(new XElement(Ns + "DomainName", dist.DomainName));
                        ds.Add(new XElement(Ns + "Enabled", dist.Enabled ? "true" : "false"));
                        var cfgDoc = XDocument.Parse(dist.ConfigXml);
                        ds.Add(new XElement(Ns + "Comment", cfgDoc.Root is not null ? GetChildText(cfgDoc.Root, "Comment") : ""));
                        itemsEl.Add(ds);
                    }

                    root.Add(itemsEl);
                }
            });
        }
    }

    private (int, Dictionary<string, string>, byte[]) UpdateDistribution(
        string distId, IReadOnlyDictionary<string, string> headers, byte[] body)
    {
        lock (_lock)
        {
            if (!_distributions.TryGetValue(distId, out var dist))
            {
                return ErrorResponse("NoSuchDistribution", "The specified distribution does not exist.", 404);
            }

            var ifMatch = "";
            foreach (var kv in headers)
            {
                if (string.Equals(kv.Key, "if-match", StringComparison.OrdinalIgnoreCase))
                {
                    ifMatch = kv.Value;
                    break;
                }
            }

            if (string.IsNullOrEmpty(ifMatch))
            {
                return ErrorResponse("InvalidIfMatchVersion",
                    "The If-Match version is missing or not valid for the resource.", 400);
            }

            if (ifMatch != dist.ETag)
            {
                return ErrorResponse("PreconditionFailed",
                    "The precondition given in one or more of the request-header fields evaluated to false.", 412);
            }

            var doc = ParseBody(body);
            if (doc?.Root is null)
            {
                return ErrorResponse("MalformedXML", "The XML document is malformed.", 400);
            }

            var newEtag = HashHelpers.NewUuid();
            dist.ConfigXml = doc.Root.ToString(SaveOptions.DisableFormatting);
            dist.Enabled = GetEnabled(doc.Root);
            dist.ETag = newEtag;
            dist.LastModifiedTime = TimeHelpers.NowIso();

            return XmlResponse("Distribution", root => BuildDistributionXml(root, dist),
                extraHeaders: new Dictionary<string, string>(StringComparer.Ordinal) { ["ETag"] = newEtag });
        }
    }

    private (int, Dictionary<string, string>, byte[]) DeleteDistribution(
        string distId, IReadOnlyDictionary<string, string> headers)
    {
        lock (_lock)
        {
            if (!_distributions.TryGetValue(distId, out var dist))
            {
                return ErrorResponse("NoSuchDistribution", "The specified distribution does not exist.", 404);
            }

            var ifMatch = "";
            foreach (var kv in headers)
            {
                if (string.Equals(kv.Key, "if-match", StringComparison.OrdinalIgnoreCase))
                {
                    ifMatch = kv.Value;
                    break;
                }
            }

            if (string.IsNullOrEmpty(ifMatch))
            {
                return ErrorResponse("InvalidIfMatchVersion",
                    "The If-Match version is missing or not valid for the resource.", 400);
            }

            if (ifMatch != dist.ETag)
            {
                return ErrorResponse("PreconditionFailed",
                    "The precondition given in one or more of the request-header fields evaluated to false.", 412);
            }

            if (dist.Enabled)
            {
                return ErrorResponse("DistributionNotDisabled",
                    "The distribution you are trying to delete has not been disabled.", 409);
            }

            _distributions.TryRemove(distId, out _);
            _invalidations.TryRemove(distId, out _);

            return (204, new Dictionary<string, string>(StringComparer.Ordinal), []);
        }
    }

    // ── Invalidation handlers ───────────────────────────────────────────────

    private (int, Dictionary<string, string>, byte[]) CreateInvalidation(string distId, byte[] body)
    {
        lock (_lock)
        {
            if (!_distributions.ContainsKey(distId))
            {
                return ErrorResponse("NoSuchDistribution", "The specified distribution does not exist.", 404);
            }

            var doc = ParseBody(body);
            if (doc?.Root is null)
            {
                return ErrorResponse("MalformedXML", "The XML document is malformed.", 400);
            }

            var batchEl = doc.Root;
            var callerRef = GetChildText(batchEl, "CallerReference");
            var pathItems = new List<string>();
            var pathsEl = FindChild(batchEl, "Paths");
            if (pathsEl is not null)
            {
                var itemsEl = FindChild(pathsEl, "Items");
                if (itemsEl is not null)
                {
                    foreach (var child in itemsEl.Elements())
                    {
                        if (!string.IsNullOrEmpty(child.Value))
                        {
                            pathItems.Add(child.Value);
                        }
                    }
                }
            }

            var invId = GenerateInvId();
            var now = TimeHelpers.NowIso();
            var inv = new CfInvalidation
            {
                Id = invId,
                Status = "Completed",
                CreateTime = now,
                PathItems = pathItems,
                CallerReference = callerRef,
            };

            if (!_invalidations.TryGetValue(distId, out var invList))
            {
                invList = [];
                _invalidations[distId] = invList;
            }

            invList.Add(inv);

            return XmlResponse("Invalidation", root => BuildInvalidationXml(root, inv),
                status: 201,
                extraHeaders: new Dictionary<string, string>(StringComparer.Ordinal)
                {
                    ["Location"] = $"/2020-05-31/distribution/{distId}/invalidation/{invId}",
                });
        }
    }

    private (int, Dictionary<string, string>, byte[]) ListInvalidations(string distId)
    {
        lock (_lock)
        {
            if (!_distributions.ContainsKey(distId))
            {
                return ErrorResponse("NoSuchDistribution", "The specified distribution does not exist.", 404);
            }

            var invs = _invalidations.TryGetValue(distId, out var list) ? list : [];
            return XmlResponse("InvalidationList", root =>
            {
                root.Add(new XElement(Ns + "Marker", ""));
                root.Add(new XElement(Ns + "MaxItems", "100"));
                root.Add(new XElement(Ns + "IsTruncated", "false"));
                root.Add(new XElement(Ns + "Quantity", invs.Count.ToString()));
                if (invs.Count > 0)
                {
                    var itemsEl = new XElement(Ns + "Items");
                    foreach (var inv in invs)
                    {
                        var summary = new XElement(Ns + "InvalidationSummary");
                        summary.Add(new XElement(Ns + "Id", inv.Id));
                        summary.Add(new XElement(Ns + "Status", inv.Status));
                        summary.Add(new XElement(Ns + "CreateTime", inv.CreateTime));
                        itemsEl.Add(summary);
                    }

                    root.Add(itemsEl);
                }
            });
        }
    }

    private (int, Dictionary<string, string>, byte[]) GetInvalidation(string distId, string invId)
    {
        lock (_lock)
        {
            if (!_distributions.ContainsKey(distId))
            {
                return ErrorResponse("NoSuchDistribution", "The specified distribution does not exist.", 404);
            }

            var invs = _invalidations.TryGetValue(distId, out var list) ? list : [];
            var inv = invs.Find(i => i.Id == invId);
            if (inv is null)
            {
                return ErrorResponse("NoSuchInvalidation", "The specified invalidation does not exist.", 404);
            }

            return XmlResponse("Invalidation", root => BuildInvalidationXml(root, inv));
        }
    }

    // ── Tagging ─────────────────────────────────────────────────────────────

    private (int, Dictionary<string, string>, byte[]) ListTags(string resourceArn)
    {
        lock (_lock)
        {
            var tags = _tags.TryGetValue(resourceArn, out var t) ? t : [];
            var root = new XElement(Ns + "Tags");
            var itemsEl = new XElement(Ns + "Items");
            foreach (var tag in tags)
            {
                var tagEl = new XElement(Ns + "Tag");
                tagEl.Add(new XElement(Ns + "Key", tag.Key));
                tagEl.Add(new XElement(Ns + "Value", tag.Value));
                itemsEl.Add(tagEl);
            }

            root.Add(itemsEl);
            var bodyStr = XmlDecl + root.ToString(SaveOptions.DisableFormatting);
            return (200, new Dictionary<string, string>(StringComparer.Ordinal) { ["Content-Type"] = "application/xml" },
                Encoding.UTF8.GetBytes(bodyStr));
        }
    }

    private (int, Dictionary<string, string>, byte[]) TagResource(string resourceArn, byte[] body)
    {
        lock (_lock)
        {
            var doc = ParseBody(body);
            if (doc?.Root is null)
            {
                return (204, new Dictionary<string, string>(StringComparer.Ordinal), []);
            }

            var itemsContainer = FindChild(doc.Root, "Items") ?? FindChild(doc.Root, "Tags") ?? doc.Root;
            var existing = new Dictionary<string, CfTag>(StringComparer.Ordinal);
            if (_tags.TryGetValue(resourceArn, out var tagList))
            {
                foreach (var t in tagList)
                {
                    existing[t.Key] = t;
                }
            }

            foreach (var tagEl in itemsContainer.Elements())
            {
                var local = tagEl.Name.LocalName;
                if (local == "Tag")
                {
                    var key = GetChildText(tagEl, "Key");
                    var val = GetChildText(tagEl, "Value");
                    if (!string.IsNullOrEmpty(key))
                    {
                        existing[key] = new CfTag { Key = key, Value = val };
                    }
                }
            }

            _tags[resourceArn] = existing.Values.ToList();
            return (204, new Dictionary<string, string>(StringComparer.Ordinal), []);
        }
    }

    private (int, Dictionary<string, string>, byte[]) UntagResource(string resourceArn, byte[] body)
    {
        lock (_lock)
        {
            var doc = ParseBody(body);
            if (doc?.Root is null)
            {
                return (204, new Dictionary<string, string>(StringComparer.Ordinal), []);
            }

            var itemsContainer = FindChild(doc.Root, "Items") ?? FindChild(doc.Root, "Keys") ?? doc.Root;
            var removeKeys = new HashSet<string>(StringComparer.Ordinal);
            foreach (var child in itemsContainer.Elements())
            {
                if (child.Name.LocalName == "Key" && !string.IsNullOrEmpty(child.Value))
                {
                    removeKeys.Add(child.Value);
                }
            }

            if (_tags.TryGetValue(resourceArn, out var tagList))
            {
                _tags[resourceArn] = tagList.Where(t => !removeKeys.Contains(t.Key)).ToList();
            }

            return (204, new Dictionary<string, string>(StringComparer.Ordinal), []);
        }
    }

    // ── Data models ─────────────────────────────────────────────────────────

    private sealed class CfDistribution
    {
        internal string Id { get; init; } = "";
        internal string Arn { get; init; } = "";
        internal string Status { get; init; } = "";
        internal string DomainName { get; init; } = "";
        internal string LastModifiedTime { get; set; } = "";
        internal string ETag { get; set; } = "";
        internal string ConfigXml { get; set; } = "";
        internal bool Enabled { get; set; }
    }

    private sealed class CfInvalidation
    {
        internal string Id { get; init; } = "";
        internal string Status { get; init; } = "";
        internal string CreateTime { get; init; } = "";
        internal List<string> PathItems { get; init; } = [];
        internal string CallerReference { get; init; } = "";
    }

    private sealed class CfTag
    {
        internal string Key { get; init; } = "";
        internal string Value { get; init; } = "";
    }
}
