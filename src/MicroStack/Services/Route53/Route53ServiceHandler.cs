using System.Text;
using System.Text.RegularExpressions;
using System.Xml.Linq;
using MicroStack.Internal;

namespace MicroStack.Services.Route53;

/// <summary>
/// Route53 service handler — REST/XML protocol with path-based routing.
///
/// Port of ministack/services/route53.py.
/// </summary>
internal sealed partial class Route53ServiceHandler : IServiceHandler
{
    // ── Constants ────────────────────────────────────────────────────────────────

    private const string R53Ns = "https://route53.amazonaws.com/doc/2013-04-01/";
    private static readonly XNamespace Ns = R53Ns;
    private const string XmlDecl = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n";
    private const string ApiVersion = "2013-04-01";

    private static readonly string[] DefaultNameServers =
    [
        "ns-1.awsdns-1.com.",
        "ns-2.awsdns-2.net.",
        "ns-3.awsdns-3.org.",
        "ns-4.awsdns-4.co.uk.",
    ];

    private static readonly char[] IdChars =
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789".ToCharArray();

    // ── Compiled regex patterns ──────────────────────────────────────────────────

    [GeneratedRegex(@"^/hostedzone/([^/]+)$")]
    private static partial Regex HostedZoneIdRegex();

    [GeneratedRegex(@"^/hostedzone/([^/]+)/rrset/?$")]
    private static partial Regex RrsetRegex();

    [GeneratedRegex(@"^/change/([^/]+)$")]
    private static partial Regex ChangeIdRegex();

    [GeneratedRegex(@"^/healthcheck/([^/]+)$")]
    private static partial Regex HealthCheckIdRegex();

    [GeneratedRegex(@"^/tags/([^/]+)/([^/]+)$")]
    private static partial Regex TagsRegex();

    // ── State ────────────────────────────────────────────────────────────────────

    private readonly AccountScopedDictionary<string, HostedZone> _zones = new();
    private readonly AccountScopedDictionary<string, List<RecordSet>> _records = new();
    private readonly AccountScopedDictionary<string, ChangeInfo> _changes = new();
    private readonly AccountScopedDictionary<string, HealthCheck> _healthChecks = new();
    private readonly AccountScopedDictionary<(string ResourceType, string ResourceId), Dictionary<string, string>> _tags = new();
    private readonly AccountScopedDictionary<string, string> _callerRefs = new();
    private readonly AccountScopedDictionary<string, string> _hcCallerRefs = new();
    private readonly Lock _lock = new();

    // ── IServiceHandler ──────────────────────────────────────────────────────────

    public string ServiceName => "route53";

    public Task<ServiceResponse> HandleAsync(ServiceRequest request)
    {
        var (status, headers, body) = HandleRequest(request);
        headers.TryAdd("x-amz-request-id", HashHelpers.NewUuid());
        return Task.FromResult(new ServiceResponse(status, headers, body));
    }

    public void Reset()
    {
        lock (_lock)
        {
            _zones.Clear();
            _records.Clear();
            _changes.Clear();
            _healthChecks.Clear();
            _tags.Clear();
            _callerRefs.Clear();
            _hcCallerRefs.Clear();
        }
    }

    public object? GetState() => null;

    public void RestoreState(object state)
    {
        // Not implementing restore in Phase 1.
    }

    // ── Request router ───────────────────────────────────────────────────────────

    private (int Status, Dictionary<string, string> Headers, byte[] Body) HandleRequest(ServiceRequest request)
    {
        var path = request.Path;
        var prefix = $"/{ApiVersion}";
        if (path.StartsWith(prefix, StringComparison.Ordinal))
        {
            path = path[prefix.Length..];
        }

        var method = request.Method;

        // POST /hostedzone
        if (method == "POST" && path == "/hostedzone")
        {
            return CreateHostedZone(request.Body);
        }

        // GET /hostedzone
        if (method == "GET" && path == "/hostedzone")
        {
            return ListHostedZones(request.QueryParams);
        }

        // GET /hostedzonesbyname
        if (method == "GET" && path == "/hostedzonesbyname")
        {
            return ListHostedZonesByName(request.QueryParams);
        }

        // GET /hostedzonecount
        if (method == "GET" && path == "/hostedzonecount")
        {
            return GetHostedZoneCount();
        }

        // GET|DELETE|POST /hostedzone/{id}
        var m = HostedZoneIdRegex().Match(path);
        if (m.Success)
        {
            var zoneId = m.Groups[1].Value;
            if (method == "GET")
                return GetHostedZone(zoneId);
            if (method == "DELETE")
                return DeleteHostedZone(zoneId);
            if (method == "POST")
                return UpdateHostedZoneComment(zoneId, request.Body);
        }

        // POST|GET /hostedzone/{id}/rrset
        m = RrsetRegex().Match(path);
        if (m.Success)
        {
            var zoneId = m.Groups[1].Value;
            if (method == "POST")
                return ChangeResourceRecordSets(zoneId, request.Body);
            if (method == "GET")
                return ListResourceRecordSets(zoneId, request.QueryParams);
        }

        // GET /change/{id}
        m = ChangeIdRegex().Match(path);
        if (m.Success && method == "GET")
        {
            return GetChange(m.Groups[1].Value);
        }

        // POST /healthcheck
        if (method == "POST" && path == "/healthcheck")
        {
            return CreateHealthCheck(request.Body);
        }

        // GET /healthcheck
        if (method == "GET" && path == "/healthcheck")
        {
            return ListHealthChecks(request.QueryParams);
        }

        // GET|DELETE|POST /healthcheck/{id}
        m = HealthCheckIdRegex().Match(path);
        if (m.Success)
        {
            var hcId = m.Groups[1].Value;
            if (method == "GET")
                return GetHealthCheck(hcId);
            if (method == "DELETE")
                return DeleteHealthCheck(hcId);
            if (method == "POST")
                return UpdateHealthCheck(hcId, request.Body);
        }

        // POST|GET /tags/{resourceType}/{resourceId}
        m = TagsRegex().Match(path);
        if (m.Success)
        {
            var resourceType = m.Groups[1].Value;
            var resourceId = m.Groups[2].Value;
            if (method == "POST")
                return ChangeTagsForResource(resourceType, resourceId, request.Body);
            if (method == "GET")
                return ListTagsForResource(resourceType, resourceId);
        }

        return ErrorResponse("InvalidInput", $"Unknown Route53 endpoint: {request.Method} {request.Path}", 400);
    }

    // ── ID generators ────────────────────────────────────────────────────────────

    private static string GenerateZoneId()
    {
        return "Z" + GenerateRandomId(13);
    }

    private static string GenerateChangeId()
    {
        return "C" + GenerateRandomId(13);
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

    // ── Domain name helpers ──────────────────────────────────────────────────────

    private static string NormaliseName(string name)
    {
        if (string.IsNullOrEmpty(name))
            return name;
        return name.EndsWith('.') ? name : name + ".";
    }

    private static string[] NameSortKey(string name)
    {
        var normalized = NormaliseName(name).TrimEnd('.');
        var labels = normalized.Split('.');
        Array.Reverse(labels);
        return labels;
    }

    private static int CompareNameSortKeys(string[] a, string[] b)
    {
        var minLen = Math.Min(a.Length, b.Length);
        for (var i = 0; i < minLen; i++)
        {
            var cmp = string.Compare(a[i], b[i], StringComparison.Ordinal);
            if (cmp != 0)
                return cmp;
        }
        return a.Length.CompareTo(b.Length);
    }

    // ── Default records ──────────────────────────────────────────────────────────

    private static List<RecordSet> DefaultRecords(string zoneName)
    {
        return
        [
            new RecordSet
            {
                Name = zoneName,
                Type = "SOA",
                Ttl = "900",
                ResourceRecords =
                [
                    $"{DefaultNameServers[0]} awsdns-hostmaster.amazon.com. 1 7200 900 1209600 86400",
                ],
            },
            new RecordSet
            {
                Name = zoneName,
                Type = "NS",
                Ttl = "172800",
                ResourceRecords = [.. DefaultNameServers],
            },
        ];
    }

    // ── Record set key ───────────────────────────────────────────────────────────

    private static (string Name, string Type, string SetId) RsKey(RecordSet rs)
    {
        return (rs.Name, rs.Type, rs.SetIdentifier ?? "");
    }

    // ── XML helpers ──────────────────────────────────────────────────────────────

    private static (int, Dictionary<string, string>, byte[]) XmlResponse(
        string rootTag, Action<XElement> builder, int status = 200)
    {
        var root = new XElement(Ns + rootTag);
        builder(root);
        var body = XmlDecl + root.ToString(SaveOptions.DisableFormatting);
        return (status, XmlHeaders(), Encoding.UTF8.GetBytes(body));
    }

    private static (int, Dictionary<string, string>, byte[]) ErrorResponse(
        string code, string message, int status = 400)
    {
        var root = new XElement(Ns + "ErrorResponse",
            new XElement(Ns + "Error",
                new XElement(Ns + "Type", "Sender"),
                new XElement(Ns + "Code", code),
                new XElement(Ns + "Message", message)),
            new XElement(Ns + "RequestId", HashHelpers.NewUuid()));
        var body = XmlDecl + root.ToString(SaveOptions.DisableFormatting);
        return (status, XmlHeaders(), Encoding.UTF8.GetBytes(body));
    }

    private static Dictionary<string, string> XmlHeaders()
    {
        return new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["Content-Type"] = "text/xml",
        };
    }

    private static XDocument? ParseBody(byte[] body)
    {
        if (body.Length == 0)
            return null;
        try
        {
            return XDocument.Parse(Encoding.UTF8.GetString(body));
        }
        catch
        {
            return null;
        }
    }

    /// <summary>Find child element by local name, ignoring namespace.</summary>
    private static XElement? FindChild(XElement parent, string localName)
    {
        return parent.Elements().FirstOrDefault(e => e.Name.LocalName == localName);
    }

    /// <summary>Find all child elements by local name, ignoring namespace.</summary>
    private static IEnumerable<XElement> FindAllChildren(XElement parent, string localName)
    {
        return parent.Elements().Where(e => e.Name.LocalName == localName);
    }

    /// <summary>Get text of a child element by local name.</summary>
    private static string GetChildText(XElement parent, string localName, string defaultValue = "")
    {
        var child = FindChild(parent, localName);
        return child?.Value ?? defaultValue;
    }

    private static string Qp(IReadOnlyDictionary<string, string[]> queryParams, string key)
    {
        if (queryParams.TryGetValue(key, out var values) && values.Length > 0)
            return values[0];
        return "";
    }

    // ── XML builders ─────────────────────────────────────────────────────────────

    private void BuildHostedZoneElement(XElement parent, HostedZone zone)
    {
        var hz = new XElement(Ns + "HostedZone",
            new XElement(Ns + "Id", $"/hostedzone/{zone.Id}"),
            new XElement(Ns + "Name", zone.Name),
            new XElement(Ns + "CallerReference", zone.CallerReference),
            new XElement(Ns + "Config",
                new XElement(Ns + "Comment", zone.Comment ?? ""),
                new XElement(Ns + "PrivateZone", zone.Private ? "true" : "false")),
            new XElement(Ns + "ResourceRecordSetCount",
                _records.TryGetValue(zone.Id, out var recs) ? recs.Count.ToString() : "0"));
        parent.Add(hz);
    }

    private static void BuildDelegationSetElement(XElement parent)
    {
        var ds = new XElement(Ns + "DelegationSet",
            new XElement(Ns + "NameServers",
                DefaultNameServers.Select(ns => new XElement(Ns + "NameServer", ns))));
        parent.Add(ds);
    }

    private static void BuildChangeInfoElement(XElement parent, ChangeInfo change)
    {
        var ci = new XElement(Ns + "ChangeInfo",
            new XElement(Ns + "Id", $"/change/{change.Id}"),
            new XElement(Ns + "Status", change.Status),
            new XElement(Ns + "SubmittedAt", change.SubmittedAt));
        if (!string.IsNullOrEmpty(change.Comment))
        {
            ci.Add(new XElement(Ns + "Comment", change.Comment));
        }
        parent.Add(ci);
    }

    private static void BuildRecordSetElement(XElement parent, RecordSet rs)
    {
        var rrs = new XElement(Ns + "ResourceRecordSet",
            new XElement(Ns + "Name", rs.Name),
            new XElement(Ns + "Type", rs.Type));

        if (!string.IsNullOrEmpty(rs.SetIdentifier))
            rrs.Add(new XElement(Ns + "SetIdentifier", rs.SetIdentifier));
        if (rs.Weight.HasValue)
            rrs.Add(new XElement(Ns + "Weight", rs.Weight.Value));
        if (!string.IsNullOrEmpty(rs.Region))
            rrs.Add(new XElement(Ns + "Region", rs.Region));
        if (!string.IsNullOrEmpty(rs.Failover))
            rrs.Add(new XElement(Ns + "Failover", rs.Failover));
        if (rs.MultiValueAnswer.HasValue)
            rrs.Add(new XElement(Ns + "MultiValueAnswer", rs.MultiValueAnswer.Value ? "true" : "false"));
        if (!string.IsNullOrEmpty(rs.Ttl))
            rrs.Add(new XElement(Ns + "TTL", rs.Ttl));
        if (rs.AliasTarget is not null)
        {
            rrs.Add(new XElement(Ns + "AliasTarget",
                new XElement(Ns + "HostedZoneId", rs.AliasTarget.HostedZoneId ?? ""),
                new XElement(Ns + "DNSName", rs.AliasTarget.DnsName ?? ""),
                new XElement(Ns + "EvaluateTargetHealth",
                    rs.AliasTarget.EvaluateTargetHealth ? "true" : "false")));
        }
        if (rs.ResourceRecords is not null && rs.ResourceRecords.Count > 0)
        {
            rrs.Add(new XElement(Ns + "ResourceRecords",
                rs.ResourceRecords.Select(v =>
                    new XElement(Ns + "ResourceRecord",
                        new XElement(Ns + "Value", v)))));
        }
        if (!string.IsNullOrEmpty(rs.HealthCheckId))
            rrs.Add(new XElement(Ns + "HealthCheckId", rs.HealthCheckId));
        if (rs.GeoLocation is not null)
        {
            var geo = new XElement(Ns + "GeoLocation");
            if (!string.IsNullOrEmpty(rs.GeoLocation.ContinentCode))
                geo.Add(new XElement(Ns + "ContinentCode", rs.GeoLocation.ContinentCode));
            if (!string.IsNullOrEmpty(rs.GeoLocation.CountryCode))
                geo.Add(new XElement(Ns + "CountryCode", rs.GeoLocation.CountryCode));
            if (!string.IsNullOrEmpty(rs.GeoLocation.SubdivisionCode))
                geo.Add(new XElement(Ns + "SubdivisionCode", rs.GeoLocation.SubdivisionCode));
            rrs.Add(geo);
        }

        parent.Add(rrs);
    }

    // ── Record set XML parser ────────────────────────────────────────────────────

    private static RecordSet ParseRecordSet(XElement el)
    {
        var rs = new RecordSet
        {
            Name = NormaliseName(GetChildText(el, "Name")),
            Type = GetChildText(el, "Type"),
        };

        if (FindChild(el, "SetIdentifier") is not null)
            rs.SetIdentifier = GetChildText(el, "SetIdentifier");
        if (FindChild(el, "Weight") is not null)
            rs.Weight = int.TryParse(GetChildText(el, "Weight", "0"), out var w) ? w : 0;
        if (FindChild(el, "Region") is not null)
            rs.Region = GetChildText(el, "Region");
        if (FindChild(el, "Failover") is not null)
            rs.Failover = GetChildText(el, "Failover");
        if (FindChild(el, "MultiValueAnswer") is not null)
            rs.MultiValueAnswer = string.Equals(GetChildText(el, "MultiValueAnswer"), "true", StringComparison.OrdinalIgnoreCase);
        if (FindChild(el, "TTL") is not null)
            rs.Ttl = GetChildText(el, "TTL");

        var atEl = FindChild(el, "AliasTarget");
        if (atEl is not null)
        {
            var dnsName = GetChildText(atEl, "DNSName");
            if (!string.IsNullOrEmpty(dnsName) && !dnsName.EndsWith('.'))
                dnsName += ".";
            rs.AliasTarget = new AliasTargetInfo
            {
                HostedZoneId = GetChildText(atEl, "HostedZoneId"),
                DnsName = dnsName,
                EvaluateTargetHealth = string.Equals(
                    GetChildText(atEl, "EvaluateTargetHealth", "false"), "true", StringComparison.OrdinalIgnoreCase),
            };
        }

        var rrContainer = FindChild(el, "ResourceRecords");
        if (rrContainer is not null)
        {
            rs.ResourceRecords = FindAllChildren(rrContainer, "ResourceRecord")
                .Select(rr => GetChildText(rr, "Value"))
                .ToList();
        }

        if (FindChild(el, "HealthCheckId") is not null)
            rs.HealthCheckId = GetChildText(el, "HealthCheckId");

        var geoEl = FindChild(el, "GeoLocation");
        if (geoEl is not null)
        {
            rs.GeoLocation = new GeoLocationInfo
            {
                ContinentCode = FindChild(geoEl, "ContinentCode") is not null ? GetChildText(geoEl, "ContinentCode") : null,
                CountryCode = FindChild(geoEl, "CountryCode") is not null ? GetChildText(geoEl, "CountryCode") : null,
                SubdivisionCode = FindChild(geoEl, "SubdivisionCode") is not null ? GetChildText(geoEl, "SubdivisionCode") : null,
            };
        }

        return rs;
    }

    // ── Health check XML builder / parser ────────────────────────────────────────

    private static void BuildHealthCheckElement(XElement parent, HealthCheck hc)
    {
        var h = new XElement(Ns + "HealthCheck",
            new XElement(Ns + "Id", hc.Id),
            new XElement(Ns + "CallerReference", hc.CallerReference),
            new XElement(Ns + "HealthCheckVersion", hc.Version.ToString()));

        var cfgEl = new XElement(Ns + "HealthCheckConfig");
        var cfg = hc.Config;

        foreach (var field in new[] { "Type", "IPAddress", "FullyQualifiedDomainName", "ResourcePath", "SearchString" })
        {
            if (cfg.TryGetValue(field, out var val) && !string.IsNullOrEmpty(val))
                cfgEl.Add(new XElement(Ns + field, val));
        }
        foreach (var field in new[] { "Port", "RequestInterval", "FailureThreshold", "HealthThreshold" })
        {
            if (cfg.TryGetValue(field, out var val) && !string.IsNullOrEmpty(val))
                cfgEl.Add(new XElement(Ns + field, val));
        }
        foreach (var field in new[] { "MeasureLatency", "EnableSNI", "Inverted", "Disabled" })
        {
            if (cfg.TryGetValue(field, out var val) && !string.IsNullOrEmpty(val))
                cfgEl.Add(new XElement(Ns + field, val));
        }

        h.Add(cfgEl);
        parent.Add(h);
    }

    private static Dictionary<string, string> ParseHealthCheckConfig(XElement el)
    {
        var cfg = new Dictionary<string, string>(StringComparer.Ordinal);

        foreach (var field in new[] { "Type", "IPAddress", "FullyQualifiedDomainName", "ResourcePath", "SearchString",
                                      "InsufficientDataHealthStatus", "RoutingControlArn" })
        {
            if (FindChild(el, field) is not null)
                cfg[field] = GetChildText(el, field);
        }
        foreach (var field in new[] { "Port", "RequestInterval", "FailureThreshold", "HealthThreshold" })
        {
            if (FindChild(el, field) is not null)
                cfg[field] = GetChildText(el, field, "0");
        }
        foreach (var field in new[] { "MeasureLatency", "EnableSNI", "Inverted", "Disabled" })
        {
            if (FindChild(el, field) is not null)
                cfg[field] = string.Equals(GetChildText(el, field), "true", StringComparison.OrdinalIgnoreCase)
                    ? "true" : "false";
        }

        return cfg;
    }

    // ── Operations ───────────────────────────────────────────────────────────────

    private (int, Dictionary<string, string>, byte[]) CreateHostedZone(byte[] body)
    {
        var doc = ParseBody(body);
        if (doc?.Root is null)
            return ErrorResponse("InvalidInput", "Missing or invalid request body.");

        var root = doc.Root;
        var callerRef = GetChildText(root, "CallerReference");
        var name = NormaliseName(GetChildText(root, "Name"));

        if (string.IsNullOrEmpty(name) || string.IsNullOrEmpty(callerRef))
            return ErrorResponse("InvalidInput", "Name and CallerReference are required.");

        var cfgEl = FindChild(root, "HostedZoneConfig");
        var comment = cfgEl is not null ? GetChildText(cfgEl, "Comment") : "";
        var isPrivate = cfgEl is not null &&
            string.Equals(GetChildText(cfgEl, "PrivateZone", "false"), "true", StringComparison.OrdinalIgnoreCase);

        lock (_lock)
        {
            // Idempotency: same CallerReference returns existing zone
            if (_callerRefs.TryGetValue(callerRef, out var existingId))
            {
                var existingZone = _zones[existingId];
                var existingChange = new ChangeInfo
                {
                    Id = GenerateChangeId(),
                    Status = "INSYNC",
                    SubmittedAt = TimeHelpers.NowIso(),
                    Comment = "",
                };
                return XmlResponse("CreateHostedZoneResponse", r =>
                {
                    BuildHostedZoneElement(r, existingZone);
                    BuildChangeInfoElement(r, existingChange);
                    BuildDelegationSetElement(r);
                }, 201);
            }

            var zoneId = GenerateZoneId();
            var zone = new HostedZone
            {
                Id = zoneId,
                Name = name,
                CallerReference = callerRef,
                Comment = comment,
                Private = isPrivate,
            };
            _zones[zoneId] = zone;
            _records[zoneId] = DefaultRecords(name);
            _callerRefs[callerRef] = zoneId;

            var changeId = GenerateChangeId();
            var change = new ChangeInfo
            {
                Id = changeId,
                Status = "INSYNC",
                SubmittedAt = TimeHelpers.NowIso(),
                Comment = "",
            };
            _changes[changeId] = change;

            return XmlResponse("CreateHostedZoneResponse", r =>
            {
                BuildHostedZoneElement(r, zone);
                BuildChangeInfoElement(r, change);
                BuildDelegationSetElement(r);
            }, 201);
        }
    }

    private (int, Dictionary<string, string>, byte[]) GetHostedZone(string zoneId)
    {
        lock (_lock)
        {
            if (!_zones.TryGetValue(zoneId, out var zone))
                return ErrorResponse("NoSuchHostedZone", $"No hosted zone found with ID: {zoneId}", 404);

            return XmlResponse("GetHostedZoneResponse", r =>
            {
                BuildHostedZoneElement(r, zone);
                BuildDelegationSetElement(r);
            });
        }
    }

    private (int, Dictionary<string, string>, byte[]) DeleteHostedZone(string zoneId)
    {
        lock (_lock)
        {
            if (!_zones.TryGetValue(zoneId, out var zone))
                return ErrorResponse("NoSuchHostedZone", $"No hosted zone found with ID: {zoneId}", 404);

            if (_records.TryGetValue(zoneId, out var recs))
            {
                var nonDefault = recs.Where(r =>
                    !(r.Type is "SOA" or "NS" && r.Name == zone.Name)).ToList();
                if (nonDefault.Count > 0)
                    return ErrorResponse("HostedZoneNotEmpty",
                        "The hosted zone contains resource record sets other than the default SOA and NS records.");
            }

            _zones.TryRemove(zoneId, out _);
            _records.TryRemove(zoneId, out _);
            _callerRefs.TryRemove(zone.CallerReference, out _);

            var changeId = GenerateChangeId();
            var change = new ChangeInfo
            {
                Id = changeId,
                Status = "INSYNC",
                SubmittedAt = TimeHelpers.NowIso(),
                Comment = "",
            };
            _changes[changeId] = change;

            return XmlResponse("DeleteHostedZoneResponse", r =>
            {
                BuildChangeInfoElement(r, change);
            });
        }
    }

    private (int, Dictionary<string, string>, byte[]) ListHostedZones(
        IReadOnlyDictionary<string, string[]> queryParams)
    {
        var marker = Qp(queryParams, "marker");
        var maxItemsStr = Qp(queryParams, "maxitems");
        var maxItems = Math.Min(
            !string.IsNullOrEmpty(maxItemsStr) && int.TryParse(maxItemsStr, out var mi) ? mi : 100,
            100);

        List<HostedZone> zones;
        lock (_lock)
        {
            zones = _zones.Values.OrderBy(z => z.Id, StringComparer.Ordinal).ToList();
        }

        if (!string.IsNullOrEmpty(marker))
        {
            zones = zones.Where(z => string.Compare(z.Id, marker, StringComparison.Ordinal) > 0).ToList();
        }

        var isTruncated = zones.Count > maxItems;
        var page = zones.Take(maxItems).ToList();
        var nextMarker = isTruncated ? page[^1].Id : null;

        return XmlResponse("ListHostedZonesResponse", root =>
        {
            var hzList = new XElement(Ns + "HostedZones");
            foreach (var zone in page)
            {
                BuildHostedZoneElement(hzList, zone);
            }
            root.Add(hzList);
            root.Add(new XElement(Ns + "IsTruncated", isTruncated ? "true" : "false"));
            root.Add(new XElement(Ns + "Marker", marker ?? ""));
            root.Add(new XElement(Ns + "MaxItems", maxItems.ToString()));
            if (nextMarker is not null)
                root.Add(new XElement(Ns + "NextMarker", nextMarker));
        });
    }

    private (int, Dictionary<string, string>, byte[]) ListHostedZonesByName(
        IReadOnlyDictionary<string, string[]> queryParams)
    {
        var dnsName = Qp(queryParams, "dnsname");
        if (!string.IsNullOrEmpty(dnsName))
            dnsName = NormaliseName(dnsName);
        var hzId = Qp(queryParams, "hostedzoneid");
        var maxItemsStr = Qp(queryParams, "maxitems");
        var maxItems = Math.Min(
            !string.IsNullOrEmpty(maxItemsStr) && int.TryParse(maxItemsStr, out var mi) ? mi : 100,
            100);

        List<HostedZone> zones;
        lock (_lock)
        {
            zones = _zones.Values.OrderBy(z => z.Name, StringComparer.Ordinal).ToList();
        }

        if (!string.IsNullOrEmpty(dnsName))
            zones = zones.Where(z => string.Compare(z.Name, dnsName, StringComparison.Ordinal) >= 0).ToList();
        if (!string.IsNullOrEmpty(hzId))
            zones = zones.Where(z => string.Compare(z.Id, hzId, StringComparison.Ordinal) >= 0).ToList();

        var isTruncated = zones.Count > maxItems;
        var page = zones.Take(maxItems).ToList();
        var nextDns = isTruncated ? page[^1].Name : null;
        var nextHz = isTruncated ? page[^1].Id : null;

        return XmlResponse("ListHostedZonesByNameResponse", root =>
        {
            root.Add(new XElement(Ns + "DNSName", dnsName ?? ""));
            root.Add(new XElement(Ns + "HostedZoneId", hzId ?? ""));
            var hzList = new XElement(Ns + "HostedZones");
            foreach (var zone in page)
            {
                BuildHostedZoneElement(hzList, zone);
            }
            root.Add(hzList);
            root.Add(new XElement(Ns + "IsTruncated", isTruncated ? "true" : "false"));
            root.Add(new XElement(Ns + "MaxItems", maxItems.ToString()));
            if (nextDns is not null)
                root.Add(new XElement(Ns + "NextDNSName", nextDns));
            if (nextHz is not null)
                root.Add(new XElement(Ns + "NextHostedZoneId", nextHz));
        });
    }

    private (int, Dictionary<string, string>, byte[]) GetHostedZoneCount()
    {
        int count;
        lock (_lock)
        {
            count = _zones.Count;
        }

        return XmlResponse("GetHostedZoneCountResponse", root =>
        {
            root.Add(new XElement(Ns + "HostedZoneCount", count.ToString()));
        });
    }

    private (int, Dictionary<string, string>, byte[]) UpdateHostedZoneComment(string zoneId, byte[] body)
    {
        var doc = ParseBody(body);
        if (doc?.Root is null)
            return ErrorResponse("InvalidInput", "Missing or invalid request body.");

        lock (_lock)
        {
            if (!_zones.TryGetValue(zoneId, out var zone))
                return ErrorResponse("NoSuchHostedZone", $"No hosted zone found with ID: {zoneId}", 404);

            var commentEl = FindChild(doc.Root, "Comment");
            if (commentEl is not null)
                zone.Comment = commentEl.Value;

            return XmlResponse("UpdateHostedZoneCommentResponse", r =>
            {
                BuildHostedZoneElement(r, zone);
            });
        }
    }

    private (int, Dictionary<string, string>, byte[]) ChangeResourceRecordSets(string zoneId, byte[] body)
    {
        var doc = ParseBody(body);
        if (doc?.Root is null)
            return ErrorResponse("InvalidInput", "Missing or invalid request body.");

        lock (_lock)
        {
            if (!_zones.TryGetValue(zoneId, out _))
                return ErrorResponse("NoSuchHostedZone", $"No hosted zone found with ID: {zoneId}", 404);

            var batchEl = FindChild(doc.Root, "ChangeBatch");
            if (batchEl is null)
                return ErrorResponse("InvalidInput", "Missing ChangeBatch element.");

            var comment = GetChildText(batchEl, "Comment");
            var changesEl = FindChild(batchEl, "Changes");
            if (changesEl is null)
                return ErrorResponse("InvalidInput", "Missing Changes element.");

            var ops = new List<(string Action, RecordSet Rs)>();
            foreach (var changeEl in FindAllChildren(changesEl, "Change"))
            {
                var action = GetChildText(changeEl, "Action");
                var rsEl = FindChild(changeEl, "ResourceRecordSet");
                if (rsEl is null)
                    return ErrorResponse("InvalidInput", "Missing ResourceRecordSet element.");

                var rs = ParseRecordSet(rsEl);
                if (string.IsNullOrEmpty(rs.Name) || string.IsNullOrEmpty(rs.Type))
                    return ErrorResponse("InvalidInput", "Name and Type are required in ResourceRecordSet.");

                ops.Add((action, rs));
            }

            if (!_records.TryGetValue(zoneId, out var existingRecords))
                existingRecords = [];
            var current = new List<RecordSet>(existingRecords);

            foreach (var (action, rs) in ops)
            {
                var key = RsKey(rs);
                var existing = current.FirstOrDefault(r => RsKey(r) == key);

                switch (action)
                {
                    case "CREATE":
                        if (existing is not null)
                            return ErrorResponse("InvalidChangeBatch",
                                $"Tried to create resource record set {rs.Name} type {rs.Type} but it already exists.");
                        current.Add(rs);
                        break;

                    case "DELETE":
                        if (existing is null)
                            return ErrorResponse("InvalidChangeBatch",
                                $"Tried to delete resource record set {rs.Name} type {rs.Type} but it does not exist.");
                        current = current.Where(r => RsKey(r) != key).ToList();
                        break;

                    case "UPSERT":
                        if (existing is not null)
                            current = current.Select(r => RsKey(r) == key ? rs : r).ToList();
                        else
                            current.Add(rs);
                        break;

                    default:
                        return ErrorResponse("InvalidInput", $"Unknown action: {action}");
                }
            }

            _records[zoneId] = current;

            var changeId = GenerateChangeId();
            var change = new ChangeInfo
            {
                Id = changeId,
                Status = "INSYNC",
                SubmittedAt = TimeHelpers.NowIso(),
                Comment = comment,
            };
            _changes[changeId] = change;

            return XmlResponse("ChangeResourceRecordSetsResponse", r =>
            {
                BuildChangeInfoElement(r, change);
            });
        }
    }

    private (int, Dictionary<string, string>, byte[]) ListResourceRecordSets(
        string zoneId, IReadOnlyDictionary<string, string[]> queryParams)
    {
        var startName = Qp(queryParams, "name");
        if (!string.IsNullOrEmpty(startName))
            startName = NormaliseName(startName);
        var startType = Qp(queryParams, "type");
        var startId = Qp(queryParams, "identifier");
        var maxItemsStr = Qp(queryParams, "maxitems");
        var maxItems = Math.Min(
            !string.IsNullOrEmpty(maxItemsStr) && int.TryParse(maxItemsStr, out var mi) ? mi : 300,
            300);

        List<RecordSet> records;
        lock (_lock)
        {
            if (!_zones.TryGetValue(zoneId, out _))
                return ErrorResponse("NoSuchHostedZone", $"No hosted zone found with ID: {zoneId}", 404);

            records = _records.TryGetValue(zoneId, out var recs)
                ? new List<RecordSet>(recs)
                : [];
        }

        // Sort by reversed-label name, then type, then set identifier
        records.Sort((a, b) =>
        {
            var nameCompare = CompareNameSortKeys(NameSortKey(a.Name), NameSortKey(b.Name));
            if (nameCompare != 0) return nameCompare;
            var typeCompare = string.Compare(a.Type, b.Type, StringComparison.Ordinal);
            if (typeCompare != 0) return typeCompare;
            return string.Compare(a.SetIdentifier ?? "", b.SetIdentifier ?? "", StringComparison.Ordinal);
        });

        if (!string.IsNullOrEmpty(startName))
        {
            var startKey = NameSortKey(startName);
            records = records.Where(r =>
            {
                var rKey = NameSortKey(r.Name);
                var nameCompare = CompareNameSortKeys(rKey, startKey);
                if (nameCompare != 0) return nameCompare > 0;
                var typeCompare = string.Compare(r.Type, startType, StringComparison.Ordinal);
                if (typeCompare != 0) return typeCompare > 0;
                return string.Compare(r.SetIdentifier ?? "", startId, StringComparison.Ordinal) >= 0;
            }).ToList();
        }

        var isTruncated = records.Count > maxItems;
        var page = records.Take(maxItems).ToList();
        string? nextName = null;
        string? nextType = null;
        string? nextId = null;
        if (isTruncated && records.Count > maxItems)
        {
            var nextRecord = records[maxItems];
            nextName = nextRecord.Name;
            nextType = nextRecord.Type;
            nextId = nextRecord.SetIdentifier ?? "";
        }

        return XmlResponse("ListResourceRecordSetsResponse", root =>
        {
            var rrsList = new XElement(Ns + "ResourceRecordSets");
            foreach (var rs in page)
            {
                BuildRecordSetElement(rrsList, rs);
            }
            root.Add(rrsList);
            root.Add(new XElement(Ns + "IsTruncated", isTruncated ? "true" : "false"));
            root.Add(new XElement(Ns + "MaxItems", maxItems.ToString()));
            if (nextName is not null)
                root.Add(new XElement(Ns + "NextRecordName", nextName));
            if (nextType is not null)
                root.Add(new XElement(Ns + "NextRecordType", nextType));
            if (!string.IsNullOrEmpty(nextId))
                root.Add(new XElement(Ns + "NextRecordIdentifier", nextId));
        });
    }

    private (int, Dictionary<string, string>, byte[]) GetChange(string changeId)
    {
        lock (_lock)
        {
            if (!_changes.TryGetValue(changeId, out var change))
                return ErrorResponse("NoSuchChange", $"A change with the ID {changeId} does not exist.", 404);

            return XmlResponse("GetChangeResponse", r =>
            {
                BuildChangeInfoElement(r, change);
            });
        }
    }

    // ── Health checks ────────────────────────────────────────────────────────────

    private (int, Dictionary<string, string>, byte[]) CreateHealthCheck(byte[] body)
    {
        var doc = ParseBody(body);
        if (doc?.Root is null)
            return ErrorResponse("InvalidInput", "Missing or invalid request body.");

        var callerRef = GetChildText(doc.Root, "CallerReference");
        if (string.IsNullOrEmpty(callerRef))
            return ErrorResponse("InvalidInput", "CallerReference is required.");

        var cfgEl = FindChild(doc.Root, "HealthCheckConfig");
        var cfg = cfgEl is not null ? ParseHealthCheckConfig(cfgEl) : new Dictionary<string, string>(StringComparer.Ordinal);

        lock (_lock)
        {
            if (_hcCallerRefs.TryGetValue(callerRef, out var existingHcId))
            {
                var existingHc = _healthChecks[existingHcId];
                return XmlResponse("CreateHealthCheckResponse", r =>
                {
                    BuildHealthCheckElement(r, existingHc);
                }, 201);
            }

            var hcId = HashHelpers.NewUuid();
            var hc = new HealthCheck
            {
                Id = hcId,
                CallerReference = callerRef,
                Config = cfg,
                Version = 1,
            };
            _healthChecks[hcId] = hc;
            _hcCallerRefs[callerRef] = hcId;

            return XmlResponse("CreateHealthCheckResponse", r =>
            {
                BuildHealthCheckElement(r, hc);
            }, 201);
        }
    }

    private (int, Dictionary<string, string>, byte[]) GetHealthCheck(string hcId)
    {
        lock (_lock)
        {
            if (!_healthChecks.TryGetValue(hcId, out var hc))
                return ErrorResponse("NoSuchHealthCheck", $"No health check exists with the specified ID {hcId}.", 404);

            return XmlResponse("GetHealthCheckResponse", r =>
            {
                BuildHealthCheckElement(r, hc);
            });
        }
    }

    private (int, Dictionary<string, string>, byte[]) DeleteHealthCheck(string hcId)
    {
        lock (_lock)
        {
            if (!_healthChecks.TryGetValue(hcId, out var hc))
                return ErrorResponse("NoSuchHealthCheck", $"No health check exists with the specified ID {hcId}.", 404);

            _healthChecks.TryRemove(hcId, out _);
            _hcCallerRefs.TryRemove(hc.CallerReference, out _);
        }

        return XmlResponse("DeleteHealthCheckResponse", _ => { });
    }

    private (int, Dictionary<string, string>, byte[]) ListHealthChecks(
        IReadOnlyDictionary<string, string[]> queryParams)
    {
        var marker = Qp(queryParams, "marker");
        var maxItemsStr = Qp(queryParams, "maxitems");
        var maxItems = Math.Min(
            !string.IsNullOrEmpty(maxItemsStr) && int.TryParse(maxItemsStr, out var mi) ? mi : 100,
            1000);

        List<HealthCheck> checks;
        lock (_lock)
        {
            checks = _healthChecks.Values.OrderBy(h => h.Id, StringComparer.Ordinal).ToList();
        }

        if (!string.IsNullOrEmpty(marker))
            checks = checks.Where(h => string.Compare(h.Id, marker, StringComparison.Ordinal) > 0).ToList();

        var isTruncated = checks.Count > maxItems;
        var page = checks.Take(maxItems).ToList();
        var nextMarker = isTruncated ? page[^1].Id : null;

        return XmlResponse("ListHealthChecksResponse", root =>
        {
            var hcList = new XElement(Ns + "HealthChecks");
            foreach (var hc in page)
            {
                BuildHealthCheckElement(hcList, hc);
            }
            root.Add(hcList);
            root.Add(new XElement(Ns + "IsTruncated", isTruncated ? "true" : "false"));
            root.Add(new XElement(Ns + "Marker", marker ?? ""));
            root.Add(new XElement(Ns + "MaxItems", maxItems.ToString()));
            if (nextMarker is not null)
                root.Add(new XElement(Ns + "NextMarker", nextMarker));
        });
    }

    private (int, Dictionary<string, string>, byte[]) UpdateHealthCheck(string hcId, byte[] body)
    {
        var doc = ParseBody(body);
        if (doc?.Root is null)
            return ErrorResponse("InvalidInput", "Missing or invalid request body.");

        lock (_lock)
        {
            if (!_healthChecks.TryGetValue(hcId, out var hc))
                return ErrorResponse("NoSuchHealthCheck", $"No health check exists with the specified ID {hcId}.", 404);

            var updates = ParseHealthCheckConfig(doc.Root);
            foreach (var kv in updates)
            {
                hc.Config[kv.Key] = kv.Value;
            }
            hc.Version++;

            return XmlResponse("UpdateHealthCheckResponse", r =>
            {
                BuildHealthCheckElement(r, hc);
            });
        }
    }

    // ── Tags ─────────────────────────────────────────────────────────────────────

    private (int, Dictionary<string, string>, byte[]) ChangeTagsForResource(
        string resourceType, string resourceId, byte[] body)
    {
        var doc = ParseBody(body);
        if (doc?.Root is null)
            return ErrorResponse("InvalidInput", "Missing or invalid request body.");

        lock (_lock)
        {
            if (resourceType == "hostedzone" && !_zones.ContainsKey(resourceId))
                return ErrorResponse("NoSuchHostedZone", $"No hosted zone found with ID: {resourceId}", 404);
            if (resourceType == "healthcheck" && !_healthChecks.ContainsKey(resourceId))
                return ErrorResponse("NoSuchHealthCheck", $"No health check exists with the specified ID {resourceId}.", 404);

            var tagKey = (resourceType, resourceId);
            if (!_tags.TryGetValue(tagKey, out var tagMap))
            {
                tagMap = new Dictionary<string, string>(StringComparer.Ordinal);
                _tags[tagKey] = tagMap;
            }

            var addEl = FindChild(doc.Root, "AddTags");
            if (addEl is not null)
            {
                foreach (var tagEl in FindAllChildren(addEl, "Tag"))
                {
                    var k = GetChildText(tagEl, "Key");
                    var v = GetChildText(tagEl, "Value");
                    if (!string.IsNullOrEmpty(k))
                        tagMap[k] = v;
                }
            }

            var removeEl = FindChild(doc.Root, "RemoveTagKeys");
            if (removeEl is not null)
            {
                foreach (var keyEl in FindAllChildren(removeEl, "Key"))
                {
                    tagMap.Remove(keyEl.Value);
                }
            }

            _tags[tagKey] = tagMap;
        }

        return XmlResponse("ChangeTagsForResourceResponse", _ => { });
    }

    private (int, Dictionary<string, string>, byte[]) ListTagsForResource(
        string resourceType, string resourceId)
    {
        lock (_lock)
        {
            if (resourceType == "hostedzone" && !_zones.ContainsKey(resourceId))
                return ErrorResponse("NoSuchHostedZone", $"No hosted zone found with ID: {resourceId}", 404);
            if (resourceType == "healthcheck" && !_healthChecks.ContainsKey(resourceId))
                return ErrorResponse("NoSuchHealthCheck", $"No health check exists with the specified ID {resourceId}.", 404);

            var tagKey = (resourceType, resourceId);
            _tags.TryGetValue(tagKey, out var tagMap);
            tagMap ??= new Dictionary<string, string>(StringComparer.Ordinal);

            return XmlResponse("ListTagsForResourceResponse", root =>
            {
                var rts = new XElement(Ns + "ResourceTagSet",
                    new XElement(Ns + "ResourceType", resourceType),
                    new XElement(Ns + "ResourceId", resourceId));
                var tagsEl = new XElement(Ns + "Tags");
                foreach (var kv in tagMap)
                {
                    tagsEl.Add(new XElement(Ns + "Tag",
                        new XElement(Ns + "Key", kv.Key),
                        new XElement(Ns + "Value", kv.Value)));
                }
                rts.Add(tagsEl);
                root.Add(rts);
            });
        }
    }

    // ── Data models ──────────────────────────────────────────────────────────────

    private sealed class HostedZone
    {
        internal string Id { get; init; } = "";
        internal string Name { get; init; } = "";
        internal string CallerReference { get; init; } = "";
        internal string? Comment { get; set; }
        internal bool Private { get; init; }
    }

    private sealed class ChangeInfo
    {
        internal string Id { get; init; } = "";
        internal string Status { get; init; } = "";
        internal string SubmittedAt { get; init; } = "";
        internal string? Comment { get; init; }
    }

    private sealed class RecordSet
    {
        internal string Name { get; init; } = "";
        internal string Type { get; init; } = "";
        internal string? SetIdentifier { get; set; }
        internal int? Weight { get; set; }
        internal string? Region { get; set; }
        internal string? Failover { get; set; }
        internal bool? MultiValueAnswer { get; set; }
        internal string? Ttl { get; set; }
        internal AliasTargetInfo? AliasTarget { get; set; }
        internal List<string>? ResourceRecords { get; set; }
        internal string? HealthCheckId { get; set; }
        internal GeoLocationInfo? GeoLocation { get; set; }
    }

    private sealed class AliasTargetInfo
    {
        internal string? HostedZoneId { get; init; }
        internal string? DnsName { get; init; }
        internal bool EvaluateTargetHealth { get; init; }
    }

    private sealed class GeoLocationInfo
    {
        internal string? ContinentCode { get; init; }
        internal string? CountryCode { get; init; }
        internal string? SubdivisionCode { get; init; }
    }

    private sealed class HealthCheck
    {
        internal string Id { get; init; } = "";
        internal string CallerReference { get; init; } = "";
        internal Dictionary<string, string> Config { get; init; } = new(StringComparer.Ordinal);
        internal int Version { get; set; } = 1;
    }
}
