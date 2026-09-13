using System.Globalization;
using MicroStack.Admin.Contracts;
using MicroStack.Internal.Admin;

namespace MicroStack.Services.SecretsManager;

internal sealed partial class SecretsManagerServiceHandler
{
    private static readonly AdminResourceKind[] AdminKinds =
    [
        new("secret", "Secrets"),
        new("secret-version", "Secret versions") { IsRoot = false }
    ];

    public IReadOnlyList<AdminResourceKind> GetAdminResourceKinds(string serviceId) => AdminKinds;

    public IEnumerable<AdminNode> GetAdminResources(string serviceId)
    {
        lock (_lock)
        {
            return _secrets.Items.OrderBy(x => x.Key, StringComparer.Ordinal)
                .Select(x => SecretNode(x.Key, x.Value)).ToArray();
        }
    }

    private AdminNode SecretNode(string key, SmSecret snapshot) =>
        AdminData.Node("secret", key, snapshot.Name, snapshot.Arn,
            snapshot.DeletedDate is null ? "Active" : "Pending deletion") with
        {
            ReadFields = () =>
            {
                lock (_lock)
                {
                    if (!_secrets.TryGetValue(key, out var secret)) return [];
                    return
                    [
                        AdminData.Field("Description", secret.Description),
                        AdminData.Field("KMS key ID", secret.KmsKeyId),
                        AdminData.Field("Created", Epoch(secret.CreatedDate), format: "datetime"),
                        AdminData.Field("Last changed", Epoch(secret.LastChangedDate), format: "datetime"),
                        AdminData.Field("Last accessed", secret.LastAccessedDate is { } a ? Epoch(a) : null, format: "datetime"),
                        AdminData.Field("Deleted", secret.DeletedDate is { } d ? Epoch(d) : null, format: "datetime"),
                        AdminData.Field("Rotation enabled", secret.RotationEnabled.ToString()),
                        AdminData.Field("Rotation Lambda ARN", secret.RotationLambdaArn),
                        AdminData.Field("Tags", string.Join(", ", secret.Tags.Select(t => $"{t.Key}={t.Value}"))),
                        AdminData.Field("Secret value", null, sensitive: true),
                        AdminData.Field("Resource policy", _resourcePolicies.ContainsKey(secret.Arn) ? "Configured" : "Not configured")
                    ];
                }
            },
            ChildKinds = [AdminKinds[1]],
            ReadChildren = () =>
            {
                lock (_lock)
                {
                    if (!_secrets.TryGetValue(key, out var secret)) return [];
                    return secret.Versions.OrderBy(x => x.Key, StringComparer.Ordinal)
                        .Select(x => VersionNode(key, x.Key, x.Value)).ToArray();
                }
            },
            ReadContent = () =>
            {
                lock (_lock)
                {
                    if (!_secrets.TryGetValue(key, out var secret) ||
                        !_resourcePolicies.TryGetValue(secret.Arn, out var policy))
                        return AdminData.Unavailable("No resource policy is retained.", "application/json");
                    return AdminData.JsonText(policy);
                }
            },
            ReadConnections = () =>
            {
                lock (_lock)
                {
                    if (!_secrets.TryGetValue(key, out var secret) ||
                        string.IsNullOrEmpty(secret.KmsKeyId))
                        return [];
                    var alias = secret.KmsKeyId.StartsWith("alias/", StringComparison.Ordinal) ||
                                secret.KmsKeyId.Contains(":alias/", StringComparison.Ordinal);
                    var keyId = secret.KmsKeyId.Contains(':')
                        ? (alias ? "alias/" : "") + secret.KmsKeyId[(secret.KmsKeyId.LastIndexOf('/') + 1)..]
                        : secret.KmsKeyId;
                    return
                    [
                        new AdminConnection("KMS key", "encrypted-by", "kms",
                            [new AdminKey(alias ? "alias" : "key", keyId)])
                    ];
                }
            }
        };

    private AdminNode VersionNode(string secretKey, string versionId, SmSecretVersion snapshot)
    {
        var revealable = new List<string>();
        if (snapshot.SecretString is not null) revealable.Add("SecretString");
        if (snapshot.SecretBinary is not null) revealable.Add("SecretBinary");
        return AdminData.Node("secret-version", versionId, versionId,
            status: string.Join(", ", snapshot.Stages)) with
        {
            ReadFields = () =>
            {
                lock (_lock)
                {
                    if (!TryVersion(secretKey, versionId, out var version)) return [];
                    return
                    [
                        AdminData.Field("Created", Epoch(version.CreatedDate), format: "datetime"),
                        AdminData.Field("Stages", string.Join(", ", version.Stages)),
                        AdminData.Field("SecretString", version.SecretString, sensitive: true,
                            canReveal: version.SecretString is not null),
                        AdminData.Field("SecretBinary", version.SecretBinary, sensitive: true,
                            canReveal: version.SecretBinary is not null, format: "base64")
                    ];
                }
            },
            RevealableFields = revealable,
            RevealField = field =>
            {
                lock (_lock)
                {
                    if (!TryVersion(secretKey, versionId, out var version))
                        return AdminData.Unavailable("The secret version no longer exists.", sensitive: true);
                    return field switch
                    {
                        "SecretString" when version.SecretString is not null =>
                            AdminData.Text(version.SecretString, sensitive: true),
                        "SecretBinary" when version.SecretBinary is not null =>
                            RevealBinary(version.SecretBinary),
                        _ => AdminData.Unavailable("The requested value is not retained or revealable.", sensitive: true)
                    };
                }
            }
        };
    }

    private bool TryVersion(string secretKey, string versionId,
        [System.Diagnostics.CodeAnalysis.NotNullWhen(true)] out SmSecretVersion? version)
    {
        version = null;
        return _secrets.TryGetValue(secretKey, out var secret) &&
               secret.Versions.TryGetValue(versionId, out version);
    }

    private static AdminContent RevealBinary(string base64)
    {
        try
        {
            _ = Convert.FromBase64String(base64);
            return AdminData.Text(base64, "application/base64", sensitive: true);
        }
        catch (FormatException)
        {
            return AdminData.Unavailable("The retained binary value is invalid.", sensitive: true);
        }
    }

    private static string Epoch(double value) =>
        AdminData.IsoUtc(DateTimeOffset.FromUnixTimeMilliseconds((long)(value * 1000)));
}
