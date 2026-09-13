using System.Globalization;
using MicroStack.Admin.Contracts;
using MicroStack.Internal.Admin;

namespace MicroStack.Services.Kms;

internal sealed partial class KmsServiceHandler
{
    private static readonly AdminResourceKind[] AdminKinds =
    [
        new("key", "Keys"),
        new("alias", "Aliases")
    ];

    public IReadOnlyList<AdminResourceKind> GetAdminResourceKinds(string serviceId) => AdminKinds;

    public IEnumerable<AdminNode> GetAdminResources(string serviceId)
    {
        lock (_lock)
        {
            var keys = _keys.Items.OrderBy(x => x.Key, StringComparer.Ordinal)
                .Select(x => KeyNode(x.Key, x.Value)).ToList();
            keys.AddRange(_aliases.Items.OrderBy(x => x.Key, StringComparer.Ordinal)
                .Select(x => AliasNode(x.Key, x.Value)));
            return keys;
        }
    }

    private AdminNode KeyNode(string keyId, KmsKeyRecord snapshot) =>
        AdminData.Node("key", keyId, string.IsNullOrEmpty(snapshot.Description) ? keyId : snapshot.Description,
            snapshot.Arn, snapshot.KeyState) with
        {
            ReadFields = () =>
            {
                lock (_lock)
                {
                    if (!_keys.TryGetValue(keyId, out var key)) return [];
                    return
                    [
                        AdminData.Field("Key ID", key.KeyId),
                        AdminData.Field("Description", key.Description),
                        AdminData.Field("Usage", key.KeyUsage),
                        AdminData.Field("Spec", key.KeySpec),
                        AdminData.Field("Origin", key.Origin),
                        AdminData.Field("Enabled", key.Enabled.ToString()),
                        AdminData.Field("Created", Epoch(key.CreationDate), format: "datetime"),
                        AdminData.Field("Rotation enabled", key.KeyRotationEnabled.ToString()),
                        AdminData.Field("Rotation period (days)", key.RotationPeriodInDays.ToString(CultureInfo.InvariantCulture)),
                        AdminData.Field("Deletion date", key.DeletionDate is { } d ? Epoch(d) : null, format: "datetime"),
                        AdminData.Field("Encryption algorithms", string.Join(", ", key.EncryptionAlgorithms)),
                        AdminData.Field("Signing algorithms", string.Join(", ", key.SigningAlgorithms)),
                        AdminData.Field("Tags", string.Join(", ", key.Tags.Select(t => $"{t.TagKey}={t.TagValue}")))
                    ];
                }
            },
            ReadContent = () =>
            {
                lock (_lock)
                {
                    return _keys.TryGetValue(keyId, out var key) && key.Policy is not null
                        ? AdminData.JsonText(key.Policy)
                        : AdminData.Unavailable("No key policy is retained.", "application/json");
                }
            },
            ReadConnections = () =>
            {
                lock (_lock)
                {
                    return _aliases.Items.Where(x => x.Value == keyId)
                        .OrderBy(x => x.Key, StringComparer.Ordinal)
                        .Select(x => new AdminConnection(x.Key, "aliases", "kms",
                            [new AdminKey("alias", x.Key)]))
                        .ToArray();
                }
            }
        };

    private AdminNode AliasNode(string name, string targetKeyId) =>
        AdminData.Node("alias", name, name, status: "Configured") with
        {
            ReadFields = () =>
            {
                lock (_lock)
                {
                    return _aliases.TryGetValue(name, out var target)
                        ? [AdminData.Field("Target key ID", target)]
                        : [];
                }
            },
            ReadConnections = () =>
            {
                lock (_lock)
                {
                    return _aliases.TryGetValue(name, out var target) && _keys.ContainsKey(target)
                        ? [new AdminConnection("Target key", "targets", "kms", [new AdminKey("key", target)])]
                        : [];
                }
            }
        };

    private static string Epoch(double value) =>
        AdminData.IsoUtc(DateTimeOffset.FromUnixTimeMilliseconds((long)(value * 1000)));
}
