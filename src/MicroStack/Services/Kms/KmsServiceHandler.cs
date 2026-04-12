using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using MicroStack.Internal;

namespace MicroStack.Services.Kms;

/// <summary>
/// KMS (Key Management Service) handler -- JSON protocol via X-Amz-Target (TrentService).
///
/// Port of ministack/services/kms.py.
///
/// Supports: CreateKey, ListKeys, DescribeKey, GetPublicKey, Sign, Verify,
///           Encrypt, Decrypt, GenerateDataKey, GenerateDataKeyWithoutPlaintext,
///           CreateAlias, DeleteAlias, ListAliases, UpdateAlias,
///           EnableKeyRotation, DisableKeyRotation, GetKeyRotationStatus,
///           GetKeyPolicy, PutKeyPolicy, ListKeyPolicies,
///           EnableKey, DisableKey, ScheduleKeyDeletion, CancelKeyDeletion,
///           TagResource, UntagResource, ListResourceTags.
/// </summary>
internal sealed class KmsServiceHandler : IServiceHandler
{
    private readonly AccountScopedDictionary<string, KmsKeyRecord> _keys = new(); // keyed by KeyId
    private readonly AccountScopedDictionary<string, string> _aliases = new(); // alias_name -> key_id
    private readonly Lock _lock = new();

    private static string Region => MicroStackOptions.Instance.Region;

    // -- IServiceHandler -------------------------------------------------------

    public string ServiceName => "kms";

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
            "CreateKey" => ActCreateKey(data),
            "ListKeys" => ActListKeys(data),
            "DescribeKey" => ActDescribeKey(data),
            "GetPublicKey" => ActGetPublicKey(data),
            "Sign" => ActSign(data),
            "Verify" => ActVerify(data),
            "Encrypt" => ActEncrypt(data),
            "Decrypt" => ActDecrypt(data),
            "GenerateDataKey" => ActGenerateDataKey(data),
            "GenerateDataKeyWithoutPlaintext" => ActGenerateDataKeyWithoutPlaintext(data),
            "CreateAlias" => ActCreateAlias(data),
            "DeleteAlias" => ActDeleteAlias(data),
            "ListAliases" => ActListAliases(data),
            "UpdateAlias" => ActUpdateAlias(data),
            "EnableKeyRotation" => ActEnableKeyRotation(data),
            "DisableKeyRotation" => ActDisableKeyRotation(data),
            "GetKeyRotationStatus" => ActGetKeyRotationStatus(data),
            "GetKeyPolicy" => ActGetKeyPolicy(data),
            "PutKeyPolicy" => ActPutKeyPolicy(data),
            "ListKeyPolicies" => ActListKeyPolicies(data),
            "EnableKey" => ActEnableKey(data),
            "DisableKey" => ActDisableKey(data),
            "ScheduleKeyDeletion" => ActScheduleKeyDeletion(data),
            "CancelKeyDeletion" => ActCancelKeyDeletion(data),
            "TagResource" => ActTagResource(data),
            "UntagResource" => ActUntagResource(data),
            "ListResourceTags" => ActListResourceTags(data),
            _ => AwsResponseHelpers.ErrorResponseJson("InvalidAction", $"Unknown action: {action}", 400),
        };

        return Task.FromResult(response);
    }

    public void Reset()
    {
        lock (_lock)
        {
            _keys.Clear();
            _aliases.Clear();
        }
    }

    public object? GetState() => null;

    public void RestoreState(object state) { }

    // -- Helpers ---------------------------------------------------------------

    private static string MakeArn(string keyId)
    {
        return $"arn:aws:kms:{Region}:{AccountContext.GetAccountId()}:key/{keyId}";
    }

    private static Dictionary<string, object?> BuildKeyMetadata(KmsKeyRecord rec)
    {
        return new Dictionary<string, object?>
        {
            ["KeyId"] = rec.KeyId,
            ["Arn"] = rec.Arn,
            ["CreationDate"] = rec.CreationDate,
            ["Enabled"] = rec.Enabled,
            ["Description"] = rec.Description,
            ["KeyUsage"] = rec.KeyUsage,
            ["KeyState"] = rec.KeyState,
            ["Origin"] = rec.Origin,
            ["KeyManager"] = "CUSTOMER",
            ["CustomerMasterKeySpec"] = rec.KeySpec,
            ["KeySpec"] = rec.KeySpec,
            ["EncryptionAlgorithms"] = rec.EncryptionAlgorithms,
            ["SigningAlgorithms"] = rec.SigningAlgorithms,
        };
    }

    private KmsKeyRecord? ResolveKey(string? keyIdOrArn)
    {
        if (string.IsNullOrEmpty(keyIdOrArn))
        {
            return null;
        }

        // Direct key ID lookup
        if (_keys.TryGetValue(keyIdOrArn, out var direct))
        {
            return direct;
        }

        // ARN lookup
        foreach (var rec in _keys.Values)
        {
            if (string.Equals(rec.Arn, keyIdOrArn, StringComparison.Ordinal))
            {
                return rec;
            }
        }

        // Alias lookup: "alias/my-key" or "arn:aws:kms:...:alias/my-key"
        var aliasName = keyIdOrArn;
        if (aliasName.Contains(":alias/", StringComparison.Ordinal))
        {
            aliasName = "alias/" + aliasName.Split(":alias/")[^1];
        }

        if (_aliases.TryGetValue(aliasName, out var targetKeyId))
        {
            if (_keys.TryGetValue(targetKeyId, out var aliasTarget))
            {
                return aliasTarget;
            }
        }

        return null;
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

    private static Dictionary<string, string> GetEncryptionContext(JsonElement el)
    {
        var result = new Dictionary<string, string>(StringComparer.Ordinal);
        if (el.TryGetProperty("EncryptionContext", out var ctxEl) && ctxEl.ValueKind == JsonValueKind.Object)
        {
            foreach (var prop in ctxEl.EnumerateObject())
            {
                result[prop.Name] = prop.Value.GetString() ?? "";
            }
        }

        return result;
    }

    // -- Crypto helpers --------------------------------------------------------

    private static byte[] DeriveWithContext(byte[] keyBytes, Dictionary<string, string> encContext)
    {
        var sorted = encContext
            .OrderBy(kv => kv.Key, StringComparer.Ordinal)
            .ToDictionary(kv => kv.Key, kv => kv.Value);
        var ctxJson = JsonSerializer.SerializeToUtf8Bytes(sorted);
        var combined = new byte[keyBytes.Length + ctxJson.Length];
        Buffer.BlockCopy(keyBytes, 0, combined, 0, keyBytes.Length);
        Buffer.BlockCopy(ctxJson, 0, combined, keyBytes.Length, ctxJson.Length);
        return SHA256.HashData(combined);
    }

    private static byte[] ExpandKey(byte[] keyBytes, int length)
    {
        var result = new List<byte>();
        var counter = 0;
        while (result.Count < length)
        {
            var counterBytes = BitConverter.GetBytes(counter);
            if (BitConverter.IsLittleEndian)
            {
                Array.Reverse(counterBytes);
            }

            var combined = new byte[keyBytes.Length + counterBytes.Length];
            Buffer.BlockCopy(keyBytes, 0, combined, 0, keyBytes.Length);
            Buffer.BlockCopy(counterBytes, 0, combined, keyBytes.Length, counterBytes.Length);
            result.AddRange(SHA256.HashData(combined));
            counter++;
        }

        return result.GetRange(0, length).ToArray();
    }

    private static byte[] XorBytes(byte[] a, byte[] b)
    {
        var result = new byte[a.Length];
        for (var i = 0; i < a.Length; i++)
        {
            result[i] = (byte)(a[i] ^ b[i]);
        }

        return result;
    }

    private static (RSASignaturePadding? Padding, HashAlgorithmName Hash) GetSigningParams(string algorithm)
    {
        return algorithm switch
        {
            "RSASSA_PKCS1_V1_5_SHA_256" => (RSASignaturePadding.Pkcs1, HashAlgorithmName.SHA256),
            "RSASSA_PKCS1_V1_5_SHA_384" => (RSASignaturePadding.Pkcs1, HashAlgorithmName.SHA384),
            "RSASSA_PKCS1_V1_5_SHA_512" => (RSASignaturePadding.Pkcs1, HashAlgorithmName.SHA512),
            "RSASSA_PSS_SHA_256" => (RSASignaturePadding.Pss, HashAlgorithmName.SHA256),
            "RSASSA_PSS_SHA_384" => (RSASignaturePadding.Pss, HashAlgorithmName.SHA384),
            "RSASSA_PSS_SHA_512" => (RSASignaturePadding.Pss, HashAlgorithmName.SHA512),
            _ => (null, default),
        };
    }

    // -- Actions ---------------------------------------------------------------

    private ServiceResponse ActCreateKey(JsonElement data)
    {
        lock (_lock)
        {
            var keyId = HashHelpers.NewUuid();
            var keySpec = GetString(data, "KeySpec")
                          ?? GetString(data, "CustomerMasterKeySpec")
                          ?? "SYMMETRIC_DEFAULT";
            var keyUsage = GetString(data, "KeyUsage") ?? "ENCRYPT_DECRYPT";
            var description = GetString(data, "Description") ?? "";

            var defaultPolicy = JsonSerializer.Serialize(new Dictionary<string, object>
            {
                ["Version"] = "2012-10-17",
                ["Id"] = "key-default-1",
                ["Statement"] = new[]
                {
                    new Dictionary<string, object>
                    {
                        ["Sid"] = "Enable IAM User Permissions",
                        ["Effect"] = "Allow",
                        ["Principal"] = new Dictionary<string, string>
                        {
                            ["AWS"] = $"arn:aws:iam::{AccountContext.GetAccountId()}:root",
                        },
                        ["Action"] = "kms:*",
                        ["Resource"] = "*",
                    },
                },
            });

            var policy = GetString(data, "Policy") ?? defaultPolicy;

            var rec = new KmsKeyRecord
            {
                KeyId = keyId,
                Arn = MakeArn(keyId),
                KeyState = "Enabled",
                Enabled = true,
                KeySpec = keySpec,
                KeyUsage = keyUsage,
                Description = description,
                CreationDate = TimeHelpers.NowEpoch(),
                Origin = "AWS_KMS",
                Tags = ParseTags(data),
                Policy = policy,
                KeyRotationEnabled = false,
                RotationPeriodInDays = 365,
            };

            if (keySpec == "SYMMETRIC_DEFAULT")
            {
                rec.SymmetricKey = RandomNumberGenerator.GetBytes(32);
                rec.EncryptionAlgorithms = ["SYMMETRIC_DEFAULT"];
                rec.SigningAlgorithms = [];
            }
            else if (keySpec is "RSA_2048" or "RSA_4096")
            {
                var bits = keySpec == "RSA_2048" ? 2048 : 4096;
                var rsa = RSA.Create(bits);
                rec.RsaKey = rsa;
                rec.PublicKeyDer = rsa.ExportSubjectPublicKeyInfo();

                if (keyUsage == "SIGN_VERIFY")
                {
                    rec.SigningAlgorithms =
                    [
                        "RSASSA_PKCS1_V1_5_SHA_256",
                        "RSASSA_PKCS1_V1_5_SHA_384",
                        "RSASSA_PKCS1_V1_5_SHA_512",
                        "RSASSA_PSS_SHA_256",
                        "RSASSA_PSS_SHA_384",
                        "RSASSA_PSS_SHA_512",
                    ];
                    rec.EncryptionAlgorithms = [];
                }
                else
                {
                    rec.EncryptionAlgorithms =
                    [
                        "RSAES_OAEP_SHA_1",
                        "RSAES_OAEP_SHA_256",
                    ];
                    rec.SigningAlgorithms = [];
                }
            }
            else
            {
                return AwsResponseHelpers.ErrorResponseJson(
                    "UnsupportedOperationException",
                    $"KeySpec {keySpec} is not supported in this emulator",
                    400);
            }

            _keys[keyId] = rec;

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>
            {
                ["KeyMetadata"] = BuildKeyMetadata(rec),
            });
        }
    }

    private ServiceResponse ActListKeys(JsonElement data)
    {
        lock (_lock)
        {
            var limit = GetInt(data, "Limit", 1000);
            var keys = new List<Dictionary<string, object?>>();
            var count = 0;

            foreach (var rec in _keys.Values)
            {
                if (count >= limit)
                {
                    break;
                }

                keys.Add(new Dictionary<string, object?>
                {
                    ["KeyId"] = rec.KeyId,
                    ["KeyArn"] = rec.Arn,
                });
                count++;
            }

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>
            {
                ["Keys"] = keys,
                ["Truncated"] = false,
            });
        }
    }

    private ServiceResponse ActDescribeKey(JsonElement data)
    {
        lock (_lock)
        {
            var keyId = GetString(data, "KeyId") ?? "";
            var rec = ResolveKey(keyId);
            if (rec is null)
            {
                return AwsResponseHelpers.ErrorResponseJson("NotFoundException", $"Key {keyId} not found", 400);
            }

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>
            {
                ["KeyMetadata"] = BuildKeyMetadata(rec),
            });
        }
    }

    private ServiceResponse ActGetPublicKey(JsonElement data)
    {
        lock (_lock)
        {
            var keyId = GetString(data, "KeyId") ?? "";
            var rec = ResolveKey(keyId);
            if (rec is null)
            {
                return AwsResponseHelpers.ErrorResponseJson("NotFoundException", $"Key {keyId} not found", 400);
            }

            if (rec.PublicKeyDer is null)
            {
                return AwsResponseHelpers.ErrorResponseJson(
                    "UnsupportedOperationException",
                    "GetPublicKey is only valid for asymmetric keys",
                    400);
            }

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>
            {
                ["KeyId"] = rec.KeyId,
                ["KeyUsage"] = rec.KeyUsage,
                ["KeySpec"] = rec.KeySpec,
                ["PublicKey"] = Convert.ToBase64String(rec.PublicKeyDer),
                ["SigningAlgorithms"] = rec.SigningAlgorithms,
                ["EncryptionAlgorithms"] = rec.EncryptionAlgorithms,
            });
        }
    }

    private ServiceResponse ActSign(JsonElement data)
    {
        lock (_lock)
        {
            var keyId = GetString(data, "KeyId") ?? "";
            var rec = ResolveKey(keyId);
            if (rec is null)
            {
                return AwsResponseHelpers.ErrorResponseJson("NotFoundException", $"Key {keyId} not found", 400);
            }

            if (rec.RsaKey is null)
            {
                return AwsResponseHelpers.ErrorResponseJson(
                    "UnsupportedOperationException",
                    "Sign is only valid for asymmetric SIGN_VERIFY keys",
                    400);
            }

            var messageB64 = GetString(data, "Message") ?? "";
            var algorithm = GetString(data, "SigningAlgorithm") ?? "RSASSA_PKCS1_V1_5_SHA_256";

            var message = Convert.FromBase64String(messageB64);

            var (padding, hash) = GetSigningParams(algorithm);
            if (padding is null)
            {
                return AwsResponseHelpers.ErrorResponseJson(
                    "UnsupportedOperationException",
                    $"Signing algorithm {algorithm} is not supported",
                    400);
            }

            var signature = rec.RsaKey.SignData(message, hash, padding);

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>
            {
                ["KeyId"] = rec.KeyId,
                ["Signature"] = Convert.ToBase64String(signature),
                ["SigningAlgorithm"] = algorithm,
            });
        }
    }

    private ServiceResponse ActVerify(JsonElement data)
    {
        lock (_lock)
        {
            var keyId = GetString(data, "KeyId") ?? "";
            var rec = ResolveKey(keyId);
            if (rec is null)
            {
                return AwsResponseHelpers.ErrorResponseJson("NotFoundException", $"Key {keyId} not found", 400);
            }

            if (rec.RsaKey is null)
            {
                return AwsResponseHelpers.ErrorResponseJson(
                    "UnsupportedOperationException",
                    "Verify is only valid for asymmetric SIGN_VERIFY keys",
                    400);
            }

            var messageB64 = GetString(data, "Message") ?? "";
            var signatureB64 = GetString(data, "Signature") ?? "";
            var algorithm = GetString(data, "SigningAlgorithm") ?? "RSASSA_PKCS1_V1_5_SHA_256";

            var message = Convert.FromBase64String(messageB64);
            var signature = Convert.FromBase64String(signatureB64);

            var (padding, hash) = GetSigningParams(algorithm);
            if (padding is null)
            {
                return AwsResponseHelpers.ErrorResponseJson(
                    "UnsupportedOperationException",
                    $"Signing algorithm {algorithm} is not supported",
                    400);
            }

            var valid = rec.RsaKey.VerifyData(message, signature, hash, padding);

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>
            {
                ["KeyId"] = rec.KeyId,
                ["SignatureValid"] = valid,
                ["SigningAlgorithm"] = algorithm,
            });
        }
    }

    private ServiceResponse ActEncrypt(JsonElement data)
    {
        lock (_lock)
        {
            var keyId = GetString(data, "KeyId") ?? "";
            var rec = ResolveKey(keyId);
            if (rec is null)
            {
                return AwsResponseHelpers.ErrorResponseJson("NotFoundException", $"Key {keyId} not found", 400);
            }

            var plaintextB64 = GetString(data, "Plaintext") ?? "";
            var plaintext = Convert.FromBase64String(plaintextB64);
            var encContext = GetEncryptionContext(data);

            byte[] ciphertext;

            if (rec.SymmetricKey is not null)
            {
                var keyBytes = DeriveWithContext(rec.SymmetricKey, encContext);
                var padStream = ExpandKey(keyBytes, plaintext.Length);
                var encrypted = XorBytes(plaintext, padStream);
                var ctxHash = SHA256.HashData(
                    JsonSerializer.SerializeToUtf8Bytes(
                        encContext.OrderBy(kv => kv.Key, StringComparer.Ordinal)
                                  .ToDictionary(kv => kv.Key, kv => kv.Value)));
                var keyIdBytes = Encoding.UTF8.GetBytes(rec.KeyId);
                ciphertext = new byte[keyIdBytes.Length + ctxHash.Length + encrypted.Length];
                Buffer.BlockCopy(keyIdBytes, 0, ciphertext, 0, keyIdBytes.Length);
                Buffer.BlockCopy(ctxHash, 0, ciphertext, keyIdBytes.Length, ctxHash.Length);
                Buffer.BlockCopy(encrypted, 0, ciphertext, keyIdBytes.Length + ctxHash.Length, encrypted.Length);
            }
            else if (rec.RsaKey is not null && rec.KeyUsage == "ENCRYPT_DECRYPT")
            {
                if (encContext.Count > 0)
                {
                    return AwsResponseHelpers.ErrorResponseJson(
                        "UnsupportedOperationException",
                        "EncryptionContext is not supported with asymmetric keys",
                        400);
                }

                ciphertext = rec.RsaKey.Encrypt(plaintext, RSAEncryptionPadding.OaepSHA256);
            }
            else
            {
                return AwsResponseHelpers.ErrorResponseJson(
                    "UnsupportedOperationException",
                    "This key cannot be used for encryption",
                    400);
            }

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>
            {
                ["KeyId"] = rec.KeyId,
                ["CiphertextBlob"] = Convert.ToBase64String(ciphertext),
                ["EncryptionAlgorithm"] = GetString(data, "EncryptionAlgorithm") ?? "SYMMETRIC_DEFAULT",
            });
        }
    }

    private ServiceResponse ActDecrypt(JsonElement data)
    {
        lock (_lock)
        {
            var ciphertextB64 = GetString(data, "CiphertextBlob") ?? "";
            var ciphertext = Convert.FromBase64String(ciphertextB64);
            var encContext = GetEncryptionContext(data);

            var keyIdFromData = GetString(data, "KeyId") ?? "";
            KmsKeyRecord? rec = null;

            if (!string.IsNullOrEmpty(keyIdFromData))
            {
                rec = ResolveKey(keyIdFromData);
            }

            // Try extracting key ID from ciphertext prefix (symmetric)
            if (rec is null && ciphertext.Length > 68)
            {
                var embeddedId = Encoding.UTF8.GetString(ciphertext, 0, 36);
                rec = ResolveKey(embeddedId);
            }

            if (rec is null)
            {
                return AwsResponseHelpers.ErrorResponseJson(
                    "NotFoundException",
                    "Unable to find the key for decryption",
                    400);
            }

            byte[] plaintext;

            if (rec.SymmetricKey is not null)
            {
                var storedCtxHash = new byte[32];
                Buffer.BlockCopy(ciphertext, 36, storedCtxHash, 0, 32);

                var providedCtxHash = SHA256.HashData(
                    JsonSerializer.SerializeToUtf8Bytes(
                        encContext.OrderBy(kv => kv.Key, StringComparer.Ordinal)
                                  .ToDictionary(kv => kv.Key, kv => kv.Value)));

                if (!storedCtxHash.AsSpan().SequenceEqual(providedCtxHash))
                {
                    return AwsResponseHelpers.ErrorResponseJson(
                        "InvalidCiphertextException",
                        "EncryptionContext does not match",
                        400);
                }

                var encryptedData = new byte[ciphertext.Length - 68];
                Buffer.BlockCopy(ciphertext, 68, encryptedData, 0, encryptedData.Length);

                var keyBytes = DeriveWithContext(rec.SymmetricKey, encContext);
                var padStream = ExpandKey(keyBytes, encryptedData.Length);
                plaintext = XorBytes(encryptedData, padStream);
            }
            else if (rec.RsaKey is not null)
            {
                if (encContext.Count > 0)
                {
                    return AwsResponseHelpers.ErrorResponseJson(
                        "UnsupportedOperationException",
                        "EncryptionContext is not supported with asymmetric keys",
                        400);
                }

                try
                {
                    plaintext = rec.RsaKey.Decrypt(ciphertext, RSAEncryptionPadding.OaepSHA256);
                }
                catch (CryptographicException e)
                {
                    return AwsResponseHelpers.ErrorResponseJson(
                        "InvalidCiphertextException",
                        e.Message,
                        400);
                }
            }
            else
            {
                return AwsResponseHelpers.ErrorResponseJson(
                    "UnsupportedOperationException",
                    "This key cannot be used for decryption",
                    400);
            }

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>
            {
                ["KeyId"] = rec.KeyId,
                ["Plaintext"] = Convert.ToBase64String(plaintext),
                ["EncryptionAlgorithm"] = GetString(data, "EncryptionAlgorithm") ?? "SYMMETRIC_DEFAULT",
            });
        }
    }

    private (KmsKeyRecord? Record, byte[]? DataKey, object? ErrorOrCiphertext) GenerateDataKeyCommon(JsonElement data)
    {
        var keyId = GetString(data, "KeyId") ?? "";
        var rec = ResolveKey(keyId);
        if (rec is null)
        {
            return (null, null, AwsResponseHelpers.ErrorResponseJson("NotFoundException", $"Key {keyId} not found", 400));
        }

        if (rec.SymmetricKey is null)
        {
            return (null, null, AwsResponseHelpers.ErrorResponseJson(
                "UnsupportedOperationException",
                "GenerateDataKey requires a symmetric key",
                400));
        }

        var spec = GetString(data, "KeySpec") ?? "AES_256";
        byte[] dataKey;

        if (data.TryGetProperty("NumberOfBytes", out var nobProp) && nobProp.TryGetInt32(out var numberOfBytes))
        {
            dataKey = RandomNumberGenerator.GetBytes(numberOfBytes);
        }
        else if (spec == "AES_128")
        {
            dataKey = RandomNumberGenerator.GetBytes(16);
        }
        else
        {
            dataKey = RandomNumberGenerator.GetBytes(32);
        }

        var encContext = GetEncryptionContext(data);
        var cmkBytes = DeriveWithContext(rec.SymmetricKey, encContext);
        var padStream = ExpandKey(cmkBytes, dataKey.Length);
        var encrypted = XorBytes(dataKey, padStream);
        var ctxHash = SHA256.HashData(
            JsonSerializer.SerializeToUtf8Bytes(
                encContext.OrderBy(kv => kv.Key, StringComparer.Ordinal)
                          .ToDictionary(kv => kv.Key, kv => kv.Value)));

        var keyIdBytes = Encoding.UTF8.GetBytes(rec.KeyId);
        var ciphertext = new byte[keyIdBytes.Length + ctxHash.Length + encrypted.Length];
        Buffer.BlockCopy(keyIdBytes, 0, ciphertext, 0, keyIdBytes.Length);
        Buffer.BlockCopy(ctxHash, 0, ciphertext, keyIdBytes.Length, ctxHash.Length);
        Buffer.BlockCopy(encrypted, 0, ciphertext, keyIdBytes.Length + ctxHash.Length, encrypted.Length);

        return (rec, dataKey, ciphertext);
    }

    private ServiceResponse ActGenerateDataKey(JsonElement data)
    {
        lock (_lock)
        {
            var (rec, dataKey, result) = GenerateDataKeyCommon(data);
            if (rec is null)
            {
                return (ServiceResponse)result!;
            }

            var ciphertext = (byte[])result!;

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>
            {
                ["KeyId"] = rec.KeyId,
                ["Plaintext"] = Convert.ToBase64String(dataKey!),
                ["CiphertextBlob"] = Convert.ToBase64String(ciphertext),
            });
        }
    }

    private ServiceResponse ActGenerateDataKeyWithoutPlaintext(JsonElement data)
    {
        lock (_lock)
        {
            var (rec, _, result) = GenerateDataKeyCommon(data);
            if (rec is null)
            {
                return (ServiceResponse)result!;
            }

            var ciphertext = (byte[])result!;

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>
            {
                ["KeyId"] = rec.KeyId,
                ["CiphertextBlob"] = Convert.ToBase64String(ciphertext),
            });
        }
    }

    // -- Alias operations ------------------------------------------------------

    private ServiceResponse ActCreateAlias(JsonElement data)
    {
        lock (_lock)
        {
            var aliasName = GetString(data, "AliasName") ?? "";
            var targetKeyId = GetString(data, "TargetKeyId") ?? "";

            if (string.IsNullOrEmpty(aliasName) || !aliasName.StartsWith("alias/", StringComparison.Ordinal))
            {
                return AwsResponseHelpers.ErrorResponseJson(
                    "ValidationException", "AliasName must start with alias/", 400);
            }

            if (string.IsNullOrEmpty(targetKeyId))
            {
                return AwsResponseHelpers.ErrorResponseJson(
                    "ValidationException", "TargetKeyId is required", 400);
            }

            var rec = ResolveKey(targetKeyId);
            if (rec is null)
            {
                return AwsResponseHelpers.ErrorResponseJson(
                    "NotFoundException", $"Key {targetKeyId} not found", 400);
            }

            if (_aliases.ContainsKey(aliasName))
            {
                return AwsResponseHelpers.ErrorResponseJson(
                    "AlreadyExistsException", $"Alias {aliasName} already exists", 400);
            }

            _aliases[aliasName] = rec.KeyId;

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>());
        }
    }

    private ServiceResponse ActDeleteAlias(JsonElement data)
    {
        lock (_lock)
        {
            var aliasName = GetString(data, "AliasName") ?? "";

            if (!_aliases.ContainsKey(aliasName))
            {
                return AwsResponseHelpers.ErrorResponseJson(
                    "NotFoundException", $"Alias {aliasName} not found", 400);
            }

            _aliases.TryRemove(aliasName, out _);

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>());
        }
    }

    private ServiceResponse ActListAliases(JsonElement data)
    {
        lock (_lock)
        {
            var filterKeyId = GetString(data, "KeyId");
            var items = new List<Dictionary<string, object?>>();

            foreach (var (aliasName, targetId) in _aliases.Items)
            {
                if (filterKeyId is not null)
                {
                    if (targetId != filterKeyId)
                    {
                        var filterRec = ResolveKey(filterKeyId);
                        if (filterRec is null || filterRec.KeyId != targetId)
                        {
                            continue;
                        }
                    }
                }

                items.Add(new Dictionary<string, object?>
                {
                    ["AliasName"] = aliasName,
                    ["AliasArn"] = $"arn:aws:kms:{Region}:{AccountContext.GetAccountId()}:{aliasName}",
                    ["TargetKeyId"] = targetId,
                });
            }

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>
            {
                ["Aliases"] = items,
                ["Truncated"] = false,
            });
        }
    }

    private ServiceResponse ActUpdateAlias(JsonElement data)
    {
        lock (_lock)
        {
            var aliasName = GetString(data, "AliasName") ?? "";
            var targetKeyId = GetString(data, "TargetKeyId") ?? "";

            if (!_aliases.ContainsKey(aliasName))
            {
                return AwsResponseHelpers.ErrorResponseJson(
                    "NotFoundException", $"Alias {aliasName} not found", 400);
            }

            var rec = ResolveKey(targetKeyId);
            if (rec is null)
            {
                return AwsResponseHelpers.ErrorResponseJson(
                    "NotFoundException", $"Key {targetKeyId} not found", 400);
            }

            _aliases[aliasName] = rec.KeyId;

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>());
        }
    }

    // -- Key Rotation ----------------------------------------------------------

    private ServiceResponse ActEnableKeyRotation(JsonElement data)
    {
        lock (_lock)
        {
            var keyId = GetString(data, "KeyId") ?? "";
            var rec = ResolveKey(keyId);
            if (rec is null)
            {
                return AwsResponseHelpers.ErrorResponseJson("NotFoundException", $"Key {keyId} not found", 400);
            }

            rec.KeyRotationEnabled = true;
            rec.RotationPeriodInDays = GetInt(data, "RotationPeriodInDays", 365);

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>());
        }
    }

    private ServiceResponse ActDisableKeyRotation(JsonElement data)
    {
        lock (_lock)
        {
            var keyId = GetString(data, "KeyId") ?? "";
            var rec = ResolveKey(keyId);
            if (rec is null)
            {
                return AwsResponseHelpers.ErrorResponseJson("NotFoundException", $"Key {keyId} not found", 400);
            }

            rec.KeyRotationEnabled = false;

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>());
        }
    }

    private ServiceResponse ActGetKeyRotationStatus(JsonElement data)
    {
        lock (_lock)
        {
            var keyId = GetString(data, "KeyId") ?? "";
            var rec = ResolveKey(keyId);
            if (rec is null)
            {
                return AwsResponseHelpers.ErrorResponseJson("NotFoundException", $"Key {keyId} not found", 400);
            }

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>
            {
                ["KeyRotationEnabled"] = rec.KeyRotationEnabled,
                ["RotationPeriodInDays"] = rec.RotationPeriodInDays,
            });
        }
    }

    // -- Key Policy ------------------------------------------------------------

    private ServiceResponse ActGetKeyPolicy(JsonElement data)
    {
        lock (_lock)
        {
            var keyId = GetString(data, "KeyId") ?? "";
            var rec = ResolveKey(keyId);
            if (rec is null)
            {
                return AwsResponseHelpers.ErrorResponseJson("NotFoundException", $"Key {keyId} not found", 400);
            }

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>
            {
                ["Policy"] = rec.Policy,
                ["PolicyName"] = "default",
            });
        }
    }

    private ServiceResponse ActPutKeyPolicy(JsonElement data)
    {
        lock (_lock)
        {
            var keyId = GetString(data, "KeyId") ?? "";
            var rec = ResolveKey(keyId);
            if (rec is null)
            {
                return AwsResponseHelpers.ErrorResponseJson("NotFoundException", $"Key {keyId} not found", 400);
            }

            rec.Policy = GetString(data, "Policy") ?? "";

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>());
        }
    }

    private ServiceResponse ActListKeyPolicies(JsonElement data)
    {
        lock (_lock)
        {
            var keyId = GetString(data, "KeyId") ?? "";
            var rec = ResolveKey(keyId);
            if (rec is null)
            {
                return AwsResponseHelpers.ErrorResponseJson("NotFoundException", $"Key {keyId} not found", 400);
            }

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>
            {
                ["PolicyNames"] = new List<string> { "default" },
                ["Truncated"] = false,
            });
        }
    }

    // -- Enable / Disable / Schedule Deletion ----------------------------------

    private ServiceResponse ActEnableKey(JsonElement data)
    {
        lock (_lock)
        {
            var keyId = GetString(data, "KeyId") ?? "";
            var rec = ResolveKey(keyId);
            if (rec is null)
            {
                return AwsResponseHelpers.ErrorResponseJson("NotFoundException", $"Key {keyId} not found", 400);
            }

            rec.Enabled = true;
            rec.KeyState = "Enabled";

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>());
        }
    }

    private ServiceResponse ActDisableKey(JsonElement data)
    {
        lock (_lock)
        {
            var keyId = GetString(data, "KeyId") ?? "";
            var rec = ResolveKey(keyId);
            if (rec is null)
            {
                return AwsResponseHelpers.ErrorResponseJson("NotFoundException", $"Key {keyId} not found", 400);
            }

            rec.Enabled = false;
            rec.KeyState = "Disabled";

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>());
        }
    }

    private ServiceResponse ActScheduleKeyDeletion(JsonElement data)
    {
        lock (_lock)
        {
            var keyId = GetString(data, "KeyId") ?? "";
            var rec = ResolveKey(keyId);
            if (rec is null)
            {
                return AwsResponseHelpers.ErrorResponseJson("NotFoundException", $"Key {keyId} not found", 400);
            }

            var days = GetInt(data, "PendingWindowInDays", 30);
            rec.KeyState = "PendingDeletion";
            rec.Enabled = false;
            rec.DeletionDate = TimeHelpers.NowEpoch() + (days * 86400);

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>
            {
                ["KeyId"] = rec.KeyId,
                ["KeyState"] = "PendingDeletion",
                ["DeletionDate"] = rec.DeletionDate,
            });
        }
    }

    private ServiceResponse ActCancelKeyDeletion(JsonElement data)
    {
        lock (_lock)
        {
            var keyId = GetString(data, "KeyId") ?? "";
            var rec = ResolveKey(keyId);
            if (rec is null)
            {
                return AwsResponseHelpers.ErrorResponseJson("NotFoundException", $"Key {keyId} not found", 400);
            }

            rec.KeyState = "Disabled";
            rec.DeletionDate = null;

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>
            {
                ["KeyId"] = rec.KeyId,
            });
        }
    }

    // -- Tags ------------------------------------------------------------------

    private ServiceResponse ActTagResource(JsonElement data)
    {
        lock (_lock)
        {
            var keyId = GetString(data, "KeyId") ?? "";
            var rec = ResolveKey(keyId);
            if (rec is null)
            {
                return AwsResponseHelpers.ErrorResponseJson("NotFoundException", $"Key {keyId} not found", 400);
            }

            if (data.TryGetProperty("Tags", out var tagsEl) && tagsEl.ValueKind == JsonValueKind.Array)
            {
                foreach (var tagEl in tagsEl.EnumerateArray())
                {
                    var tagKey = GetString(tagEl, "TagKey");
                    var tagValue = GetString(tagEl, "TagValue") ?? "";

                    if (tagKey is null)
                    {
                        continue;
                    }

                    var existing = rec.Tags.Find(t => t.TagKey == tagKey);
                    if (existing is not null)
                    {
                        existing.TagValue = tagValue;
                    }
                    else
                    {
                        rec.Tags.Add(new KmsTag { TagKey = tagKey, TagValue = tagValue });
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
            var keyId = GetString(data, "KeyId") ?? "";
            var rec = ResolveKey(keyId);
            if (rec is null)
            {
                return AwsResponseHelpers.ErrorResponseJson("NotFoundException", $"Key {keyId} not found", 400);
            }

            var removeKeys = new HashSet<string>(StringComparer.Ordinal);
            if (data.TryGetProperty("TagKeys", out var keysEl) && keysEl.ValueKind == JsonValueKind.Array)
            {
                foreach (var item in keysEl.EnumerateArray())
                {
                    var s = item.GetString();
                    if (s is not null)
                    {
                        removeKeys.Add(s);
                    }
                }
            }

            rec.Tags = rec.Tags.FindAll(t => !removeKeys.Contains(t.TagKey));

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>());
        }
    }

    private ServiceResponse ActListResourceTags(JsonElement data)
    {
        lock (_lock)
        {
            var keyId = GetString(data, "KeyId") ?? "";
            var rec = ResolveKey(keyId);
            if (rec is null)
            {
                return AwsResponseHelpers.ErrorResponseJson("NotFoundException", $"Key {keyId} not found", 400);
            }

            var tags = rec.Tags.ConvertAll(t => new Dictionary<string, string>
            {
                ["TagKey"] = t.TagKey,
                ["TagValue"] = t.TagValue,
            });

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>
            {
                ["Tags"] = tags,
                ["Truncated"] = false,
            });
        }
    }

    // -- Tag parsing -----------------------------------------------------------

    private static List<KmsTag> ParseTags(JsonElement data)
    {
        var tags = new List<KmsTag>();
        if (data.TryGetProperty("Tags", out var tagsEl) && tagsEl.ValueKind == JsonValueKind.Array)
        {
            foreach (var tagEl in tagsEl.EnumerateArray())
            {
                var tagKey = GetString(tagEl, "TagKey");
                var tagValue = GetString(tagEl, "TagValue") ?? "";
                if (tagKey is not null)
                {
                    tags.Add(new KmsTag { TagKey = tagKey, TagValue = tagValue });
                }
            }
        }

        return tags;
    }
}

// -- Data models ---------------------------------------------------------------

internal sealed class KmsKeyRecord
{
    internal required string KeyId { get; set; }
    internal required string Arn { get; set; }
    internal required string KeyState { get; set; }
    internal required string KeyUsage { get; set; }
    internal required string KeySpec { get; set; }
    internal required string Description { get; set; }
    internal required string Origin { get; set; }
    internal required bool Enabled { get; set; }
    internal required double CreationDate { get; set; }
    internal List<KmsTag> Tags { get; set; } = [];
    internal string? Policy { get; set; }
    internal bool KeyRotationEnabled { get; set; }
    internal int RotationPeriodInDays { get; set; } = 365;
    internal double? DeletionDate { get; set; }
    internal byte[]? SymmetricKey { get; set; }
    internal RSA? RsaKey { get; set; }
    internal byte[]? PublicKeyDer { get; set; }
    internal List<string> EncryptionAlgorithms { get; set; } = [];
    internal List<string> SigningAlgorithms { get; set; } = [];
}

internal sealed class KmsTag
{
    internal required string TagKey { get; set; }
    internal required string TagValue { get; set; }
}
