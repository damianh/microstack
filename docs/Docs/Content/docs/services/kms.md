---
title: KMS
description: KMS emulation — symmetric and RSA keys, encrypt/decrypt, sign/verify, aliases, data keys.
order: 12
section: Services
---

# KMS

MicroStack's KMS handler supports symmetric (AES-256) and RSA (2048/4096) keys with full encrypt/decrypt and sign/verify operations. Key aliases, rotation status, and key policies are all supported.

## Supported Operations

- CreateKey, ListKeys, DescribeKey, GetPublicKey
- Encrypt, Decrypt, GenerateDataKey, GenerateDataKeyWithoutPlaintext
- Sign, Verify
- CreateAlias, DeleteAlias, ListAliases, UpdateAlias
- EnableKeyRotation, DisableKeyRotation, GetKeyRotationStatus
- GetKeyPolicy, PutKeyPolicy, ListKeyPolicies
- EnableKey, DisableKey, ScheduleKeyDeletion, CancelKeyDeletion
- TagResource, UntagResource, ListResourceTags

## Usage

```csharp
var kms = new AmazonKeyManagementServiceClient(
    new BasicAWSCredentials("test", "test"),
    new AmazonKeyManagementServiceConfig { ServiceURL = "http://localhost:4566" });

// Create a symmetric encryption key
var key = await kms.CreateKeyAsync(new CreateKeyRequest
{
    KeySpec = KeySpec.SYMMETRIC_DEFAULT,
    KeyUsage = KeyUsageType.ENCRYPT_DECRYPT,
    Description = "my encryption key",
});
var keyId = key.KeyMetadata.KeyId;

// Encrypt data
var plaintext = "sensitive data"u8.ToArray();
var encrypted = await kms.EncryptAsync(new EncryptRequest
{
    KeyId = keyId,
    Plaintext = new MemoryStream(plaintext),
});

// Decrypt data
var decrypted = await kms.DecryptAsync(new DecryptRequest
{
    CiphertextBlob = new MemoryStream(encrypted.CiphertextBlob.ToArray()),
});

Console.WriteLine(System.Text.Encoding.UTF8.GetString(decrypted.Plaintext.ToArray()));
// Output: sensitive data
```

## RSA Sign and Verify

```csharp
// Create an RSA signing key
var rsaKey = await kms.CreateKeyAsync(new CreateKeyRequest
{
    KeySpec = KeySpec.RSA_2048,
    KeyUsage = KeyUsageType.SIGN_VERIFY,
});
var rsaKeyId = rsaKey.KeyMetadata.KeyId;

var message = "document to sign"u8.ToArray();

// Sign
var signed = await kms.SignAsync(new SignRequest
{
    KeyId = rsaKeyId,
    Message = new MemoryStream(message),
    MessageType = MessageType.RAW,
    SigningAlgorithm = SigningAlgorithmSpec.RSASSA_PKCS1_V1_5_SHA_256,
});

// Verify
var verified = await kms.VerifyAsync(new VerifyRequest
{
    KeyId = rsaKeyId,
    Message = new MemoryStream(message),
    MessageType = MessageType.RAW,
    Signature = new MemoryStream(signed.Signature.ToArray()),
    SigningAlgorithm = SigningAlgorithmSpec.RSASSA_PKCS1_V1_5_SHA_256,
});

Console.WriteLine(verified.SignatureValid); // True
```

## Aliases

```csharp
// Create an alias for a key
await kms.CreateAliasAsync(new CreateAliasRequest
{
    AliasName = "alias/my-app-key",
    TargetKeyId = keyId,
});

// Reference the key by alias in encrypt/decrypt operations
var encrypted = await kms.EncryptAsync(new EncryptRequest
{
    KeyId = "alias/my-app-key",
    Plaintext = new MemoryStream("data"u8.ToArray()),
});
```

:::aside{type="note" title="Supported key types"}
Supported key specs: `SYMMETRIC_DEFAULT` (AES-256-GCM), `RSA_2048`, `RSA_4096`. Signing algorithms: `RSASSA_PKCS1_V1_5_SHA_256`, `RSASSA_PSS_SHA_256`, `RSASSA_PKCS1_V1_5_SHA_384`, `RSASSA_PSS_SHA_384`, `RSASSA_PKCS1_V1_5_SHA_512`, `RSASSA_PSS_SHA_512`.
:::
