using Amazon;
using Amazon.KeyManagementService;
using Amazon.KeyManagementService.Model;
using Amazon.Runtime;

namespace MicroStack.Tests;

/// <summary>
/// Integration tests for the KMS service handler.
/// Uses the AWS SDK for .NET pointed at the in-process MicroStack server.
///
/// Mirrors coverage from ministack tests/test_kms.py (33 test cases).
/// </summary>
public sealed class KmsTests(MicroStackFixture fixture) : IClassFixture<MicroStackFixture>, IAsyncLifetime
{
    private readonly AmazonKeyManagementServiceClient _kms = CreateClient(fixture);

    private static AmazonKeyManagementServiceClient CreateClient(MicroStackFixture fixture)
    {
        var innerHandler = fixture.Factory.Server.CreateHandler();
        var httpClient = new HttpClient(new CanonicalizeUriHandler(innerHandler))
        {
            BaseAddress = new Uri("http://localhost/"),
        };

        var config = new AmazonKeyManagementServiceConfig
        {
            RegionEndpoint = RegionEndpoint.USEast1,
            ServiceURL = "http://localhost/",
            HttpClientFactory = new FixedHttpClientFactory(httpClient),
        };

        return new AmazonKeyManagementServiceClient(
            new BasicAWSCredentials("test", "test"), config);
    }

    public async ValueTask InitializeAsync()
    {
        await fixture.HttpClient.PostAsync("/_microstack/reset", null);
    }

    public ValueTask DisposeAsync()
    {
        _kms.Dispose();
        return ValueTask.CompletedTask;
    }

    // -- CreateKey tests -------------------------------------------------------

    [Fact]
    public async Task CreateSymmetricKey()
    {
        var resp = await _kms.CreateKeyAsync(new CreateKeyRequest
        {
            KeySpec = KeySpec.SYMMETRIC_DEFAULT,
            KeyUsage = KeyUsageType.ENCRYPT_DECRYPT,
            Description = "test symmetric key",
            Tags = [new Tag { TagKey = "env", TagValue = "test" }],
            Policy = "{}",
        });

        var meta = resp.KeyMetadata;
        meta.KeyId.ShouldNotBeEmpty();
        meta.Arn.ShouldStartWith("arn:aws:kms:");
        meta.KeySpec.ShouldBe(KeySpec.SYMMETRIC_DEFAULT);
        meta.KeyUsage.ShouldBe(KeyUsageType.ENCRYPT_DECRYPT);
        meta.Enabled.ShouldBe(true);
        meta.KeyState.ShouldBe(KeyState.Enabled);
        meta.Description.ShouldBe("test symmetric key");

        var tags = await _kms.ListResourceTagsAsync(new ListResourceTagsRequest { KeyId = meta.KeyId });
        tags.Tags.ShouldContain(t => t.TagKey == "env" && t.TagValue == "test");

        var policy = await _kms.GetKeyPolicyAsync(new GetKeyPolicyRequest
        {
            KeyId = meta.KeyId,
            PolicyName = "default",
        });
        policy.Policy.ShouldBe("{}");
    }

    [Fact]
    public async Task CreateRsa2048SignKey()
    {
        var resp = await _kms.CreateKeyAsync(new CreateKeyRequest
        {
            KeySpec = KeySpec.RSA_2048,
            KeyUsage = KeyUsageType.SIGN_VERIFY,
            Description = "test RSA signing key",
        });

        var meta = resp.KeyMetadata;
        meta.KeySpec.ShouldBe(KeySpec.RSA_2048);
        meta.KeyUsage.ShouldBe(KeyUsageType.SIGN_VERIFY);
        meta.SigningAlgorithms.ShouldContain("RSASSA_PKCS1_V1_5_SHA_256");
    }

    [Fact]
    public async Task CreateRsa4096EncryptKey()
    {
        var resp = await _kms.CreateKeyAsync(new CreateKeyRequest
        {
            KeySpec = KeySpec.RSA_4096,
            KeyUsage = KeyUsageType.ENCRYPT_DECRYPT,
        });

        var meta = resp.KeyMetadata;
        meta.KeySpec.ShouldBe(KeySpec.RSA_4096);
        meta.EncryptionAlgorithms.ShouldContain("RSAES_OAEP_SHA_256");
    }

    // -- ListKeys --------------------------------------------------------------

    [Fact]
    public async Task ListKeys()
    {
        var created = await _kms.CreateKeyAsync(new CreateKeyRequest
        {
            KeySpec = KeySpec.SYMMETRIC_DEFAULT,
        });
        var keyId = created.KeyMetadata.KeyId;

        var resp = await _kms.ListKeysAsync(new ListKeysRequest());
        var keyIds = resp.Keys.Select(k => k.KeyId).ToList();
        keyIds.ShouldContain(keyId);
    }

    // -- DescribeKey -----------------------------------------------------------

    [Fact]
    public async Task DescribeKey()
    {
        var created = await _kms.CreateKeyAsync(new CreateKeyRequest
        {
            KeySpec = KeySpec.SYMMETRIC_DEFAULT,
            Description = "describe me",
        });
        var keyId = created.KeyMetadata.KeyId;

        var resp = await _kms.DescribeKeyAsync(new DescribeKeyRequest { KeyId = keyId });
        resp.KeyMetadata.Description.ShouldBe("describe me");
        resp.KeyMetadata.KeyId.ShouldBe(keyId);
    }

    [Fact]
    public async Task DescribeKeyByArn()
    {
        var created = await _kms.CreateKeyAsync(new CreateKeyRequest
        {
            KeySpec = KeySpec.SYMMETRIC_DEFAULT,
        });
        var arn = created.KeyMetadata.Arn;

        var resp = await _kms.DescribeKeyAsync(new DescribeKeyRequest { KeyId = arn });
        resp.KeyMetadata.Arn.ShouldBe(arn);
    }

    [Fact]
    public async Task DescribeNonexistentKey()
    {
        await Should.ThrowAsync<NotFoundException>(() =>
            _kms.DescribeKeyAsync(new DescribeKeyRequest { KeyId = "nonexistent-key-id" }));
    }

    // -- Sign and Verify -------------------------------------------------------

    [Fact]
    public async Task SignAndVerifyPkcs1()
    {
        var key = await _kms.CreateKeyAsync(new CreateKeyRequest
        {
            KeySpec = KeySpec.RSA_2048,
            KeyUsage = KeyUsageType.SIGN_VERIFY,
        });
        var keyId = key.KeyMetadata.KeyId;
        var message = "header.payload"u8.ToArray();

        var signResp = await _kms.SignAsync(new SignRequest
        {
            KeyId = keyId,
            Message = new MemoryStream(message),
            MessageType = MessageType.RAW,
            SigningAlgorithm = SigningAlgorithmSpec.RSASSA_PKCS1_V1_5_SHA_256,
        });

        signResp.KeyId.ShouldBe(keyId);
        signResp.SigningAlgorithm.ShouldBe(SigningAlgorithmSpec.RSASSA_PKCS1_V1_5_SHA_256);
        (signResp.Signature.Length > 0).ShouldBe(true);

        var verifyResp = await _kms.VerifyAsync(new VerifyRequest
        {
            KeyId = keyId,
            Message = new MemoryStream(message),
            MessageType = MessageType.RAW,
            Signature = new MemoryStream(signResp.Signature.ToArray()),
            SigningAlgorithm = SigningAlgorithmSpec.RSASSA_PKCS1_V1_5_SHA_256,
        });

        verifyResp.SignatureValid.ShouldBe(true);
    }

    [Fact]
    public async Task SignAndVerifyPss()
    {
        var key = await _kms.CreateKeyAsync(new CreateKeyRequest
        {
            KeySpec = KeySpec.RSA_2048,
            KeyUsage = KeyUsageType.SIGN_VERIFY,
        });
        var keyId = key.KeyMetadata.KeyId;
        var message = "test-pss-message"u8.ToArray();

        var signResp = await _kms.SignAsync(new SignRequest
        {
            KeyId = keyId,
            Message = new MemoryStream(message),
            MessageType = MessageType.RAW,
            SigningAlgorithm = SigningAlgorithmSpec.RSASSA_PSS_SHA_256,
        });

        var verifyResp = await _kms.VerifyAsync(new VerifyRequest
        {
            KeyId = keyId,
            Message = new MemoryStream(message),
            MessageType = MessageType.RAW,
            Signature = new MemoryStream(signResp.Signature.ToArray()),
            SigningAlgorithm = SigningAlgorithmSpec.RSASSA_PSS_SHA_256,
        });

        verifyResp.SignatureValid.ShouldBe(true);
    }

    [Fact]
    public async Task VerifyWrongMessage()
    {
        var key = await _kms.CreateKeyAsync(new CreateKeyRequest
        {
            KeySpec = KeySpec.RSA_2048,
            KeyUsage = KeyUsageType.SIGN_VERIFY,
        });
        var keyId = key.KeyMetadata.KeyId;

        var signResp = await _kms.SignAsync(new SignRequest
        {
            KeyId = keyId,
            Message = new MemoryStream("original"u8.ToArray()),
            MessageType = MessageType.RAW,
            SigningAlgorithm = SigningAlgorithmSpec.RSASSA_PKCS1_V1_5_SHA_256,
        });

        var verifyResp = await _kms.VerifyAsync(new VerifyRequest
        {
            KeyId = keyId,
            Message = new MemoryStream("tampered"u8.ToArray()),
            MessageType = MessageType.RAW,
            Signature = new MemoryStream(signResp.Signature.ToArray()),
            SigningAlgorithm = SigningAlgorithmSpec.RSASSA_PKCS1_V1_5_SHA_256,
        });

        verifyResp.SignatureValid.ShouldBe(false);
    }

    [Fact]
    public async Task JwtSigningFlow()
    {
        var key = await _kms.CreateKeyAsync(new CreateKeyRequest
        {
            KeySpec = KeySpec.RSA_2048,
            KeyUsage = KeyUsageType.SIGN_VERIFY,
        });
        var keyId = key.KeyMetadata.KeyId;

        var header = Convert.ToBase64String("{\"alg\":\"RS256\",\"typ\":\"JWT\"}"u8.ToArray())
            .TrimEnd('=');
        var payload = Convert.ToBase64String("{\"sub\":\"user-2001\",\"iss\":\"auth-service\"}"u8.ToArray())
            .TrimEnd('=');
        var signingInput = $"{header}.{payload}";
        var signingInputBytes = System.Text.Encoding.UTF8.GetBytes(signingInput);

        var signResp = await _kms.SignAsync(new SignRequest
        {
            KeyId = keyId,
            Message = new MemoryStream(signingInputBytes),
            MessageType = MessageType.RAW,
            SigningAlgorithm = SigningAlgorithmSpec.RSASSA_PKCS1_V1_5_SHA_256,
        });

        (signResp.Signature.Length > 0).ShouldBe(true);

        var verifyResp = await _kms.VerifyAsync(new VerifyRequest
        {
            KeyId = keyId,
            Message = new MemoryStream(signingInputBytes),
            MessageType = MessageType.RAW,
            Signature = new MemoryStream(signResp.Signature.ToArray()),
            SigningAlgorithm = SigningAlgorithmSpec.RSASSA_PKCS1_V1_5_SHA_256,
        });

        verifyResp.SignatureValid.ShouldBe(true);
    }

    // -- Encrypt / Decrypt -----------------------------------------------------

    [Fact]
    public async Task EncryptDecryptRoundtrip()
    {
        var key = await _kms.CreateKeyAsync(new CreateKeyRequest
        {
            KeySpec = KeySpec.SYMMETRIC_DEFAULT,
            KeyUsage = KeyUsageType.ENCRYPT_DECRYPT,
        });
        var keyId = key.KeyMetadata.KeyId;
        var plaintext = "sensitive document content"u8.ToArray();

        var encResp = await _kms.EncryptAsync(new EncryptRequest
        {
            KeyId = keyId,
            Plaintext = new MemoryStream(plaintext),
        });
        encResp.KeyId.ShouldBe(keyId);

        var decResp = await _kms.DecryptAsync(new DecryptRequest
        {
            CiphertextBlob = new MemoryStream(encResp.CiphertextBlob.ToArray()),
        });
        decResp.Plaintext.ToArray().ShouldBe(plaintext);
    }

    [Fact]
    public async Task EncryptDecryptWithExplicitKey()
    {
        var key = await _kms.CreateKeyAsync(new CreateKeyRequest
        {
            KeySpec = KeySpec.SYMMETRIC_DEFAULT,
            KeyUsage = KeyUsageType.ENCRYPT_DECRYPT,
        });
        var keyId = key.KeyMetadata.KeyId;
        var plaintext = "another secret"u8.ToArray();

        var encResp = await _kms.EncryptAsync(new EncryptRequest
        {
            KeyId = keyId,
            Plaintext = new MemoryStream(plaintext),
        });

        var decResp = await _kms.DecryptAsync(new DecryptRequest
        {
            KeyId = keyId,
            CiphertextBlob = new MemoryStream(encResp.CiphertextBlob.ToArray()),
        });
        decResp.Plaintext.ToArray().ShouldBe(plaintext);
    }

    // -- GenerateDataKey -------------------------------------------------------

    [Fact]
    public async Task GenerateDataKeyAes256()
    {
        var key = await _kms.CreateKeyAsync(new CreateKeyRequest
        {
            KeySpec = KeySpec.SYMMETRIC_DEFAULT,
            KeyUsage = KeyUsageType.ENCRYPT_DECRYPT,
        });
        var keyId = key.KeyMetadata.KeyId;

        var resp = await _kms.GenerateDataKeyAsync(new GenerateDataKeyRequest
        {
            KeyId = keyId,
            KeySpec = DataKeySpec.AES_256,
        });

        resp.KeyId.ShouldBe(keyId);
        resp.Plaintext.Length.ShouldBe(32);
        (resp.CiphertextBlob.Length > 0).ShouldBe(true);
    }

    [Fact]
    public async Task GenerateDataKeyAes128()
    {
        var key = await _kms.CreateKeyAsync(new CreateKeyRequest
        {
            KeySpec = KeySpec.SYMMETRIC_DEFAULT,
            KeyUsage = KeyUsageType.ENCRYPT_DECRYPT,
        });
        var keyId = key.KeyMetadata.KeyId;

        var resp = await _kms.GenerateDataKeyAsync(new GenerateDataKeyRequest
        {
            KeyId = keyId,
            KeySpec = DataKeySpec.AES_128,
        });

        resp.Plaintext.Length.ShouldBe(16);
    }

    [Fact]
    public async Task GenerateDataKeyDecryptRoundtrip()
    {
        var key = await _kms.CreateKeyAsync(new CreateKeyRequest
        {
            KeySpec = KeySpec.SYMMETRIC_DEFAULT,
            KeyUsage = KeyUsageType.ENCRYPT_DECRYPT,
        });
        var keyId = key.KeyMetadata.KeyId;

        var genResp = await _kms.GenerateDataKeyAsync(new GenerateDataKeyRequest
        {
            KeyId = keyId,
            KeySpec = DataKeySpec.AES_256,
        });

        var decResp = await _kms.DecryptAsync(new DecryptRequest
        {
            CiphertextBlob = new MemoryStream(genResp.CiphertextBlob.ToArray()),
        });

        decResp.Plaintext.ToArray().ShouldBe(genResp.Plaintext.ToArray());
    }

    [Fact]
    public async Task GenerateDataKeyWithoutPlaintext()
    {
        var key = await _kms.CreateKeyAsync(new CreateKeyRequest
        {
            KeySpec = KeySpec.SYMMETRIC_DEFAULT,
            KeyUsage = KeyUsageType.ENCRYPT_DECRYPT,
        });
        var keyId = key.KeyMetadata.KeyId;

        var resp = await _kms.GenerateDataKeyWithoutPlaintextAsync(
            new GenerateDataKeyWithoutPlaintextRequest
            {
                KeyId = keyId,
                KeySpec = DataKeySpec.AES_256,
            });

        resp.KeyId.ShouldBe(keyId);
        (resp.CiphertextBlob.Length > 0).ShouldBe(true);
    }

    // -- GetPublicKey ----------------------------------------------------------

    [Fact]
    public async Task GetPublicKey()
    {
        var key = await _kms.CreateKeyAsync(new CreateKeyRequest
        {
            KeySpec = KeySpec.RSA_2048,
            KeyUsage = KeyUsageType.SIGN_VERIFY,
        });
        var keyId = key.KeyMetadata.KeyId;

        var resp = await _kms.GetPublicKeyAsync(new GetPublicKeyRequest { KeyId = keyId });
        resp.KeyId.ShouldBe(keyId);
        resp.KeySpec.ShouldBe(KeySpec.RSA_2048);
        (resp.PublicKey.Length > 0).ShouldBe(true);
    }

    // -- EncryptionContext ------------------------------------------------------

    [Fact]
    public async Task EncryptDecryptWithEncryptionContext()
    {
        var key = await _kms.CreateKeyAsync(new CreateKeyRequest
        {
            KeySpec = KeySpec.SYMMETRIC_DEFAULT,
            KeyUsage = KeyUsageType.ENCRYPT_DECRYPT,
        });
        var keyId = key.KeyMetadata.KeyId;
        var plaintext = "context-sensitive data"u8.ToArray();
        var context = new Dictionary<string, string>
        {
            ["service"] = "storage",
            ["bucket"] = "documents",
        };

        var encResp = await _kms.EncryptAsync(new EncryptRequest
        {
            KeyId = keyId,
            Plaintext = new MemoryStream(plaintext),
            EncryptionContext = context,
        });

        var decResp = await _kms.DecryptAsync(new DecryptRequest
        {
            CiphertextBlob = new MemoryStream(encResp.CiphertextBlob.ToArray()),
            EncryptionContext = context,
        });

        decResp.Plaintext.ToArray().ShouldBe(plaintext);
    }

    [Fact]
    public async Task DecryptWrongContextFails()
    {
        var key = await _kms.CreateKeyAsync(new CreateKeyRequest
        {
            KeySpec = KeySpec.SYMMETRIC_DEFAULT,
            KeyUsage = KeyUsageType.ENCRYPT_DECRYPT,
        });
        var keyId = key.KeyMetadata.KeyId;

        var encResp = await _kms.EncryptAsync(new EncryptRequest
        {
            KeyId = keyId,
            Plaintext = new MemoryStream("secret"u8.ToArray()),
            EncryptionContext = new Dictionary<string, string> { ["env"] = "prod" },
        });

        var ex = await Should.ThrowAsync<InvalidCiphertextException>(() =>
            _kms.DecryptAsync(new DecryptRequest
            {
                CiphertextBlob = new MemoryStream(encResp.CiphertextBlob.ToArray()),
                EncryptionContext = new Dictionary<string, string> { ["env"] = "dev" },
            }));

        ex.Message.ShouldContain("EncryptionContext");
    }

    // -- Alias operations ------------------------------------------------------

    [Fact]
    public async Task CreateAndListAlias()
    {
        var key = await _kms.CreateKeyAsync(new CreateKeyRequest
        {
            KeySpec = KeySpec.SYMMETRIC_DEFAULT,
        });
        var keyId = key.KeyMetadata.KeyId;

        await _kms.CreateAliasAsync(new CreateAliasRequest
        {
            AliasName = "alias/test-alias",
            TargetKeyId = keyId,
        });

        var resp = await _kms.ListAliasesAsync(new ListAliasesRequest());
        var aliasNames = resp.Aliases.Select(a => a.AliasName).ToList();
        aliasNames.ShouldContain("alias/test-alias");
    }

    [Fact]
    public async Task UseAliasForEncrypt()
    {
        var key = await _kms.CreateKeyAsync(new CreateKeyRequest
        {
            KeySpec = KeySpec.SYMMETRIC_DEFAULT,
            KeyUsage = KeyUsageType.ENCRYPT_DECRYPT,
        });
        var keyId = key.KeyMetadata.KeyId;

        await _kms.CreateAliasAsync(new CreateAliasRequest
        {
            AliasName = "alias/enc-alias",
            TargetKeyId = keyId,
        });

        var encResp = await _kms.EncryptAsync(new EncryptRequest
        {
            KeyId = "alias/enc-alias",
            Plaintext = new MemoryStream("via alias"u8.ToArray()),
        });

        var decResp = await _kms.DecryptAsync(new DecryptRequest
        {
            CiphertextBlob = new MemoryStream(encResp.CiphertextBlob.ToArray()),
        });

        decResp.Plaintext.ToArray().ShouldBe("via alias"u8.ToArray());
    }

    [Fact]
    public async Task DescribeKeyByAlias()
    {
        var key = await _kms.CreateKeyAsync(new CreateKeyRequest
        {
            KeySpec = KeySpec.SYMMETRIC_DEFAULT,
        });
        var keyId = key.KeyMetadata.KeyId;

        await _kms.CreateAliasAsync(new CreateAliasRequest
        {
            AliasName = "alias/desc-alias",
            TargetKeyId = keyId,
        });

        var resp = await _kms.DescribeKeyAsync(new DescribeKeyRequest
        {
            KeyId = "alias/desc-alias",
        });
        resp.KeyMetadata.KeyId.ShouldBe(keyId);
    }

    [Fact]
    public async Task UpdateAlias()
    {
        var key1 = await _kms.CreateKeyAsync(new CreateKeyRequest { KeySpec = KeySpec.SYMMETRIC_DEFAULT });
        var key2 = await _kms.CreateKeyAsync(new CreateKeyRequest { KeySpec = KeySpec.SYMMETRIC_DEFAULT });

        await _kms.CreateAliasAsync(new CreateAliasRequest
        {
            AliasName = "alias/upd-alias",
            TargetKeyId = key1.KeyMetadata.KeyId,
        });

        await _kms.UpdateAliasAsync(new UpdateAliasRequest
        {
            AliasName = "alias/upd-alias",
            TargetKeyId = key2.KeyMetadata.KeyId,
        });

        var resp = await _kms.DescribeKeyAsync(new DescribeKeyRequest
        {
            KeyId = "alias/upd-alias",
        });
        resp.KeyMetadata.KeyId.ShouldBe(key2.KeyMetadata.KeyId);
    }

    [Fact]
    public async Task DeleteAlias()
    {
        var key = await _kms.CreateKeyAsync(new CreateKeyRequest
        {
            KeySpec = KeySpec.SYMMETRIC_DEFAULT,
        });

        await _kms.CreateAliasAsync(new CreateAliasRequest
        {
            AliasName = "alias/del-alias",
            TargetKeyId = key.KeyMetadata.KeyId,
        });

        await _kms.DeleteAliasAsync(new DeleteAliasRequest
        {
            AliasName = "alias/del-alias",
        });

        await Should.ThrowAsync<NotFoundException>(() =>
            _kms.DescribeKeyAsync(new DescribeKeyRequest { KeyId = "alias/del-alias" }));
    }

    // -- Key Rotation ----------------------------------------------------------

    [Fact]
    public async Task EnableDisableKeyRotation()
    {
        var key = await _kms.CreateKeyAsync(new CreateKeyRequest
        {
            KeyUsage = KeyUsageType.ENCRYPT_DECRYPT,
        });
        var keyId = key.KeyMetadata.KeyId;

        var status = await _kms.GetKeyRotationStatusAsync(new GetKeyRotationStatusRequest { KeyId = keyId });
        status.KeyRotationEnabled.ShouldBe(false);

        await _kms.EnableKeyRotationAsync(new EnableKeyRotationRequest { KeyId = keyId });
        status = await _kms.GetKeyRotationStatusAsync(new GetKeyRotationStatusRequest { KeyId = keyId });
        status.KeyRotationEnabled.ShouldBe(true);

        await _kms.DisableKeyRotationAsync(new DisableKeyRotationRequest { KeyId = keyId });
        status = await _kms.GetKeyRotationStatusAsync(new GetKeyRotationStatusRequest { KeyId = keyId });
        status.KeyRotationEnabled.ShouldBe(false);

        await _kms.ScheduleKeyDeletionAsync(new ScheduleKeyDeletionRequest
        {
            KeyId = keyId,
            PendingWindowInDays = 7,
        });
    }

    // -- Key Policy ------------------------------------------------------------

    [Fact]
    public async Task GetPutKeyPolicy()
    {
        var key = await _kms.CreateKeyAsync(new CreateKeyRequest());
        var keyId = key.KeyMetadata.KeyId;

        var policy = await _kms.GetKeyPolicyAsync(new GetKeyPolicyRequest
        {
            KeyId = keyId,
            PolicyName = "default",
        });
        policy.Policy.ShouldContain("Statement");

        var custom = "{\"Version\":\"2012-10-17\",\"Statement\":[]}";
        await _kms.PutKeyPolicyAsync(new PutKeyPolicyRequest
        {
            KeyId = keyId,
            PolicyName = "default",
            Policy = custom,
        });

        var got = await _kms.GetKeyPolicyAsync(new GetKeyPolicyRequest
        {
            KeyId = keyId,
            PolicyName = "default",
        });
        got.Policy.ShouldBe(custom);

        await _kms.ScheduleKeyDeletionAsync(new ScheduleKeyDeletionRequest
        {
            KeyId = keyId,
            PendingWindowInDays = 7,
        });
    }

    // -- Tags ------------------------------------------------------------------

    [Fact]
    public async Task TagUntagList()
    {
        var key = await _kms.CreateKeyAsync(new CreateKeyRequest());
        var keyId = key.KeyMetadata.KeyId;

        await _kms.TagResourceAsync(new TagResourceRequest
        {
            KeyId = keyId,
            Tags =
            [
                new Tag { TagKey = "env", TagValue = "test" },
                new Tag { TagKey = "team", TagValue = "platform" },
            ],
        });

        var tags = await _kms.ListResourceTagsAsync(new ListResourceTagsRequest { KeyId = keyId });
        var tagMap = tags.Tags.ToDictionary(t => t.TagKey, t => t.TagValue);
        tagMap["env"].ShouldBe("test");
        tagMap["team"].ShouldBe("platform");

        await _kms.UntagResourceAsync(new UntagResourceRequest
        {
            KeyId = keyId,
            TagKeys = ["team"],
        });

        tags = await _kms.ListResourceTagsAsync(new ListResourceTagsRequest { KeyId = keyId });
        tags.Tags.ShouldHaveSingleItem();
        tags.Tags[0].TagKey.ShouldBe("env");

        await _kms.ScheduleKeyDeletionAsync(new ScheduleKeyDeletionRequest
        {
            KeyId = keyId,
            PendingWindowInDays = 7,
        });
    }

    // -- Enable / Disable Key --------------------------------------------------

    [Fact]
    public async Task EnableDisableKey()
    {
        var key = await _kms.CreateKeyAsync(new CreateKeyRequest());
        var keyId = key.KeyMetadata.KeyId;
        key.KeyMetadata.KeyState.ShouldBe(KeyState.Enabled);

        await _kms.DisableKeyAsync(new DisableKeyRequest { KeyId = keyId });
        var desc = await _kms.DescribeKeyAsync(new DescribeKeyRequest { KeyId = keyId });
        desc.KeyMetadata.KeyState.ShouldBe(KeyState.Disabled);

        await _kms.EnableKeyAsync(new EnableKeyRequest { KeyId = keyId });
        desc = await _kms.DescribeKeyAsync(new DescribeKeyRequest { KeyId = keyId });
        desc.KeyMetadata.KeyState.ShouldBe(KeyState.Enabled);

        await _kms.ScheduleKeyDeletionAsync(new ScheduleKeyDeletionRequest
        {
            KeyId = keyId,
            PendingWindowInDays = 7,
        });
    }

    // -- Schedule / Cancel Deletion --------------------------------------------

    [Fact]
    public async Task ScheduleCancelDeletion()
    {
        var key = await _kms.CreateKeyAsync(new CreateKeyRequest());
        var keyId = key.KeyMetadata.KeyId;

        var resp = await _kms.ScheduleKeyDeletionAsync(new ScheduleKeyDeletionRequest
        {
            KeyId = keyId,
            PendingWindowInDays = 7,
        });
        resp.KeyState.ShouldBe(KeyState.PendingDeletion);

        await _kms.CancelKeyDeletionAsync(new CancelKeyDeletionRequest { KeyId = keyId });

        var desc = await _kms.DescribeKeyAsync(new DescribeKeyRequest { KeyId = keyId });
        desc.KeyMetadata.KeyState.ShouldBe(KeyState.Disabled);
    }

    // -- TerraformFullFlow -----------------------------------------------------

    [Fact]
    public async Task TerraformFullFlow()
    {
        var key = await _kms.CreateKeyAsync(new CreateKeyRequest
        {
            KeySpec = KeySpec.SYMMETRIC_DEFAULT,
            KeyUsage = KeyUsageType.ENCRYPT_DECRYPT,
            Description = "RDS key",
        });
        var keyId = key.KeyMetadata.KeyId;

        await _kms.EnableKeyRotationAsync(new EnableKeyRotationRequest { KeyId = keyId });
        var status = await _kms.GetKeyRotationStatusAsync(new GetKeyRotationStatusRequest { KeyId = keyId });
        status.KeyRotationEnabled.ShouldBe(true);

        var pol = await _kms.GetKeyPolicyAsync(new GetKeyPolicyRequest
        {
            KeyId = keyId,
            PolicyName = "default",
        });
        (pol.Policy.Length > 0).ShouldBe(true);

        await _kms.TagResourceAsync(new TagResourceRequest
        {
            KeyId = keyId,
            Tags = [new Tag { TagKey = "Name", TagValue = "rds-key" }],
        });

        var tags = await _kms.ListResourceTagsAsync(new ListResourceTagsRequest { KeyId = keyId });
        tags.Tags[0].TagValue.ShouldBe("rds-key");

        var desc = await _kms.DescribeKeyAsync(new DescribeKeyRequest { KeyId = keyId });
        desc.KeyMetadata.Description.ShouldBe("RDS key");

        await _kms.ScheduleKeyDeletionAsync(new ScheduleKeyDeletionRequest
        {
            KeyId = keyId,
            PendingWindowInDays = 7,
        });
    }

    // -- ListKeyPolicies -------------------------------------------------------

    [Fact]
    public async Task ListKeyPolicies()
    {
        var key = await _kms.CreateKeyAsync(new CreateKeyRequest());
        var keyId = key.KeyMetadata.KeyId;

        var resp = await _kms.ListKeyPoliciesAsync(new ListKeyPoliciesRequest { KeyId = keyId });
        resp.PolicyNames.ShouldContain("default");

        await _kms.ScheduleKeyDeletionAsync(new ScheduleKeyDeletionRequest
        {
            KeyId = keyId,
            PendingWindowInDays = 7,
        });
    }

    // -- Key Rotation with period ----------------------------------------------

    [Fact]
    public async Task KeyRotationWithPeriod()
    {
        var key = await _kms.CreateKeyAsync(new CreateKeyRequest());
        var keyId = key.KeyMetadata.KeyId;

        await _kms.EnableKeyRotationAsync(new EnableKeyRotationRequest
        {
            KeyId = keyId,
            RotationPeriodInDays = 180,
        });

        var status = await _kms.GetKeyRotationStatusAsync(new GetKeyRotationStatusRequest { KeyId = keyId });
        status.KeyRotationEnabled.ShouldBe(true);
        status.RotationPeriodInDays.ShouldBe(180);

        await _kms.ScheduleKeyDeletionAsync(new ScheduleKeyDeletionRequest
        {
            KeyId = keyId,
            PendingWindowInDays = 7,
        });
    }
}
