using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using MicroStack.Internal;

namespace MicroStack.Services.Ecr;

/// <summary>
/// ECR (Elastic Container Registry) service handler -- JSON protocol via X-Amz-Target.
///
/// Port of ministack/services/ecr.py.
///
/// Supports: CreateRepository, DescribeRepositories, DeleteRepository,
///           ListImages, PutImage, BatchGetImage, BatchDeleteImage,
///           DescribeImages, GetAuthorizationToken,
///           GetRepositoryPolicy, SetRepositoryPolicy, DeleteRepositoryPolicy,
///           PutLifecyclePolicy, GetLifecyclePolicy, DeleteLifecyclePolicy,
///           TagResource, UntagResource, ListTagsForResource,
///           PutImageTagMutability, PutImageScanningConfiguration,
///           DescribeRegistry, GetDownloadUrlForLayer,
///           BatchCheckLayerAvailability, InitiateLayerUpload,
///           UploadLayerPart, CompleteLayerUpload.
/// </summary>
internal sealed class EcrServiceHandler : IServiceHandler
{
    public string ServiceName => "ecr";

    private static string Region =>
        Environment.GetEnvironmentVariable("MINISTACK_REGION") ?? "us-east-1";

    private readonly AccountScopedDictionary<string, Dictionary<string, object?>> _repositories = new();
    private readonly AccountScopedDictionary<string, List<Dictionary<string, object?>>> _images = new();
    private readonly AccountScopedDictionary<string, string> _lifecyclePolicies = new();
    private readonly AccountScopedDictionary<string, string> _repoPolicies = new();

    private readonly Lock _lock = new();

    // ── IServiceHandler ───────────────────────────────────────────────────────

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

        ServiceResponse response;
        lock (_lock) { response = DispatchAction(action, data); }
        return Task.FromResult(response);
    }

    public void Reset()
    {
        lock (_lock)
        {
            _repositories.Clear();
            _images.Clear();
            _lifecyclePolicies.Clear();
            _repoPolicies.Clear();
        }
    }

    public object? GetState() => null;

    public void RestoreState(object state) { }

    // ── Dispatch ──────────────────────────────────────────────────────────────

    private ServiceResponse DispatchAction(string action, JsonElement data)
    {
        return action switch
        {
            "CreateRepository" => CreateRepository(data),
            "DescribeRepositories" => DescribeRepositories(data),
            "DeleteRepository" => DeleteRepository(data),
            "ListImages" => ListImages(data),
            "PutImage" => PutImage(data),
            "BatchGetImage" => BatchGetImage(data),
            "BatchDeleteImage" => BatchDeleteImage(data),
            "DescribeImages" => DescribeImages(data),
            "GetAuthorizationToken" => GetAuthorizationToken(),
            "GetRepositoryPolicy" => GetRepositoryPolicy(data),
            "SetRepositoryPolicy" => SetRepositoryPolicy(data),
            "DeleteRepositoryPolicy" => DeleteRepositoryPolicy(data),
            "PutLifecyclePolicy" => PutLifecyclePolicy(data),
            "GetLifecyclePolicy" => GetLifecyclePolicy(data),
            "DeleteLifecyclePolicy" => DeleteLifecyclePolicy(data),
            "ListTagsForResource" => ListTagsForResource(data),
            "TagResource" => TagResource(data),
            "UntagResource" => UntagResource(data),
            "PutImageTagMutability" => PutImageTagMutability(data),
            "PutImageScanningConfiguration" => PutImageScanningConfiguration(data),
            "DescribeRegistry" => DescribeRegistry(),
            "GetDownloadUrlForLayer" => GetDownloadUrlForLayer(data),
            "BatchCheckLayerAvailability" => BatchCheckLayerAvailability(data),
            "InitiateLayerUpload" => InitiateLayerUpload(data),
            "UploadLayerPart" => UploadLayerPart(data),
            "CompleteLayerUpload" => CompleteLayerUpload(data),
            _ => AwsResponseHelpers.ErrorResponseJson("InvalidAction", $"Unknown action: {action}", 400),
        };
    }

    // ── Helpers ────────────────────────────────────────────────────────────────

    private static string RegistryId() => AccountContext.GetAccountId();

    private static string RepoArn(string name) =>
        $"arn:aws:ecr:{Region}:{AccountContext.GetAccountId()}:repository/{name}";

    private static string RepoUri(string name) =>
        $"{AccountContext.GetAccountId()}.dkr.ecr.{Region}.amazonaws.com/{name}";

    private static string ImageDigest(string manifest)
    {
        var raw = Encoding.UTF8.GetBytes(manifest);
        var hash = SHA256.HashData(raw);
        return "sha256:" + Convert.ToHexStringLower(hash);
    }

    private static string GetString(JsonElement el, string prop)
    {
        return el.TryGetProperty(prop, out var v) && v.ValueKind == JsonValueKind.String
            ? v.GetString() ?? ""
            : "";
    }

    private static bool GetBool(JsonElement el, string prop)
    {
        return el.TryGetProperty(prop, out var v) && v.ValueKind == JsonValueKind.True;
    }

    private static int GetInt(JsonElement el, string prop, int defaultValue)
    {
        return el.TryGetProperty(prop, out var v) && v.ValueKind == JsonValueKind.Number
            ? v.GetInt32()
            : defaultValue;
    }

    // ── Repository CRUD ───────────────────────────────────────────────────────

    private ServiceResponse CreateRepository(JsonElement data)
    {
        var name = GetString(data, "repositoryName");
        if (string.IsNullOrEmpty(name))
            return ErrorJson("InvalidParameterException", "repositoryName is required");
        if (_repositories.ContainsKey(name))
            return ErrorJson("RepositoryAlreadyExistsException", $"The repository with name '{name}' already exists");

        var tags = new List<Dictionary<string, string>>();
        if (data.TryGetProperty("tags", out var tagsEl) && tagsEl.ValueKind == JsonValueKind.Array)
        {
            foreach (var t in tagsEl.EnumerateArray())
            {
                tags.Add(new Dictionary<string, string>
                {
                    ["Key"] = GetString(t, "Key"),
                    ["Value"] = GetString(t, "Value"),
                });
            }
        }

        var scanConfig = new Dictionary<string, object> { ["scanOnPush"] = false };
        if (data.TryGetProperty("imageScanningConfiguration", out var scanEl)
            && scanEl.TryGetProperty("scanOnPush", out var scanOnPush))
        {
            scanConfig["scanOnPush"] = scanOnPush.ValueKind == JsonValueKind.True;
        }

        var encConfig = new Dictionary<string, object> { ["encryptionType"] = "AES256" };
        if (data.TryGetProperty("encryptionConfiguration", out var encEl)
            && encEl.TryGetProperty("encryptionType", out var encType))
        {
            encConfig["encryptionType"] = encType.GetString() ?? "AES256";
        }

        var repo = new Dictionary<string, object?>
        {
            ["repositoryArn"] = RepoArn(name),
            ["registryId"] = RegistryId(),
            ["repositoryName"] = name,
            ["repositoryUri"] = RepoUri(name),
            ["createdAt"] = TimeHelpers.NowEpoch(),
            ["imageTagMutability"] = data.TryGetProperty("imageTagMutability", out var mutEl)
                ? mutEl.GetString() ?? "MUTABLE" : "MUTABLE",
            ["imageScanningConfiguration"] = scanConfig,
            ["encryptionConfiguration"] = encConfig,
            ["tags"] = tags,
        };
        _repositories[name] = repo;
        _images[name] = [];

        return JsonResp(new { repository = RepoShape(repo) });
    }

    private ServiceResponse DescribeRepositories(JsonElement data)
    {
        var names = new List<string>();
        if (data.TryGetProperty("repositoryNames", out var namesEl) && namesEl.ValueKind == JsonValueKind.Array)
        {
            foreach (var n in namesEl.EnumerateArray())
                names.Add(n.GetString() ?? "");
        }
        var maxResults = GetInt(data, "maxResults", 1000);

        List<Dictionary<string, object?>> repos;
        if (names.Count > 0)
        {
            repos = [];
            foreach (var n in names)
            {
                if (!_repositories.TryGetValue(n, out var r))
                    return ErrorJson("RepositoryNotFoundException", $"The repository with name '{n}' does not exist");
                repos.Add(r);
            }
        }
        else
        {
            repos = [.. _repositories.Values];
        }

        var result = repos.Take(maxResults).Select(RepoShape).ToList();
        return JsonResp(new { repositories = result });
    }

    private ServiceResponse DeleteRepository(JsonElement data)
    {
        var name = GetString(data, "repositoryName");
        var force = GetBool(data, "force");

        if (!_repositories.TryGetValue(name, out var repo))
            return ErrorJson("RepositoryNotFoundException", $"The repository with name '{name}' does not exist");

        if (!force && _images.TryGetValue(name, out var imgs) && imgs.Count > 0)
            return ErrorJson("RepositoryNotEmptyException", $"The repository with name '{name}' is not empty");

        _repositories.TryRemove(name, out _);
        _images.TryRemove(name, out _);
        _lifecyclePolicies.TryRemove(name, out _);
        _repoPolicies.TryRemove(name, out _);
        return JsonResp(new { repository = RepoShape(repo) });
    }

    // ── Image operations ──────────────────────────────────────────────────────

    private ServiceResponse PutImage(JsonElement data)
    {
        var name = GetString(data, "repositoryName");
        if (!_repositories.TryGetValue(name, out var repo))
            return ErrorJson("RepositoryNotFoundException", $"The repository with name '{name}' does not exist");

        var manifest = GetString(data, "imageManifest");
        var manifestType = data.TryGetProperty("imageManifestMediaType", out var mmtEl)
            ? mmtEl.GetString() ?? "application/vnd.docker.distribution.manifest.v2+json"
            : "application/vnd.docker.distribution.manifest.v2+json";
        var tag = GetString(data, "imageTag");
        var digest = GetString(data, "imageDigest");
        if (string.IsNullOrEmpty(digest))
            digest = ImageDigest(manifest);

        var images = _images[name];

        // Immutable tag check
        if (!string.IsNullOrEmpty(tag) && repo.GetValueOrDefault("imageTagMutability")?.ToString() == "IMMUTABLE")
        {
            foreach (var img in images)
            {
                var imgTags = img.GetValueOrDefault("imageTags") as List<string>;
                if (imgTags is not null && imgTags.Contains(tag))
                    return ErrorJson("ImageTagAlreadyExistsException", $"The image tag '{tag}' already exists");
            }
        }

        // Remove tag from existing images
        if (!string.IsNullOrEmpty(tag))
        {
            foreach (var img in images)
            {
                var imgTags = img.GetValueOrDefault("imageTags") as List<string>;
                imgTags?.Remove(tag);
            }
        }

        var imageId = new Dictionary<string, object?> { ["imageDigest"] = digest };
        if (!string.IsNullOrEmpty(tag))
            imageId["imageTag"] = tag;

        var image = new Dictionary<string, object?>
        {
            ["registryId"] = RegistryId(),
            ["repositoryName"] = name,
            ["imageId"] = imageId,
            ["imageManifest"] = manifest,
            ["imageManifestMediaType"] = manifestType,
            ["imageTags"] = !string.IsNullOrEmpty(tag) ? new List<string> { tag } : new List<string>(),
            ["imagePushedAt"] = TimeHelpers.NowEpoch(),
            ["imageDigest"] = digest,
        };

        // Check for existing image with same digest
        var existing = images.FirstOrDefault(img => img.GetValueOrDefault("imageDigest")?.ToString() == digest);
        if (existing is not null)
        {
            if (!string.IsNullOrEmpty(tag))
            {
                var existingTags = existing.GetValueOrDefault("imageTags") as List<string>;
                if (existingTags is null)
                {
                    existingTags = [];
                    existing["imageTags"] = existingTags;
                }
                if (!existingTags.Contains(tag))
                    existingTags.Add(tag);
                var existingId = existing.GetValueOrDefault("imageId") as Dictionary<string, object?>;
                if (existingId is not null)
                    existingId["imageTag"] = tag;
            }
            image = existing;
        }
        else
        {
            images.Add(image);
        }

        var respImageId = image.GetValueOrDefault("imageId") as Dictionary<string, object?>;
        return JsonResp(new
        {
            image = new
            {
                registryId = RegistryId(),
                repositoryName = name,
                imageId = respImageId,
                imageManifest = manifest,
                imageManifestMediaType = manifestType,
            },
        });
    }

    private ServiceResponse ListImages(JsonElement data)
    {
        var name = GetString(data, "repositoryName");
        if (!_repositories.ContainsKey(name))
            return ErrorJson("RepositoryNotFoundException", $"The repository with name '{name}' does not exist");

        var tagStatus = "";
        if (data.TryGetProperty("filter", out var filterEl))
            tagStatus = GetString(filterEl, "tagStatus");

        if (!_images.TryGetValue(name, out var images))
            images = [];

        var result = new List<object>();
        foreach (var img in images)
        {
            var tags = img.GetValueOrDefault("imageTags") as List<string>;
            var hasTags = tags is not null && tags.Count > 0;
            if (tagStatus == "TAGGED" && !hasTags) continue;
            if (tagStatus == "UNTAGGED" && hasTags) continue;
            result.Add(img.GetValueOrDefault("imageId")!);
        }

        return JsonResp(new { imageIds = result });
    }

    private ServiceResponse DescribeImages(JsonElement data)
    {
        var name = GetString(data, "repositoryName");
        if (!_repositories.ContainsKey(name))
            return ErrorJson("RepositoryNotFoundException", $"The repository with name '{name}' does not exist");

        if (!_images.TryGetValue(name, out var images))
            images = [];

        List<Dictionary<string, object?>> filtered;
        if (data.TryGetProperty("imageIds", out var imageIdsEl) && imageIdsEl.ValueKind == JsonValueKind.Array)
        {
            filtered = [];
            foreach (var iid in imageIdsEl.EnumerateArray())
            {
                var match = FindImage(name, iid);
                if (match is not null)
                    filtered.Add(match);
            }
        }
        else
        {
            filtered = [.. images];
        }

        var details = filtered.Select(img =>
        {
            var mfst = img.GetValueOrDefault("imageManifest")?.ToString() ?? "{}";
            return new Dictionary<string, object?>
            {
                ["registryId"] = RegistryId(),
                ["repositoryName"] = name,
                ["imageDigest"] = img.GetValueOrDefault("imageDigest"),
                ["imageTags"] = img.GetValueOrDefault("imageTags"),
                ["imageSizeInBytes"] = mfst.Length,
                ["imagePushedAt"] = img.GetValueOrDefault("imagePushedAt") ?? TimeHelpers.NowEpoch(),
                ["imageManifestMediaType"] = img.GetValueOrDefault("imageManifestMediaType")
                    ?? "application/vnd.docker.distribution.manifest.v2+json",
                ["artifactMediaType"] = img.GetValueOrDefault("imageManifestMediaType")
                    ?? "application/vnd.docker.distribution.manifest.v2+json",
            };
        }).ToList();

        return JsonResp(new { imageDetails = details });
    }

    private ServiceResponse BatchGetImage(JsonElement data)
    {
        var name = GetString(data, "repositoryName");
        if (!_repositories.ContainsKey(name))
            return ErrorJson("RepositoryNotFoundException", $"The repository with name '{name}' does not exist");

        var found = new List<object>();
        var failures = new List<object>();

        if (data.TryGetProperty("imageIds", out var imageIdsEl) && imageIdsEl.ValueKind == JsonValueKind.Array)
        {
            foreach (var iid in imageIdsEl.EnumerateArray())
            {
                var match = FindImage(name, iid);
                if (match is not null)
                {
                    found.Add(new
                    {
                        registryId = RegistryId(),
                        repositoryName = name,
                        imageId = match.GetValueOrDefault("imageId"),
                        imageManifest = match.GetValueOrDefault("imageManifest")?.ToString() ?? "{}",
                        imageManifestMediaType = match.GetValueOrDefault("imageManifestMediaType")
                            ?? "application/vnd.docker.distribution.manifest.v2+json",
                    });
                }
                else
                {
                    failures.Add(new
                    {
                        imageId = JsonElementToObj(iid),
                        failureCode = "ImageNotFound",
                        failureReason = "Requested image not found",
                    });
                }
            }
        }

        return JsonResp(new { images = found, failures });
    }

    private ServiceResponse BatchDeleteImage(JsonElement data)
    {
        var name = GetString(data, "repositoryName");
        if (!_repositories.ContainsKey(name))
            return ErrorJson("RepositoryNotFoundException", $"The repository with name '{name}' does not exist");

        var deleted = new List<object>();
        var failures = new List<object>();

        if (!_images.TryGetValue(name, out var images))
            images = [];

        if (data.TryGetProperty("imageIds", out var imageIdsEl) && imageIdsEl.ValueKind == JsonValueKind.Array)
        {
            foreach (var iid in imageIdsEl.EnumerateArray())
            {
                var match = FindImage(name, iid);
                if (match is not null)
                {
                    images.Remove(match);
                    deleted.Add(match.GetValueOrDefault("imageId")!);
                }
                else
                {
                    failures.Add(new
                    {
                        imageId = JsonElementToObj(iid),
                        failureCode = "ImageNotFound",
                        failureReason = "Requested image not found",
                    });
                }
            }
        }

        return JsonResp(new { imageIds = deleted, failures });
    }

    // ── Authorization ─────────────────────────────────────────────────────────

    private ServiceResponse GetAuthorizationToken()
    {
        var token = Convert.ToBase64String("AWS:ministack-auth-token"u8.ToArray());
        return JsonResp(new
        {
            authorizationData = new[]
            {
                new
                {
                    authorizationToken = token,
                    expiresAt = TimeHelpers.NowEpoch() + 43200,
                    proxyEndpoint = $"https://{AccountContext.GetAccountId()}.dkr.ecr.{Region}.amazonaws.com",
                },
            },
        });
    }

    // ── Repository Policy ─────────────────────────────────────────────────────

    private ServiceResponse GetRepositoryPolicy(JsonElement data)
    {
        var name = GetString(data, "repositoryName");
        if (!_repositories.ContainsKey(name))
            return ErrorJson("RepositoryNotFoundException", $"The repository with name '{name}' does not exist");
        if (!_repoPolicies.TryGetValue(name, out var policy))
            return ErrorJson("RepositoryPolicyNotFoundException", $"Repository policy does not exist for '{name}'");
        return JsonResp(new { registryId = RegistryId(), repositoryName = name, policyText = policy });
    }

    private ServiceResponse SetRepositoryPolicy(JsonElement data)
    {
        var name = GetString(data, "repositoryName");
        if (!_repositories.ContainsKey(name))
            return ErrorJson("RepositoryNotFoundException", $"The repository with name '{name}' does not exist");
        var policy = GetString(data, "policyText");
        _repoPolicies[name] = policy;
        return JsonResp(new { registryId = RegistryId(), repositoryName = name, policyText = policy });
    }

    private ServiceResponse DeleteRepositoryPolicy(JsonElement data)
    {
        var name = GetString(data, "repositoryName");
        if (!_repositories.ContainsKey(name))
            return ErrorJson("RepositoryNotFoundException", $"The repository with name '{name}' does not exist");
        if (!_repoPolicies.TryGetValue(name, out var policy))
            return ErrorJson("RepositoryPolicyNotFoundException", $"Repository policy does not exist for '{name}'");
        _repoPolicies.TryRemove(name, out _);
        return JsonResp(new { registryId = RegistryId(), repositoryName = name, policyText = policy });
    }

    // ── Lifecycle Policy ──────────────────────────────────────────────────────

    private ServiceResponse PutLifecyclePolicy(JsonElement data)
    {
        var name = GetString(data, "repositoryName");
        if (!_repositories.ContainsKey(name))
            return ErrorJson("RepositoryNotFoundException", $"The repository with name '{name}' does not exist");
        var policy = GetString(data, "lifecyclePolicyText");
        _lifecyclePolicies[name] = policy;
        return JsonResp(new { registryId = RegistryId(), repositoryName = name, lifecyclePolicyText = policy });
    }

    private ServiceResponse GetLifecyclePolicy(JsonElement data)
    {
        var name = GetString(data, "repositoryName");
        if (!_repositories.ContainsKey(name))
            return ErrorJson("RepositoryNotFoundException", $"The repository with name '{name}' does not exist");
        if (!_lifecyclePolicies.TryGetValue(name, out var policy))
            return ErrorJson("LifecyclePolicyNotFoundException", $"Lifecycle policy does not exist for '{name}'");
        return JsonResp(new
        {
            registryId = RegistryId(), repositoryName = name,
            lifecyclePolicyText = policy, lastEvaluatedAt = TimeHelpers.NowEpoch(),
        });
    }

    private ServiceResponse DeleteLifecyclePolicy(JsonElement data)
    {
        var name = GetString(data, "repositoryName");
        if (!_repositories.ContainsKey(name))
            return ErrorJson("RepositoryNotFoundException", $"The repository with name '{name}' does not exist");
        if (!_lifecyclePolicies.TryGetValue(name, out var policy))
            return ErrorJson("LifecyclePolicyNotFoundException", $"Lifecycle policy does not exist for '{name}'");
        _lifecyclePolicies.TryRemove(name, out _);
        return JsonResp(new
        {
            registryId = RegistryId(), repositoryName = name,
            lifecyclePolicyText = policy, lastEvaluatedAt = TimeHelpers.NowEpoch(),
        });
    }

    // ── Tags ──────────────────────────────────────────────────────────────────

    private ServiceResponse ListTagsForResource(JsonElement data)
    {
        var arn = GetString(data, "resourceArn");
        var repo = FindRepoByArn(arn);
        if (repo is null)
            return ErrorJson("RepositoryNotFoundException", "Repository not found");
        return JsonResp(new { tags = repo.GetValueOrDefault("tags") ?? new List<Dictionary<string, string>>() });
    }

    private ServiceResponse TagResource(JsonElement data)
    {
        var arn = GetString(data, "resourceArn");
        var repo = FindRepoByArn(arn);
        if (repo is null)
            return ErrorJson("RepositoryNotFoundException", "Repository not found");

        var existingTags = repo.GetValueOrDefault("tags") as List<Dictionary<string, string>> ?? [];
        var existingMap = new Dictionary<string, Dictionary<string, string>>();
        foreach (var t in existingTags)
            existingMap[t["Key"]] = t;

        if (data.TryGetProperty("tags", out var tagsEl) && tagsEl.ValueKind == JsonValueKind.Array)
        {
            foreach (var t in tagsEl.EnumerateArray())
            {
                var key = GetString(t, "Key");
                var value = GetString(t, "Value");
                existingMap[key] = new Dictionary<string, string> { ["Key"] = key, ["Value"] = value };
            }
        }
        repo["tags"] = existingMap.Values.ToList();
        return JsonResp(new { });
    }

    private ServiceResponse UntagResource(JsonElement data)
    {
        var arn = GetString(data, "resourceArn");
        var repo = FindRepoByArn(arn);
        if (repo is null)
            return ErrorJson("RepositoryNotFoundException", "Repository not found");

        var keys = new HashSet<string>();
        if (data.TryGetProperty("tagKeys", out var keysEl) && keysEl.ValueKind == JsonValueKind.Array)
        {
            foreach (var k in keysEl.EnumerateArray())
                keys.Add(k.GetString() ?? "");
        }

        var existingTags = repo.GetValueOrDefault("tags") as List<Dictionary<string, string>> ?? [];
        repo["tags"] = existingTags.Where(t => !keys.Contains(t["Key"])).ToList();
        return JsonResp(new { });
    }

    // ── Mutability / Scanning ─────────────────────────────────────────────────

    private ServiceResponse PutImageTagMutability(JsonElement data)
    {
        var name = GetString(data, "repositoryName");
        if (!_repositories.TryGetValue(name, out var repo))
            return ErrorJson("RepositoryNotFoundException", $"The repository with name '{name}' does not exist");
        var mutability = data.TryGetProperty("imageTagMutability", out var mutEl)
            ? mutEl.GetString() ?? "MUTABLE" : "MUTABLE";
        repo["imageTagMutability"] = mutability;
        return JsonResp(new { registryId = RegistryId(), repositoryName = name, imageTagMutability = mutability });
    }

    private ServiceResponse PutImageScanningConfiguration(JsonElement data)
    {
        var name = GetString(data, "repositoryName");
        if (!_repositories.TryGetValue(name, out var repo))
            return ErrorJson("RepositoryNotFoundException", $"The repository with name '{name}' does not exist");

        var scanConfig = new Dictionary<string, object> { ["scanOnPush"] = false };
        if (data.TryGetProperty("imageScanningConfiguration", out var scanEl)
            && scanEl.TryGetProperty("scanOnPush", out var scanOnPush))
        {
            scanConfig["scanOnPush"] = scanOnPush.ValueKind == JsonValueKind.True;
        }
        repo["imageScanningConfiguration"] = scanConfig;
        return JsonResp(new { registryId = RegistryId(), repositoryName = name, imageScanningConfiguration = scanConfig });
    }

    // ── Registry ──────────────────────────────────────────────────────────────

    private ServiceResponse DescribeRegistry()
    {
        return JsonResp(new
        {
            registryId = RegistryId(),
            replicationConfiguration = new { rules = Array.Empty<object>() },
        });
    }

    // ── Layer operations ──────────────────────────────────────────────────────

    private ServiceResponse GetDownloadUrlForLayer(JsonElement data)
    {
        var name = GetString(data, "repositoryName");
        if (!_repositories.ContainsKey(name))
            return ErrorJson("RepositoryNotFoundException", $"The repository with name '{name}' does not exist");
        var layerDigest = GetString(data, "layerDigest");
        return JsonResp(new
        {
            downloadUrl = $"https://{AccountContext.GetAccountId()}.dkr.ecr.{Region}.amazonaws.com/v2/{name}/blobs/{layerDigest}",
            layerDigest,
        });
    }

    private ServiceResponse BatchCheckLayerAvailability(JsonElement data)
    {
        var name = GetString(data, "repositoryName");
        if (!_repositories.ContainsKey(name))
            return ErrorJson("RepositoryNotFoundException", $"The repository with name '{name}' does not exist");

        var layers = new List<object>();
        if (data.TryGetProperty("layerDigests", out var digestsEl) && digestsEl.ValueKind == JsonValueKind.Array)
        {
            foreach (var d in digestsEl.EnumerateArray())
            {
                layers.Add(new { layerDigest = d.GetString(), layerAvailability = "UNAVAILABLE", layerSize = 0 });
            }
        }
        return JsonResp(new { layers, failures = Array.Empty<object>() });
    }

    private ServiceResponse InitiateLayerUpload(JsonElement data)
    {
        var name = GetString(data, "repositoryName");
        if (!_repositories.ContainsKey(name))
            return ErrorJson("RepositoryNotFoundException", $"The repository with name '{name}' does not exist");
        return JsonResp(new
        {
            registryId = RegistryId(),
            repositoryName = name,
            uploadId = Guid.NewGuid().ToString(),
            partSize = 10485760,
        });
    }

    private ServiceResponse UploadLayerPart(JsonElement data)
    {
        return JsonResp(new
        {
            registryId = RegistryId(),
            repositoryName = GetString(data, "repositoryName"),
            uploadId = GetString(data, "uploadId"),
            lastByteReceived = 0,
        });
    }

    private ServiceResponse CompleteLayerUpload(JsonElement data)
    {
        var name = GetString(data, "repositoryName");
        var layerDigest = "sha256:" + Guid.NewGuid().ToString("N");
        if (data.TryGetProperty("layerDigests", out var digestsEl) && digestsEl.ValueKind == JsonValueKind.Array)
        {
            var first = digestsEl.EnumerateArray().FirstOrDefault();
            if (first.ValueKind == JsonValueKind.String)
                layerDigest = first.GetString() ?? layerDigest;
        }
        return JsonResp(new
        {
            registryId = RegistryId(),
            repositoryName = name,
            uploadId = GetString(data, "uploadId"),
            layerDigest,
        });
    }

    // ── Private helpers ───────────────────────────────────────────────────────

    private Dictionary<string, object?>? FindImage(string repoName, JsonElement imageId)
    {
        var digest = GetString(imageId, "imageDigest");
        var tag = GetString(imageId, "imageTag");

        if (!_images.TryGetValue(repoName, out var images))
            return null;

        foreach (var img in images)
        {
            if (!string.IsNullOrEmpty(digest) && img.GetValueOrDefault("imageDigest")?.ToString() == digest)
                return img;
            if (!string.IsNullOrEmpty(tag))
            {
                var tags = img.GetValueOrDefault("imageTags") as List<string>;
                if (tags is not null && tags.Contains(tag))
                    return img;
            }
        }
        return null;
    }

    private Dictionary<string, object?>? FindRepoByArn(string arn)
    {
        foreach (var repo in _repositories.Values)
        {
            if (repo.GetValueOrDefault("repositoryArn")?.ToString() == arn)
                return repo;
        }
        return null;
    }

    private static Dictionary<string, object?> RepoShape(Dictionary<string, object?> repo)
    {
        return new Dictionary<string, object?>
        {
            ["repositoryArn"] = repo.GetValueOrDefault("repositoryArn"),
            ["registryId"] = repo.GetValueOrDefault("registryId"),
            ["repositoryName"] = repo.GetValueOrDefault("repositoryName"),
            ["repositoryUri"] = repo.GetValueOrDefault("repositoryUri"),
            ["createdAt"] = repo.GetValueOrDefault("createdAt"),
            ["imageTagMutability"] = repo.GetValueOrDefault("imageTagMutability") ?? "MUTABLE",
            ["imageScanningConfiguration"] = repo.GetValueOrDefault("imageScanningConfiguration")
                ?? new Dictionary<string, object> { ["scanOnPush"] = false },
            ["encryptionConfiguration"] = repo.GetValueOrDefault("encryptionConfiguration")
                ?? new Dictionary<string, object> { ["encryptionType"] = "AES256" },
        };
    }

    private static object? JsonElementToObj(JsonElement el)
    {
        return el.ValueKind switch
        {
            JsonValueKind.Object => el.EnumerateObject()
                .ToDictionary(p => p.Name, p => JsonElementToObj(p.Value)),
            JsonValueKind.Array => el.EnumerateArray().Select(JsonElementToObj).ToList(),
            JsonValueKind.String => el.GetString(),
            JsonValueKind.Number => el.TryGetInt64(out var l) ? l : el.GetDouble(),
            JsonValueKind.True => true,
            JsonValueKind.False => false,
            _ => null,
        };
    }

    private static ServiceResponse JsonResp(object data) =>
        AwsResponseHelpers.JsonResponse(data);

    private static ServiceResponse ErrorJson(string code, string message) =>
        AwsResponseHelpers.ErrorResponseJson(code, message, 400);
}
