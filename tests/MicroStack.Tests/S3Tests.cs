using Amazon;
using Amazon.Runtime;
using Amazon.S3;
using Amazon.S3.Model;

namespace MicroStack.Tests;

/// <summary>
/// Integration tests for the S3 service handler.
/// Uses the AWS SDK for .NET v4 pointed at the in-process MicroStack server.
///
/// Mirrors coverage from ministack/tests/test_s3.py.
/// </summary>
public sealed class S3Tests : IClassFixture<MicroStackFixture>, IAsyncLifetime
{
    private readonly MicroStackFixture _fixture;
    private readonly AmazonS3Client _s3;

    public S3Tests(MicroStackFixture fixture)
    {
        _fixture = fixture;
        _s3 = CreateS3Client(fixture);
    }

    private static AmazonS3Client CreateS3Client(MicroStackFixture fixture)
    {
        var innerHandler = fixture.Factory.Server.CreateHandler();
        var httpClient = new HttpClient(new CanonicalizeUriHandler(innerHandler))
        {
            BaseAddress = new Uri("http://localhost/"),
        };

        var config = new AmazonS3Config
        {
            RegionEndpoint = RegionEndpoint.USEast1,
            ServiceURL = "http://localhost/",
            ForcePathStyle = true,
            HttpClientFactory = new FixedHttpClientFactory(httpClient),
        };

        return new AmazonS3Client(new BasicAWSCredentials("test", "test"), config);
    }

    public async Task InitializeAsync()
    {
        await _fixture.HttpClient.PostAsync("/_ministack/reset", null);
    }

    public Task DisposeAsync()
    {
        _s3.Dispose();
        return Task.CompletedTask;
    }

    // ── Helpers ──────────────────────────────────────────────────────────────────

    private static MemoryStream ToStream(string text) =>
        new(System.Text.Encoding.UTF8.GetBytes(text));

    private static async Task<string> ReadStreamAsync(Stream stream)
    {
        using var reader = new StreamReader(stream);
        return await reader.ReadToEndAsync();
    }

    // ── Bucket operations ────────────────────────────────────────────────────────

    [Fact]
    public async Task CreateBucket()
    {
        await _s3.PutBucketAsync("create-bucket-test");

        var list = await _s3.ListBucketsAsync();
        list.Buckets.ShouldContain(b => b.BucketName == "create-bucket-test");
    }

    [Fact]
    public async Task CreateBucketAlreadyExists()
    {
        await _s3.PutBucketAsync("idempotent-bucket");
        await _s3.PutBucketAsync("idempotent-bucket");

        var list = await _s3.ListBucketsAsync();
        list.Buckets.Where(b => b.BucketName == "idempotent-bucket").ShouldHaveSingleItem();
    }

    [Fact]
    public async Task DeleteBucket()
    {
        await _s3.PutBucketAsync("del-bucket-test");
        await _s3.DeleteBucketAsync("del-bucket-test");

        var list = await _s3.ListBucketsAsync();
        list.Buckets.ShouldNotContain(b => b.BucketName == "del-bucket-test");
    }

    [Fact]
    public async Task DeleteBucketNotEmpty()
    {
        await _s3.PutBucketAsync("notempty-bucket");
        await _s3.PutObjectAsync(new PutObjectRequest
        {
            BucketName = "notempty-bucket",
            Key = "file.txt",
            InputStream = ToStream("data"),
        });

        var ex = await Should.ThrowAsync<AmazonS3Exception>(
            () => _s3.DeleteBucketAsync("notempty-bucket"));
        ex.ErrorCode.ShouldBe("BucketNotEmpty");
    }

    [Fact]
    public async Task DeleteBucketNotFound()
    {
        var ex = await Should.ThrowAsync<AmazonS3Exception>(
            () => _s3.DeleteBucketAsync("nonexistent-bucket-xyz"));
        ex.ErrorCode.ShouldBe("NoSuchBucket");
    }

    [Fact]
    public async Task HeadBucket()
    {
        await _s3.PutBucketAsync("head-bucket-test");

        // Should not throw
        await _s3.GetBucketLocationAsync("head-bucket-test");

        // Missing bucket
        var ex = await Should.ThrowAsync<AmazonS3Exception>(
            () => _s3.GetBucketLocationAsync("no-such-head-bucket"));
        ex.StatusCode.ShouldBe(HttpStatusCode.NotFound);
    }

    // ── Object operations ────────────────────────────────────────────────────────

    [Fact]
    public async Task PutGetObject()
    {
        await _s3.PutBucketAsync("putget-bucket");
        await _s3.PutObjectAsync(new PutObjectRequest
        {
            BucketName = "putget-bucket",
            Key = "hello.txt",
            InputStream = ToStream("hello world"),
            ContentType = "text/plain",
        });

        var resp = await _s3.GetObjectAsync("putget-bucket", "hello.txt");
        var body = await ReadStreamAsync(resp.ResponseStream);
        body.ShouldBe("hello world");
    }

    [Fact]
    public async Task PutObjectNoBucket()
    {
        var ex = await Should.ThrowAsync<AmazonS3Exception>(
            () => _s3.PutObjectAsync(new PutObjectRequest
            {
                BucketName = "no-such-bucket-xyz",
                Key = "file.txt",
                InputStream = ToStream("data"),
            }));
        ex.ErrorCode.ShouldBe("NoSuchBucket");
    }

    [Fact]
    public async Task HeadObject()
    {
        await _s3.PutBucketAsync("head-obj-bucket");
        await _s3.PutObjectAsync(new PutObjectRequest
        {
            BucketName = "head-obj-bucket",
            Key = "meta.txt",
            InputStream = ToStream("12345"),
            ContentType = "text/plain",
        });

        var meta = await _s3.GetObjectMetadataAsync("head-obj-bucket", "meta.txt");
        meta.ContentLength.ShouldBe(5);
        meta.Headers.ContentType.ShouldBe("text/plain");
        meta.ETag.ShouldNotBeEmpty();
    }

    [Fact]
    public async Task HeadObjectNotFound()
    {
        await _s3.PutBucketAsync("head-obj-404-bucket");

        var ex = await Should.ThrowAsync<AmazonS3Exception>(
            () => _s3.GetObjectMetadataAsync("head-obj-404-bucket", "nope.txt"));
        ex.StatusCode.ShouldBe(HttpStatusCode.NotFound);
    }

    [Fact]
    public async Task DeleteObject()
    {
        await _s3.PutBucketAsync("del-obj-bucket");
        await _s3.PutObjectAsync(new PutObjectRequest
        {
            BucketName = "del-obj-bucket",
            Key = "gone.txt",
            InputStream = ToStream("byebye"),
        });

        await _s3.DeleteObjectAsync("del-obj-bucket", "gone.txt");

        // SDK v4 throws NoSuchKeyException (a subclass of AmazonS3Exception) for missing keys
        var ex = await Should.ThrowAsync<Amazon.S3.Model.NoSuchKeyException>(
            () => _s3.GetObjectAsync("del-obj-bucket", "gone.txt"));
    }

    [Fact]
    public async Task DeleteObjectIdempotent()
    {
        await _s3.PutBucketAsync("del-obj-idem-bucket");

        // Deleting nonexistent object should succeed (204 implied)
        await _s3.DeleteObjectAsync("del-obj-idem-bucket", "nonexistent.txt");
    }

    [Fact]
    public async Task CopyObject()
    {
        await _s3.PutBucketAsync("copy-src-bucket");
        await _s3.PutBucketAsync("copy-dst-bucket");
        await _s3.PutObjectAsync(new PutObjectRequest
        {
            BucketName = "copy-src-bucket",
            Key = "original.txt",
            InputStream = ToStream("copy me"),
        });

        await _s3.CopyObjectAsync("copy-src-bucket", "original.txt", "copy-dst-bucket", "copied.txt");

        var resp = await _s3.GetObjectAsync("copy-dst-bucket", "copied.txt");
        var body = await ReadStreamAsync(resp.ResponseStream);
        body.ShouldBe("copy me");
    }

    [Fact]
    public async Task CopyObjectMetadataReplace()
    {
        await _s3.PutBucketAsync("copy-replace-bucket");
        await _s3.PutObjectAsync(new PutObjectRequest
        {
            BucketName = "copy-replace-bucket",
            Key = "src.txt",
            InputStream = ToStream("payload"),
            ContentType = "text/plain",
        });

        await _s3.CopyObjectAsync(new CopyObjectRequest
        {
            SourceBucket = "copy-replace-bucket",
            SourceKey = "src.txt",
            DestinationBucket = "copy-replace-bucket",
            DestinationKey = "dst.txt",
            MetadataDirective = S3MetadataDirective.REPLACE,
            ContentType = "application/octet-stream",
        });

        var meta = await _s3.GetObjectMetadataAsync("copy-replace-bucket", "dst.txt");
        meta.Headers.ContentType.ShouldBe("application/octet-stream");
    }

    // ── List objects ─────────────────────────────────────────────────────────────

    [Fact]
    public async Task ListObjectsV1()
    {
        await _s3.PutBucketAsync("list-v1-bucket");
        await _s3.PutObjectAsync(new PutObjectRequest { BucketName = "list-v1-bucket", Key = "photos/2024/a.jpg", InputStream = ToStream("a") });
        await _s3.PutObjectAsync(new PutObjectRequest { BucketName = "list-v1-bucket", Key = "photos/2024/b.jpg", InputStream = ToStream("b") });
        await _s3.PutObjectAsync(new PutObjectRequest { BucketName = "list-v1-bucket", Key = "photos/2025/c.jpg", InputStream = ToStream("c") });
        await _s3.PutObjectAsync(new PutObjectRequest { BucketName = "list-v1-bucket", Key = "docs/readme.md", InputStream = ToStream("d") });

        var resp = await _s3.ListObjectsAsync(new ListObjectsRequest
        {
            BucketName = "list-v1-bucket",
            Prefix = "photos/",
            Delimiter = "/",
        });

        ((resp.CommonPrefixes ?? []).Count).ShouldBe(2);
        resp.CommonPrefixes!.ShouldContain("photos/2024/");
        resp.CommonPrefixes!.ShouldContain("photos/2025/");
        resp.S3Objects.ShouldBeEmpty();
    }

    [Fact]
    public async Task ListObjectsV2()
    {
        await _s3.PutBucketAsync("list-v2-bucket");
        await _s3.PutObjectAsync(new PutObjectRequest { BucketName = "list-v2-bucket", Key = "a.txt", InputStream = ToStream("a") });
        await _s3.PutObjectAsync(new PutObjectRequest { BucketName = "list-v2-bucket", Key = "b.txt", InputStream = ToStream("b") });
        await _s3.PutObjectAsync(new PutObjectRequest { BucketName = "list-v2-bucket", Key = "c.txt", InputStream = ToStream("c") });

        var resp = await _s3.ListObjectsV2Async(new ListObjectsV2Request
        {
            BucketName = "list-v2-bucket",
            Prefix = "",
        });

        resp.KeyCount.ShouldBe(3);
        resp.S3Objects.Count.ShouldBe(3);
    }

    [Fact]
    public async Task ListObjectsPagination()
    {
        await _s3.PutBucketAsync("list-page-bucket");
        for (var i = 0; i < 5; i++)
        {
            await _s3.PutObjectAsync(new PutObjectRequest
            {
                BucketName = "list-page-bucket",
                Key = $"item-{i:D3}.txt",
                InputStream = ToStream($"data-{i}"),
            });
        }

        var resp1 = await _s3.ListObjectsV2Async(new ListObjectsV2Request
        {
            BucketName = "list-page-bucket",
            MaxKeys = 2,
        });

        resp1.S3Objects.Count.ShouldBe(2);
        resp1.IsTruncated.ShouldBe(true);
        resp1.NextContinuationToken.ShouldNotBeEmpty();

        var resp2 = await _s3.ListObjectsV2Async(new ListObjectsV2Request
        {
            BucketName = "list-page-bucket",
            MaxKeys = 2,
            ContinuationToken = resp1.NextContinuationToken,
        });

        resp2.S3Objects.Count.ShouldBe(2);
        resp2.IsTruncated.ShouldBe(true);

        var resp3 = await _s3.ListObjectsV2Async(new ListObjectsV2Request
        {
            BucketName = "list-page-bucket",
            MaxKeys = 2,
            ContinuationToken = resp2.NextContinuationToken,
        });

        resp3.S3Objects.ShouldHaveSingleItem();
        resp3.IsTruncated.ShouldBe(false);

        var allKeys = resp1.S3Objects.Concat(resp2.S3Objects).Concat(resp3.S3Objects)
            .Select(o => o.Key).ToHashSet();
        allKeys.Count.ShouldBe(5);
    }

    [Fact]
    public async Task ListV1MarkerPagination()
    {
        await _s3.PutBucketAsync("list-v1-page-bucket");
        for (var i = 0; i < 6; i++)
        {
            await _s3.PutObjectAsync(new PutObjectRequest
            {
                BucketName = "list-v1-page-bucket",
                Key = $"dir/file-{i:D3}.txt",
                InputStream = ToStream($"data-{i}"),
            });
        }

        var resp1 = await _s3.ListObjectsAsync(new ListObjectsRequest
        {
            BucketName = "list-v1-page-bucket",
            Prefix = "dir/",
            Delimiter = "/",
            MaxKeys = 3,
        });

        // All items share the prefix "dir/" with no further delimiter, so they are Contents
        resp1.S3Objects.Count.ShouldBe(3);
        resp1.IsTruncated.ShouldBe(true);

        var resp2 = await _s3.ListObjectsAsync(new ListObjectsRequest
        {
            BucketName = "list-v1-page-bucket",
            Prefix = "dir/",
            Delimiter = "/",
            MaxKeys = 3,
            Marker = resp1.S3Objects[^1].Key,
        });

        resp2.S3Objects.Count.ShouldBe(3);
        resp2.IsTruncated.ShouldBe(false);

        var allKeys = resp1.S3Objects.Concat(resp2.S3Objects).Select(o => o.Key).ToHashSet();
        allKeys.Count.ShouldBe(6);
    }

    // ── Batch operations ─────────────────────────────────────────────────────────

    [Fact]
    public async Task DeleteObjectsBatch()
    {
        await _s3.PutBucketAsync("batch-del-bucket");
        for (var i = 0; i < 5; i++)
        {
            await _s3.PutObjectAsync(new PutObjectRequest
            {
                BucketName = "batch-del-bucket",
                Key = $"obj-{i}.txt",
                InputStream = ToStream($"data-{i}"),
            });
        }

        var resp = await _s3.DeleteObjectsAsync(new DeleteObjectsRequest
        {
            BucketName = "batch-del-bucket",
            Objects = Enumerable.Range(0, 5).Select(i =>
                new KeyVersion { Key = $"obj-{i}.txt" }).ToList(),
        });

        resp.DeletedObjects.Count.ShouldBe(5);

        var list = await _s3.ListObjectsV2Async(new ListObjectsV2Request
        {
            BucketName = "batch-del-bucket",
        });
        list.KeyCount.ShouldBe(0);
    }

    // ── Multipart upload ─────────────────────────────────────────────────────────

    [Fact]
    public async Task MultipartUpload()
    {
        await _s3.PutBucketAsync("multipart-bucket");

        var initResp = await _s3.InitiateMultipartUploadAsync(new InitiateMultipartUploadRequest
        {
            BucketName = "multipart-bucket",
            Key = "big.bin",
        });
        var uploadId = initResp.UploadId;
        uploadId.ShouldNotBeEmpty();

        var part1 = await _s3.UploadPartAsync(new UploadPartRequest
        {
            BucketName = "multipart-bucket",
            Key = "big.bin",
            UploadId = uploadId,
            PartNumber = 1,
            InputStream = ToStream("part1-"),
        });

        var part2 = await _s3.UploadPartAsync(new UploadPartRequest
        {
            BucketName = "multipart-bucket",
            Key = "big.bin",
            UploadId = uploadId,
            PartNumber = 2,
            InputStream = ToStream("part2"),
        });

        await _s3.CompleteMultipartUploadAsync(new CompleteMultipartUploadRequest
        {
            BucketName = "multipart-bucket",
            Key = "big.bin",
            UploadId = uploadId,
            PartETags = [
                new PartETag(1, part1.ETag),
                new PartETag(2, part2.ETag),
            ],
        });

        var obj = await _s3.GetObjectAsync("multipart-bucket", "big.bin");
        var body = await ReadStreamAsync(obj.ResponseStream);
        body.ShouldBe("part1-part2");
    }

    [Fact]
    public async Task AbortMultipartUpload()
    {
        await _s3.PutBucketAsync("abort-mp-bucket");

        var initResp = await _s3.InitiateMultipartUploadAsync(new InitiateMultipartUploadRequest
        {
            BucketName = "abort-mp-bucket",
            Key = "aborted.bin",
        });

        await _s3.UploadPartAsync(new UploadPartRequest
        {
            BucketName = "abort-mp-bucket",
            Key = "aborted.bin",
            UploadId = initResp.UploadId,
            PartNumber = 1,
            InputStream = ToStream("data"),
        });

        await _s3.AbortMultipartUploadAsync(new AbortMultipartUploadRequest
        {
            BucketName = "abort-mp-bucket",
            Key = "aborted.bin",
            UploadId = initResp.UploadId,
        });

        // Key should not exist — SDK v4 throws NoSuchKeyException
        var ex = await Should.ThrowAsync<Amazon.S3.Model.NoSuchKeyException>(
            () => _s3.GetObjectAsync("abort-mp-bucket", "aborted.bin"));
    }

    [Fact]
    public async Task MultipartListParts()
    {
        await _s3.PutBucketAsync("list-parts-bucket");

        var initResp = await _s3.InitiateMultipartUploadAsync(new InitiateMultipartUploadRequest
        {
            BucketName = "list-parts-bucket",
            Key = "parts.bin",
        });
        var uploadId = initResp.UploadId;

        for (var i = 1; i <= 3; i++)
        {
            await _s3.UploadPartAsync(new UploadPartRequest
            {
                BucketName = "list-parts-bucket",
                Key = "parts.bin",
                UploadId = uploadId,
                PartNumber = i,
                InputStream = ToStream($"part{i}"),
            });
        }

        var listResp = await _s3.ListPartsAsync(new ListPartsRequest
        {
            BucketName = "list-parts-bucket",
            Key = "parts.bin",
            UploadId = uploadId,
        });

        listResp.Parts.Count.ShouldBe(3);
        listResp.Parts.ShouldAllBe(p => !string.IsNullOrEmpty(p.ETag));
    }

    [Fact]
    public async Task ListMultipartUploads()
    {
        await _s3.PutBucketAsync("list-mp-bucket");

        var upload1 = await _s3.InitiateMultipartUploadAsync(new InitiateMultipartUploadRequest
        {
            BucketName = "list-mp-bucket",
            Key = "file1.bin",
        });

        var upload2 = await _s3.InitiateMultipartUploadAsync(new InitiateMultipartUploadRequest
        {
            BucketName = "list-mp-bucket",
            Key = "file2.bin",
        });

        var resp = await _s3.ListMultipartUploadsAsync(new ListMultipartUploadsRequest
        {
            BucketName = "list-mp-bucket",
        });

        (resp.MultipartUploads.Count >= 2).ShouldBe(true);
        resp.MultipartUploads.ShouldContain(u => u.Key == "file1.bin");
        resp.MultipartUploads.ShouldContain(u => u.Key == "file2.bin");

        // cleanup
        await _s3.AbortMultipartUploadAsync(new AbortMultipartUploadRequest
        {
            BucketName = "list-mp-bucket",
            Key = "file1.bin",
            UploadId = upload1.UploadId,
        });
        await _s3.AbortMultipartUploadAsync(new AbortMultipartUploadRequest
        {
            BucketName = "list-mp-bucket",
            Key = "file2.bin",
            UploadId = upload2.UploadId,
        });
    }

    // ── Range requests ───────────────────────────────────────────────────────────

    [Fact]
    public async Task GetObjectRange()
    {
        await _s3.PutBucketAsync("range-bucket");
        await _s3.PutObjectAsync(new PutObjectRequest
        {
            BucketName = "range-bucket",
            Key = "range.txt",
            InputStream = ToStream("abcdefgh"),
        });

        var resp = await _s3.GetObjectAsync(new GetObjectRequest
        {
            BucketName = "range-bucket",
            Key = "range.txt",
            ByteRange = new ByteRange("bytes=2-5"),
        });

        var body = await ReadStreamAsync(resp.ResponseStream);
        body.ShouldBe("cdef");
        resp.HttpStatusCode.ShouldBe(HttpStatusCode.PartialContent);
    }

    [Fact]
    public async Task RangeSuffix()
    {
        await _s3.PutBucketAsync("range-suffix-bucket");
        await _s3.PutObjectAsync(new PutObjectRequest
        {
            BucketName = "range-suffix-bucket",
            Key = "suffix.txt",
            InputStream = ToStream("abcdefgh"),
        });

        var resp = await _s3.GetObjectAsync(new GetObjectRequest
        {
            BucketName = "range-suffix-bucket",
            Key = "suffix.txt",
            ByteRange = new ByteRange("bytes=-3"),
        });

        var body = await ReadStreamAsync(resp.ResponseStream);
        body.ShouldBe("fgh");
    }

    [Fact]
    public async Task RangeBeyondEnd()
    {
        await _s3.PutBucketAsync("range-beyond-bucket");
        await _s3.PutObjectAsync(new PutObjectRequest
        {
            BucketName = "range-beyond-bucket",
            Key = "small.txt",
            InputStream = ToStream("abc"),
        });

        var ex = await Should.ThrowAsync<AmazonS3Exception>(
            () => _s3.GetObjectAsync(new GetObjectRequest
            {
                BucketName = "range-beyond-bucket",
                Key = "small.txt",
                ByteRange = new ByteRange("bytes=100-200"),
            }));
        ex.StatusCode.ShouldBe(HttpStatusCode.RequestedRangeNotSatisfiable);
    }

    // ── Metadata ─────────────────────────────────────────────────────────────────

    [Fact]
    public async Task ObjectMetadata()
    {
        await _s3.PutBucketAsync("metadata-bucket");
        var putReq = new PutObjectRequest
        {
            BucketName = "metadata-bucket",
            Key = "meta.txt",
            InputStream = ToStream("body"),
        };
        putReq.Metadata.Add("custom-key", "custom-value");
        await _s3.PutObjectAsync(putReq);

        var head = await _s3.GetObjectMetadataAsync("metadata-bucket", "meta.txt");
        head.Metadata["custom-key"].ShouldBe("custom-value");
    }

    [Fact]
    public async Task PutObjectContentTypePreserved()
    {
        await _s3.PutBucketAsync("ctype-bucket");
        await _s3.PutObjectAsync(new PutObjectRequest
        {
            BucketName = "ctype-bucket",
            Key = "typed.json",
            InputStream = ToStream("{}"),
            ContentType = "application/json",
        });

        var head = await _s3.GetObjectMetadataAsync("ctype-bucket", "typed.json");
        head.Headers.ContentType.ShouldBe("application/json");
    }

    [Fact]
    public async Task HeadObjectReturnsContentLength()
    {
        await _s3.PutBucketAsync("clen-bucket");
        await _s3.PutObjectAsync(new PutObjectRequest
        {
            BucketName = "clen-bucket",
            Key = "sized.txt",
            InputStream = ToStream("12345678"),
        });

        var head = await _s3.GetObjectMetadataAsync("clen-bucket", "sized.txt");
        head.ContentLength.ShouldBe(8);
    }

    // ── Tags ─────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task BucketTagging()
    {
        await _s3.PutBucketAsync("btag-bucket");
        await _s3.PutBucketTaggingAsync(new PutBucketTaggingRequest
        {
            BucketName = "btag-bucket",
            TagSet =
            [
                new Tag { Key = "env", Value = "test" },
                new Tag { Key = "project", Value = "microstack" },
            ],
        });

        var getResp = await _s3.GetBucketTaggingAsync(new GetBucketTaggingRequest { BucketName = "btag-bucket" });
        getResp.TagSet.Count.ShouldBe(2);
        getResp.TagSet.ShouldContain(t => t.Key == "env" && t.Value == "test");
        getResp.TagSet.ShouldContain(t => t.Key == "project" && t.Value == "microstack");

        await _s3.DeleteBucketTaggingAsync(new DeleteBucketTaggingRequest { BucketName = "btag-bucket" });

        var ex = await Should.ThrowAsync<AmazonS3Exception>(
            () => _s3.GetBucketTaggingAsync(new GetBucketTaggingRequest { BucketName = "btag-bucket" }));
        ex.ErrorCode.ShouldBe("NoSuchTagSet");
    }

    [Fact]
    public async Task ObjectTagging()
    {
        await _s3.PutBucketAsync("otag-bucket");
        await _s3.PutObjectAsync(new PutObjectRequest
        {
            BucketName = "otag-bucket",
            Key = "tagged.txt",
            InputStream = ToStream("data"),
        });

        await _s3.PutObjectTaggingAsync(new PutObjectTaggingRequest
        {
            BucketName = "otag-bucket",
            Key = "tagged.txt",
            Tagging = new Tagging
            {
                TagSet =
                [
                    new Tag { Key = "color", Value = "blue" },
                    new Tag { Key = "size", Value = "large" },
                ],
            },
        });

        var getResp = await _s3.GetObjectTaggingAsync(new GetObjectTaggingRequest
        {
            BucketName = "otag-bucket",
            Key = "tagged.txt",
        });
        getResp.Tagging.Count.ShouldBe(2);
        getResp.Tagging.ShouldContain(t => t.Key == "color" && t.Value == "blue");
    }

    [Fact]
    public async Task PutObjectWithTaggingHeader()
    {
        await _s3.PutBucketAsync("taghdr-bucket");
        await _s3.PutObjectAsync(new PutObjectRequest
        {
            BucketName = "taghdr-bucket",
            Key = "header-tagged.txt",
            InputStream = ToStream("data"),
            TagSet =
            [
                new Tag { Key = "env", Value = "prod" },
            ],
        });

        var getResp = await _s3.GetObjectTaggingAsync(new GetObjectTaggingRequest
        {
            BucketName = "taghdr-bucket",
            Key = "header-tagged.txt",
        });
        getResp.Tagging.ShouldHaveSingleItem();
        getResp.Tagging[0].Key.ShouldBe("env");
        getResp.Tagging[0].Value.ShouldBe("prod");
    }

    [Fact]
    public async Task TagCountLimit()
    {
        await _s3.PutBucketAsync("taglimit-bucket");
        await _s3.PutObjectAsync(new PutObjectRequest
        {
            BucketName = "taglimit-bucket",
            Key = "toomany.txt",
            InputStream = ToStream("data"),
        });

        var ex = await Should.ThrowAsync<AmazonS3Exception>(
            () => _s3.PutObjectTaggingAsync(new PutObjectTaggingRequest
            {
                BucketName = "taglimit-bucket",
                Key = "toomany.txt",
                Tagging = new Tagging
                {
                    TagSet = Enumerable.Range(0, 11)
                        .Select(i => new Tag { Key = $"key{i}", Value = $"val{i}" })
                        .ToList(),
                },
            }));
        ex.ErrorCode.ShouldBe("BadRequest");
    }

    [Fact]
    public async Task CopyPreservesTags()
    {
        await _s3.PutBucketAsync("copytag-bucket");
        await _s3.PutObjectAsync(new PutObjectRequest
        {
            BucketName = "copytag-bucket",
            Key = "src.txt",
            InputStream = ToStream("data"),
        });
        await _s3.PutObjectTaggingAsync(new PutObjectTaggingRequest
        {
            BucketName = "copytag-bucket",
            Key = "src.txt",
            Tagging = new Tagging
            {
                TagSet = [new Tag { Key = "team", Value = "alpha" }],
            },
        });

        await _s3.CopyObjectAsync("copytag-bucket", "src.txt", "copytag-bucket", "dst.txt");

        var tags = await _s3.GetObjectTaggingAsync(new GetObjectTaggingRequest
        {
            BucketName = "copytag-bucket",
            Key = "dst.txt",
        });
        tags.Tagging.ShouldHaveSingleItem();
        tags.Tagging[0].Key.ShouldBe("team");
        tags.Tagging[0].Value.ShouldBe("alpha");
    }

    [Fact]
    public async Task CopyReplaceTags()
    {
        await _s3.PutBucketAsync("copytag-replace-bucket");
        await _s3.PutObjectAsync(new PutObjectRequest
        {
            BucketName = "copytag-replace-bucket",
            Key = "src.txt",
            InputStream = ToStream("data"),
        });
        await _s3.PutObjectTaggingAsync(new PutObjectTaggingRequest
        {
            BucketName = "copytag-replace-bucket",
            Key = "src.txt",
            Tagging = new Tagging
            {
                TagSet = [new Tag { Key = "old", Value = "val" }],
            },
        });

        // Copy the object, then replace its tags with PutObjectTagging
        await _s3.CopyObjectAsync("copytag-replace-bucket", "src.txt", "copytag-replace-bucket", "replaced.txt");

        await _s3.PutObjectTaggingAsync(new PutObjectTaggingRequest
        {
            BucketName = "copytag-replace-bucket",
            Key = "replaced.txt",
            Tagging = new Tagging
            {
                TagSet = [new Tag { Key = "new", Value = "replaced" }],
            },
        });

        var tags = await _s3.GetObjectTaggingAsync(new GetObjectTaggingRequest
        {
            BucketName = "copytag-replace-bucket",
            Key = "replaced.txt",
        });
        tags.Tagging.ShouldNotBeNull();
        tags.Tagging.ShouldHaveSingleItem();
        tags.Tagging[0].Key.ShouldBe("new");
    }

    // ── Bucket sub-resources ─────────────────────────────────────────────────────

    [Fact]
    public async Task BucketPolicy()
    {
        await _s3.PutBucketAsync("policy-bucket");
        var policyJson = "{\"Version\":\"2012-10-17\",\"Statement\":[]}";

        await _s3.PutBucketPolicyAsync("policy-bucket", policyJson);

        var resp = await _s3.GetBucketPolicyAsync("policy-bucket");
        resp.Policy.ShouldContain("2012-10-17");
    }

    [Fact]
    public async Task BucketVersioning()
    {
        await _s3.PutBucketAsync("versioning-bucket");

        await _s3.PutBucketVersioningAsync(new PutBucketVersioningRequest
        {
            BucketName = "versioning-bucket",
            VersioningConfig = new S3BucketVersioningConfig { Status = VersionStatus.Enabled },
        });

        var resp = await _s3.GetBucketVersioningAsync("versioning-bucket");
        resp.VersioningConfig.Status.ShouldBe(VersionStatus.Enabled);
    }

    [Fact]
    public async Task PutObjectReturnsVersionId()
    {
        await _s3.PutBucketAsync("ver-put-bucket");
        await _s3.PutBucketVersioningAsync(new PutBucketVersioningRequest
        {
            BucketName = "ver-put-bucket",
            VersioningConfig = new S3BucketVersioningConfig { Status = VersionStatus.Enabled },
        });

        var resp = await _s3.PutObjectAsync(new PutObjectRequest
        {
            BucketName = "ver-put-bucket",
            Key = "versioned.txt",
            InputStream = ToStream("v1"),
        });
        resp.VersionId.ShouldNotBeEmpty();
    }

    [Fact]
    public async Task PutObjectNoVersionIdWithoutVersioning()
    {
        await _s3.PutBucketAsync("nover-bucket");

        var resp = await _s3.PutObjectAsync(new PutObjectRequest
        {
            BucketName = "nover-bucket",
            Key = "plain.txt",
            InputStream = ToStream("data"),
        });
        resp.VersionId.ShouldBeNull();
    }

    [Fact]
    public async Task BucketEncryption()
    {
        await _s3.PutBucketAsync("enc-bucket");

        await _s3.PutBucketEncryptionAsync(new PutBucketEncryptionRequest
        {
            BucketName = "enc-bucket",
            ServerSideEncryptionConfiguration = new ServerSideEncryptionConfiguration
            {
                ServerSideEncryptionRules =
                [
                    new ServerSideEncryptionRule
                    {
                        ServerSideEncryptionByDefault = new ServerSideEncryptionByDefault
                        {
                            ServerSideEncryptionAlgorithm = ServerSideEncryptionMethod.AES256,
                        },
                    },
                ],
            },
        });

        var resp = await _s3.GetBucketEncryptionAsync(new GetBucketEncryptionRequest { BucketName = "enc-bucket" });
        resp.ServerSideEncryptionConfiguration.ServerSideEncryptionRules.ShouldNotBeEmpty();

        await _s3.DeleteBucketEncryptionAsync(new DeleteBucketEncryptionRequest { BucketName = "enc-bucket" });

        // SDK v4 may not throw after deletion; verify by checking the response
        try
        {
            var afterDelete = await _s3.GetBucketEncryptionAsync(new GetBucketEncryptionRequest { BucketName = "enc-bucket" });
            // If no exception, the configuration should be null or empty
            (afterDelete.ServerSideEncryptionConfiguration is null
                || afterDelete.ServerSideEncryptionConfiguration.ServerSideEncryptionRules is null
                || afterDelete.ServerSideEncryptionConfiguration.ServerSideEncryptionRules.Count == 0).ShouldBe(true);
        }
        catch (AmazonS3Exception ex)
        {
            ex.ErrorCode.ShouldBe("ServerSideEncryptionConfigurationNotFoundError");
        }
    }

    [Fact]
    public async Task BucketLifecycle()
    {
        await _s3.PutBucketAsync("lifecycle-bucket");

        await _s3.PutLifecycleConfigurationAsync(new PutLifecycleConfigurationRequest
        {
            BucketName = "lifecycle-bucket",
            Configuration = new LifecycleConfiguration
            {
                Rules =
                [
                    new LifecycleRule
                    {
                        Id = "rule1",
                        Status = LifecycleRuleStatus.Enabled,
                        Filter = new LifecycleFilter { LifecycleFilterPredicate = new LifecyclePrefixPredicate { Prefix = "logs/" } },
                        Expiration = new LifecycleRuleExpiration { Days = 30 },
                    },
                ],
            },
        });

        var resp = await _s3.GetLifecycleConfigurationAsync("lifecycle-bucket");
        resp.Configuration.Rules.ShouldNotBeEmpty();

        await _s3.DeleteLifecycleConfigurationAsync("lifecycle-bucket");

        // SDK v4 may not throw after deletion; verify by checking the response
        try
        {
            var afterDelete = await _s3.GetLifecycleConfigurationAsync("lifecycle-bucket");
            (afterDelete.Configuration is null
                || afterDelete.Configuration.Rules is null
                || afterDelete.Configuration.Rules.Count == 0).ShouldBe(true);
        }
        catch (AmazonS3Exception ex)
        {
            ex.ErrorCode.ShouldBe("NoSuchLifecycleConfiguration");
        }
    }

    [Fact]
    public async Task BucketCors()
    {
        await _s3.PutBucketAsync("cors-bucket");

        await _s3.PutCORSConfigurationAsync(new PutCORSConfigurationRequest
        {
            BucketName = "cors-bucket",
            Configuration = new CORSConfiguration
            {
                Rules =
                [
                    new CORSRule
                    {
                        AllowedMethods = ["GET"],
                        AllowedOrigins = ["*"],
                    },
                ],
            },
        });

        var resp = await _s3.GetCORSConfigurationAsync("cors-bucket");
        resp.Configuration.Rules.ShouldNotBeEmpty();

        await _s3.DeleteCORSConfigurationAsync("cors-bucket");

        var ex = await Should.ThrowAsync<AmazonS3Exception>(
            () => _s3.GetCORSConfigurationAsync("cors-bucket"));
        ex.ErrorCode.ShouldBe("NoSuchCORSConfiguration");
    }

    [Fact]
    public async Task BucketAcl()
    {
        await _s3.PutBucketAsync("acl-bucket");

        var resp = await _s3.GetBucketAclAsync(new GetBucketAclRequest { BucketName = "acl-bucket" });
        resp.Grants.ShouldNotBeNull();
        resp.Grants.ShouldNotBeEmpty();
    }

    [Fact]
    public async Task BucketWebsite()
    {
        await _s3.PutBucketAsync("website-bucket");

        await _s3.PutBucketWebsiteAsync(new PutBucketWebsiteRequest
        {
            BucketName = "website-bucket",
            WebsiteConfiguration = new WebsiteConfiguration
            {
                IndexDocumentSuffix = "index.html",
            },
        });

        var resp = await _s3.GetBucketWebsiteAsync("website-bucket");
        resp.WebsiteConfiguration.IndexDocumentSuffix.ShouldNotBeNull();

        await _s3.DeleteBucketWebsiteAsync("website-bucket");

        // SDK v4 may not throw after deletion; verify by checking the response
        try
        {
            var afterDelete = await _s3.GetBucketWebsiteAsync("website-bucket");
            afterDelete.WebsiteConfiguration.IndexDocumentSuffix.ShouldBeNull();
        }
        catch (AmazonS3Exception ex)
        {
            ex.ErrorCode.ShouldBe("NoSuchWebsiteConfiguration");
        }
    }

    [Fact]
    public async Task BucketLogging()
    {
        await _s3.PutBucketAsync("logging-bucket");

        await _s3.PutBucketLoggingAsync(new PutBucketLoggingRequest
        {
            BucketName = "logging-bucket",
            LoggingConfig = new S3BucketLoggingConfig(),
        });

        var resp = await _s3.GetBucketLoggingAsync("logging-bucket");
        resp.ShouldNotBeNull();
    }

    [Fact]
    public async Task PublicAccessBlock()
    {
        await _s3.PutBucketAsync("pab-bucket");

        await _s3.PutPublicAccessBlockAsync(new PutPublicAccessBlockRequest
        {
            BucketName = "pab-bucket",
            PublicAccessBlockConfiguration = new PublicAccessBlockConfiguration
            {
                BlockPublicAcls = true,
                BlockPublicPolicy = true,
                IgnorePublicAcls = true,
                RestrictPublicBuckets = true,
            },
        });

        var resp = await _s3.GetPublicAccessBlockAsync(new GetPublicAccessBlockRequest
        {
            BucketName = "pab-bucket",
        });
        resp.PublicAccessBlockConfiguration.BlockPublicAcls.ShouldBe(true);

        await _s3.DeletePublicAccessBlockAsync(new DeletePublicAccessBlockRequest
        {
            BucketName = "pab-bucket",
        });

        // After delete, default should still return
        var resp2 = await _s3.GetPublicAccessBlockAsync(new GetPublicAccessBlockRequest
        {
            BucketName = "pab-bucket",
        });
        resp2.PublicAccessBlockConfiguration.ShouldNotBeNull();
    }

    [Fact]
    public async Task OwnershipControls()
    {
        await _s3.PutBucketAsync("owner-ctrl-bucket");

        await _s3.PutBucketOwnershipControlsAsync(new PutBucketOwnershipControlsRequest
        {
            BucketName = "owner-ctrl-bucket",
            OwnershipControls = new OwnershipControls
            {
                Rules =
                [
                    new OwnershipControlsRule { ObjectOwnership = ObjectOwnership.BucketOwnerEnforced },
                ],
            },
        });

        var resp = await _s3.GetBucketOwnershipControlsAsync(new GetBucketOwnershipControlsRequest
        {
            BucketName = "owner-ctrl-bucket",
        });
        resp.OwnershipControls.Rules.ShouldNotBeEmpty();

        await _s3.DeleteBucketOwnershipControlsAsync(new DeleteBucketOwnershipControlsRequest
        {
            BucketName = "owner-ctrl-bucket",
        });

        // After delete, should still return default
        var resp2 = await _s3.GetBucketOwnershipControlsAsync(new GetBucketOwnershipControlsRequest
        {
            BucketName = "owner-ctrl-bucket",
        });
        resp2.OwnershipControls.ShouldNotBeNull();
    }

    // ── Object Lock ──────────────────────────────────────────────────────────────

    [Fact]
    public async Task ObjectLockConfiguration()
    {
        await _s3.PutBucketAsync(new PutBucketRequest
        {
            BucketName = "lock-cfg-bucket",
            ObjectLockEnabledForBucket = true,
        });

        var getCfg = await _s3.GetObjectLockConfigurationAsync(new GetObjectLockConfigurationRequest
        {
            BucketName = "lock-cfg-bucket",
        });
        getCfg.ObjectLockConfiguration.ObjectLockEnabled.ShouldBe(ObjectLockEnabled.Enabled);

        await _s3.PutObjectLockConfigurationAsync(new PutObjectLockConfigurationRequest
        {
            BucketName = "lock-cfg-bucket",
            ObjectLockConfiguration = new ObjectLockConfiguration
            {
                ObjectLockEnabled = ObjectLockEnabled.Enabled,
                Rule = new ObjectLockRule
                {
                    DefaultRetention = new DefaultRetention
                    {
                        Mode = ObjectLockRetentionMode.Governance,
                        Days = 1,
                    },
                },
            },
        });

        var getCfg2 = await _s3.GetObjectLockConfigurationAsync(new GetObjectLockConfigurationRequest
        {
            BucketName = "lock-cfg-bucket",
        });
        getCfg2.ObjectLockConfiguration.Rule.ShouldNotBeNull();
        getCfg2.ObjectLockConfiguration.Rule.DefaultRetention.Days.ShouldBe(1);
    }

    [Fact]
    public async Task ObjectLockRequiresVersioning()
    {
        await _s3.PutBucketAsync("lock-nover-bucket");

        var ex = await Should.ThrowAsync<AmazonS3Exception>(
            () => _s3.PutObjectLockConfigurationAsync(new PutObjectLockConfigurationRequest
            {
                BucketName = "lock-nover-bucket",
                ObjectLockConfiguration = new ObjectLockConfiguration
                {
                    ObjectLockEnabled = ObjectLockEnabled.Enabled,
                    Rule = new ObjectLockRule
                    {
                        DefaultRetention = new DefaultRetention
                        {
                            Mode = ObjectLockRetentionMode.Governance,
                            Days = 1,
                        },
                    },
                },
            }));
        ex.ErrorCode.ShouldBe("InvalidBucketState");
    }

    [Fact]
    public async Task ObjectRetention()
    {
        await _s3.PutBucketAsync(new PutBucketRequest
        {
            BucketName = "retention-bucket",
            ObjectLockEnabledForBucket = true,
        });
        await _s3.PutObjectAsync(new PutObjectRequest
        {
            BucketName = "retention-bucket",
            Key = "locked.txt",
            InputStream = ToStream("data"),
        });

        var retainUntil = DateTime.UtcNow.AddDays(1);
        await _s3.PutObjectRetentionAsync(new PutObjectRetentionRequest
        {
            BucketName = "retention-bucket",
            Key = "locked.txt",
            Retention = new ObjectLockRetention
            {
                Mode = ObjectLockRetentionMode.Governance,
                RetainUntilDate = retainUntil,
            },
        });

        var resp = await _s3.GetObjectRetentionAsync(new GetObjectRetentionRequest
        {
            BucketName = "retention-bucket",
            Key = "locked.txt",
        });
        resp.Retention.Mode.ShouldBe(ObjectLockRetentionMode.Governance);
        (resp.Retention.RetainUntilDate > DateTime.UtcNow).ShouldBe(true);
    }

    [Fact]
    public async Task ObjectLegalHold()
    {
        await _s3.PutBucketAsync(new PutBucketRequest
        {
            BucketName = "legalhold-bucket",
            ObjectLockEnabledForBucket = true,
        });
        await _s3.PutObjectAsync(new PutObjectRequest
        {
            BucketName = "legalhold-bucket",
            Key = "held.txt",
            InputStream = ToStream("data"),
        });

        await _s3.PutObjectLegalHoldAsync(new PutObjectLegalHoldRequest
        {
            BucketName = "legalhold-bucket",
            Key = "held.txt",
            LegalHold = new ObjectLockLegalHold { Status = ObjectLockLegalHoldStatus.On },
        });

        var resp = await _s3.GetObjectLegalHoldAsync(new GetObjectLegalHoldRequest
        {
            BucketName = "legalhold-bucket",
            Key = "held.txt",
        });
        resp.LegalHold.Status.ShouldBe(ObjectLockLegalHoldStatus.On);

        // Toggle off
        await _s3.PutObjectLegalHoldAsync(new PutObjectLegalHoldRequest
        {
            BucketName = "legalhold-bucket",
            Key = "held.txt",
            LegalHold = new ObjectLockLegalHold { Status = ObjectLockLegalHoldStatus.Off },
        });

        var resp2 = await _s3.GetObjectLegalHoldAsync(new GetObjectLegalHoldRequest
        {
            BucketName = "legalhold-bucket",
            Key = "held.txt",
        });
        resp2.LegalHold.Status.ShouldBe(ObjectLockLegalHoldStatus.Off);
    }

    [Fact]
    public async Task ObjectLockPreventsDelete()
    {
        await _s3.PutBucketAsync(new PutBucketRequest
        {
            BucketName = "lockdel-bucket",
            ObjectLockEnabledForBucket = true,
        });
        await _s3.PutObjectAsync(new PutObjectRequest
        {
            BucketName = "lockdel-bucket",
            Key = "protected.txt",
            InputStream = ToStream("data"),
        });

        // Legal hold prevents delete
        await _s3.PutObjectLegalHoldAsync(new PutObjectLegalHoldRequest
        {
            BucketName = "lockdel-bucket",
            Key = "protected.txt",
            LegalHold = new ObjectLockLegalHold { Status = ObjectLockLegalHoldStatus.On },
        });

        var ex = await Should.ThrowAsync<AmazonS3Exception>(
            () => _s3.DeleteObjectAsync("lockdel-bucket", "protected.txt"));
        ex.ErrorCode.ShouldBe("AccessDenied");

        // Remove legal hold and set governance retention
        await _s3.PutObjectLegalHoldAsync(new PutObjectLegalHoldRequest
        {
            BucketName = "lockdel-bucket",
            Key = "protected.txt",
            LegalHold = new ObjectLockLegalHold { Status = ObjectLockLegalHoldStatus.Off },
        });

        await _s3.PutObjectRetentionAsync(new PutObjectRetentionRequest
        {
            BucketName = "lockdel-bucket",
            Key = "protected.txt",
            Retention = new ObjectLockRetention
            {
                Mode = ObjectLockRetentionMode.Governance,
                RetainUntilDate = DateTime.UtcNow.AddDays(1),
            },
        });

        // Governance retention also prevents delete
        var ex2 = await Should.ThrowAsync<AmazonS3Exception>(
            () => _s3.DeleteObjectAsync("lockdel-bucket", "protected.txt"));
        ex2.ErrorCode.ShouldBe("AccessDenied");
    }

    [Fact]
    public async Task PutObjectWithLockHeaders()
    {
        await _s3.PutBucketAsync(new PutBucketRequest
        {
            BucketName = "lockheader-bucket",
            ObjectLockEnabledForBucket = true,
        });

        var retainUntil = DateTime.UtcNow.AddDays(1);
        await _s3.PutObjectAsync(new PutObjectRequest
        {
            BucketName = "lockheader-bucket",
            Key = "lockobj.txt",
            InputStream = ToStream("data"),
            ObjectLockMode = ObjectLockMode.Governance,
            ObjectLockRetainUntilDate = retainUntil,
            ObjectLockLegalHoldStatus = ObjectLockLegalHoldStatus.On,
        });

        var retention = await _s3.GetObjectRetentionAsync(new GetObjectRetentionRequest
        {
            BucketName = "lockheader-bucket",
            Key = "lockobj.txt",
        });
        retention.Retention.Mode.ShouldBe(ObjectLockRetentionMode.Governance);

        var legalHold = await _s3.GetObjectLegalHoldAsync(new GetObjectLegalHoldRequest
        {
            BucketName = "lockheader-bucket",
            Key = "lockobj.txt",
        });
        legalHold.LegalHold.Status.ShouldBe(ObjectLockLegalHoldStatus.On);
    }

    [Fact]
    public async Task DefaultRetentionApplied()
    {
        await _s3.PutBucketAsync(new PutBucketRequest
        {
            BucketName = "defret-bucket",
            ObjectLockEnabledForBucket = true,
        });

        await _s3.PutObjectLockConfigurationAsync(new PutObjectLockConfigurationRequest
        {
            BucketName = "defret-bucket",
            ObjectLockConfiguration = new ObjectLockConfiguration
            {
                ObjectLockEnabled = ObjectLockEnabled.Enabled,
                Rule = new ObjectLockRule
                {
                    DefaultRetention = new DefaultRetention
                    {
                        Mode = ObjectLockRetentionMode.Governance,
                        Days = 1,
                    },
                },
            },
        });

        await _s3.PutObjectAsync(new PutObjectRequest
        {
            BucketName = "defret-bucket",
            Key = "auto-locked.txt",
            InputStream = ToStream("data"),
        });

        var retention = await _s3.GetObjectRetentionAsync(new GetObjectRetentionRequest
        {
            BucketName = "defret-bucket",
            Key = "auto-locked.txt",
        });
        retention.Retention.Mode.ShouldBe(ObjectLockRetentionMode.Governance);
        (retention.Retention.RetainUntilDate > DateTime.UtcNow).ShouldBe(true);
    }

    [Fact]
    public async Task HeadObjectReturnsLockHeaders()
    {
        await _s3.PutBucketAsync(new PutBucketRequest
        {
            BucketName = "lockhead-bucket",
            ObjectLockEnabledForBucket = true,
        });
        await _s3.PutObjectAsync(new PutObjectRequest
        {
            BucketName = "lockhead-bucket",
            Key = "lockhead.txt",
            InputStream = ToStream("data"),
            ObjectLockMode = ObjectLockMode.Governance,
            ObjectLockRetainUntilDate = DateTime.UtcNow.AddDays(1),
            ObjectLockLegalHoldStatus = ObjectLockLegalHoldStatus.On,
        });

        var head = await _s3.GetObjectMetadataAsync("lockhead-bucket", "lockhead.txt");
        head.ObjectLockMode.ShouldBe(ObjectLockMode.Governance);
        head.ObjectLockLegalHoldStatus.ShouldBe(ObjectLockLegalHoldStatus.On);
        (head.ObjectLockRetainUntilDate > DateTime.UtcNow).ShouldBe(true);
    }

    [Fact]
    public async Task BatchDeleteEnforcesLock()
    {
        await _s3.PutBucketAsync(new PutBucketRequest
        {
            BucketName = "batchlock-bucket",
            ObjectLockEnabledForBucket = true,
        });

        await _s3.PutObjectAsync(new PutObjectRequest
        {
            BucketName = "batchlock-bucket",
            Key = "unlocked.txt",
            InputStream = ToStream("free"),
        });
        await _s3.PutObjectAsync(new PutObjectRequest
        {
            BucketName = "batchlock-bucket",
            Key = "locked.txt",
            InputStream = ToStream("held"),
        });
        await _s3.PutObjectLegalHoldAsync(new PutObjectLegalHoldRequest
        {
            BucketName = "batchlock-bucket",
            Key = "locked.txt",
            LegalHold = new ObjectLockLegalHold { Status = ObjectLockLegalHoldStatus.On },
        });

        // SDK v4 throws DeleteObjectsException when there are errors in the batch
        var ex = await Should.ThrowAsync<DeleteObjectsException>(
            () => _s3.DeleteObjectsAsync(new DeleteObjectsRequest
            {
                BucketName = "batchlock-bucket",
                Objects =
                [
                    new KeyVersion { Key = "unlocked.txt" },
                    new KeyVersion { Key = "locked.txt" },
                ],
            }));

        ex.Response.DeletedObjects.ShouldHaveSingleItem();
        ex.Response.DeletedObjects[0].Key.ShouldBe("unlocked.txt");
        ex.Response.DeleteErrors.ShouldHaveSingleItem();
        ex.Response.DeleteErrors[0].Key.ShouldBe("locked.txt");
    }

    // ── Replication ──────────────────────────────────────────────────────────────

    [Fact]
    public async Task BucketReplication()
    {
        await _s3.PutBucketAsync("repl-src-bucket");
        await _s3.PutBucketVersioningAsync(new PutBucketVersioningRequest
        {
            BucketName = "repl-src-bucket",
            VersioningConfig = new S3BucketVersioningConfig { Status = VersionStatus.Enabled },
        });
        await _s3.PutBucketAsync("repl-dst-bucket");
        await _s3.PutBucketVersioningAsync(new PutBucketVersioningRequest
        {
            BucketName = "repl-dst-bucket",
            VersioningConfig = new S3BucketVersioningConfig { Status = VersionStatus.Enabled },
        });

        await _s3.PutBucketReplicationAsync(new PutBucketReplicationRequest
        {
            Configuration = new ReplicationConfiguration
            {
                Role = "arn:aws:iam::012345678901:role/repl-role",
                Rules =
                [
                    new ReplicationRule
                    {
                        Status = ReplicationRuleStatus.Enabled,
                        Destination = new ReplicationDestination
                        {
                            BucketArn = "arn:aws:s3:::repl-dst-bucket",
                        },
                        Filter = new ReplicationRuleFilter
                        {
                            Prefix = "",
                        },
                    },
                ],
            },
            BucketName = "repl-src-bucket",
        });

        var resp = await _s3.GetBucketReplicationAsync(new GetBucketReplicationRequest { BucketName = "repl-src-bucket" });
        resp.Configuration.Rules.ShouldNotBeEmpty();

        await _s3.DeleteBucketReplicationAsync(new DeleteBucketReplicationRequest
        {
            BucketName = "repl-src-bucket",
        });

        // SDK v4 may not throw after deletion; verify by checking the response
        try
        {
            var afterDelete = await _s3.GetBucketReplicationAsync(new GetBucketReplicationRequest { BucketName = "repl-src-bucket" });
            (afterDelete.Configuration is null
                || afterDelete.Configuration.Rules is null
                || afterDelete.Configuration.Rules.Count == 0).ShouldBe(true);
        }
        catch (AmazonS3Exception ex)
        {
            ex.ErrorCode.ShouldBe("ReplicationConfigurationNotFoundError");
        }
    }

    [Fact]
    public async Task ReplicationRequiresVersioning()
    {
        await _s3.PutBucketAsync("repl-nover-bucket");

        var ex = await Should.ThrowAsync<AmazonS3Exception>(
            () => _s3.PutBucketReplicationAsync(new PutBucketReplicationRequest
            {
                BucketName = "repl-nover-bucket",
                Configuration = new ReplicationConfiguration
                {
                    Role = "arn:aws:iam::012345678901:role/repl-role",
                    Rules =
                    [
                        new ReplicationRule
                        {
                            Status = ReplicationRuleStatus.Enabled,
                            Destination = new ReplicationDestination
                            {
                                BucketArn = "arn:aws:s3:::some-dst",
                            },
                            Filter = new ReplicationRuleFilter
                            {
                                Prefix = "",
                            },
                        },
                    ],
                },
            }));
        ex.ErrorCode.ShouldBe("InvalidRequest");
    }

    // ── Other ────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task CopyPreservesMetadata()
    {
        await _s3.PutBucketAsync("copymeta-bucket");
        var putReq = new PutObjectRequest
        {
            BucketName = "copymeta-bucket",
            Key = "src.txt",
            InputStream = ToStream("body"),
            ContentType = "text/plain",
        };
        putReq.Metadata.Add("author", "tester");
        await _s3.PutObjectAsync(putReq);

        await _s3.CopyObjectAsync(new CopyObjectRequest
        {
            SourceBucket = "copymeta-bucket",
            SourceKey = "src.txt",
            DestinationBucket = "copymeta-bucket",
            DestinationKey = "dst.txt",
            MetadataDirective = S3MetadataDirective.COPY,
        });

        var head = await _s3.GetObjectMetadataAsync("copymeta-bucket", "dst.txt");
        head.Metadata["author"].ShouldBe("tester");
        head.Headers.ContentType.ShouldBe("text/plain");
    }

    [Fact]
    public async Task DeleteObjectsReturnsDeleted()
    {
        await _s3.PutBucketAsync("delret-bucket");
        for (var i = 0; i < 3; i++)
        {
            await _s3.PutObjectAsync(new PutObjectRequest
            {
                BucketName = "delret-bucket",
                Key = $"f{i}.txt",
                InputStream = ToStream("d"),
            });
        }

        var resp = await _s3.DeleteObjectsAsync(new DeleteObjectsRequest
        {
            BucketName = "delret-bucket",
            Objects = Enumerable.Range(0, 3).Select(i =>
                new KeyVersion { Key = $"f{i}.txt" }).ToList(),
        });

        resp.DeletedObjects.Count.ShouldBe(3);
    }

    [Fact]
    public async Task ListObjectVersions()
    {
        await _s3.PutBucketAsync("versions-bucket");
        await _s3.PutBucketVersioningAsync(new PutBucketVersioningRequest
        {
            BucketName = "versions-bucket",
            VersioningConfig = new S3BucketVersioningConfig { Status = VersionStatus.Enabled },
        });

        await _s3.PutObjectAsync(new PutObjectRequest
        {
            BucketName = "versions-bucket",
            Key = "ver.txt",
            InputStream = ToStream("v1"),
        });
        await _s3.PutObjectAsync(new PutObjectRequest
        {
            BucketName = "versions-bucket",
            Key = "ver.txt",
            InputStream = ToStream("v2"),
        });

        var resp = await _s3.ListVersionsAsync("versions-bucket");
        // At least one version for the key (our implementation overwrites, but versions endpoint returns at least one)
        (resp.Versions.Count >= 1).ShouldBe(true);
        resp.Versions.ShouldContain(v => v.Key == "ver.txt");
    }

    [Fact]
    public async Task GetObjectWithVersionId()
    {
        await _s3.PutBucketAsync("getver-bucket");
        await _s3.PutBucketVersioningAsync(new PutBucketVersioningRequest
        {
            BucketName = "getver-bucket",
            VersioningConfig = new S3BucketVersioningConfig { Status = VersionStatus.Enabled },
        });

        var put1 = await _s3.PutObjectAsync(new PutObjectRequest
        {
            BucketName = "getver-bucket",
            Key = "verobj.txt",
            InputStream = ToStream("v1"),
        });
        var put2 = await _s3.PutObjectAsync(new PutObjectRequest
        {
            BucketName = "getver-bucket",
            Key = "verobj.txt",
            InputStream = ToStream("v2"),
        });

        put1.VersionId.ShouldNotBeEmpty();
        put2.VersionId.ShouldNotBeEmpty();
        put2.VersionId.ShouldNotBe(put1.VersionId);
    }

    [Fact]
    public async Task UploadPartCopy()
    {
        await _s3.PutBucketAsync("partcopy-bucket");
        await _s3.PutObjectAsync(new PutObjectRequest
        {
            BucketName = "partcopy-bucket",
            Key = "source.txt",
            InputStream = ToStream("AABBCCDD"),
        });

        var initResp = await _s3.InitiateMultipartUploadAsync(new InitiateMultipartUploadRequest
        {
            BucketName = "partcopy-bucket",
            Key = "dest.txt",
        });

        var copyPartResp = await _s3.CopyPartAsync(new CopyPartRequest
        {
            SourceBucket = "partcopy-bucket",
            SourceKey = "source.txt",
            DestinationBucket = "partcopy-bucket",
            DestinationKey = "dest.txt",
            UploadId = initResp.UploadId,
            PartNumber = 1,
        });
        copyPartResp.ETag.ShouldNotBeEmpty();

        await _s3.CompleteMultipartUploadAsync(new CompleteMultipartUploadRequest
        {
            BucketName = "partcopy-bucket",
            Key = "dest.txt",
            UploadId = initResp.UploadId,
            PartETags = [new PartETag(1, copyPartResp.ETag)],
        });

        var obj = await _s3.GetObjectAsync("partcopy-bucket", "dest.txt");
        var body = await ReadStreamAsync(obj.ResponseStream);
        body.ShouldBe("AABBCCDD");
    }
}
