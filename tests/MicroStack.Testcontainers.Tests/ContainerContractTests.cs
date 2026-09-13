using Amazon;
using Amazon.Runtime;
using Amazon.S3;
using Amazon.S3.Model;
using Amazon.SQS;
using Amazon.SQS.Model;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;

namespace MicroStack.Testcontainers.Tests;

public sealed class ContainerContractTests : IAsyncLifetime
{
    private const int MicroStackPort = 4566;

    private readonly IContainer _container = new ContainerBuilder(GetTestImage())
        .WithPortBinding(MicroStackPort, assignRandomHostPort: true)
        .WithWaitStrategy(Wait.ForUnixContainer().UntilHttpRequestIsSucceeded(
            request => request.ForPort(MicroStackPort).ForPath("/_microstack/health")))
        .Build();

    private string Endpoint =>
        $"http://{_container.Hostname}:{_container.GetMappedPublicPort(MicroStackPort)}";

    public async ValueTask InitializeAsync() => await _container.StartAsync();

    public async ValueTask DisposeAsync() => await _container.DisposeAsync();

    [Fact]
    public async Task RandomMappedEndpointSupportsSqsAndS3()
    {
        var credentials = new BasicAWSCredentials("test", "test");
        using var sqs = new AmazonSQSClient(credentials, new AmazonSQSConfig
        {
            ServiceURL = Endpoint,
            AuthenticationRegion = RegionEndpoint.USEast1.SystemName,
        });
        using var s3 = new AmazonS3Client(credentials, new AmazonS3Config
        {
            ServiceURL = Endpoint,
            AuthenticationRegion = RegionEndpoint.USEast1.SystemName,
            ForcePathStyle = true,
        });

        var created = await sqs.CreateQueueAsync("testcontainers-queue");
        new Uri(created.QueueUrl).GetLeftPart(UriPartial.Authority).ShouldBe(Endpoint);

        var resolved = await sqs.GetQueueUrlAsync("testcontainers-queue");
        resolved.QueueUrl.ShouldBe(created.QueueUrl);
        (await sqs.ListQueuesAsync("testcontainers-")).QueueUrls.ShouldContain(created.QueueUrl);

        await sqs.SendMessageAsync(created.QueueUrl, "hello from testcontainers");
        var received = await sqs.ReceiveMessageAsync(new ReceiveMessageRequest
        {
            QueueUrl = created.QueueUrl,
            MaxNumberOfMessages = 1,
        });
        received.Messages.ShouldHaveSingleItem().Body.ShouldBe("hello from testcontainers");
        await sqs.DeleteQueueAsync(created.QueueUrl);

        const string bucketName = "testcontainers-bucket";
        const string objectKey = "payload.txt";
        const string payload = "hello from testcontainers";
        await s3.PutBucketAsync(bucketName);
        await s3.PutObjectAsync(new PutObjectRequest
        {
            BucketName = bucketName,
            Key = objectKey,
            ContentBody = payload,
        });

        using var response = await s3.GetObjectAsync(bucketName, objectKey);
        using var reader = new StreamReader(response.ResponseStream);
        (await reader.ReadToEndAsync()).ShouldBe(payload);
    }

    [Fact]
    public async Task LegacyEndpointStrategyPreservesConfiguredQueueUrls()
    {
        await using var legacyContainer = new ContainerBuilder(GetTestImage())
            .WithEnvironment("MICROSTACK_SQS_ENDPOINT_STRATEGY", "legacy")
            .WithPortBinding(MicroStackPort, assignRandomHostPort: true)
            .WithWaitStrategy(Wait.ForUnixContainer().UntilHttpRequestIsSucceeded(
                request => request.ForPort(MicroStackPort).ForPath("/_microstack/health")))
            .Build();

        await legacyContainer.StartAsync();
        var endpoint =
            $"http://{legacyContainer.Hostname}:{legacyContainer.GetMappedPublicPort(MicroStackPort)}";
        using var sqs = new AmazonSQSClient(
            new BasicAWSCredentials("test", "test"),
            new AmazonSQSConfig
            {
                ServiceURL = endpoint,
                AuthenticationRegion = RegionEndpoint.USEast1.SystemName,
            });

        var created = await sqs.CreateQueueAsync("legacy-address-queue");
        created.QueueUrl.ShouldBe(
            "http://localhost:4566/000000000000/legacy-address-queue");
    }

    private static string GetTestImage() =>
        Environment.GetEnvironmentVariable("MICROSTACK_TEST_IMAGE")
        ?? throw new InvalidOperationException(
            "Set MICROSTACK_TEST_IMAGE to the MicroStack image under test.");
}
