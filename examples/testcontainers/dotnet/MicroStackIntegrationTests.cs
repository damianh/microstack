using Amazon;
using Amazon.Runtime;
using Amazon.S3;
using Amazon.S3.Model;
using Amazon.SQS;
using Amazon.SQS.Model;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using Xunit;

namespace MicroStack.Testcontainers.Example;

public sealed class MicroStackIntegrationTests : IAsyncLifetime
{
    private const int MicroStackPort = 4566;
    private const string Region = "us-east-1";

    private readonly IContainer _container = new ContainerBuilder(RequireImage())
        .WithPortBinding(MicroStackPort, assignRandomHostPort: true)
        .WithWaitStrategy(Wait.ForUnixContainer().UntilHttpRequestIsSucceeded(
            request => request
                .ForPort(MicroStackPort)
                .ForPath("/_microstack/health")))
        .Build();

    private string Endpoint =>
        $"http://{_container.Hostname}:{_container.GetMappedPublicPort(MicroStackPort)}";

    public async ValueTask InitializeAsync() => await _container.StartAsync();

    public async ValueTask DisposeAsync() => await _container.DisposeAsync();

    [Fact]
    public async Task AwsSdkCanUseSqsAndS3ThroughTheMappedEndpoint()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var credentials = new BasicAWSCredentials("test", "test");
        using var sqs = new AmazonSQSClient(credentials, new AmazonSQSConfig
        {
            ServiceURL = Endpoint,
            AuthenticationRegion = Region,
        });
        using var s3 = new AmazonS3Client(credentials, new AmazonS3Config
        {
            ServiceURL = Endpoint,
            AuthenticationRegion = Region,
            ForcePathStyle = true,
        });

        const string queueName = "testcontainers-example";
        var created = await sqs.CreateQueueAsync(queueName, cancellationToken);
        var queueUrl = created.QueueUrl;

        Assert.Equal(
            new Uri(Endpoint).Authority,
            new Uri(queueUrl).Authority,
            ignoreCase: true);

        var resolved = await sqs.GetQueueUrlAsync(queueName, cancellationToken);
        Assert.Equal(queueUrl, resolved.QueueUrl);

        var listed = await sqs.ListQueuesAsync("testcontainers-", cancellationToken);
        Assert.Contains(queueUrl, listed.QueueUrls);

        const string messageBody = "hello from the .NET Testcontainers example";
        await sqs.SendMessageAsync(queueUrl, messageBody, cancellationToken);
        var received = await sqs.ReceiveMessageAsync(new ReceiveMessageRequest
        {
            QueueUrl = queueUrl,
            MaxNumberOfMessages = 1,
        }, cancellationToken);
        Assert.Single(received.Messages);
        Assert.Equal(messageBody, received.Messages[0].Body);
        await sqs.DeleteQueueAsync(queueUrl, cancellationToken);

        const string bucketName = "testcontainers-example";
        const string objectKey = "payload.bin";
        byte[] payload = [0, 1, 2, 3, 127, 128, 254, 255];

        await s3.PutBucketAsync(bucketName, cancellationToken);
        await using (var input = new MemoryStream(payload))
        {
            await s3.PutObjectAsync(new PutObjectRequest
            {
                BucketName = bucketName,
                Key = objectKey,
                InputStream = input,
            }, cancellationToken);
        }

        using var getResponse = await s3.GetObjectAsync(bucketName, objectKey, cancellationToken);
        await using var actual = new MemoryStream();
        await getResponse.ResponseStream.CopyToAsync(actual, cancellationToken);
        Assert.Equal(payload, actual.ToArray());
    }

    private static string RequireImage() =>
        Environment.GetEnvironmentVariable("MICROSTACK_TEST_IMAGE") is { Length: > 0 } image
            ? image
            : throw new InvalidOperationException(
                "Set MICROSTACK_TEST_IMAGE to a MicroStack image that implements the current container contract.");
}
