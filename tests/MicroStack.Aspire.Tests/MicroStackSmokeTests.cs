// Copyright (c) MicroStack contributors. All rights reserved.
// Licensed under the MIT License.

using Amazon.Runtime;
using Amazon.S3;
using Amazon.S3.Model;
using Amazon.SimpleNotificationService;
using Amazon.SQS;
using Microsoft.Extensions.DependencyInjection;

namespace MicroStack.Aspire.Tests;

public sealed class MicroStackSmokeTests : IAsyncLifetime
{
    private static readonly TimeSpan DefaultTimeout = TimeSpan.FromSeconds(120);

    private DistributedApplication? _app;
    private string? _connectionString;

    public async ValueTask InitializeAsync()
    {
        var builder = await DistributedApplicationTestingBuilder
            .CreateAsync<Projects.MicroStack_Aspire_Tests_AppHost>();

        builder.Services.ConfigureHttpClientDefaults(clientBuilder =>
        {
            clientBuilder.AddStandardResilienceHandler();
        });

        _app = await builder.BuildAsync();
        await _app.StartAsync();

        using var cts = new CancellationTokenSource(DefaultTimeout);
        await _app.ResourceNotifications.WaitForResourceHealthyAsync(
            "microstack", cts.Token);

        _connectionString = await _app.GetConnectionStringAsync("microstack");
    }

    public async ValueTask DisposeAsync()
    {
        if (_app is not null)
        {
            await _app.DisposeAsync();
        }
    }

    [Fact]
    public void ConnectionStringShouldBeAvailable()
    {
        _connectionString.ShouldNotBeNullOrWhiteSpace();
        _connectionString.ShouldStartWith("http://");
    }

    [Fact]
    public async Task HealthEndpointShouldReturnOk()
    {
        var httpClient = _app!.CreateHttpClient("microstack");

        using var response = await httpClient.GetAsync("/_microstack/health");

        response.StatusCode.ShouldBe(HttpStatusCode.OK);
    }

    [Fact]
    public async Task S3CreateBucketAndPutObject()
    {
        using var s3 = CreateClient<AmazonS3Client>();

        await s3.PutBucketAsync("test-bucket");

        await s3.PutObjectAsync(new PutObjectRequest
        {
            BucketName = "test-bucket",
            Key = "hello.txt",
            ContentBody = "Hello from Aspire!",
        });

        var obj = await s3.GetObjectAsync("test-bucket", "hello.txt");

        using var reader = new StreamReader(obj.ResponseStream);
        var content = await reader.ReadToEndAsync();
        content.ShouldBe("Hello from Aspire!");
    }

    [Fact]
    public async Task SqsCreateQueueAndSendMessage()
    {
        using var sqs = CreateClient<AmazonSQSClient>();

        var createResult = await sqs.CreateQueueAsync("test-queue");
        createResult.HttpStatusCode.ShouldBe(HttpStatusCode.OK);

        await sqs.SendMessageAsync(createResult.QueueUrl, "hello from aspire");

        var receiveResult = await sqs.ReceiveMessageAsync(createResult.QueueUrl);
        receiveResult.Messages.ShouldHaveSingleItem();
        receiveResult.Messages[0].Body.ShouldBe("hello from aspire");
    }

    [Fact]
    public async Task SnsCreateTopic()
    {
        using var sns = CreateClient<AmazonSimpleNotificationServiceClient>();

        var createResult = await sns.CreateTopicAsync("test-topic");
        createResult.HttpStatusCode.ShouldBe(HttpStatusCode.OK);
        createResult.TopicArn.ShouldNotBeNullOrWhiteSpace();

        var listResult = await sns.ListTopicsAsync();
        listResult.Topics.ShouldContain(t => t.TopicArn == createResult.TopicArn);
    }

    private T CreateClient<T>() where T : AmazonServiceClient
    {
        var credentials = new BasicAWSCredentials("test", "test");
        var config = (ClientConfig)Activator.CreateInstance(typeof(T).Assembly
            .GetTypes()
            .Single(t => t.IsSubclassOf(typeof(ClientConfig)) && !t.IsAbstract))!;
        config.ServiceURL = _connectionString;
        config.AuthenticationRegion = "us-east-1";
        return (T)Activator.CreateInstance(typeof(T), credentials, config)!;
    }
}
