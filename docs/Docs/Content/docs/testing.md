---
title: Integration Testing
description: Using MicroStack with WebApplicationFactory for fast, in-process integration tests.
order: 4
section: Guides
---

# Integration Testing

MicroStack is designed for integration testing with the AWS SDK for .NET. Use `WebApplicationFactory<Program>` to run MicroStack in-process — no Docker, no network, sub-millisecond request times.

## Setup

Add MicroStack as a project reference and the AWS SDK packages you need:

```xml
<ItemGroup>
  <ProjectReference Include="..\..\src\MicroStack\MicroStack.csproj" />
  <PackageReference Include="Microsoft.AspNetCore.Mvc.Testing" Version="10.*" />
  <PackageReference Include="AWSSDK.SQS" Version="4.*" />
  <PackageReference Include="AWSSDK.S3" Version="4.*" />
  <!-- Add more AWSSDK.* packages as needed -->
</ItemGroup>
```

## Test Fixture

Create a shared fixture using `WebApplicationFactory`:

```csharp
using Microsoft.AspNetCore.Mvc.Testing;

public sealed class MicroStackFixture : IDisposable
{
    public WebApplicationFactory<Program> Factory { get; }
    public HttpClient HttpClient { get; }

    public MicroStackFixture()
    {
        Factory    = new WebApplicationFactory<Program>();
        HttpClient = Factory.CreateClient();
    }

    public void Dispose()
    {
        HttpClient.Dispose();
        Factory.Dispose();
    }
}
```

## AWS SDK Client Setup

AWS SDK v4 requires a `CanonicalizeUriHandler` workaround for `TestServer`:

```csharp
internal sealed class CanonicalizeUriHandler(HttpMessageHandler inner)
    : DelegatingHandler(inner)
{
    protected override Task<HttpResponseMessage> SendAsync(
        HttpRequestMessage request, CancellationToken cancellationToken)
    {
        if (request.RequestUri is not null)
        {
            request.RequestUri = new Uri(request.RequestUri.AbsoluteUri);
        }
        return base.SendAsync(request, cancellationToken);
    }
}

internal sealed class FixedHttpClientFactory(HttpClient client)
    : Amazon.Runtime.HttpClientFactory
{
    public override HttpClient CreateHttpClient(IClientConfig clientConfig) => client;
    public override bool DisposeHttpClientsAfterUse(IClientConfig clientConfig) => false;
    public override bool UseSDKHttpClientCaching(IClientConfig clientConfig) => false;
}
```

Then create SDK clients:

```csharp
var innerHandler = fixture.Factory.Server.CreateHandler();
var httpClient = new HttpClient(new CanonicalizeUriHandler(innerHandler))
{
    BaseAddress = new Uri("http://localhost/"),
};

var config = new AmazonSQSConfig
{
    RegionEndpoint = RegionEndpoint.USEast1,
    ServiceURL = "http://localhost/",
    HttpClientFactory = new FixedHttpClientFactory(httpClient),
};

var sqs = new AmazonSQSClient(new BasicAWSCredentials("test", "test"), config);
```

## Example Test

```csharp
public sealed class SqsTests : IClassFixture<MicroStackFixture>, IAsyncLifetime
{
    private readonly MicroStackFixture _fixture;
    private readonly AmazonSQSClient _sqs;

    public SqsTests(MicroStackFixture fixture)
    {
        _fixture = fixture;
        _sqs = CreateSqsClient(fixture);
    }

    public async Task InitializeAsync()
    {
        await _fixture.HttpClient.PostAsync("/_ministack/reset", null);
    }

    public Task DisposeAsync() { _sqs.Dispose(); return Task.CompletedTask; }

    [Fact]
    public async Task CreateQueueAndSendMessage()
    {
        var created = await _sqs.CreateQueueAsync("test-queue");
        Assert.NotEmpty(created.QueueUrl);

        await _sqs.SendMessageAsync(new SendMessageRequest
        {
            QueueUrl = created.QueueUrl,
            MessageBody = "hello world",
        });

        var received = await _sqs.ReceiveMessageAsync(new ReceiveMessageRequest
        {
            QueueUrl = created.QueueUrl,
        });

        Assert.Single(received.Messages);
        Assert.Equal("hello world", received.Messages[0].Body);
    }
}
```

## Multi-Tenancy in Tests

Use 12-digit AWS access key IDs to simulate separate accounts:

```csharp
// Account A
var clientA = new AmazonSQSClient(
    new BasicAWSCredentials("111111111111", "test"), config);

// Account B
var clientB = new AmazonSQSClient(
    new BasicAWSCredentials("222222222222", "test"), config);
```

Each account has isolated state — resources created by Account A are not visible to Account B.
