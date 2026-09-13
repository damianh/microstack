---
title: Integration Testing
description: Using MicroStack with .NET Aspire for integration testing.
order: 4
section: Guides
---

# Integration Testing

MicroStack is designed for integration testing with the AWS SDK for .NET. The recommended
approach uses **.NET Aspire** to spin up MicroStack as a container — matching how you
run in production.

## Testcontainers

MicroStack works with generic Testcontainers APIs in any language. The container
contract is:

- container port `4566`
- HTTP readiness at `/_microstack/health`
- dummy AWS credentials and an explicit signing region such as `us-east-1`
- path-style addressing for S3
- random host-port mappings; do not assume host port `4566`

Runnable examples are available for
[.NET](https://github.com/damianh/microstack/tree/main/examples/testcontainers/dotnet),
[Java](https://github.com/damianh/microstack/tree/main/examples/testcontainers/java),
[Go](https://github.com/damianh/microstack/tree/main/examples/testcontainers/go),
[Python](https://github.com/damianh/microstack/tree/main/examples/testcontainers/python),
and [TypeScript](https://github.com/damianh/microstack/tree/main/examples/testcontainers/typescript).
Each starts an isolated generic container and verifies SQS and S3 using the
language's AWS SDK.

Set `MICROSTACK_TEST_IMAGE` to test a local image:

```bash
docker build -t microstack:test .
export MICROSTACK_TEST_IMAGE=microstack:test
```

SQS queue URLs follow the incoming request's scheme, host, and mapped port, so they
can be passed back to the SDK unchanged. If tests run inside another container,
use an endpoint reachable on that container network rather than the host-published
endpoint. A single advertised address cannot be reachable from every network.

The release Dockerfile currently publishes a Linux x64 image. Docker Desktop can
run it from Windows and macOS hosts; ARM hosts require container emulation until
the Dockerfile release pipeline publishes ARM64 as well.

## Aspire-Based Testing

### 1. AppHost Setup

In your Aspire AppHost project, add MicroStack as a resource:

```bash
dotnet add package MicroStack.Aspire.Hosting
```

```csharp
var builder = DistributedApplication.CreateBuilder(args);

var microstack = builder.AddMicroStack("microstack");

builder.AddProject<Projects.MyApi>("api")
    .WithReference(microstack);

builder.Build().Run();
```

### 2. Test Project Setup

Add the Aspire testing package and the AWS SDK packages you need:

```xml
<ItemGroup>
  <PackageReference Include="Aspire.Hosting.Testing" Version="13.*" />
  <PackageReference Include="AWSSDK.SQS" Version="4.*" />
  <PackageReference Include="AWSSDK.S3" Version="4.*" />
  <!-- Add more AWSSDK.* packages as needed -->
</ItemGroup>
```

### 3. Test Fixture

Use `DistributedApplicationTestingBuilder` to start MicroStack as a container and
retrieve the connection string:

```csharp
public sealed class MicroStackFixture : IAsyncLifetime
{
    public DistributedApplication App { get; private set; } = null!;
    public string ConnectionString { get; private set; } = null!;

    public async Task InitializeAsync()
    {
        var builder = await DistributedApplicationTestingBuilder
            .CreateAsync<Projects.MyAspireAppHost>();

        App = await builder.BuildAsync();
        await App.StartAsync();

        ConnectionString = await App.GetConnectionStringAsync("microstack")
            ?? throw new InvalidOperationException("MicroStack connection string not found");
    }

    public async Task DisposeAsync()
    {
        await App.StopAsync();
        await App.DisposeAsync();
    }
}
```

### 4. Creating AWS SDK Clients

Use the connection string from the fixture to create AWS SDK clients:

```csharp
private static AmazonSQSClient CreateSqsClient(MicroStackFixture fixture)
{
    var config = new AmazonSQSConfig
    {
        ServiceURL = fixture.ConnectionString,
    };

    return new AmazonSQSClient(new BasicAWSCredentials("test", "test"), config);
}

private static AmazonS3Client CreateS3Client(MicroStackFixture fixture)
{
    var config = new AmazonS3Config
    {
        ServiceURL = fixture.ConnectionString,
        ForcePathStyle = true,
    };

    return new AmazonS3Client(new BasicAWSCredentials("test", "test"), config);
}
```

### 5. Example Test

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
        // Reset state between tests
        using var http = new HttpClient { BaseAddress = new Uri(_fixture.ConnectionString) };
        await http.PostAsync("/_microstack/reset", null);
    }

    public Task DisposeAsync()
    {
        _sqs.Dispose();
        return Task.CompletedTask;
    }

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

## Aspire Resource Configuration

You can customize the MicroStack resource in your AppHost:

```csharp
var microstack = builder.AddMicroStack("microstack")
    .WithDataVolume()                    // persistent state across restarts
    .WithServices("s3,sqs,dynamodb")     // limit enabled services
    .WithRegion("eu-west-1");            // set AWS region
```

| Method | Description |
|---|---|
| `AddMicroStack(name)` | Adds a MicroStack container on the default port |
| `AddMicroStack(name, port)` | Adds a MicroStack container on a specific host port |
| `WithDataVolume()` | Attaches a named volume for persistent state |
| `WithServices(services)` | Limits which AWS services are enabled |
| `WithRegion(region)` | Sets the AWS region |

## WebApplicationFactory (In-Process)

For projects that don't use Aspire, you can run MicroStack in-process using
`WebApplicationFactory<Program>`. This requires a project reference to MicroStack
and some additional setup for AWS SDK v4 compatibility.

Add MicroStack as a project reference:

```xml
<ItemGroup>
  <ProjectReference Include="..\..\src\MicroStack\MicroStack.csproj" />
  <PackageReference Include="Microsoft.AspNetCore.Mvc.Testing" Version="10.*" />
  <PackageReference Include="AWSSDK.SQS" Version="4.*" />
</ItemGroup>
```

### Test Fixture

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

### AWS SDK Client Setup

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
