# MicroStack with Testcontainers for .NET

This xUnit integration test starts MicroStack through the generic Testcontainers
API, waits for its HTTP health endpoint, and exercises SQS and path-style S3 with
the AWS SDK for .NET.

## Run

Prerequisites: .NET 10 SDK and a Docker-compatible container runtime.

Build the current repository image and run the test from the repository root:

```powershell
docker build -t microstack:test .
$env:MICROSTACK_TEST_IMAGE = "microstack:test"
dotnet test examples\testcontainers\dotnet
```

On Bash-compatible shells:

```bash
docker build -t microstack:test .
MICROSTACK_TEST_IMAGE=microstack:test dotnet test examples/testcontainers/dotnet
```

`MICROSTACK_TEST_IMAGE` is required. The released tags present when this example
was added predate the request-derived SQS queue URLs that make random host-port
mapping work, so this example intentionally does not default to a released image.

The test publishes container port `4566` to a random host port and waits for
`/_microstack/health`. It passes the queue URL returned by MicroStack back to SQS
unchanged and verifies that URL points at the mapped endpoint.
