# MicroStack with testcontainers-go

This integration test starts the image named by `MICROSTACK_TEST_IMAGE`, publishes
container port `4566` on a random host port, and waits for
`/_microstack/health`. It verifies SQS queue URL handling and path-style S3 access
with AWS SDK for Go v2.

From this directory, with Docker running:

```bash
export MICROSTACK_TEST_IMAGE=microstack:test
go test -v ./...
```

PowerShell:

```powershell
$env:MICROSTACK_TEST_IMAGE = "microstack:test"
go test -v ./...
```
