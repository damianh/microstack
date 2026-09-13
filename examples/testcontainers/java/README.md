# MicroStack with Testcontainers Java

This Maven project runs an integration test against a MicroStack image using
generic Testcontainers and AWS SDK for Java v2. It requires Java 17+, Maven,
Docker, and an image named by `MICROSTACK_TEST_IMAGE`.

PowerShell:

```powershell
$env:MICROSTACK_TEST_IMAGE = "ghcr.io/damianh/microstack:latest"
mvn test
```

Bash:

```bash
MICROSTACK_TEST_IMAGE=ghcr.io/damianh/microstack:latest mvn test
```

The test exposes container port 4566 on a random host port, waits for
`/_microstack/health`, then exercises SQS queue and message operations and
path-style S3 object storage.
