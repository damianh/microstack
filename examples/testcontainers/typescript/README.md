# MicroStack with Testcontainers for TypeScript

This example starts MicroStack on a random host port and verifies SQS and S3
through AWS SDK for JavaScript v3.

Requirements: Node.js 22.22+ and a running Docker daemon.

```bash
npm ci
npm test
```

The test uses `ghcr.io/damianh/microstack:latest` by default. To test another
image:

```bash
MICROSTACK_TEST_IMAGE=microstack:test npm test
```

In PowerShell, set it with
`$env:MICROSTACK_TEST_IMAGE = "microstack:test"` before running the test.
