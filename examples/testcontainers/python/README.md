# Python testcontainers example

This integration test starts the image named by `MICROSTACK_TEST_IMAGE` on a
random host port, waits for MicroStack's HTTP health endpoint, and exercises SQS
and path-style S3 with `boto3`.

Requirements: Python 3.9+ and a running Docker daemon.

```sh
cd examples/testcontainers/python
python -m venv .venv
# Activate .venv, then:
python -m pip install -r requirements.lock
MICROSTACK_TEST_IMAGE=microstack:test python -m pytest
```

In PowerShell, set the image with
`$env:MICROSTACK_TEST_IMAGE = "microstack:test"` before running pytest.
