---
title: Step Functions
description: Step Functions emulation with a full ASL execution engine.
order: 7
section: Services
---

# Step Functions

MicroStack includes a full AWS States Language (ASL) execution engine supporting all state types.

## Supported State Types

- **Pass** — Inject fixed results or transform input
- **Task** — Invoke Lambda functions or mock responses
- **Choice** — Conditional branching with comparison operators
- **Wait** — Time-based delays (seconds, timestamp)
- **Parallel** — Execute branches concurrently
- **Map** — Iterate over arrays
- **Succeed** — Terminal success state
- **Fail** — Terminal failure with error/cause

## Mock Configuration

Configure mock responses for Task states via the config endpoint:

```bash
curl -X POST http://localhost:4566/_ministack/config \
  -H 'Content-Type: application/json' \
  -d '{
    "stepfunctions": {
      "_sfn_mock_config": {
        "StateMachines": {
          "MyStateMachine": {
            "TestCases": {
              "HappyPath": {
                "LambdaInvoke": {
                  "0": { "Return": {"result": "ok"} }
                }
              }
            }
          }
        }
      }
    }
  }'
```
