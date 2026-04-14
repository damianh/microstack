---
title: EventBridge
description: EventBridge emulation — event buses, rules, targets, PutEvents, archives, replays, connections, API destinations.
order: 16
section: Services
---

# EventBridge

MicroStack's EventBridge handler supports custom event buses, rules with event patterns and schedule expressions, targets, PutEvents, archives, replays, connections, and API destinations. A `default` event bus is always present.

## Supported Operations

**Event Buses**
- CreateEventBus, UpdateEventBus, DeleteEventBus, ListEventBuses, DescribeEventBus

**Rules and Targets**
- PutRule, DeleteRule, ListRules, DescribeRule, EnableRule, DisableRule
- PutTargets, RemoveTargets, ListTargetsByRule, ListRuleNamesByTarget
- TestEventPattern

**Events**
- PutEvents, PutPartnerEvents

**Archives and Replays**
- CreateArchive, DeleteArchive, DescribeArchive, UpdateArchive, ListArchives
- StartReplay, DescribeReplay, ListReplays, CancelReplay

**Connections and API Destinations**
- CreateConnection, DescribeConnection, DeleteConnection, ListConnections, UpdateConnection, DeauthorizeConnection
- CreateApiDestination, DescribeApiDestination, DeleteApiDestination, ListApiDestinations, UpdateApiDestination

**Endpoints and Partner Events**
- CreateEndpoint, DeleteEndpoint, DescribeEndpoint, ListEndpoints, UpdateEndpoint
- ActivateEventSource, DeactivateEventSource, DescribeEventSource
- CreatePartnerEventSource, DescribePartnerEventSource, ListPartnerEventSources, ListPartnerEventSourceAccounts
- ListEventSources, DeletePartnerEventSource

**Tags**
- TagResource, UntagResource, ListTagsForResource

**Permissions**
- PutPermission, RemovePermission

## Usage

```csharp
var eb = new AmazonEventBridgeClient(
    new BasicAWSCredentials("test", "test"),
    new AmazonEventBridgeConfig { ServiceURL = "http://localhost:4566" });

// Create a custom event bus
var bus = await eb.CreateEventBusAsync(new CreateEventBusRequest
{
    Name = "my-app-bus",
});

// Create a rule on the bus
await eb.PutRuleAsync(new PutRuleRequest
{
    Name = "order-events-rule",
    EventBusName = "my-app-bus",
    EventPattern = """{"source": ["my.app.orders"]}""",
    State = RuleState.ENABLED,
});

// Add a Lambda target
await eb.PutTargetsAsync(new PutTargetsRequest
{
    Rule = "order-events-rule",
    EventBusName = "my-app-bus",
    Targets =
    [
        new Target
        {
            Id = "order-processor",
            Arn = "arn:aws:lambda:us-east-1:000000000000:function:process-order",
        },
    ],
});
```

## Publishing Events

```csharp
// Send events to a bus
await eb.PutEventsAsync(new PutEventsRequest
{
    Entries =
    [
        new PutEventsRequestEntry
        {
            EventBusName = "my-app-bus",
            Source = "my.app.orders",
            DetailType = "OrderPlaced",
            Detail = """{"orderId": "ORD-001", "amount": 99.99}""",
        },
    ],
});
```

## Schedule-Based Rules

```csharp
// Create a rule that fires on a schedule (metadata only — no actual invocations)
await eb.PutRuleAsync(new PutRuleRequest
{
    Name = "hourly-cleanup",
    ScheduleExpression = "rate(1 hour)",
    State = RuleState.ENABLED,
});
```

:::aside{type="note" title="Event delivery"}
EventBridge rule targets are registered and listed but events are not automatically delivered to targets such as Lambda or SQS. To test event-driven flows, use the [Lambda](lambda) handler's event source mappings or invoke functions directly.
:::
