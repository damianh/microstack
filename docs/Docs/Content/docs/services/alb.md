---
title: ALB (ELBv2)
description: ALB/ELBv2 emulation — load balancers, target groups, listeners, rules, ALB-to-Lambda routing.
order: 19
section: Services
---

# ALB (ELBv2)

MicroStack's ALB handler emulates the Elastic Load Balancing v2 API — Application and Network Load Balancers, target groups, listeners, rules, and target health. ALB-to-Lambda live traffic routing is supported.

## Supported Operations

**Load Balancers**
- CreateLoadBalancer, DescribeLoadBalancers, DeleteLoadBalancer
- DescribeLoadBalancerAttributes, ModifyLoadBalancerAttributes
- SetSecurityGroups, SetSubnets

**Target Groups**
- CreateTargetGroup, DescribeTargetGroups, ModifyTargetGroup, DeleteTargetGroup
- DescribeTargetGroupAttributes, ModifyTargetGroupAttributes
- RegisterTargets, DeregisterTargets, DescribeTargetHealth

**Listeners**
- CreateListener, DescribeListeners, ModifyListener, DeleteListener

**Rules**
- CreateRule, DescribeRules, ModifyRule, DeleteRule, SetRulePriorities

**Tags**
- AddTags, RemoveTags, DescribeTags

## Usage

```csharp
var elb = new AmazonElasticLoadBalancingV2Client(
    new BasicAWSCredentials("test", "test"),
    new AmazonElasticLoadBalancingV2Config { ServiceURL = "http://localhost:4566" });

// Create a load balancer
var lb = await elb.CreateLoadBalancerAsync(new CreateLoadBalancerRequest
{
    Name = "my-alb",
    Type = LoadBalancerTypeEnum.Application,
    Scheme = LoadBalancerSchemeEnum.InternetFacing,
});

var lbArn = lb.LoadBalancers[0].LoadBalancerArn;
Console.WriteLine(lb.LoadBalancers[0].DNSName);    // synthetic DNS name

// Create a target group
var tg = await elb.CreateTargetGroupAsync(new CreateTargetGroupRequest
{
    Name = "my-targets",
    Protocol = ProtocolEnum.HTTP,
    Port = 80,
    VpcId = "vpc-00000001",
    HealthCheckPath = "/health",
});

var tgArn = tg.TargetGroups[0].TargetGroupArn;

// Create a listener
var listener = await elb.CreateListenerAsync(new CreateListenerRequest
{
    LoadBalancerArn = lbArn,
    Protocol = ProtocolEnum.HTTP,
    Port = 80,
    DefaultActions =
    [
        new Action
        {
            Type = ActionTypeEnum.Forward,
            TargetGroupArn = tgArn,
        },
    ],
});
```

## Listener Rules

```csharp
// Add a path-based routing rule
await elb.CreateRuleAsync(new CreateRuleRequest
{
    ListenerArn = listener.Listeners[0].ListenerArn,
    Priority = 10,
    Conditions =
    [
        new RuleCondition
        {
            Field = "path-pattern",
            Values = ["/api/*"],
        },
    ],
    Actions =
    [
        new Action
        {
            Type = ActionTypeEnum.Forward,
            TargetGroupArn = tgArn,
        },
    ],
});
```

:::aside{type="note" title="Lambda routing"}
ALB-to-Lambda routing is live in MicroStack — create a target group with `TargetType = Lambda`, register a Lambda function ARN as a target, and actual HTTP requests forwarded through the ALB will invoke the function.
:::
