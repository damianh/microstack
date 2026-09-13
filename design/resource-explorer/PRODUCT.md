# Product

<!-- impeccable:product-schema 1 -->

## Platform

web

## Users

Developers inspecting and debugging a local MicroStack instance. Their primary
path is service -> resource -> contents, with connected resources and relevant
activity available in context.

## Product Purpose

MicroStack emulates AWS services locally. This prototype explores how developers
can inspect resources and contents without switching repeatedly to SDK or CLI
commands. It is a design artifact, not an extension of the running emulator.

## Operating Context

The existing admin application is Blazor WebAssembly on a separate UI port.
The emulator is a Native AOT .NET application. Resources may belong to different
accounts, while the configured region must not be mistaken for per-region
isolation. The prototype uses only clearly labeled synthetic data.
The UI assumes one local instance, so it needs no local-instance indicator or
instance switcher.

## Capabilities and Constraints

- Representative inspectors: S3, SQS, DynamoDB, SNS, and EventBridge.
- Inspect and debug, not provision or mutate resources.
- SNS subscriptions and EventBridge rules/targets provide configured connections;
  they do not establish that an event matched or was delivered.
- Existing admin resource summaries cover SQS, S3, and DynamoDB only.
- Current request logs record service, action, account, timestamp, status, and
  duration, not reliable resource IDs or causal trace IDs.
- Non-consuming SQS inspection is a proposed admin capability, not ReceiveMessage.
- Backend APIs, live integration, destructive actions, and production changes are
  excluded from this prototype.
- Navigation should accommodate other services without invented inspectors.
- Confirmed navigation decision (11 September 2026): service-first home and a
  split inspector per service. This is the selected direction, not merely a
  comparison default. The searchable, grouped services home opens a resource
  list beside an inspector, without a permanent services rail.
- All services navigation and a compact service switcher remain available inside
  inspection. Alternative layouts and comparison controls have been removed.
  This design decision does not itself authorize production implementation.
- The current-service breadcrumb opens a searchable, alphabetically sorted
  switcher with arrow-key navigation, Enter selection, and Escape dismissal.

## Brand Commitments

MicroStack name and factual terminology remain. The user explicitly approved
replacing the Bootstrap template appearance with a purpose-designed developer
tool. The user subsequently selected the familiar developer console direction.
No specific reference products were supplied: conventional cloud-console
navigation and database-inspector patterns are working assumptions, not endorsed
references. Use a restrained light interface for reading data alongside other
development windows; this is a prototype decision, not a product requirement.

Use official AWS service/resource icons to identify all 40 catalog entries,
with corresponding family icons for related APIs. Keep AWS artwork unmodified
and local, alongside explicit text labels. Retain the MicroStack logo as the
product identity; AWS icons do not imply AWS endorsement.

## Evidence on Hand

The repository has a working initial dashboard, request log, and generic resource
lists, plus a MicroStack logo. Service handlers provide related AWS operations.
Synthetic order-processing examples are illustrations, never evidence of live
resources or observed deliveries.

## Product Principles

- Preserve context while moving from a resource to its contents or connections.
- Separate configured state, observed requests, and inferred relationships.
- Expose partial, stale, missing, and unsupported data rather than hiding limits.
- Optimize for task clarity and scanning rather than a marketing presentation.

## Accessibility & Inclusion

The prototype plan includes keyboard-accessible controls, visible focus, text
status labels, readable payloads, and a focused detail view on narrow screens.
