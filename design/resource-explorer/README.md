# Resource Explorer design reference

This is the preserved, runnable prototype of the selected service-first directory
and split inspector. It is a **frozen synthetic design reference**, not the
production Admin UI or an emulator integration. All resources, payloads, counts,
account context, activity, and timestamps are fixtures. No AWS credentials,
emulator, .NET build, or live API calls are required.

## Run from the repository

Install a currently supported Node.js release (Node 22 or newer), then run this
from the repository root in PowerShell:

```powershell
node .\design\resource-explorer\server.mjs
```

There are no npm dependencies and no install or build step. The server binds
**only to `127.0.0.1`** and chooses an available ephemeral port by default. Open
the `PROTOTYPE_URL=http://127.0.0.1:<port>/` printed in the terminal.

To request a specific port in PowerShell:

```powershell
$env:PORT = '8080'
node .\design\resource-explorer\server.mjs
```

Stop the server with **Ctrl+C**. Remove the override with
`Remove-Item Env:PORT` to restore automatic port selection on the next launch.
An explicit `PORT=0` also selects an available port. This server serves only its
allowlisted pages and static assets; unknown paths return 404. It is a local
review tool, not a production host.

## What is preserved

- All 40 catalog entries and locally served AWS icons.
- Five example inspectors: S3, SQS, DynamoDB, SNS, and EventBridge.
- Searchable grouped home and alphabetically sorted breadcrumb service switcher.
- Resource-specific contents, configuration, configured connections, return
  trails, service/account activity, and example-state controls.
- Responsive layouts, keyboard interactions, and UTC ISO 8601 sample timestamps.

Start with `/`, read `/notes.html` for decisions and a guided review path, and
use `/qa.html` for side-by-side 1440px and 390px layout frames.
[PRODUCT.md](PRODUCT.md) and [DESIGN.md](DESIGN.md) retain the original product
and visual-design snapshot. Their statements about the then-current application
and unresolved implementation work are historical, not live production status.

Production implementation has since been authorized separately, including
all-service inspection, account switching, and text/JSON previews up to 1 MiB.
That does not change this five-inspector mock or make its synthetic behavior an
approved production API contract. Production must use actual modeled data,
read-only inspection APIs, bounded previews, and appropriate sensitive-data
handling; it must not ship the fixtures, example-state selector, or demo banner.
The mock has no account discovery, real request log, resource mutation, or
delivery tracing. Configured relationships are not proof of event delivery.

Keep this snapshot useful for deliberate future design work; it need not mirror
every production change. Make any later design revisions explicit rather than
silently replacing fixtures with emulator calls. The reference lives outside
`src` and the published docs tree, is excluded from Docker contexts by
`.dockerignore`, and is not registered in application build/publish projects.

## Repeatable manual review

- [ ] Search the 40-entry directory; filter to entries with an inspector and
  distinguish unsupported examples from the synthetic disabled EMR entry.
- [ ] Open each of the five inspectors; change selected resources and tabs.
  In SQS inspect nested JSON; in DynamoDB inspect typed attributes and filter
  the loaded subset; in SNS inspect subscriptions; in EventBridge switch rules.
- [ ] In S3 navigate prefixes and inspect JSON, CSV, binary, and oversized
  examples. Binary and oversized payloads should remain metadata-only.
- [ ] Follow an SQS connection to SNS, then use the explicit return link.
  Check restored selection, tab, filter, page, and prefix where applicable;
  also exercise browser Back/Forward and service deep links.
- [ ] Exercise the breadcrumb switcher with search, arrows, Home/End, Enter,
  Escape, Tab/Shift+Tab, and focus restoration. Check the skip link and tab
  keyboard navigation without a mouse.
- [ ] Review empty, loading, error, disabled, unsupported, stale, missing-link,
  and large-list scenarios. Large content samples paginate 125 entries by 10
  where supported; these are examples, not real service capabilities.
- [ ] Check full-date UTC timestamps ending in `Z`, desktop and narrow layouts,
  long identifiers, internal data scrolling, and readable status labels.
- [ ] Confirm all local images load, unknown URLs return 404, and browser
  network activity contains no emulator or AWS API requests.

These are repeatable review steps, not a claim that historical test results
remain a current guarantee.

## AWS artwork provenance and usage

The 40 files in `icons` are **unmodified Amazon Web Services artwork**, from the
**July 31, 2026 AWS Architecture Icons release**. They are third-party assets,
**not licensed under MicroStack's MIT license**.

- Publisher, original archive URL, exact source paths, and family mappings:
  [aws-icon-sources.json](aws-icon-sources.json).
- Official source and published usage guidance:
  [AWS Architecture Icons](https://aws.amazon.com/architecture/icons/).

Related API variants share their corresponding family artwork; dedicated
resource icons identify S3 Files, CloudWatch Logs, STS, and Application Load
Balancer. Preserve original colors, proportions, and provenance. AWS artwork
identifies the corresponding emulated services, not MicroStack branding or AWS
endorsement. The MicroStack logo remains the product identity. Consult AWS's
published usage guidance and applicable terms before publishing or
redistributing these assets; inclusion here does not grant additional rights.
