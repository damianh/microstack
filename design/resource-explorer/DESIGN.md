---
name: MicroStack resource inspection prototype
description: A familiar developer console for read-only local resource inspection.
colors:
  ink: "#202c3d"
  muted: "#526176"
  line: "#dbe1e9"
  blue: "#165dca"
  blue-soft: "#edf4ff"
  surface: "#ffffff"
  nav: "#f7f9fc"
  green: "#267048"
  amber: "#795100"
typography:
  directory-heading:
    fontSize: "2rem"
    fontWeight: 650
  body:
    fontFamily: '"Segoe UI", system-ui, -apple-system, sans-serif'
    fontSize: "16px"
    lineHeight: 1.5
  headline:
    fontSize: "1.625rem"
    fontWeight: 650
    lineHeight: 1.25
    letterSpacing: "-.025em"
  title:
    fontSize: "1.25rem"
    fontWeight: 650
    letterSpacing: "-.02em"
  data:
    fontFamily: 'Consolas, "SFMono-Regular", monospace'
    fontSize: ".8125rem"
rounded:
  control: "4px"
  inspector: "7px"
spacing:
  small: "8px"
  medium: "16px"
  panel: "24px"
components:
  button-primary:
    backgroundColor: "{colors.blue}"
    textColor: "{colors.surface}"
    rounded: "{rounded.control}"
    padding: "7px 12px"
---

# Design System: MicroStack Resource Mockups

## Overview

**Creative North Star: "The local resource workbench"**

A task-oriented console for developers moving from a service to a resource, then
its contents, configuration, and configured connections. The user chose familiar
developer-console conventions rather than an experimental visual direction.
Specific reference products were not endorsed.

This document describes the isolated prototype, not an approved production
redesign. The user selected services home -> dedicated split inspector as the new
prototype direction to avoid an ever-growing services rail. Alternative
compositions and their comparison controls have been removed. The live Blazor
application is unchanged; data legibility and preservation of debugging context
take priority over decoration.

## Colors

Blue identifies navigation, selected rows, links, and active tabs. White inspection
surfaces sit beside cool-gray navigation, with subtle borders defining sections.
Ink and muted text distinguish primary data from supporting metadata. Green and
amber statuses always carry text; color alone never asserts health or delivery.
Official AWS artwork retains its own service-category colors. Those asset colors
identify services, not selection or health; do not recolor or grayscale the icons.

## Typography

System sans-serif avoids font downloads and fits native development tools.
Consolas/SFMono monospace is reserved for identifiers, keys, and payloads.
Service headings are 1.625rem and resource titles 1.25rem on desktop; data,
controls, and supporting labels use .75rem to .9375rem. Narrow headings reduce
to 1.375rem and 1.0625rem. Payload line height is 1.75 for nested JSON.

UI timestamps use UTC ISO 8601 with a full date, seconds, and a `Z` suffix
(for example, `2026-09-11T10:20:00Z`), including object metadata, message lists,
activity logs, and snapshot labels. Durations remain human-readable.

## Layout

The services home is a searchable directory, not a dashboard of status cards.
Grouped lists occupy two columns on desktop and one below 760px, with a maximum
1400px page width. Availability text distinguishes the five clickable mockup
inspectors, unimplemented inspectors, and an explicitly synthetic disabled example.
Search includes service names, identifiers, and categories. A checkbox restricts
the list to services with example inspectors.

The only workspace is a split inspector: a persistent 220px resource index beside
inspection content, without a services rail. All services and a compact switcher
provide cross-service access. The current service in the breadcrumb opens a
searchable dropdown: All services / selected service. Entries sort alphabetically
by displayed name and filter by short or full service name. All 40 catalog entries
appear in the scrollable list. Services without mock inspectors are labeled and
marked aria-disabled; they remain discoverable by keyboard but cannot navigate
to an unfinished inspector. The search uses an
accessible combobox and listbox, with arrow navigation, Enter to select, and Escape
to dismiss and restore focus. Tab moves from the filter to the active result;
the list has one roving tab stop. Arrow keys and Home/End move focus within the
list, Shift+Tab returns to the filter, and Tab from a result leaves the picker.
The focused result has an inset focus outline that is not clipped by list scrolling.
Typing on the closed trigger also starts a search.
No matches are announced explicitly; choosing the current service preserves its
resource selection. There is no separate switcher on the right.
Main padding is 18px 28px.
The breadcrumb and account/configured-region scope share one aligned desktop
header row, with scope on the right. They wrap naturally on narrow screens;
scope remains visible on the services home when the breadcrumb is hidden.
At 1200px and below the inspector stacks records above payloads.
At 1000px resource-index width and panel padding contract. At 760px contents
stack and Browse resources opens a focused index.
Selecting a resource returns to detail. Long identifiers wrap; payloads and wide
activity tables scroll within their own containers, not the page.

Lists expose loaded counts and pagination. The large-list sample generates 125
content entries in pages of 10; it is not a server-side scalability claim.

## Elevation & Depth

Borders and surface tones carry almost all grouping. Toast feedback has a modest
floating shadow; the service dropdown uses a restrained offset shadow to separate
it from the inspector beneath. No decorative
gradients, hero treatments, or animated charts compete with inspected data.
State examples and local refresh feedback do not depend on animation.

## Shapes

Controls and status tags use restrained 4px corners. The inspector uses 7px corners
on desktop and 5px on narrow screens. Data sections remain rectangular and aligned.
The existing MicroStack logo supplies product identity.

## Components

All 40 directory services have locally served, unmodified AWS SVG artwork from
the July 31, 2026 Architecture Icons release. Icons also appear in the searchable
switcher, current-service breadcrumb, inspector headings, and connection rows.
Sizes are 28px in the directory, 24px in results/connections, 20px in the breadcrumb,
and 32px at the service heading. Image elements preserve proportions and use empty
alt text because visible labels already identify each service.

The two API Gateway entries, two Cognito entries, and RDS/RDS Data share family
artwork. S3 Files, CloudWatch Logs, STS, and Application Load Balancer have dedicated
resource icons. `aws-icon-sources.json` records their official archive paths.
Functional glyphs such as search, copy, and arrows remain neutral inline SVGs;
AWS artwork is never substituted for MicroStack branding.

Service buttons use selected background and text; resource buttons also expose
pressed state. Tabs use roving tabindex with arrow, Home, and End navigation.
Controls have a visible 3px focus outline; the page provides a skip link.

Each service has a distinct content model: S3 keys and virtual prefixes with
bounded-preview examples; SQS non-consuming sample message snapshots; DynamoDB
typed items; SNS subscriptions; EventBridge buses, rules, patterns, and targets.
Configuration and service/account activity share consistent secondary tabs.

Connections describe configured relationships, never delivery traces. Following
one retains an explicit return trail, including selected item, tab, prefix,
filter, page, and example state. Missing and external destinations are not
clickable internal resources. Copy controls announce success or explain clipboard
denial; errors and empty/loading/stale states remain explicit.

## Do's and Don'ts

- Keep synthetic data, account, and configured-region scope visible.
- Keep the default services catalog on its home page, not in a growing rail.
- Preserve service -> resource -> contents as the main navigation path.
- Distinguish configuration from observations; never imply causal delivery.
- Label proposed non-consuming SQS snapshots as requiring a dedicated admin API.
- Keep binary and oversized objects metadata-only when a preview is unavailable.
- Do not equate a loaded-subset filter with a query or complete scan.
- Do not add provisioning controls, live API calls, or inferred relationships.
- Do not treat prototype navigation approval as permission to change production.

Open product and API decisions are recorded in `notes.html` and `PRODUCT.md`.
