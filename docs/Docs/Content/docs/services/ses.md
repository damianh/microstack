---
title: SES
description: SES emulation — email identities, templates, configuration sets, and send operations via both v1 (Query/XML) and v2 (REST/JSON) APIs. Emails are stored in-memory.
order: 36
section: Services
---

# SES

MicroStack emulates Amazon Simple Email Service (SES) for both the classic v1 API (`AmazonSimpleEmailServiceClient`) and the modern v2 API (`AmazonSimpleEmailServiceV2Client`). Email identities, templates, and configuration sets are persisted in-memory. Emails are accepted and stored but never actually delivered.

## Supported Operations

**SES v1 (Query/XML)**

SendEmail, SendRawEmail, SendTemplatedEmail, SendBulkTemplatedEmail, VerifyEmailIdentity, VerifyEmailAddress, VerifyDomainIdentity, VerifyDomainDkim, ListIdentities, GetIdentityVerificationAttributes, DeleteIdentity, GetSendQuota, GetSendStatistics, ListVerifiedEmailAddresses, CreateConfigurationSet, DeleteConfigurationSet, DescribeConfigurationSet, ListConfigurationSets, CreateTemplate, GetTemplate, DeleteTemplate, ListTemplates, UpdateTemplate, GetIdentityDkimAttributes, SetIdentityNotificationTopic, SetIdentityFeedbackForwardingEnabled

**SES v2 (REST/JSON)**

SendEmail, CreateEmailIdentity, GetEmailIdentity, ListEmailIdentities, DeleteEmailIdentity, CreateConfigurationSet, GetConfigurationSet, ListConfigurationSets, DeleteConfigurationSet, GetAccount

## Usage

```csharp
using Amazon.Runtime;
using Amazon.SimpleEmail;
using Amazon.SimpleEmail.Model;

var config = new AmazonSimpleEmailServiceConfig
{
    ServiceURL = "http://localhost:4566",
};

using var ses = new AmazonSimpleEmailServiceClient(
    new BasicAWSCredentials("test", "test"), config);

// Verify a sender identity
await ses.VerifyEmailIdentityAsync(new VerifyEmailIdentityRequest
{
    EmailAddress = "sender@example.com",
});

// Send an email
var sendResp = await ses.SendEmailAsync(new SendEmailRequest
{
    Source = "sender@example.com",
    Destination = new Destination
    {
        ToAddresses = ["recipient@example.com"],
    },
    Message = new Message
    {
        Subject = new Content { Data = "Hello from MicroStack" },
        Body = new Body
        {
            Text = new Content { Data = "This email is stored in-memory." },
        },
    },
});

Console.WriteLine(sendResp.MessageId); // e.g. abc123@email.amazonses.com
```

## Templates

SES v1 supports email templates with Handlebars-style `{{variable}}` placeholders.

```csharp
// Create a template
await ses.CreateTemplateAsync(new CreateTemplateRequest
{
    Template = new Template
    {
        TemplateName = "welcome-tpl",
        SubjectPart = "Welcome, {{name}}!",
        TextPart = "Hi {{name}}, thanks for signing up.",
        HtmlPart = "<h1>Hi {{name}}</h1><p>Thanks for signing up.</p>",
    },
});

// Send a templated email
var resp = await ses.SendTemplatedEmailAsync(new SendTemplatedEmailRequest
{
    Source = "sender@example.com",
    Destination = new Destination
    {
        ToAddresses = ["user@example.com"],
    },
    Template = "welcome-tpl",
    TemplateData = System.Text.Json.JsonSerializer.Serialize(new { name = "Alice" }),
});

Console.WriteLine(resp.MessageId);
```

## SES v2

The v2 client uses the modern REST/JSON API with a simplified email identity model.

```csharp
using Amazon.SimpleEmailV2;
using Amazon.SimpleEmailV2.Model;

var v2Config = new AmazonSimpleEmailServiceV2Config
{
    ServiceURL = "http://localhost:4566",
};

using var sesV2 = new AmazonSimpleEmailServiceV2Client(
    new BasicAWSCredentials("test", "test"), v2Config);

// Send via v2
var resp = await sesV2.SendEmailAsync(new SendEmailRequest
{
    FromEmailAddress = "sender@example.com",
    Destination = new Destination
    {
        ToAddresses = ["recipient@example.com"],
    },
    Content = new EmailContent
    {
        Simple = new Message
        {
            Subject = new Content { Data = "Hello from v2" },
            Body = new Body
            {
                Text = new Content { Data = "Sent via SES v2." },
            },
        },
    },
});

Console.WriteLine(resp.MessageId); // e.g. "ministack-abc123" (MicroStack internal prefix)

// Check account sending limits
var account = await sesV2.GetAccountAsync(new GetAccountRequest());
Console.WriteLine(account.SendQuota.Max24HourSend); // 50000
```

:::aside{type="note" title="Emails are not delivered"}
MicroStack stores sent emails in-memory for the lifetime of the request context. No email is ever dispatched to an SMTP server or external relay. All identities are automatically verified — `GetIdentityVerificationAttributes` returns `Success` for all registered identities.
:::
