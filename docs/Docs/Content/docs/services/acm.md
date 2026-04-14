---
title: ACM
description: ACM emulation — certificate request, import, describe, DNS validation records, tags, and lifecycle management.
order: 22
section: Services
---

# ACM

MicroStack emulates AWS Certificate Manager (ACM), allowing you to request, import, describe, and delete TLS certificates in your tests. Requested certificates are auto-issued with status `ISSUED` immediately — no pending validation period. DNS validation records are pre-populated in `DomainValidationOptions` so your code can read them without waiting.

## Supported Operations

RequestCertificate, DescribeCertificate, ListCertificates, DeleteCertificate, GetCertificate, ImportCertificate, AddTagsToCertificate, RemoveTagsFromCertificate, ListTagsForCertificate, UpdateCertificateOptions, RenewCertificate, ResendValidationEmail

## Usage

```csharp
var client = new AmazonCertificateManagerClient(
    new BasicAWSCredentials("test", "test"),
    new AmazonCertificateManagerConfig
    {
        RegionEndpoint = RegionEndpoint.USEast1,
        ServiceURL = "http://localhost:4566",
    });

// Request a certificate with DNS validation
var reqResp = await client.RequestCertificateAsync(new RequestCertificateRequest
{
    DomainName = "example.com",
    ValidationMethod = ValidationMethod.DNS,
    SubjectAlternativeNames = ["www.example.com"],
});

string certArn = reqResp.CertificateArn;
// arn:aws:acm:us-east-1:000000000000:certificate/<id>

// Describe the certificate — status is ISSUED immediately
var descResp = await client.DescribeCertificateAsync(new DescribeCertificateRequest
{
    CertificateArn = certArn,
});

Console.WriteLine(descResp.Certificate.Status);    // ISSUED
Console.WriteLine(descResp.Certificate.DomainName); // example.com

// DNS validation record is pre-populated
var record = descResp.Certificate.DomainValidationOptions[0].ResourceRecord;
Console.WriteLine(record.Name);  // _acme-challenge.example.com (CNAME)

// Retrieve the PEM certificate body
var pemResp = await client.GetCertificateAsync(new GetCertificateRequest
{
    CertificateArn = certArn,
});
Console.WriteLine(pemResp.Certificate.Contains("BEGIN CERTIFICATE")); // True

// List all certificates
var listResp = await client.ListCertificatesAsync(new ListCertificatesRequest());
Console.WriteLine(listResp.CertificateSummaryList.Count); // 1
```

## Importing Certificates

Use `ImportCertificate` to bring in externally issued certificates. Imported certificates report `Type = IMPORTED`.

```csharp
byte[] certPem = Encoding.UTF8.GetBytes(
    "-----BEGIN CERTIFICATE-----\n...\n-----END CERTIFICATE-----");
byte[] keyPem = Encoding.UTF8.GetBytes(
    "-----BEGIN RSA PRIVATE KEY-----\n...\n-----END RSA PRIVATE KEY-----");

var importResp = await client.ImportCertificateAsync(new ImportCertificateRequest
{
    Certificate = new MemoryStream(certPem),
    PrivateKey  = new MemoryStream(keyPem),
});

var descResp = await client.DescribeCertificateAsync(new DescribeCertificateRequest
{
    CertificateArn = importResp.CertificateArn,
});
Console.WriteLine(descResp.Certificate.Type); // IMPORTED
```

:::aside{type="note" title="Auto-issued certificates"}
Certificates requested via `RequestCertificate` are immediately in the `ISSUED` state — there is no `PENDING_VALIDATION` phase. DNS validation CNAME records are available in `DomainValidationOptions` right away, so tests that inspect or act on those records work without any polling delay.
:::
