using System.Security.Cryptography;
using System.Text;

namespace MicroStack.Internal;

/// <summary>
/// Hash and UUID utilities for AWS responses.
/// Port of ministack/core/responses.py hash utilities.
/// </summary>
internal static class HashHelpers
{
    /// <summary>Compute MD5 hash of bytes and return lowercase hex string.</summary>
    internal static string Md5Hash(byte[] data) =>
        Convert.ToHexStringLower(MD5.HashData(data));

    /// <summary>Compute MD5 hash of bytes and return base64 string (used in ETag headers).</summary>
    internal static string Md5HashBase64(byte[] data) =>
        Convert.ToBase64String(MD5.HashData(data));

    /// <summary>Compute SHA-256 hash of bytes and return lowercase hex string.</summary>
    internal static string Sha256Hash(byte[] data) =>
        Convert.ToHexStringLower(SHA256.HashData(data));

    /// <summary>Compute SHA-256 hash of a UTF-8 string and return lowercase hex string.</summary>
    internal static string Sha256Hash(string text) =>
        Sha256Hash(Encoding.UTF8.GetBytes(text));

    /// <summary>Generate a new UUID string (lowercase with hyphens).</summary>
    internal static string NewUuid() => Guid.NewGuid().ToString();

    /// <summary>Generate a new UUID without hyphens (used for e.g. multipart upload IDs).</summary>
    internal static string NewUuidNoDashes() => Guid.NewGuid().ToString("N");
}
