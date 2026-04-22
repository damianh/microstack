// Copyright (c) MicroStack contributors. All rights reserved.
// Licensed under the MIT License.

using Aspire.Hosting.ApplicationModel;
using MicroStack.Aspire.Hosting;

namespace Aspire.Hosting;

/// <summary>
///     Extension methods for adding MicroStack resources to an Aspire application.
/// </summary>
public static class MicroStackBuilderExtensions
{
    private const int DefaultContainerPort = 4566;

    /// <summary>
    ///     Adds a MicroStack container resource to the application model for local AWS service emulation.
    /// </summary>
    /// <param name="builder">The distributed application builder.</param>
    /// <param name="name">The resource name.</param>
    /// <returns>A resource builder for further configuration.</returns>
    public static IResourceBuilder<MicroStackResource> AddMicroStack(
        this IDistributedApplicationBuilder builder,
        [ResourceName] string name)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentException.ThrowIfNullOrEmpty(name);

        var resource = new MicroStackResource(name);

        return builder.AddResource(resource)
            .WithImage(MicroStackContainerImageTags.Image, MicroStackContainerImageTags.Tag)
            .WithImageRegistry(MicroStackContainerImageTags.Registry)
            .WithHttpEndpoint(targetPort: DefaultContainerPort, name: MicroStackResource.HttpEndpointName)
            .WithHttpHealthCheck("/_microstack/health");
    }

    /// <summary>
    ///     Adds a MicroStack container resource to the application model for local AWS service emulation.
    /// </summary>
    /// <param name="builder">The distributed application builder.</param>
    /// <param name="name">The resource name.</param>
    /// <param name="port">The host port to bind to.</param>
    /// <returns>A resource builder for further configuration.</returns>
    public static IResourceBuilder<MicroStackResource> AddMicroStack(
        this IDistributedApplicationBuilder builder,
        [ResourceName] string name,
        int port)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentException.ThrowIfNullOrEmpty(name);

        var resource = new MicroStackResource(name);

        return builder.AddResource(resource)
            .WithImage(MicroStackContainerImageTags.Image, MicroStackContainerImageTags.Tag)
            .WithImageRegistry(MicroStackContainerImageTags.Registry)
            .WithHttpEndpoint(port: port, targetPort: DefaultContainerPort, name: MicroStackResource.HttpEndpointName)
            .WithHttpHealthCheck("/_microstack/health");
    }

    /// <summary>
    ///     Adds a named volume for MicroStack persistent state data.
    /// </summary>
    /// <param name="builder">The MicroStack resource builder.</param>
    /// <param name="name">
    ///     The volume name. Defaults to a generated name based on the resource name.
    /// </param>
    /// <returns>The resource builder for chaining.</returns>
    public static IResourceBuilder<MicroStackResource> WithDataVolume(
        this IResourceBuilder<MicroStackResource> builder,
        string? name = null)
    {
        ArgumentNullException.ThrowIfNull(builder);

        return builder
            .WithVolume(name ?? VolumeNameGenerator.Generate(builder, "data"), "/tmp/ministack-state")
            .WithEnvironment("PERSIST_STATE", "1");
    }

    /// <summary>
    ///     Configures which AWS services MicroStack should enable.
    /// </summary>
    /// <param name="builder">The MicroStack resource builder.</param>
    /// <param name="services">Comma-separated list of AWS service names (e.g. "s3,sqs,dynamodb").</param>
    /// <returns>The resource builder for chaining.</returns>
    public static IResourceBuilder<MicroStackResource> WithServices(
        this IResourceBuilder<MicroStackResource> builder,
        string services)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentException.ThrowIfNullOrEmpty(services);

        return builder.WithEnvironment("SERVICES", services);
    }

    /// <summary>
    ///     Configures the AWS region for MicroStack.
    /// </summary>
    /// <param name="builder">The MicroStack resource builder.</param>
    /// <param name="region">The AWS region name (e.g. "us-east-1").</param>
    /// <returns>The resource builder for chaining.</returns>
    public static IResourceBuilder<MicroStackResource> WithRegion(
        this IResourceBuilder<MicroStackResource> builder,
        string region)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentException.ThrowIfNullOrEmpty(region);

        return builder.WithEnvironment("MINISTACK_REGION", region);
    }
}
