// Copyright (c) MicroStack contributors. All rights reserved.
// Licensed under the MIT License.

namespace Aspire.Hosting.ApplicationModel;

/// <summary>
///     A resource that represents a MicroStack container for local AWS service emulation.
/// </summary>
/// <param name="name">The resource name.</param>
public sealed class MicroStackResource(string name) : ContainerResource(name), IResourceWithConnectionString
{
    internal const string HttpEndpointName = "http";

    private EndpointReference? _httpEndpoint;

    /// <summary>
    ///     Gets the HTTP endpoint for the MicroStack service.
    /// </summary>
    public EndpointReference HttpEndpoint =>
        _httpEndpoint ??= new EndpointReference(this, HttpEndpointName);

    /// <summary>
    ///     Gets the connection string expression for the MicroStack service.
    /// </summary>
    public ReferenceExpression ConnectionStringExpression =>
        ReferenceExpression.Create(
            $"http://{HttpEndpoint.Property(EndpointProperty.Host)}:{HttpEndpoint.Property(EndpointProperty.Port)}");
}
