namespace MicroStack.Tests;

public sealed class AdminUiHostingTests(MicroStackFixture fixture) : IClassFixture<MicroStackFixture>
{
    [Fact]
    public async Task Gateway_serves_ui_and_deep_links_under_reserved_prefix()
    {
        using var client = fixture.CreateClient(allowAutoRedirect: false);

        using var slash = await client.GetAsync("/ui");
        slash.StatusCode.ShouldBe(HttpStatusCode.PermanentRedirect);
        slash.Headers.Location.ShouldBe(new Uri("/ui/", UriKind.Relative));

        using var slashWithQuery = await client.GetAsync("/ui?account=123456789012");
        slashWithQuery.StatusCode.ShouldBe(HttpStatusCode.PermanentRedirect);
        slashWithQuery.Headers.Location.ShouldBe(new Uri("/ui/?account=123456789012", UriKind.Relative));

        using var index = await client.GetAsync("/ui/");
        index.StatusCode.ShouldBe(HttpStatusCode.OK);
        index.Content.Headers.ContentType?.MediaType.ShouldBe("text/html");
        (await index.Content.ReadAsStringAsync()).ShouldContain("""<base href="/ui/" />""");

        using var deepLink = await client.GetAsync("/ui/services/s3?account=000000000000");
        deepLink.StatusCode.ShouldBe(HttpStatusCode.OK);
        deepLink.Content.Headers.ContentType?.MediaType.ShouldBe("text/html");

        using var css = await client.GetAsync("/ui/css/app.css");
        css.StatusCode.ShouldBe(HttpStatusCode.OK);
        css.Content.Headers.ContentType?.MediaType.ShouldBe("text/css");

        using var missingAsset = await client.GetAsync("/ui/missing.js");
        missingAsset.StatusCode.ShouldBe(HttpStatusCode.NotFound);

        using var wrongMethod = await client.PostAsync("/ui/services/s3", null);
        wrongMethod.StatusCode.ShouldBe(HttpStatusCode.MethodNotAllowed);
        wrongMethod.Content.Headers.Allow.ShouldBe(["GET", "HEAD"], ignoreOrder: true);

        using var nearPrefix = await client.GetAsync("/uix");
        nearPrefix.Content.Headers.ContentType?.MediaType.ShouldNotBe("text/html");
    }

    [Fact]
    public async Task Root_redirects_only_conservative_browser_navigation()
    {
        using var client = fixture.CreateClient(allowAutoRedirect: false);
        using var browser = new HttpRequestMessage(HttpMethod.Get, "/");
        browser.Headers.Accept.ParseAdd("text/html,application/xhtml+xml");
        browser.Headers.Add("Sec-Fetch-Mode", "navigate");
        browser.Headers.Add("Sec-Fetch-Dest", "document");

        using var redirected = await client.SendAsync(browser);
        redirected.StatusCode.ShouldBe(HttpStatusCode.Found);
        redirected.Headers.Location.ShouldBe(new Uri("/ui/", UriKind.Relative));
        redirected.Headers.CacheControl?.NoStore.ShouldBeTrue();

        foreach (var request in RootRequestsThatMustRemainAws())
        {
            using (request)
            {
                using var response = await client.SendAsync(request);
                ((int)response.StatusCode).ShouldNotBeInRange(300, 399);
                response.Content.Headers.ContentType?.MediaType.ShouldNotBe("text/html");
            }
        }
    }

    [Fact]
    public async Task Service_hosts_do_not_expose_or_redirect_ui_routes()
    {
        using var client = fixture.CreateClient(allowAutoRedirect: false);
        using var root = new HttpRequestMessage(HttpMethod.Get, "/");
        root.Headers.Host = "bucket.s3.localhost";
        root.Headers.Accept.ParseAdd("text/html");
        using var rootResponse = await client.SendAsync(root);
        rootResponse.StatusCode.ShouldNotBe(HttpStatusCode.Redirect);

        using var ui = new HttpRequestMessage(HttpMethod.Get, "/ui/");
        ui.Headers.Host = "bucket.s3.localhost";
        using var uiResponse = await client.SendAsync(ui);
        uiResponse.Content.Headers.ContentType?.MediaType.ShouldNotBe("text/html");

        using var asset = new HttpRequestMessage(HttpMethod.Get, "/ui/css/app.css");
        asset.Headers.Host = "bucket.s3.localhost";
        using var assetResponse = await client.SendAsync(asset);
        assetResponse.Content.Headers.ContentType?.MediaType.ShouldNotBe("text/css");
    }

    private static IEnumerable<HttpRequestMessage> RootRequestsThatMustRemainAws()
    {
        yield return new(HttpMethod.Get, "/");
        yield return new(HttpMethod.Get, "/") { Headers = { Accept = { new("text/html", 0) } } };
        yield return new(HttpMethod.Get, "/") { Headers = { Accept = { new("*/*") } } };
        yield return new(HttpMethod.Get, "/?X-Amz-Signature=signature") { Headers = { Accept = { new("text/html") } } };
        yield return new(HttpMethod.Get, "/") { Headers = { Accept = { new("text/html") }, Authorization = new("AWS4-HMAC-SHA256", "Credential=test") } };
        var target = new HttpRequestMessage(HttpMethod.Get, "/");
        target.Headers.Accept.ParseAdd("text/html");
        target.Headers.Add("X-Amz-Target", "AmazonSQS.ListQueues");
        yield return target;
        var image = new HttpRequestMessage(HttpMethod.Get, "/");
        image.Headers.Accept.ParseAdd("text/html");
        image.Headers.Add("Sec-Fetch-Dest", "image");
        yield return image;
        var body = new HttpRequestMessage(HttpMethod.Get, "/") { Content = new StringContent("request body") };
        body.Headers.Accept.ParseAdd("text/html");
        yield return body;
        var chunked = new HttpRequestMessage(HttpMethod.Get, "/")
        {
            Content = new ByteArrayContent([])
        };
        chunked.Headers.Accept.ParseAdd("text/html");
        chunked.Headers.TransferEncodingChunked = true;
        yield return chunked;
        yield return new(HttpMethod.Post, "/") { Headers = { Accept = { new("text/html") } } };
    }
}
