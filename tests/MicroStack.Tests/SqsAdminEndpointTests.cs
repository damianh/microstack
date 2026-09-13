using System.Text;
using MicroStack.Internal;
using MicroStack.Services.Sqs;

namespace MicroStack.Tests;

public sealed class SqsAdminEndpointTests
{
    [Theory]
    [InlineData("000000000000")]
    [InlineData("123456789012")]
    public async Task InspectionDerivesConfiguredUrlFromAccountAndQueueName(string account)
    {
        AccountContext.SetFromAccessKey(account);
        try
        {
            var handler = new SqsServiceHandler();
            var response = await handler.HandleAsync(new("POST", "/", new Dictionary<string, string>
            {
                ["x-amz-target"] = "AmazonSQS.CreateQueue",
                ["content-type"] = "application/x-amz-json-1.0"
            }, Encoding.UTF8.GetBytes("""{"QueueName":"inspect-endpoint"}"""), new Dictionary<string, string[]>())
            {
                Origin = "http://127.0.0.1:54321"
            });
            response.StatusCode.ShouldBe(200);
            var options = MicroStackOptions.Instance;
            var expected = $"http://{options.Host}:{options.GatewayPort}/{account}/inspect-endpoint";
            handler.GetResources().Items.Single().Attributes!["QueueUrl"].ShouldBe(expected);
            handler.GetAdminResources("sqs").Single().ReadFields!()
                .Single(field => field.Name == "Queue URL").Value.ShouldBe(expected);
        }
        finally
        {
            AccountContext.Reset();
        }
    }
}
