package io.microstack.examples;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URI;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.CreateQueueRequest;
import software.amazon.awssdk.services.sqs.model.DeleteQueueRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest;
import software.amazon.awssdk.services.sqs.model.ListQueuesRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

@Testcontainers
class MicroStackIntegrationTest {
    private static final int MICROSTACK_PORT = 4566;

    @Container
    private static final GenericContainer<?> MICROSTACK =
            new GenericContainer<>(DockerImageName.parse(requiredImage()))
                    .withExposedPorts(MICROSTACK_PORT)
                    .waitingFor(Wait.forHttp("/_microstack/health")
                            .forPort(MICROSTACK_PORT)
                            .forStatusCode(200));

    @Test
    void supportsSqsAndPathStyleS3() {
        URI endpoint = URI.create(
                "http://" + MICROSTACK.getHost() + ":" + MICROSTACK.getMappedPort(MICROSTACK_PORT));
        var credentials = StaticCredentialsProvider.create(AwsBasicCredentials.create("test", "test"));

        try (var sqs = SqsClient.builder()
                        .endpointOverride(endpoint)
                        .region(Region.US_EAST_1)
                        .credentialsProvider(credentials)
                        .httpClientBuilder(UrlConnectionHttpClient.builder())
                        .build();
                var s3 = S3Client.builder()
                        .endpointOverride(endpoint)
                        .region(Region.US_EAST_1)
                        .credentialsProvider(credentials)
                        .httpClientBuilder(UrlConnectionHttpClient.builder())
                        .serviceConfiguration(S3Configuration.builder()
                                .pathStyleAccessEnabled(true)
                                .build())
                        .build()) {
            String queueName = "testcontainers-queue";
            String queueUrl = sqs.createQueue(CreateQueueRequest.builder()
                            .queueName(queueName)
                            .build())
                    .queueUrl();

            assertEquals(endpoint.getRawAuthority(), URI.create(queueUrl).getRawAuthority());
            assertEquals(queueUrl, sqs.getQueueUrl(GetQueueUrlRequest.builder()
                            .queueName(queueName)
                            .build())
                    .queueUrl());
            assertTrue(sqs.listQueues(ListQueuesRequest.builder()
                            .queueNamePrefix("testcontainers-")
                            .build())
                    .queueUrls()
                    .contains(queueUrl));

            String payload = "hello from Testcontainers";
            sqs.sendMessage(SendMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .messageBody(payload)
                    .build());
            var messages = sqs.receiveMessage(ReceiveMessageRequest.builder()
                            .queueUrl(queueUrl)
                            .maxNumberOfMessages(1)
                            .waitTimeSeconds(1)
                            .build())
                    .messages();
            assertEquals(1, messages.size());
            assertEquals(payload, messages.get(0).body());
            sqs.deleteQueue(DeleteQueueRequest.builder().queueUrl(queueUrl).build());

            String bucket = "testcontainers-bucket";
            String key = "payload.txt";
            s3.createBucket(CreateBucketRequest.builder().bucket(bucket).build());
            s3.putObject(
                    PutObjectRequest.builder().bucket(bucket).key(key).build(),
                    RequestBody.fromString(payload, UTF_8));
            assertEquals(payload, s3.getObjectAsBytes(
                            GetObjectRequest.builder().bucket(bucket).key(key).build())
                    .asUtf8String());
        }
    }

    private static String requiredImage() {
        String image = System.getenv("MICROSTACK_TEST_IMAGE");
        if (image == null || image.isBlank()) {
            throw new IllegalStateException("MICROSTACK_TEST_IMAGE must name the image to test");
        }
        return image;
    }
}
