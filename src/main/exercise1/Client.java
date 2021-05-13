package exercise1;

import org.apache.commons.io.IOUtils;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.core.waiters.WaiterResponse;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.s3.waiters.S3Waiter;


import javax.activation.MimetypesFileTypeMap;
import java.io.*;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;


public class Client {
    private final Region region = Region.US_WEST_2;
    private final String bucketName = "comp3358resizebucket7352";
    private final String inboxQueueName = "comp3358resizequeue7352inbox";
    private final String outboxQueueName = "comp3358resizequeue7352outbox";
    private final File imageFolder = new File("images");
    private S3Client s3;
    private SqsClient sqsClient;


    public static void main(String[] args) {
        try {
            Client app = new Client();
            app.init();
            ArrayList<String> uploadedKeys = app.uploadImages();
            app.sendUploadedKeys(uploadedKeys);
            List<Message> messages = app.pollForMessages();
            app.getAllObjects(messages
                    .stream()
                    .map(Message::body)
                    .collect(Collectors.toList()));
            app.deleteMessages(messages);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private static ByteBuffer getRandomByteBuffer(int size) {
        byte[] b = new byte[size];
        new Random().nextBytes(b);
        return ByteBuffer.wrap(b);
    }

    private void init() {
        createS3Client();
        createBucket();
        createSQSClient();
        createSQSQueue();
    }

    private void createS3Client() {
        s3 = S3Client.builder()
                .region(region)
                .build();
    }

    private void createBucket() {
        try {
            CreateBucketRequest bucketRequest = CreateBucketRequest.builder()
                    .bucket(bucketName)
                    .build();

            s3.createBucket(bucketRequest);
            checkBucketExists();
        } catch (S3Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
        }

    }

    private void checkBucketExists() {
        try {
            S3Waiter s3Waiter = s3.waiter();
            HeadBucketRequest bucketRequestWait = HeadBucketRequest.builder()
                    .bucket(bucketName)
                    .build();

            WaiterResponse<HeadBucketResponse> waiterResponse = s3Waiter.waitUntilBucketExists(bucketRequestWait);
            waiterResponse.matched().response().ifPresent(System.out::println);
        } catch (S3Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
        }

    }

    private void createSQSClient() {
        sqsClient = SqsClient.builder()
                .region(region)
                .build();
    }

    private void createSQSQueue() {
        CreateQueueRequest createQueueRequest = CreateQueueRequest.builder()
                .queueName(inboxQueueName)
                .build();
        sqsClient.createQueue(createQueueRequest);

        createQueueRequest = CreateQueueRequest.builder()
                .queueName(outboxQueueName)
                .build();
        sqsClient.createQueue(createQueueRequest);
    }

    private ArrayList<String> uploadImages() {
        ArrayList<String> uploadedKeys = new ArrayList<>();
        try {
            for (File file : Objects.requireNonNull(imageFolder.listFiles())) {
                if (isImage(file)) {
                    uploadObject(file);
                    uploadedKeys.add(file.getName());
                }
            }
            Arrays.stream(Objects.requireNonNull(imageFolder.listFiles())).forEach(File::delete);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return uploadedKeys;
    }

    private void uploadObject(File file) throws IOException {
        byte[] contents = IOUtils.toByteArray(new FileInputStream(file));
        RequestBody requestBody = RequestBody.fromByteBuffer(ByteBuffer.wrap(contents));

        String mimetype = new MimetypesFileTypeMap().getContentType(file);

        PutObjectRequest objectRequest = PutObjectRequest.builder()
                .bucket(bucketName)
                .key(file.getName())
                .contentType(mimetype)
                .build();
        s3.putObject(objectRequest, requestBody);
    }

    private void sendUploadedKeys(ArrayList<String> uploadedKeys) {
        String queueUrl = getQueueUrl(inboxQueueName);

        if (queueUrl.equals("")) {
            System.out.println("Error while uploading keys: Url does not exist");
            return;
        }

        List<SendMessageBatchRequestEntry> batchEntries = new ArrayList<>();
        for (String filename : uploadedKeys) {
            String key = getFileKey(filename);
            batchEntries.add(SendMessageBatchRequestEntry.builder().id(key).messageBody(filename).build());
        }

        SendMessageBatchRequest sendMessageBatchRequest = SendMessageBatchRequest.builder()
                .queueUrl(queueUrl)
                .entries(batchEntries)
                .build();
        sqsClient.sendMessageBatch(sendMessageBatchRequest);
    }

    private String getQueueUrl(String queueName) {
        try {
            GetQueueUrlResponse getQueueUrlResponse =
                    sqsClient.getQueueUrl(GetQueueUrlRequest.builder().queueName(queueName).build());
            return getQueueUrlResponse.queueUrl();

        } catch (SqsException e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
        return "";

    }

    private String getFileKey(String filename) {
        return filename.split("\\.")[0];
    }

    private boolean isImage(File file) {
        String mimetype = new MimetypesFileTypeMap().getContentType(file);
        String type = mimetype.split("/")[0];
        return type.equals("image");
    }


    private List<Message> getMessages() {
        try {
            String queueUrl = getQueueUrl(outboxQueueName);

            ReceiveMessageRequest receiveMessageRequest = ReceiveMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .build();
            return sqsClient.receiveMessage(receiveMessageRequest).messages();
        } catch (SqsException e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
        return null;
    }

    private List<Message> pollForMessages() throws InterruptedException {
        while (true) {
            TimeUnit.SECONDS.sleep(1);
            List<Message> messages = getMessages();
            if (messages.size() != 0)
                return messages;
        }
    }

    private void getObject(String key) {
        Path path = Paths.get(imageFolder.getPath(), key);
        GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                .bucket(bucketName)
                .key(key)
                .build();
        s3.getObject(getObjectRequest, ResponseTransformer.toFile(path));
    }

    private void getAllObjects(List<String> keys) {
        for (String key : keys) {
            getObject(key);
        }
    }

    private void deleteMessage(Message message) {
        String queueUrl = getQueueUrl(outboxQueueName);
        DeleteMessageRequest deleteMessageRequest = DeleteMessageRequest.builder()
                .queueUrl(queueUrl)
                .receiptHandle(message.receiptHandle())
                .build();
        sqsClient.deleteMessage(deleteMessageRequest);
    }

    private void deleteMessages(List<Message> messages) {
        for (Message message : messages) {
            deleteMessage(message);
        }
    }
}
