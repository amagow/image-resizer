import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import javax.activation.MimetypesFileTypeMap;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class Worker {
    private final Region region = Region.US_WEST_2;
    private final String bucketName = "comp3358resizebucket7352";
    private final String inboxQueueName = "comp3358resizequeue7352inbox";
    private final String outboxQueueName = "comp3358resizequeue7352outbox";
    private final File imageFolder = new File("workerImages");
    private S3Client s3;
    private SqsClient sqsClient;

    public static void main(String[] args) {
        Worker worker = new Worker();
        try {
            worker.init();
            List<Message> messages = worker.pollForMessages();
            worker.getAllObjects(messages
                    .stream()
                    .map(Message::body)
                    .collect(Collectors.toList()));
            worker.processImages();
            ArrayList<String> uploadedKeys = worker.uploadImages();
            worker.sendUploadedKeys(uploadedKeys);
            worker.deleteMessages(messages);
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void init() {
        createS3Client();
        createSQSClient();
    }

    private void createS3Client() {
        s3 = S3Client.builder()
                .region(region)
                .build();
    }

    private void createSQSClient() {
        sqsClient = SqsClient.builder()
                .region(region)
                .build();
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

    private List<Message> getMessages() {
        try {
            String queueUrl = getQueueUrl(inboxQueueName);

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

    private void processImages() throws IOException, InterruptedException {
        for (File file : Objects.requireNonNull(imageFolder.listFiles())) {
            Runtime rt = Runtime.getRuntime();
            String command = String.format("magick convert %1$s  -polaroid 0 -transpose" +
                    " -bordercolor black -border 2  %1$s", file.getPath());
            Process p = rt.exec(command);
            p.waitFor();
        }
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


    private boolean isImage(File file) {
        String mimetype = new MimetypesFileTypeMap().getContentType(file);
        String type = mimetype.split("/")[0];
        return type.equals("image");
    }

    private void uploadObject(File file) throws IOException {
        byte[] contents = IOUtils.toByteArray(new FileInputStream(file));
        RequestBody requestBody = RequestBody.fromByteBuffer(ByteBuffer.wrap(contents));

        String mimetype = new MimetypesFileTypeMap().getContentType(file);

        PutObjectRequest objectRequest = PutObjectRequest.builder()
                .bucket(bucketName)
                .key("processed" + file.getName())
                .contentType(mimetype)
                .build();
        s3.putObject(objectRequest, requestBody);
    }

    private void sendUploadedKeys(List<String> uploadedKeys) {
        String queueUrl = getQueueUrl(outboxQueueName);

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

    private String getFileKey(String filename) {
        return filename.split("\\.")[0];
    }


    private void deleteMessages(List<Message> messages) {
        for (Message message : messages) {
            deleteMessage(message);
        }
    }

    private void deleteMessage(Message message) {
        String queueUrl = getQueueUrl(inboxQueueName);
        DeleteMessageRequest deleteMessageRequest = DeleteMessageRequest.builder()
                .queueUrl(queueUrl)
                .receiptHandle(message.receiptHandle())
                .build();
        sqsClient.deleteMessage(deleteMessageRequest);
    }
}