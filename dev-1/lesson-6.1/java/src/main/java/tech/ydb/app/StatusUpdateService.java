package tech.ydb.app;

import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import tech.ydb.topic.TopicClient;
import tech.ydb.topic.settings.WriterSettings;
import tech.ydb.topic.write.Message;
import tech.ydb.topic.write.SyncWriter;

/**
 * @author Kirill Kurdyukov
 */
public class StatusUpdateService {
    private final SyncWriter writer;
    private final IssueYdbRepository issueYdbRepository;

    public StatusUpdateService(TopicClient topicClient, IssueYdbRepository issueYdbRepository) {
        this.writer = topicClient.createSyncWriter(
                WriterSettings.newBuilder()
                        .setProducerId("producer-task_status")
                        .setTopicPath("task_status")
                        .build()
        );
        this.issueYdbRepository = issueYdbRepository;
        this.writer.init();
    }

    public void update(long uuid, String status) {
        issueYdbRepository.updateStatus(uuid, status);

        writer.send(Message.newBuilder()
                .setData(("[" + uuid + " : " + status + "]").getBytes(StandardCharsets.UTF_8))
                .build()
        );
    }

    public void shutdown() {
        try {
            writer.shutdown(10, TimeUnit.SECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            throw new RuntimeException(e);
        }
    }
}
