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
 * Сервис для обновления статусов тикетов через топики YDB
 * @author Kirill Kurdyukov
 */
public class StatusUpdateService {
    private final SyncWriter writer;
    private final IssueYdbRepository issueYdbRepository;

    public StatusUpdateService(TopicClient topicClient, IssueYdbRepository issueYdbRepository) {
        // Создаем синхронный writer для отправки сообщений в топик task_status
        this.writer = topicClient.createSyncWriter(
                WriterSettings.newBuilder()
                        .setProducerId("producer-task_status")
                        .setTopicPath("task_status")
                        .build()
        );
        this.issueYdbRepository = issueYdbRepository;
        this.writer.init();
    }

    public void update(long id, String status) {
        // Обновляем статус тикета в БД
        issueYdbRepository.updateStatus(id, status);

        // Отправляем сообщение об обновлении статуса в топик
        writer.send(Message.newBuilder()
                .setData(("[" + id + " : " + status + "]").getBytes(StandardCharsets.UTF_8))
                .build()
        );
    }

    public void shutdown() {
        try {
            // Корректно завершаем работу writer'а
            writer.shutdown(10, TimeUnit.SECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            throw new RuntimeException(e);
        }
    }
}
