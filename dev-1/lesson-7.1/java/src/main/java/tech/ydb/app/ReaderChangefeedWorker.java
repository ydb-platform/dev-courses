package tech.ydb.app;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.ydb.topic.TopicClient;
import tech.ydb.topic.read.SyncReader;
import tech.ydb.topic.settings.ReaderSettings;
import tech.ydb.topic.settings.TopicReadSettings;

/**
 * @author Kirill Kurdyukov
 */
public class ReaderChangefeedWorker {
    private static final Logger LOGGER = LoggerFactory.getLogger(Application.class);

    private final SyncReader reader;
    private final AtomicBoolean stoppedProcess = new AtomicBoolean();

    private volatile CompletableFuture<Void> readerJob;

    public ReaderChangefeedWorker(TopicClient topicClient) {
        // Создаем reader для чтения изменений из топика changefeed
        // С точки зрения читателя это обычный топик.
        this.reader = topicClient.createSyncReader(
                ReaderSettings.newBuilder()
                        .setConsumerName("test")
                        .setTopics(
                                List.of(TopicReadSettings.newBuilder().setPath("issues/updates").build())
                        )
                        .build()
        );

        reader.init();
    }

    public void run() {
        readerJob = CompletableFuture.runAsync(
                () -> {
                    LOGGER.info("Started read worker!");

                    while (!stoppedProcess.get()) {
                        try {
                            var message = reader.receive(1, TimeUnit.SECONDS);

                            if (message == null) {
                                continue;
                            }

                            LOGGER.info("Received message: {}", new String(message.getData()));
                        } catch (Exception e) {
                            // Ignored
                        }
                    }

                    LOGGER.info("Stopped read worker!");
                }
        );
    }

    public void shutdown() {
        stoppedProcess.set(true);
        readerJob.join();
    }
}
