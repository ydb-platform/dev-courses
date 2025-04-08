package tech.ydb.app;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import tech.ydb.topic.TopicClient;
import tech.ydb.topic.read.SyncReader;
import tech.ydb.topic.settings.ReaderSettings;
import tech.ydb.topic.settings.TopicReadSettings;

/*
 * @author Kirill Kurdyukov
 */
public class ReaderChangefeedWorker {
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
                    System.out.println("Started read worker!");

                    while (!stoppedProcess.get()) {
                        try {
                            var message = reader.receive(1, TimeUnit.SECONDS);

                            if (message == null) {
                                continue;
                            }

                            System.out.println("Received message: " + new String(message.getData()));
                        } catch (Exception e) {
                            // Ignored
                        }
                    }

                    System.out.println("Stopped read worker!");
                }
        );
    }

    public void shutdown() {
        stoppedProcess.set(true);
        readerJob.join();
    }
}
