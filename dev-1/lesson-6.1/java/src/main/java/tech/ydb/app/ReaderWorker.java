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
 * Воркер для чтения сообщений из топика YDB
 *
 * @author Kirill Kurdyukov
 */
public class ReaderWorker {
    private static final Logger LOGGER = LoggerFactory.getLogger(Application.class);

    private final SyncReader reader;
    private final AtomicBoolean stoppedProcess = new AtomicBoolean();

    private volatile CompletableFuture<Void> readerJob;

    public ReaderWorker(TopicClient topicClient) {
        // Создаем синхронный reader для чтения сообщений из топика task_status
        this.reader = topicClient.createSyncReader(
                ReaderSettings.newBuilder()
                        .setConsumerName("email")
                        .setTopics(List.of(TopicReadSettings.newBuilder().setPath("task_status").build()))
                        .build()
        );

        reader.init();
    }

    public void run() {
        // Запускаем фоновое чтение сообщений из топика
        readerJob = CompletableFuture.runAsync(
                () -> {
                    LOGGER.info("Started read worker!");

                    while (!stoppedProcess.get()) {
                        try {
                            // Читаем сообщение с таймаутом в 1 секунду.
                            // Если за 1 секунду сообщение не будет получено, метод вернет null
                            // и цикл продолжит работу, ожидая следующее сообщение.
                            // Таймаут, чтобы остановить чтение топика по сигналу в stoppedProcess.
                            var message = reader.receive(1, TimeUnit.SECONDS);

                            if (message == null) {
                                continue;
                            }

                            // Выводим полученное сообщение
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
        // Устанавливаем флаг остановки и ждем завершения чтения событий
        stoppedProcess.set(true);
        readerJob.join();
    }
}
