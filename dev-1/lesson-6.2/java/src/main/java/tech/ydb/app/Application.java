package tech.ydb.app;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import tech.ydb.common.transaction.TxMode;
import tech.ydb.core.Status;
import tech.ydb.core.grpc.GrpcTransport;
import tech.ydb.query.QueryClient;
import tech.ydb.query.tools.QueryReader;
import tech.ydb.query.tools.SessionRetryContext;
import tech.ydb.table.query.Params;
import tech.ydb.table.result.ResultSetReader;
import tech.ydb.table.values.PrimitiveValue;
import tech.ydb.topic.TopicClient;
import tech.ydb.topic.read.SyncReader;
import tech.ydb.topic.settings.ReaderSettings;
import tech.ydb.topic.settings.TopicReadSettings;
import tech.ydb.topic.settings.WriterSettings;
import tech.ydb.topic.write.Message;

/**
 * @author Kirill Kurdyukov
 */
public class Application {

    private static final String PATH = "/dev-1/lesson-6.2/java/file.txt";
    private static final String CONNECTION_STRING = "grpc://localhost:2136/local";

    public static void main(String[] args) throws IOException, InterruptedException {
        try (GrpcTransport grpcTransport = GrpcTransport.forConnectionString(CONNECTION_STRING).build();
             QueryClient queryClient = QueryClient.newClient(grpcTransport).build();
             TopicClient topicClient = TopicClient.newClient(grpcTransport).build()) {

            var retryCtx = SessionRetryContext.create(queryClient).build();

            createSchema(retryCtx);

            var writer = topicClient.createSyncWriter(
                    WriterSettings.newBuilder()
                            .setProducerId("producer-file")
                            .setTopicPath("file_topic")
                            .build()
            );

            var reader = topicClient.createSyncReader(
                    ReaderSettings.newBuilder()
                            .setConsumerName("file_consumer")
                            .setTopics(List.of(TopicReadSettings.newBuilder().setPath("file_topic").build()))
                            .build()
            );

            writer.init();
            reader.init();

            var stopped_read_process = new AtomicBoolean();

            var readerJob = CompletableFuture.runAsync(() -> runReadJob(stopped_read_process, reader, retryCtx));

            String currentDirectory = System.getProperty("user.dir");
            var pathFile = Path.of(currentDirectory, PATH);

            try (var lines = Files.lines(pathFile)) {
                var queryReader = retryCtx.supplyResult(
                        session -> QueryReader.readFrom(session.createQuery("""
                                        DECLARE $name AS Text;
                                        SELECT line_num FROM write_file_progress
                                        WHERE name = $name;
                                        """,
                                TxMode.SERIALIZABLE_RW,
                                Params.of("$name", PrimitiveValue.newText(pathFile.toString())))
                        )).join().getValue();

                var resultSet = queryReader.getResultSet(0);

                long lineNumberLong = 1;
                if (resultSet.next()) {
                    lineNumberLong = resultSet.getColumn(0).getInt64();
                }

                var lineNumber = new AtomicLong(lineNumberLong);
                var origLineNumber = new AtomicLong(1);

                lines.forEach(line ->
                        {
                            if (origLineNumber.getAndIncrement() < lineNumber.get()) {
                                return;
                            }

                            writer.send(Message.newBuilder()
                                    .setSeqNo(lineNumber.getAndIncrement())
                                    .setData((PATH + ":" + line).getBytes(StandardCharsets.UTF_8))
                                    .build()
                            );
                            writer.flush();

                            retryCtx.supplyResult(
                                    session -> session.createQuery("""
                                                    DECLARE $name AS Text;
                                                    DECLARE $line_num AS Int64;
                                                                                            
                                                    UPSERT INTO write_file_progress(name, line_num) VALUES ($name, $line_num);
                                                    """,
                                            TxMode.SERIALIZABLE_RW,
                                            Params.of("$name", PrimitiveValue.newText(pathFile.toString()),
                                                    "$line_num", PrimitiveValue.newInt64(lineNumber.get()))
                                    ).execute()
                            ).join().getStatus().expectSuccess();
                        }
                );
            }

            Thread.sleep(5_000);
            stopped_read_process.set(true);
            printTableFile(retryCtx);

            readerJob.join();

            dropSchema(retryCtx);
        }
    }

    private static void runReadJob(AtomicBoolean stopped_read_process, SyncReader reader, SessionRetryContext retryCtx) {
        System.out.println("Started read worker!");

        while (!stopped_read_process.get()) {
            try {
                var message = reader.receive(1, TimeUnit.SECONDS);

                if (message == null) {
                    continue;
                }

                retryCtx.supplyStatus(session -> {
                    var tx = session.createNewTransaction(TxMode.SERIALIZABLE_RW);
                    var partitionId = message.getPartitionSession().getPartitionId();

                    var queryReader = QueryReader.readFrom(
                            tx.createQuery("""
                                                DECLARE $partition_id AS Int64;
                                                SELECT last_offset FROM file_progress
                                                WHERE partition_id = $partition_id;
                                            """,
                                    Params.of("$partition_id", PrimitiveValue.newInt64(partitionId)))
                    ).join().getValue();

                    var resultSet = queryReader.getResultSet(0);
                    var lastOffset = resultSet.next() ? resultSet.getColumn(0).getInt64() : 0;

                    if (lastOffset > message.getOffset()) {
                        message.commit().join();
                        tx.rollback().join();

                        return CompletableFuture.completedFuture(Status.SUCCESS);
                    }

                    var messageData = new String(message.getData(), StandardCharsets.UTF_8).split(":");
                    var name = messageData[0];
                    var length = messageData[1].length();
                    var lineNumber = message.getSeqNo();

                    tx.createQuery(
                            """
                                        DECLARE $name AS Text;
                                        DECLARE $line AS Int64;
                                        DECLARE $length AS Int64;
                                        UPSERT INTO file(name, line, length) VALUES ($name, $line, $length);
                                    """,
                            Params.of(
                                    "$name", PrimitiveValue.newText(name),
                                    "$line", PrimitiveValue.newInt64(lineNumber),
                                    "$length", PrimitiveValue.newInt64(length)
                            )
                    ).execute().join().getStatus().expectSuccess();

                    tx.createQueryWithCommit(
                            """
                                        DECLARE $partition_id AS Int64;
                                        DECLARE $last_offset AS Int64;
                                        UPSERT INTO file_progress(partition_id, last_offset) VALUES ($partition_id, $last_offset);
                                    """,
                            Params.of(
                                    "$partition_id", PrimitiveValue.newInt64(partitionId),
                                    "$last_offset", PrimitiveValue.newInt64(lastOffset)
                            )
                    ).execute().join().getStatus().expectSuccess();

                    message.commit().join();

                    return CompletableFuture.completedFuture(Status.SUCCESS);
                }).join().expectSuccess();

            } catch (Exception e) {
                System.out.println(e.getMessage());
            }
        }

        System.out.println("Stopped read worker!");
    }

    private static void printTableFile(SessionRetryContext retryCtx) {
        var queryReader = retryCtx.supplyResult(session ->
                QueryReader.readFrom(session.createQuery("SELECT name, line, length FROM file;", TxMode.SERIALIZABLE_RW))
        ).join().getValue();

        for (ResultSetReader resultSet : queryReader) {
            while (resultSet.next()) {
                System.out.println(
                        "name: " + resultSet.getColumn(0).getText() +
                                ", line: " + resultSet.getColumn(1).getInt64() +
                                ", length: " + resultSet.getColumn(2).getInt64()
                );
            }
        }
    }

    private static void createSchema(SessionRetryContext retryCtx) {
        executeSchema(retryCtx, """
                CREATE TABLE file (
                    name Text NOT NULL,
                    line Int64 NOT NULL,
                    length Int64 NOT NULL,
                    PRIMARY KEY (name, line)
                );

                CREATE TABLE file_progress (
                    partition_id Int64 NOT NULL,
                    last_offset Int64 NOT NULL,
                    PRIMARY KEY (partition_id)
                );
                                
                CREATE TABLE write_file_progress (
                    name Text NOT NULL,
                    line_num Int64 NOT NULL,
                    PRIMARY KEY (name)
                );
                                                    
                CREATE TOPIC file_topic (
                    CONSUMER file_consumer
                ) WITH(
                    auto_partitioning_strategy='scale_up',
                    min_active_partitions=2,
                    max_active_partitions=5,
                    partition_write_speed_bytes_per_second=5000000
                );
                """);
    }

    private static void dropSchema(SessionRetryContext retryCtx) {
        executeSchema(retryCtx, """
                DROP TABLE file;
                DROP TABLE file_progress;
                DROP TABLE write_file_progress;
                DROP TOPIC file_topic;
                """);
    }

    private static void executeSchema(SessionRetryContext retryCtx, String query) {
        retryCtx.supplyResult(
                session -> session.createQuery(
                        query, TxMode.NONE
                ).execute()
        ).join().getStatus().expectSuccess("Can't create tables");
    }
}
