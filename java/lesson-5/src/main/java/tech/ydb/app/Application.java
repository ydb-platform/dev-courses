package tech.ydb.app;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import tech.ydb.common.transaction.TxMode;
import tech.ydb.core.Result;
import tech.ydb.core.grpc.GrpcTransport;
import tech.ydb.query.QueryClient;
import tech.ydb.query.tools.QueryReader;
import tech.ydb.query.tools.SessionRetryContext;
import tech.ydb.table.query.Params;
import tech.ydb.table.values.PrimitiveValue;
import tech.ydb.topic.TopicClient;
import tech.ydb.topic.settings.ReaderSettings;
import tech.ydb.topic.settings.TopicReadSettings;
import tech.ydb.topic.settings.WriterSettings;
import tech.ydb.topic.write.Message;

/**
 * @author Kirill Kurdyukov
 */
public class Application {

    private static final String CONNECTION_STRING = "grpc://localhost:2136/local";

    public static void main(String[] args) throws InterruptedException {
        try (GrpcTransport grpcTransport = GrpcTransport.forConnectionString(CONNECTION_STRING).build();
             QueryClient queryClient = QueryClient.newClient(grpcTransport).build();
             TopicClient topicClient = TopicClient.newClient(grpcTransport).build()) {
            var retryCtx = SessionRetryContext.create(queryClient).build();

            var schemaYdbRepository = new SchemaYdbRepository(retryCtx);
            var issueYdbRepository = new IssueYdbRepository(retryCtx);

            schemaYdbRepository.createSchema();

            issueYdbRepository.addIssue("Ticket 1", "Author 1");
            issueYdbRepository.addIssue("Ticket 2", "Author 2");
            issueYdbRepository.addIssue("Ticket 3", "Author 3");

            var allIssues = issueYdbRepository.findAll();

            System.out.println("Print all tickets: ");
            for (var issue : allIssues) {
                printIssue(issue);
            }

            var updateService = new StatusUpdateService(topicClient, issueYdbRepository);

            System.out.println("Update status all tickets: NULL -> OPEN ");
            for (var issue : allIssues) {
                updateService.update(issue.id(), "OPEN");
            }

            var readerWorker = new ReaderWorker(topicClient);
            readerWorker.run();

            System.out.println("Update status all tickets: OPEN -> IN_PROGRESS ");
            for (var issue : allIssues) {
                updateService.update(issue.id(), "IN_PROGRESS");
            }

            Thread.sleep(5_000);

            updateService.shutdown();
            readerWorker.shutdown();

            System.out.println("Print all tickets: ");
            for (var ticket : issueYdbRepository.findAll()) {
                printIssue(ticket);
            }

            schemaYdbRepository.dropSchema();
        }
    }

    private static void printIssue(Issue ticket) {
        System.out.println("Ticket: {id: " + ticket.id() + ", title: " + ticket.title() + ", timestamp: "
                + ticket.now() + ", author: " + ticket.author() + ", link_count: "
                + ticket.linkCounts() + ", status: " + ticket.status() + "}");
    }
}
