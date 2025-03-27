package tech.ydb.app;

import java.util.UUID;
import tech.ydb.core.grpc.GrpcTransport;
import tech.ydb.query.QueryClient;
import tech.ydb.query.tools.SessionRetryContext;
import tech.ydb.topic.TopicClient;

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

            var first = issueYdbRepository.addIssue("Ticket 1", "Author 1");
            var second = issueYdbRepository.addIssue("Ticket 2", "Author 2");
            issueYdbRepository.updateStatus(first.id(), "future");
            issueYdbRepository.delete(second.id());
            issueYdbRepository.delete(UUID.randomUUID());

            var readerWorker = new ReaderChangefeedWorker(topicClient);
            readerWorker.run();
            Thread.sleep(10_000);

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
