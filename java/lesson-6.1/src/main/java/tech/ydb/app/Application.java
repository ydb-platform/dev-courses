package tech.ydb.app;

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
