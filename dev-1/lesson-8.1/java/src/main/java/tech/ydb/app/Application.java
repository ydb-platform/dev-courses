package tech.ydb.app;

import java.util.List;
import tech.ydb.core.grpc.GrpcTransport;
import tech.ydb.query.QueryClient;
import tech.ydb.query.tools.SessionRetryContext;

/*
 * @author Kirill Kurdyukov
 */
public class Application {

    private static final String CONNECTION_STRING = "grpc://localhost:2136/local";

    public static void main(String[] args) {
        try (GrpcTransport grpcTransport = GrpcTransport.forConnectionString(CONNECTION_STRING).build();
             QueryClient queryClient = QueryClient.newClient(grpcTransport).build()) {
            var retryCtx = SessionRetryContext.create(queryClient).build();

            var schemaYdbRepository = new SchemaYdbRepository(retryCtx);
            var issueYdbRepository = new IssueYdbRepository(retryCtx);

            schemaYdbRepository.dropSchema();
            schemaYdbRepository.createSchema();
            issueYdbRepository.saveAll(List.of(
                    new TitleAuthor("Ticket 1", "Author 1"),
                    new TitleAuthor("Ticket 2", "Author 2"),
                    new TitleAuthor("Ticket 3", "Author 3"))
            );

            var allTickets = issueYdbRepository.findAll();

            for (var issue : allTickets) {
                issueYdbRepository.updateStatus(issue.id(), "future");
            }

            System.out.println("Find by ids [" + allTickets.get(0).id() + ", " + allTickets.get(1).id() + "]");
            for (var issue : issueYdbRepository.findByIds(List.of(allTickets.get(0).id(), allTickets.get(1).id()))) {
                printIssue(issue);
            }

            System.out.println("Future issues: ");
            for (var issue : issueYdbRepository.findFutures()) {
                System.out.println("Id: " + issue.id() + ", Title: " + issue.title());
            }

            System.out.println("Deleted by ids [" + allTickets.get(0).id() + ", " + allTickets.get(1).id() + "]");
            issueYdbRepository.deleteTasks(List.of(allTickets.get(0).id(), allTickets.get(1).id()));

            System.out.println("Print all issues: ");
            for (var ticket : issueYdbRepository.findAll()) {
                printIssue(ticket);
            }
        }
    }

    private static void printIssue(Issue issue) {
        System.out.println("Ticket: {id: " + issue.id() + ", title: " + issue.title() + ", timestamp: "
                + issue.now() + ", author: " + issue.author() + ", link_count: "
                + issue.linkCounts() + ", status: " + issue.status() + "}");
    }
}
