package tech.ydb.app;

import tech.ydb.core.grpc.GrpcTransport;
import tech.ydb.query.QueryClient;
import tech.ydb.query.tools.SessionRetryContext;

/**
 * @author Kirill Kurdyukov
 */
public class Application {

    private static final String CONNECTION_STRING = "grpc://localhost:2136/local";

    public static void main(String[] args) {
        try (GrpcTransport grpcTransport = GrpcTransport.forConnectionString(CONNECTION_STRING).build()) {
            try (QueryClient queryClient = QueryClient.newClient(grpcTransport).build()) {
                var retryCtx = SessionRetryContext.create(queryClient).build();

                var schemaYdbRepository = new SchemaYdbRepository(retryCtx);
                var issueYdbRepository = new IssueYdbRepository(retryCtx);

                schemaYdbRepository.createSchema();

                issueYdbRepository.addIssue("Ticket 1", "Author 1");
                issueYdbRepository.addIssue("Ticket 2", "Author 2");
                issueYdbRepository.addIssue("Ticket 3", "Author 3");

                for (var issue : issueYdbRepository.findAll()) {
                    printIssue(issue);
                }

                System.out.println("Find by index `authorIndex`: ");
                printIssue(issueYdbRepository.findByAuthor("Author 2"));

                schemaYdbRepository.dropSchema();
            }
        }
    }

    private static void printIssue(Issue issue) {
        System.out.println("Issue: {id: " + issue.id() + ", title: " + issue.title() + ", timestamp: " + issue.now() + ", author: " + issue.author() + "}");
    }
}
