package tech.ydb.app;

import tech.ydb.core.grpc.GrpcTransport;
import tech.ydb.query.QueryClient;
import tech.ydb.query.tools.SessionRetryContext;

/*
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

                schemaYdbRepository.dropSchema();
                schemaYdbRepository.createSchema();

                issueYdbRepository.addIssue("Ticket 1");
                issueYdbRepository.addIssue("Ticket 2");
                issueYdbRepository.addIssue("Ticket 3");

                for (var ticket : issueYdbRepository.findAll()) {
                    System.out.println("Ticket: {id: " + ticket.id() + ", title: " + ticket.title() + ", timestamp: " + ticket.now() + "}");
                }
            }
        }
    }
}
