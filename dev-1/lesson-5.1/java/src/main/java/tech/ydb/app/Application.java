package tech.ydb.app;

import tech.ydb.core.grpc.GrpcTransport;
import tech.ydb.query.QueryClient;
import tech.ydb.query.tools.SessionRetryContext;

import java.time.Duration;

/*
 * Пример работы с индексами в YDB: создание и использование вторичных индексов
 * @author Kirill Kurdyukov
 */
public class Application {

    // Строка подключения к локальной базе данных YDB
    private static final String CONNECTION_STRING = "grpc://localhost:2136/local";

    public static void main(String[] args) {
        try (GrpcTransport grpcTransport = GrpcTransport
                .forConnectionString(CONNECTION_STRING)
                .withConnectTimeout(Duration.ofSeconds(10)
                ).build()) {
            try (QueryClient queryClient = QueryClient.newClient(grpcTransport).build()) {
                var retryCtx = SessionRetryContext.create(queryClient).build();

                var schemaYdbRepository = new SchemaYdbRepository(retryCtx);
                var issueYdbRepository = new IssueYdbRepository(retryCtx);

                schemaYdbRepository.dropSchema();
                schemaYdbRepository.createSchema();

                issueYdbRepository.addIssue("Ticket 1", "Author 1");
                issueYdbRepository.addIssue("Ticket 2", "Author 2");
                issueYdbRepository.addIssue("Ticket 3", "Author 3");

                var allIssues = issueYdbRepository.findAll();

                System.out.println("Print all tickets: ");
                for (var issue : allIssues) {
                    printIssue(issue);
                }

                // Демонстрация поиска по вторичному индексу authorIndex
                System.out.println("Find by index `authorIndex`: ");
                printIssue(issueYdbRepository.findByAuthor("Author 2"));
            }
        }
    }

    private static void printIssue(Issue issue) {
        System.out.println("Issue: {id: " + issue.id() + ", title: " + issue.title() + ", timestamp: " + issue.now() + ", author: " + issue.author() + ", link_count: " + issue + "}");
    }
}
