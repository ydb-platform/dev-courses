package tech.ydb.app;

import tech.ydb.core.grpc.GrpcTransport;
import tech.ydb.query.QueryClient;
import tech.ydb.query.tools.SessionRetryContext;

/*
 * Пример работы с транзакциями в YDB, урок - 4.1 Распределенные транзакции
 * @author Kirill Kurdyukov
 */
public class Application {

    // Строка подключения к локальной базе данных YDB
    private static final String CONNECTION_STRING = "grpc://localhost:2136/local";

    public static void main(String[] args) {
        try (GrpcTransport grpcTransport = GrpcTransport.forConnectionString(CONNECTION_STRING).build()) {
            try (QueryClient queryClient = QueryClient.newClient(grpcTransport).build()) {
                var retryCtx = SessionRetryContext.create(queryClient).build();

                var schemaYdbRepository = new SchemaYdbRepository(retryCtx);
                var issueYdbRepository = new IssueYdbRepository(retryCtx);

                schemaYdbRepository.dropSchema();
                schemaYdbRepository.createSchema();

                // Создаем тикеты с авторами
                issueYdbRepository.addIssue("Ticket 1", "Author 1");
                issueYdbRepository.addIssue("Ticket 2", "Author 2");
                issueYdbRepository.addIssue("Ticket 3", "Author 3");

                var allIssues = issueYdbRepository.findAll();

                System.out.println("Print all tickets: ");
                for (var issue : allIssues) {
                    printIssue(issue);
                }

                var first = allIssues.get(0);
                var second = allIssues.get(1);

                // Демонстрация неинтерактивной транзакции - все запросы выполняются за один запрос к YDB
                System.out.println("Linked tickets by non-interactive transactions id1 = " + first.id() + ", id2 = " + second.id());
                var result1 = issueYdbRepository.linkTicketsNoInteractive(first.id(), second.id());
                System.out.println("Result operation:");
                for (var v : result1) {
                    System.out.println("Id = " + v.id() + ", linkCounts = " + v.linkCount());
                }

                var third = allIssues.get(2);
                // Демонстрация интерактивной транзакции - между запросами к YDB есть логика на стороне приложения
                System.out.println("Linked tickets by interactive transactions id2 = " + second.id() + ", id3 = " + third.id());
                var result2 = issueYdbRepository.linkTicketsInteractive(second.id(), third.id());
                System.out.println("Result operation:");
                for (var v : result2) {
                    System.out.println("Id = " + v.id() + ", linkCounts = " + v.linkCount());
                }

                System.out.println("Print all tickets: ");
                for (var ticket : issueYdbRepository.findAll()) {
                    printIssue(ticket);
                }
            }
        }
    }

    private static void printIssue(Issue issue) {
        System.out.println("Issue: {id: " + issue.id() + ", title: " + issue.title() + ", timestamp: " + issue.now() + ", author: " + issue.author() + ", link_count: " + issue + "}");
    }
}
