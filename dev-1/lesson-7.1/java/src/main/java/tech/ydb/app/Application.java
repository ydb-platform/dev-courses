package tech.ydb.app;

import java.util.UUID;
import tech.ydb.core.grpc.GrpcTransport;
import tech.ydb.query.QueryClient;
import tech.ydb.query.tools.SessionRetryContext;
import tech.ydb.topic.TopicClient;

/*
 * Пример работы с changefeed в YDB
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

            schemaYdbRepository.dropSchema();
            schemaYdbRepository.createSchema();

            // Создаём тикеты, информация об этом попадёт в changefeed-топик
            var first = issueYdbRepository.addIssue("Ticket 1", "Author 1");
            var second = issueYdbRepository.addIssue("Ticket 2", "Author 2");
            
            // Обновляем статус первого тикета - это изменение тоже попадёт в changefeed-топик
            issueYdbRepository.updateStatus(first.id(), "future");
            
            // Удаляем второй тикет - это изменение также будет отслежено через changefeed
            issueYdbRepository.delete(second.id());

            // Тут удаляется несуществующий тикет.
            // Не смотря на то что запись в таблице отсутствует - операция всё равно будет записана 
            // в changefeed-топик ещё раз.
            issueYdbRepository.delete(second.id());

            // Запускаем воркер для чтения изменений из changefeed
            var readerWorker = new ReaderChangefeedWorker(topicClient);
            readerWorker.run();
            
            // Ждем обработки всех изменений
            Thread.sleep(10_000);

            // Завершаем работу воркера
            readerWorker.shutdown();

            System.out.println("Print all tickets: ");
            for (var ticket : issueYdbRepository.findAll()) {
                printIssue(ticket);
            }
        }
    }

    private static void printIssue(Issue ticket) {
        System.out.println("Ticket: {id: " + ticket.id() + ", title: " + ticket.title() + ", timestamp: "
                + ticket.now() + ", author: " + ticket.author() + ", link_count: "
                + ticket.linkCounts() + ", status: " + ticket.status() + "}");
    }
}
