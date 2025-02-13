package tech.ydb.app;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import tech.ydb.common.transaction.TxMode;
import tech.ydb.core.grpc.GrpcTransport;
import tech.ydb.query.QueryClient;
import tech.ydb.query.tools.QueryReader;
import tech.ydb.query.tools.SessionRetryContext;
import tech.ydb.table.query.Params;
import tech.ydb.table.values.PrimitiveValue;

/**
 * @author Kirill Kurdyukov
 */
public class Application {

    private static final String CONNECTION_STRING = "grpc://localhost:2136/local";

    public static void main(String[] args) {
        try (GrpcTransport grpcTransport = GrpcTransport.forConnectionString(CONNECTION_STRING).build()) {
            try (QueryClient queryClient = QueryClient.newClient(grpcTransport).build()) {
                var ticketsYdbRepository = new TicketsYdbRepository(SessionRetryContext.create(queryClient).build());

                ticketsYdbRepository.createSchema();

                ticketsYdbRepository.addedTittle("Ticket 1", "Author 1");
                ticketsYdbRepository.addedTittle("Ticket 2", "Author 2");
                ticketsYdbRepository.addedTittle("Ticket 3", "Author 3");

                for (var ticket : ticketsYdbRepository.findAll()) {
                    printTitle(ticket);
                }

                System.out.println("Find by index `authorIndex`: ");
                printTitle(
                        ticketsYdbRepository.findByAuthor("Author 2")
                );

                ticketsYdbRepository.dropSchema();
            }
        }
    }

    private static void printTitle(Title ticket) {
        System.out.println("Ticket: {id: " + ticket.id() + ", title: " + ticket.title() + ", timestamp: " + ticket.now() + ", author: " + ticket.author + "}");
    }

    public static class TicketsYdbRepository {
        private final SessionRetryContext retryCtx;

        public TicketsYdbRepository(SessionRetryContext retryCtx) {
            this.retryCtx = retryCtx;
        }

        public void createSchema() {
            retryCtx.supplyResult(
                    session -> session.createQuery(
                            "CREATE TABLE issues (" +
                                    "id UUID NOT NULL, " +
                                    "title Text, " +
                                    "created_at Timestamp, " +
                                    "author Text, " +
                                    "PRIMARY KEY (id)" +
                                    ")", TxMode.NONE
                    ).execute()
            ).join().getStatus().expectSuccess("Can't create table issues");

            retryCtx.supplyResult(
                    session -> session.createQuery(
                            """
                                    ALTER TABLE issues ADD INDEX authorIndex GLOBAL ON (author);
                                    """,
                            TxMode.NONE
                    ).execute()
            ).join().getStatus().expectSuccess("Can't create an author column and index");
        }

        public void dropSchema() {
            retryCtx.supplyResult(
                    session -> session.createQuery(
                            "DROP TABLE issues;", TxMode.NONE
                    ).execute()
            ).join().getStatus().expectSuccess("Can't drop table issues");
        }

        public void addedTittle(String title, String author) {
            var id = UUID.randomUUID();
            var now = Instant.now();

            retryCtx.supplyResult(
                    session -> session.createQuery(
                            "" +
                                    "DECLARE $id AS UUID; " +
                                    "DECLARE $title AS Text; " +
                                    "DECLARE $created_at AS Timestamp; " +
                                    "DECLARE $author AS Text; " +
                                    "UPSERT INTO issues (id, title, created_at, author) " +
                                    "VALUES ($id, $title, $created_at, $author)",
                            TxMode.SERIALIZABLE_RW,
                            Params.of(
                                    "$id", PrimitiveValue.newUuid(id),
                                    "$title", PrimitiveValue.newText(title),
                                    "$created_at", PrimitiveValue.newTimestamp(now),
                                    "$author", PrimitiveValue.newText(author)
                            )
                    ).execute()
            ).join().getStatus().expectSuccess("Failed upsert title");
        }

        public List<Title> findAll() {
            var titles = new ArrayList<Title>();
            var resultSet = retryCtx.supplyResult(
                    session -> QueryReader.readFrom(
                            session.createQuery("SELECT id, title, created_at, author FROM issues;", TxMode.SNAPSHOT_RO)
                    )
            ).join().getValue();

            var resultSetReader = resultSet.getResultSet(0);

            while (resultSetReader.next()) {
                titles.add(new Title(
                        resultSetReader.getColumn(0).getUuid(),
                        resultSetReader.getColumn(1).getText(),
                        resultSetReader.getColumn(2).getTimestamp(),
                        resultSetReader.getColumn(3).getText()
                ));
            }

            return titles;
        }

        public Title findByAuthor(String author) {
            var resultSet = retryCtx.supplyResult(
                    session -> QueryReader.readFrom(
                            session.createQuery("" +
                                            "DECLARE $author AS Text; " +
                                            "SELECT id, title, created_at, author FROM issues VIEW authorIndex " +
                                            "WHERE author = $author;",
                                    TxMode.SNAPSHOT_RO,
                                    Params.of("$author", PrimitiveValue.newText(author))
                            )
                    )
            ).join().getValue();

            var resultSetReader = resultSet.getResultSet(0);
            resultSetReader.next();

            return new Title(
                    resultSetReader.getColumn(0).getUuid(),
                    resultSetReader.getColumn(1).getText(),
                    resultSetReader.getColumn(2).getTimestamp(),
                    resultSetReader.getColumn(3).getText()
            );
        }
    }

    public record Title(UUID id, String title, Instant now, String author) {
    }
}
