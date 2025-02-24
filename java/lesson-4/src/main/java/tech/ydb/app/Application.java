package tech.ydb.app;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import tech.ydb.common.transaction.TxMode;
import tech.ydb.core.Result;
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

                var allTickets = ticketsYdbRepository.findAll();

                System.out.println("Print all tickets: ");
                for (var ticket : allTickets) {
                    printTitle(ticket);
                }

                System.out.println("Find by index `authorIndex`: ");
                printTitle(
                        ticketsYdbRepository.findByAuthor("Author 2")
                );

                var first = allTickets.get(0);
                var second = allTickets.get(1);

                System.out.println("Linked tickets by non-interactive transactions id1 = " + first.id + ", id2 = " + second.id);
                var result1 = ticketsYdbRepository.linkTicketsNoInteractive(first.id, second.id);
                System.out.println("Result operation:");
                for (var v : result1) {
                    System.out.println("Id = " + v.a + ", linkCounts = " + v.b);
                }

                var third = allTickets.get(2);
                System.out.println("Linked tickets by interactive transactions id2 = " + second.id + ", id3 = " + third.id);
                var result2 = ticketsYdbRepository.linkTicketsInteractive(second.id, third.id);
                System.out.println("Result operation:");
                for (var v : result2) {
                    System.out.println("Id = " + v.a + ", linkCounts = " + v.b);
                }

                System.out.println("Print all tickets: ");
                for (var ticket : ticketsYdbRepository.findAll()) {
                    printTitle(ticket);
                }

                ticketsYdbRepository.dropSchema();
            }
        }
    }

    private static void printTitle(Title ticket) {
        System.out.println("Ticket: {id: " + ticket.id() + ", title: " + ticket.title() + ", timestamp: " + ticket.now() + ", author: " + ticket.author + ", link_count: " + ticket.linkCount + "}");
    }

    public static class TicketsYdbRepository {
        private final SessionRetryContext retryCtx;

        public TicketsYdbRepository(SessionRetryContext retryCtx) {
            this.retryCtx = retryCtx;
        }

        public List<Pair<UUID, Long>> linkTicketsNoInteractive(UUID idT1, UUID idT2) {
            var valueReader = retryCtx.supplyResult(
                    session -> QueryReader.readFrom(session.createQuery(
                            """
                                    DECLARE $t1 AS UUID;
                                    DECLARE $t2 AS UUID;
                                                                        
                                    UPDATE issues
                                    SET link_count = link_count + 1
                                    WHERE id IN ($t1, $t2);
                                                                        
                                    INSERT INTO links (source, destination)
                                    VALUES ($t1, $t2), ($t2, $t1);

                                    SELECT id, link_count FROM issues
                                    WHERE id IN ($t1, $t2)
                                    """,
                            TxMode.SERIALIZABLE_RW,
                            Params.of("$t1", PrimitiveValue.newUuid(idT1), "$t2", PrimitiveValue.newUuid(idT2))
                    ))
            ).join().getValue();

            return getLinkTicketPairs(valueReader);
        }

        public List<Pair<UUID, Long>> linkTicketsInteractive(UUID idT1, UUID idT2) {
            return retryCtx.supplyResult(
                    session -> {
                        var tx = session.createNewTransaction(TxMode.SERIALIZABLE_RW);

                        tx.createQuery("""
                                DECLARE $t1 AS UUID;
                                DECLARE $t2 AS UUID;
                                                             
                                UPDATE issues
                                SET link_count = link_count + 1
                                WHERE id IN ($t1, $t2);
                                """, Params.of("$t1", PrimitiveValue.newUuid(idT1), "$t2", PrimitiveValue.newUuid(idT2))
                        ).execute().join();

                        tx.createQuery("""
                                        DECLARE $t1 AS UUID;
                                        DECLARE $t2 AS UUID;
                                                                            
                                        INSERT INTO links (source, destination)
                                        VALUES ($t1, $t2), ($t2, $t1);
                                        """,
                                Params.of("$t1", PrimitiveValue.newUuid(idT1), "$t2", PrimitiveValue.newUuid(idT2))
                        ).execute().join();

                        var valueReader = QueryReader.readFrom(
                                tx.createQueryWithCommit("""
                                                DECLARE $t1 AS UUID;
                                                DECLARE $t2 AS UUID;
                                                                                    
                                                SELECT id, link_count FROM issues
                                                WHERE id IN ($t1, $t2)
                                                """,
                                        Params.of("$t1", PrimitiveValue.newUuid(idT1), "$t2", PrimitiveValue.newUuid(idT2)))
                        ).join().getValue();

                        var linkTicketPairs = getLinkTicketPairs(valueReader);

                        return CompletableFuture.completedFuture(Result.success(linkTicketPairs));
                    }
            ).join().getValue();
        }

        public void createSchema() {
            retryCtx.supplyResult(
                    session -> session.createQuery(
                            """
                                    CREATE TABLE issues (
                                        id UUID NOT NULL,
                                        title Text,
                                        created_at Timestamp,
                                        author Text,
                                        PRIMARY KEY (id)
                                    );
                                    """, TxMode.NONE
                    ).execute()
            ).join().getStatus().expectSuccess("Can't create table issues");

            retryCtx.supplyResult(
                    session -> session.createQuery(
                            """
                                    ALTER TABLE issues ADD INDEX authorIndex GLOBAL ON (author);
                                    """, TxMode.NONE
                    ).execute()
            ).join().getStatus().expectSuccess("Can't create an author column and index");

            retryCtx.supplyResult(
                    session -> session.createQuery(
                            """
                                    ALTER TABLE issues ADD COLUMN link_count Int64;
                                    CREATE TABLE links (
                                        source UUID NOT NULL,
                                        destination UUID NOT NULL,
                                        PRIMARY KEY(source, destination)
                                    );
                                    """, TxMode.NONE
                    ).execute()
            ).join().getStatus().expectSuccess("Can't create an author column and index");
        }

        public void dropSchema() {
            retryCtx.supplyResult(
                    session -> session.createQuery(
                            """
                                    DROP TABLE issues;
                                    DROP TABLE links;
                                    """, TxMode.NONE
                    ).execute()
            ).join().getStatus().expectSuccess("Can't drop table issues");
        }

        public void addedTittle(String title, String author) {
            var id = UUID.randomUUID();
            var now = Instant.now();

            retryCtx.supplyResult(
                    session -> session.createQuery(
                            """
                                    DECLARE $id AS UUID;
                                    DECLARE $title AS Text;
                                    DECLARE $created_at AS Timestamp;
                                    DECLARE $author AS Text;
                                    UPSERT INTO issues (id, title, created_at, author, link_count)
                                    VALUES ($id, $title, $created_at, $author, 0);
                                    """,
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
            var resultSet = retryCtx.supplyResult(session -> QueryReader.readFrom(
                    session.createQuery("SELECT id, title, created_at, author, link_count FROM issues;",
                            TxMode.SERIALIZABLE_RW)
            )).join().getValue();

            var resultSetReader = resultSet.getResultSet(0);

            while (resultSetReader.next()) {
                titles.add(new Title(
                        resultSetReader.getColumn(0).getUuid(),
                        resultSetReader.getColumn(1).getText(),
                        resultSetReader.getColumn(2).getTimestamp(),
                        resultSetReader.getColumn(3).getText(),
                        resultSetReader.getColumn(4).getInt64()
                ));
            }

            return titles;
        }

        public Title findByAuthor(String author) {
            var resultSet = retryCtx.supplyResult(
                    session -> QueryReader.readFrom(
                            session.createQuery(
                                    """
                                            DECLARE $author AS Text;
                                            SELECT id, title, created_at, author, link_count FROM issues VIEW authorIndex
                                            WHERE author = $author;
                                            """,
                                    TxMode.SERIALIZABLE_RW, Params.of("$author", PrimitiveValue.newText(author))
                            )
                    )
            ).join().getValue();

            var resultSetReader = resultSet.getResultSet(0);
            resultSetReader.next();

            return new Title(
                    resultSetReader.getColumn(0).getUuid(),
                    resultSetReader.getColumn(1).getText(),
                    resultSetReader.getColumn(2).getTimestamp(),
                    resultSetReader.getColumn(3).getText(),
                    resultSetReader.getColumn(4).getInt64()
            );
        }

        private static ArrayList<Pair<UUID, Long>> getLinkTicketPairs(QueryReader valueReader) {
            var linkTicketPairs = new ArrayList<Pair<UUID, Long>>();
            var resultSet = valueReader.getResultSet(0);

            while (resultSet.next()) {
                linkTicketPairs.add(new Pair<>(resultSet.getColumn(0).getUuid(), resultSet.getColumn(1).getInt64()));
            }
            return linkTicketPairs;
        }
    }

    public record Title(UUID id, String title, Instant now, String author, long linkCount) {
    }

    public record Pair<A, B>(A a, B b) {
    }
}
