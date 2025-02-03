package tech.ydb.app;

import java.time.Instant;
import java.util.ArrayList;
import java.util.UUID;
import java.util.List;
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

                ticketsYdbRepository.AddedTittle("Ticket 1");
                ticketsYdbRepository.AddedTittle("Ticket 2");
                ticketsYdbRepository.AddedTittle("Ticket 3");

                for (var ticket : ticketsYdbRepository.findAll()) {
                    System.out.println("Ticket: {id: " + ticket.id() + ", title: " + ticket.title() + ", timestamp: " + ticket.now() + "}");
                }

                ticketsYdbRepository.dropSchema();
            }
        }
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
                                    "PRIMARY KEY (id)" +
                                    ")", TxMode.NONE
                    ).execute()
            ).join().getStatus().expectSuccess("Can't create table issues");
        }

        public void dropSchema() {
            retryCtx.supplyResult(
                    session -> session.createQuery(
                            "DROP TABLE issues;", TxMode.NONE
                    ).execute()
            ).join().getStatus().expectSuccess("Can't drop table issues");
        }

        public Title AddedTittle(String title) {
            var id = UUID.randomUUID();
            var now = Instant.now();

            retryCtx.supplyResult(
                    session -> session.createQuery(
                            "" +
                                    "DECLARE $id AS UUID; " +
                                    "DECLARE $title AS Text; " +
                                    "DECLARE $created_at AS Timestamp; " +
                                    "UPSERT INTO issues (id, title, created_at) " +
                                    "VALUES ($id, $title, $created_at)",
                            TxMode.SERIALIZABLE_RW,
                            Params.of(
                                    "$id", PrimitiveValue.newUuid(id),
                                    "$title", PrimitiveValue.newText(title),
                                    "$created_at", PrimitiveValue.newTimestamp(now)
                            )
                    ).execute()
            ).join().getStatus().expectSuccess("Failed upsert title");

            return new Title(id, title, now);
        }

        public List<Title> findAll() {
            var titles = new ArrayList<Title>();
            var resultSet = retryCtx.supplyResult(
                    session -> QueryReader.readFrom(
                            session.createQuery("SELECT id, title, created_at FROM issues;", TxMode.SNAPSHOT_RO)
                    )
            ).join().getValue();

            var resultSetReader = resultSet.getResultSet(0);

            while (resultSetReader.next()) {
                titles.add(new Title(
                        resultSetReader.getColumn(0).getUuid(),
                        resultSetReader.getColumn(1).getText(),
                        resultSetReader.getColumn(2).getTimestamp()
                ));
            }

            return titles;
        }
    }

    public record Title(UUID id, String title, Instant now) {
    }
}
