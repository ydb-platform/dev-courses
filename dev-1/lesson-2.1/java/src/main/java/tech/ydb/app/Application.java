package tech.ydb.app;

import tech.ydb.common.transaction.TxMode;
import tech.ydb.core.grpc.GrpcTransport;
import tech.ydb.query.QueryClient;
import tech.ydb.query.tools.QueryReader;
import tech.ydb.query.tools.SessionRetryContext;
import tech.ydb.table.result.ResultSetReader;

/**
 * @author Kirill Kurdyukov
 */
public class        Application {
    private static final String CONNECTION_STRING = "grpc://localhost:2136/local";

    public static void main(String[] args) {
        try (GrpcTransport grpcTransport = GrpcTransport.forConnectionString(CONNECTION_STRING).build()) {
            try (QueryClient queryClient = QueryClient.newClient(grpcTransport).build()) {
                SessionRetryContext retryCtx = SessionRetryContext.create(queryClient).build();

                System.out.println("Database is available! Result `SELECT 1;` command: " +
                        new YdbRepository(retryCtx).SelectOne());
            }
        }
    }

    public static class YdbRepository {
        private final SessionRetryContext retryCtx;

        public YdbRepository(SessionRetryContext retryCtx) {
            this.retryCtx = retryCtx;
        }

        public int SelectOne() {
            QueryReader resultSet = retryCtx.supplyResult(session ->
                    QueryReader.readFrom(session.createQuery("SELECT 1;", TxMode.NONE))
            ).join().getValue();

            ResultSetReader resultSetReader = resultSet.getResultSet(0);

            resultSetReader.next();

            return resultSetReader.getColumn(0).getInt32();
        }
    }
}
