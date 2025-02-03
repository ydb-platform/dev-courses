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
public class Application {

    private static final String CONNECTION_STRING = "grpc://localhost:2136/local";

    public static void main(String[] args) {
        try (YdbRepository ydbRepository = new YdbRepository()) {
            System.out.println("Database is available! Result `SELECT 1;` command: " + ydbRepository.SelectOne());
        }
    }

    public static class YdbRepository implements AutoCloseable {
        private final GrpcTransport transport = GrpcTransport.forConnectionString(CONNECTION_STRING).build();
        private final QueryClient queryClient = QueryClient.newClient(transport).build();
        private final SessionRetryContext retryCtx = SessionRetryContext.create(queryClient).build();

        public int SelectOne() {
            QueryReader resultSet = retryCtx.supplyResult(session ->
                    QueryReader.readFrom(session.createQuery("SELECT 1;", TxMode.NONE))
            ).join().getValue();

            ResultSetReader resultSetReader = resultSet.getResultSet(0);

            resultSetReader.next();

            return resultSetReader.getColumn(0).getInt32();
        }

        @Override
        public void close() {
            queryClient.close();
            transport.close();
        }
    }
}
