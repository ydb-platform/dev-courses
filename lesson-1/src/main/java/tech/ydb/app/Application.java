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
        var transport = GrpcTransport.forConnectionString(CONNECTION_STRING).build();
        var queryClient = QueryClient.newClient(transport).build();
        var retryCtx = SessionRetryContext.create(queryClient).build();

        QueryReader resultSet = retryCtx.supplyResult(session ->
                QueryReader.readFrom(session.createQuery("SELECT 1;", TxMode.NONE))
        ).join().getValue();

        ResultSetReader resultSetReader = resultSet.getResultSet(0);
        if (resultSetReader.next()) {
            System.out.println("Database is available! Result `SELECT 1;` command: " + resultSetReader.getColumn(0).getInt32());
        }
    }
}
