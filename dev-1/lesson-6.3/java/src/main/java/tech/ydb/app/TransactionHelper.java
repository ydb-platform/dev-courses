package tech.ydb.app;

import tech.ydb.query.QueryTransaction;
import tech.ydb.query.tools.QueryReader;
import tech.ydb.table.query.Params;

/**
 * @author Kirill Kurdyukov
 */
public class TransactionHelper {

    private final QueryTransaction transaction;

    public TransactionHelper(QueryTransaction transaction) {
        this.transaction = transaction;
    }

    public QueryReader executeQuery(String yql, Params params) {
        return QueryReader.readFrom(transaction.createQueryWithCommit(yql, params)).join().getValue();
    }

    public QueryReader executeQueryWithCommit(String yql, Params params) {
        return QueryReader.readFrom(transaction.createQueryWithCommit(yql, params)).join().getValue();
    }
}
