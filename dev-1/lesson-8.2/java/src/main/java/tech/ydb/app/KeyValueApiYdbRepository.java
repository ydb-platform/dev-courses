package tech.ydb.app;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import tech.ydb.core.Result;
import tech.ydb.table.SessionRetryContext;
import tech.ydb.table.result.ResultSetReader;
import tech.ydb.table.settings.ReadRowsSettings;
import tech.ydb.table.settings.ReadTableSettings;
import tech.ydb.table.values.ListType;
import tech.ydb.table.values.OptionalType;
import tech.ydb.table.values.PrimitiveType;
import tech.ydb.table.values.PrimitiveValue;
import tech.ydb.table.values.StructType;

/**
 * @author Kirill Kurdyukov
 */
public class KeyValueApiYdbRepository {

    private final SessionRetryContext retryTableCtx;

    public KeyValueApiYdbRepository(SessionRetryContext retryTableCtx) {
        this.retryTableCtx = retryTableCtx;
    }

    public void bulkUpsert(String tableName, List<TitleAuthor> titleAuthorList) {
        var structType = StructType.of(
                "id", PrimitiveType.Uuid,
                "title", PrimitiveType.Text,
                "author", PrimitiveType.Text,
                "created_at", OptionalType.of(PrimitiveType.Timestamp)
        );

        var listIssues = ListType.of(structType).newValue(
                titleAuthorList.stream().map(issue -> structType.newValue(
                        "id", PrimitiveValue.newUuid(UUID.randomUUID()),
                        "title", PrimitiveValue.newText(issue.title()),
                        "author", PrimitiveValue.newText(issue.author()),
                        "created_at", OptionalType.of(PrimitiveType.Timestamp)
                                .newValue(PrimitiveValue.newTimestamp(Instant.now()))
                )).toList()
        );

        retryTableCtx.supplyStatus(session -> session.executeBulkUpsert(tableName, listIssues))
                .join().expectSuccess();
    }

    public List<Issue> readTable(String tableName) {
        return retryTableCtx.supplyResult(session -> {
                    var listResult = new ArrayList<Issue>();

                    session.executeReadTable(tableName, ReadTableSettings.newBuilder().build())
                            .start(
                                    readTablePart -> {
                                        var resultSetReader = readTablePart.getResultSetReader();

                                        fetchIssues(listResult, resultSetReader);
                                    }
                            ).join().expectSuccess();


                    return CompletableFuture.completedFuture(Result.success(listResult));
                }
        ).join().getValue();
    }

    public List<Issue> readRows(String tableName, UUID id) {
        var keyStruct = StructType.of("id", PrimitiveType.Uuid);

        return retryTableCtx.supplyResult(session -> {
                    var listResult = new ArrayList<Issue>();

                    var resultSetReader = session.readRows(tableName,
                            ReadRowsSettings.newBuilder()
                                    .addKey(keyStruct.newValue("id", PrimitiveValue.newUuid(id)))
                                    .build()
                    ).join().getValue().getResultSetReader();

                    fetchIssues(listResult, resultSetReader);

                    return CompletableFuture.completedFuture(Result.success(listResult));
                }
        ).join().getValue();
    }

    private void fetchIssues(ArrayList<Issue> listResult, ResultSetReader resultSetReader) {
        while (resultSetReader.next()) {
            var id = resultSetReader.getColumn(0).getUuid();

            if (!id.toString().contains("0")) {
                continue;
            }

            listResult.add(
                    new Issue(
                            id,
                            resultSetReader.getColumn(1).getText(),
                            resultSetReader.getColumn(2).getTimestamp(),
                            resultSetReader.getColumn(3).getText(),
                            resultSetReader.getColumn(4).getInt64(),
                            resultSetReader.getColumn(5).getText()
                    )
            );
        }
    }
}
