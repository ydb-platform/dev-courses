package tech.ydb.app;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;

import tech.ydb.common.transaction.TxMode;
import tech.ydb.core.Result;
import tech.ydb.query.tools.QueryReader;
import tech.ydb.query.tools.SessionRetryContext;
import tech.ydb.table.query.Params;
import tech.ydb.table.values.ListType;
import tech.ydb.table.values.OptionalType;
import tech.ydb.table.values.PrimitiveType;
import tech.ydb.table.values.PrimitiveValue;
import tech.ydb.table.values.StructType;

/**
 * @author Kirill Kurdyukov
 */
public class IssueYdbRepository {
    private final SessionRetryContext retryCtx;

    public IssueYdbRepository(SessionRetryContext retryCtx) {
        this.retryCtx = retryCtx;
    }

    public List<Issue> findByIds(List<Long> ids) {
        var structType = StructType.of("id", PrimitiveType.Int64);

        var idsParams = Params.of("$ids", ListType.of(structType).newValue(
                ids.stream().map(id -> structType.newValue("id", PrimitiveValue.newInt64(id))).toList())
        );
        var queryReader = retryCtx.supplyResult(session -> QueryReader.readFrom(
                session.createQuery("""
                                DECLARE $ids AS List<Struct<id: Int64>>;
                                SELECT id, title, created_at, author, link_count, status
                                FROM issues WHERE id IN (SELECT id FROM AS_TABLE($ids));
                                """,
                        TxMode.SERIALIZABLE_RW, idsParams)
        )).join().getValue();

        return fetchIssues(queryReader);
    }

    public void saveAll(List<TitleAuthor> titleAuthors) {
        var structType = StructType.of(
                "id", PrimitiveType.Int64,
                "title", PrimitiveType.Text,
                "author", OptionalType.of(PrimitiveType.Text),
                "created_at", PrimitiveType.Timestamp
        );

        var listIssues = Params.of("$args", ListType.of(structType).newValue(
                titleAuthors.stream().map(issue -> structType.newValue(
                        "id", PrimitiveValue.newInt64(ThreadLocalRandom.current().nextLong()),
                        "title", PrimitiveValue.newText(issue.title()),
                        "author", OptionalType.of(PrimitiveType.Text).newValue(PrimitiveValue.newText(issue.author())),
                        "created_at", PrimitiveValue.newTimestamp(Instant.now())
                )).toList()
        ));

        retryCtx.supplyResult(
                session -> session.createQuery(
                        """
                                DECLARE $args AS List<Struct<
                                id: Int64,
                                title: Text,
                                author: Text?, -- тут знак вопроса означает, что в Timestamp может быть передан NULL
                                created_at: Timestamp, 
                                >>;

                                UPSERT INTO issues
                                SELECT * FROM AS_TABLE($args);
                                """,
                        TxMode.SERIALIZABLE_RW,
                        listIssues
                ).execute()
        ).join().getStatus().expectSuccess("Failed upsert title");
    }

    public void updateStatus(long id, String status) {
        retryCtx.supplyResult(
                session -> session.createQuery(
                        """
                                DECLARE $id AS Int64;
                                DECLARE $new_status AS Text;
                                                                    
                                UPDATE issues SET status = $new_status WHERE id = $id;
                                """,
                        TxMode.SERIALIZABLE_RW,
                        Params.of("$id", PrimitiveValue.newInt64(id),
                                "$new_status", PrimitiveValue.newText(status))
                ).execute()
        ).join().getStatus().expectSuccess();
    }

    public List<IssueLinkCount> linkTicketsNoInteractive(long idT1, long idT2) {
        var valueReader = retryCtx.supplyResult(
                session -> QueryReader.readFrom(session.createQuery(
                        """
                                DECLARE $t1 AS Int64;
                                DECLARE $t2 AS Int64;
                                                                    
                                UPDATE issues
                                SET link_count = COALESCE(link_count, 0) + 1
                                WHERE id IN ($t1, $t2);
                                                                    
                                INSERT INTO links (source, destination)
                                VALUES ($t1, $t2), ($t2, $t1);

                                SELECT id, link_count FROM issues
                                WHERE id IN ($t1, $t2)
                                """,
                        TxMode.SERIALIZABLE_RW,
                        Params.of("$t1", PrimitiveValue.newInt64(idT1), "$t2", PrimitiveValue.newInt64(idT2))
                ))
        ).join().getValue();

        return getIssueLinkCount(valueReader);
    }

    public List<IssueLinkCount> linkTicketsInteractive(long idT1, long idT2) {
        return retryCtx.supplyResult(
                session -> {
                    var tx = session.createNewTransaction(TxMode.SERIALIZABLE_RW);

                    tx.createQuery("""
                                    DECLARE $t1 AS Int64;
                                    DECLARE $t2 AS Int64;
                                                                 
                                    UPDATE issues
                                    SET link_count = COALESCE(link_count, 0) + 1
                                    WHERE id IN ($t1, $t2);
                                    """,
                            Params.of("$t1", PrimitiveValue.newInt64(idT1), "$t2", PrimitiveValue.newInt64(idT2))
                    ).execute().join().getStatus().expectSuccess();

                    tx.createQuery("""
                                    DECLARE $t1 AS Int64;
                                    DECLARE $t2 AS Int64;
                                                                        
                                    INSERT INTO links (source, destination)
                                    VALUES ($t1, $t2), ($t2, $t1);
                                    """,
                            Params.of("$t1", PrimitiveValue.newInt64(idT1), "$t2", PrimitiveValue.newInt64(idT2))
                    ).execute().join().getStatus().expectSuccess();

                    var valueReader = QueryReader.readFrom(
                            tx.createQueryWithCommit("""
                                            DECLARE $t1 AS Int64;
                                            DECLARE $t2 AS Int64;
                                                                                
                                            SELECT id, link_count FROM issues
                                            WHERE id IN ($t1, $t2)
                                            """,
                                    Params.of("$t1", PrimitiveValue.newInt64(idT1), "$t2", PrimitiveValue.newInt64(idT2)))
                    ).join().getValue();

                    var linkTicketPairs = getIssueLinkCount(valueReader);

                    return CompletableFuture.completedFuture(Result.success(linkTicketPairs));
                }
        ).join().getValue();
    }

    public void addIssue(String title, String author) {
        var id = ThreadLocalRandom.current().nextLong();
        var now = Instant.now();

        retryCtx.supplyResult(
                session -> session.createQuery(
                        """
                                DECLARE $id AS Int64;
                                DECLARE $title AS Text;
                                DECLARE $created_at AS Timestamp;
                                DECLARE $author AS Text;
                                UPSERT INTO issues (id, title, created_at, author)
                                VALUES ($id, $title, $created_at, $author);
                                """,
                        TxMode.SERIALIZABLE_RW,
                        Params.of(
                                "$id", PrimitiveValue.newInt64(id),
                                "$title", PrimitiveValue.newText(title),
                                "$created_at", PrimitiveValue.newTimestamp(now),
                                "$author", PrimitiveValue.newText(author)
                        )
                ).execute()
        ).join().getStatus().expectSuccess("Failed upsert title");
    }

    public List<Issue> findAll() {
        var resultSet = retryCtx.supplyResult(
                session -> QueryReader.readFrom(
                        session.createQuery("SELECT id, title, created_at, author, COALESCE(link_count, 0), status FROM issues;", TxMode.SNAPSHOT_RO)
                )
        ).join().getValue();

        return fetchIssues(resultSet);
    }

    public List<IssueTitle> findFutures() {
        var queryReader = retryCtx.supplyResult(
                session -> QueryReader.readFrom(
                        session.createQuery("""
                                -- выбираем ID и заголовки задач, которые должны быть созданы в будущем
                                $future =
                                SELECT id, title
                                FROM issues
                                WHERE status = 'future';
                                                                
                                -- возвращаем их как результат запроса
                                SELECT * FROM $future;
                                                                
                                -- и обновляем статус/время точно у этих же задач
                                UPDATE issues ON
                                                                
                                SELECT id, CurrentUtcTimestamp() AS created_at, CAST('new' AS Utf8) AS status
                                                                
                                FROM $future
                                """, TxMode.SERIALIZABLE_RW)
                )
        ).join().getValue();

        var linkTicketPairs = new ArrayList<IssueTitle>();
        var resultSet = queryReader.getResultSet(0);

        while (resultSet.next()) {
            linkTicketPairs.add(new IssueTitle(resultSet.getColumn(0).getInt64(), resultSet.getColumn(1).getText()));
        }

        return linkTicketPairs;
    }

    public void deleteTasks(List<Long> ids) {
        var idsParam = ListType.of(PrimitiveType.Int64).newValue(
                ids.stream().map(PrimitiveValue::newInt64).toList()
        );

        retryCtx.supplyResult(
                session -> session.createQuery(
                        """
                                -- принимаем id задач для удаления
                                DECLARE $issues_ids_arg AS List<Int64>;

                                -- это лямбда-функция для преобразования отдельного элемента списка в структуру
                                $list_to_id_struct = ($id) -> { RETURN <|id:$id|>};

                                -- тут удаляем из списка возможные дубли и преобразовываем список id в список структур
                                $issue_ids_list = ListMap(ListUniq($issues_ids_arg), $list_to_id_struct);

                                -- внутри других запросов проще работать с результатом запроса к таблице,
                                -- чтобы не помнить везде о том что это когда-то было списком структур или
                                -- параметром
                                $issues = SELECT id FROM AS_TABLE($issue_ids_list);


                                -- выбираем связи этих задач
                                $linked_issues =
                                SELECT source, destination

                                FROM links

                                WHERE source IN $issues;


                                -- и связи в обратную сторону
                                $linked_issues_mirrored =
                                SELECT destination AS source, source AS destination
                                FROM $linked_issues;


                                $mirrored_dec_map =
                                SELECT source AS id, COUNT(*) AS cnt
                                FROM $linked_issues_mirrored

                                GROUP BY source;


                                -- именованные выражения это просто подстановка запросов, т.е. промежуточного сохранения данных не происходит
                                -- поэтому важно выполнять запросы в таком порядке, чтобы данные, на которые опирается выражение ещё не были испорчены
                                -- к моменту его выполнения, проще всего идти с конца

                                -- сначала обновляем счётчики у связанных тикетов
                                UPDATE issues ON

                                SELECT i.id AS id, i.link_count - d.cnt AS link_count

                                FROM $mirrored_dec_map AS d JOIN issues AS i ON d.id = i.id;


                                -- теперь обновляем счётчики у переданных тикетов
                                UPDATE issues
                                SET link_count=link_count-1

                                WHERE id IN $issues;


                                -- и удаляем сами тикеты
                                -- если тикеты удалить раньше, то
                                DELETE FROM issues

                                WHERE id IN $issues;
                                """,
                        TxMode.SERIALIZABLE_RW,
                        Params.of("$issues_ids_arg", idsParam)
                ).execute()
        ).join().getStatus().expectSuccess("Failed upsert title");
    }

    public Issue findByAuthor(String author) {
        var resultSet = retryCtx.supplyResult(
                session -> QueryReader.readFrom(
                        session.createQuery(
                                """
                                        DECLARE $author AS Text;
                                        SELECT id, title, created_at, author, COALESCE(link_count, 0), status FROM issues VIEW authorIndex
                                        WHERE author = $author;
                                        """,
                                TxMode.SNAPSHOT_RO,
                                Params.of("$author", PrimitiveValue.newText(author))
                        )
                )
        ).join().getValue();

        var resultSetReader = resultSet.getResultSet(0);
        resultSetReader.next();

        return new Issue(
                resultSetReader.getColumn(0).getInt64(),
                resultSetReader.getColumn(1).getText(),
                resultSetReader.getColumn(2).getTimestamp(),
                resultSetReader.getColumn(3).getText(),
                resultSetReader.getColumn(4).getInt64(),
                resultSetReader.getColumn(5).getText()
        );
    }

    private static List<IssueLinkCount> getIssueLinkCount(QueryReader valueReader) {
        var linkTicketPairs = new ArrayList<IssueLinkCount>();
        var resultSet = valueReader.getResultSet(0);

        while (resultSet.next()) {
            linkTicketPairs.add(new IssueLinkCount(resultSet.getColumn(0).getInt64(), resultSet.getColumn(1).getInt64()));
        }
        return linkTicketPairs;
    }

    private static List<Issue> fetchIssues(QueryReader queryReader) {
        var issues = new ArrayList<Issue>();

        var resultSetReader = queryReader.getResultSet(0);

        while (resultSetReader.next()) {
            issues.add(new Issue(
                    resultSetReader.getColumn(0).getInt64(),
                    resultSetReader.getColumn(1).getText(),
                    resultSetReader.getColumn(2).getTimestamp(),
                    resultSetReader.getColumn(3).getText(),
                    resultSetReader.getColumn(4).getInt64(),
                    resultSetReader.getColumn(5).getText()
            ));
        }

        return issues;
    }
}
