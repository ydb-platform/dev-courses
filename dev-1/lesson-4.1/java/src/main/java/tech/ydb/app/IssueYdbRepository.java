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
import tech.ydb.table.values.PrimitiveValue;

/*
 * Репозиторий для работы с тикетами в базе данных YDB
 * Реализует операции добавления, чтения и связывания тикетов
 * @author Kirill Kurdyukov
 */
public class IssueYdbRepository {
    // Контекст для автоматических повторных попыток выполнения запросов
    private final SessionRetryContext retryCtx;

    public IssueYdbRepository(SessionRetryContext retryCtx) {
        this.retryCtx = retryCtx;
    }

    /*
     * Связывает два тикета в рамках неинтерактивной транзакции
     * Все операции (обновление счетчиков, добавление связей, чтение результатов) 
     * выполняются за один запрос к YDB.
     */
    public List<IssueLinkCount> linkTicketsNoInteractive(long idT1, long idT2) {
        var valueReader = retryCtx.supplyResult(
                session -> QueryReader.readFrom(session.createQuery(
                        """
                                DECLARE $t1 AS Int64;
                                DECLARE $t2 AS Int64;
                                                         
                                -- Обновляем счетчики связей
                                UPDATE issues
                                SET link_count = COALESCE(link_count, 0) + 1
                                WHERE id IN ($t1, $t2);
                                                            
                                -- Добавляем записи о связях между тикетами
                                INSERT INTO links (source, destination)
                                VALUES ($t1, $t2), ($t2, $t1);

                                -- Читаем обновленные данные
                                SELECT id, link_count FROM issues
                                WHERE id IN ($t1, $t2)
                                """,
                        TxMode.SERIALIZABLE_RW,
                        Params.of("$t1", PrimitiveValue.newInt64(idT1), "$t2", PrimitiveValue.newInt64(idT2))
                ))
        ).join().getValue();

        return getLinkTicketPairs(valueReader);
    }

    /*
     * Связывает два тикета в рамках интерактивной транзакции
     * Операции выполняются последовательными запросами к YDB:
     * 1. Обновление счетчиков связей
     * 2. Добавление записей о связях
     * 3. Чтение обновленных данных
     * 
     * Между запросами к YDB может быть выполнена логика на стороне приложения, 
     * для определения стоит ли продолжать транзакцию и какой запрос выполнить следующим.
     */
    public List<IssueLinkCount> linkTicketsInteractive(long idT1, long idT2) {
        return retryCtx.supplyResult(
                session -> {
                    // Транзакция будет изменять данные, поэтому используем режим SERIALIZABLE_RW
                    var tx = session.createNewTransaction(TxMode.SERIALIZABLE_RW);

                    // Обновляем счетчики связей
                    tx.createQuery("""
                                    DECLARE $t1 AS Int64;
                                    DECLARE $t2 AS Int64;
                                                                 
                                    UPDATE issues
                                    SET link_count = COALESCE(link_count, 0) + 1
                                    WHERE id IN ($t1, $t2);
                                    """,
                            Params.of("$t1", PrimitiveValue.newInt64(idT1), "$t2", PrimitiveValue.newInt64(idT2))
                    ).execute().join().getStatus().expectSuccess();

                    // Добавляем записи о связях между тикетами
                    tx.createQuery("""
                                    DECLARE $t1 AS Int64;
                                    DECLARE $t2 AS Int64;
                                                                        
                                    INSERT INTO links (source, destination)
                                    VALUES ($t1, $t2), ($t2, $t1);
                                    """,
                            Params.of("$t1", PrimitiveValue.newInt64(idT1), "$t2", PrimitiveValue.newInt64(idT2))
                    ).execute().join().getStatus().expectSuccess();

                    // Читаем обновленные данные и фиксируем транзакцию
                    var valueReader = QueryReader.readFrom(
                            tx.createQueryWithCommit("""
                                            DECLARE $t1 AS Int64;
                                            DECLARE $t2 AS Int64;
                                                                                
                                            SELECT id, link_count FROM issues
                                            WHERE id IN ($t1, $t2)
                                            """,
                                    Params.of("$t1", PrimitiveValue.newInt64(idT1), "$t2", PrimitiveValue.newInt64(idT2)))
                    ).join().getValue();

                    var linkTicketPairs = getLinkTicketPairs(valueReader);

                    return CompletableFuture.completedFuture(Result.success(linkTicketPairs));
                }
        ).join().getValue();
    }

    /*
     * Добавляет новый тикет в базу данных
     * @param title название тикета
     * @param author автор тикета
     */
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

    /*
     * Получает все тикеты из базы данных
     * @return список всех тикетов
     */
    public List<Issue> findAll() {
        var titles = new ArrayList<Issue>();
        var resultSet = retryCtx.supplyResult(
                session -> QueryReader.readFrom(
                        session.createQuery("SELECT id, title, created_at, author, COALESCE(link_count, 0) FROM issues;", TxMode.SNAPSHOT_RO)
                )
        ).join().getValue();

        var resultSetReader = resultSet.getResultSet(0);

        while (resultSetReader.next()) {
            titles.add(new Issue(
                    resultSetReader.getColumn(0).getInt64(),
                    resultSetReader.getColumn(1).getText(),
                    resultSetReader.getColumn(2).getTimestamp(),
                    resultSetReader.getColumn(3).getText(),
                    resultSetReader.getColumn(4).getInt64()
            ));
        }

        return titles;
    }

    /*
     * Преобразует результаты запроса в список объектов IssueLinkCount
     */
    private static List<IssueLinkCount> getLinkTicketPairs(QueryReader valueReader) {
        var linkTicketPairs = new ArrayList<IssueLinkCount>();
        var resultSet = valueReader.getResultSet(0);

        while (resultSet.next()) {
            linkTicketPairs.add(
                    new IssueLinkCount(resultSet.getColumn(0).getInt64(), resultSet.getColumn(1).getInt64())
            );
        }
        return linkTicketPairs;
    }
}
