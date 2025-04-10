package tech.ydb.app;

import tech.ydb.common.transaction.TxMode;
import tech.ydb.query.tools.SessionRetryContext;

/**
 * Репозиторий для управления схемой базы данных YDB
 * Отвечает за создание и удаление таблиц
 *
 * @author Kirill Kurdyukov
 */
public class SchemaYdbRepository {

    // Контекст для автоматических повторных попыток выполнения запросов
    private final SessionRetryContext retryCtx;

    public SchemaYdbRepository(SessionRetryContext retryCtx) {
        this.retryCtx = retryCtx;
    }

    /**
     * Создает таблицу issues в базе данных
     * Таблица содержит поля:
     * - id: уникальный идентификатор тикета
     * - title: название тикета
     * - created_at: время создания тикета
     * <p>
     * Все поля являются обязательными.
     */
    public void createSchema() {
        retryCtx.supplyResult(
                session -> session.createQuery(
                        """
                                CREATE TABLE IF NOT EXISTS issues (
                                    id Int64 NOT NULL,
                                    title Text NOT NULL,
                                    created_at Timestamp NOT NULL,
                                    PRIMARY KEY (id)
                                );
                                """, TxMode.NONE
                ).execute()
        ).join().getStatus().expectSuccess("Can't create table issues");
    }

    /**
     * Удаляет таблицу issues из базы данных
     * Используется для очистки схемы перед созданием новой
     */
    public void dropSchema() {
        retryCtx.supplyResult(
                session -> session.createQuery("DROP TABLE IF EXISTS issues;", TxMode.NONE).execute()
        ).join().getStatus().expectSuccess("Can't drop table issues");
    }
}
