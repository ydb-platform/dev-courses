package tech.ydb.app;

import tech.ydb.query.tools.SessionRetryContext;

/**
 * Репозиторий для управления схемой базы данных YDB
 * Отвечает за создание и удаление таблиц
 *
 * @author Kirill Kurdyukov
 */
public class SchemaYdbRepository {

    private final QueryServiceHelper queryServiceHelper;

    public SchemaYdbRepository(SessionRetryContext retryCtx) {
        this.queryServiceHelper = new QueryServiceHelper(retryCtx);
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
        queryServiceHelper.executeQuery("""
                CREATE TABLE IF NOT EXISTS issues (
                    id Int64 NOT NULL,
                    title Text NOT NULL,
                    created_at Timestamp NOT NULL,
                    PRIMARY KEY (id)
                );
                """
        );
    }

    /**
     * Удаляет таблицу issues из базы данных
     * Используется для очистки схемы перед созданием новой
     */
    public void dropSchema() {
        queryServiceHelper.executeQuery("DROP TABLE IF EXISTS issues;");
    }
}
