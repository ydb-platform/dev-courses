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

    public void createSchema() {
        // Создаем основную таблицу issues
        queryServiceHelper.executeQuery("""
                CREATE TABLE IF NOT EXISTS issues (
                    id Int64 NOT NULL,
                    title Text NOT NULL,
                    created_at Timestamp NOT NULL,
                    author Text,
                    PRIMARY KEY (id)
                );
                """
        );

        // Добавляем колонку link_count и создаем таблицу для связей между тикетами
        queryServiceHelper.executeQuery("""
                ALTER TABLE issues ADD COLUMN link_count Int64;
                                                
                CREATE TABLE IF NOT EXISTS links (
                    source Int64 NOT NULL,
                    destination Int64 NOT NULL,
                    PRIMARY KEY(source, destination)
                );
                """
        );
    }

    public void dropSchema() {
        queryServiceHelper.executeQuery("""
                DROP TABLE IF EXISTS issues;
                DROP TABLE IF EXISTS links;
                """
        );
    }
}
