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

    public void createSchema() {
        // Создаем основную таблицу issues
        retryCtx.supplyResult(
                session -> session.createQuery(
                        """
                                CREATE TABLE IF NOT EXISTS issues (
                                    id Int64 NOT NULL,
                                    title Text NOT NULL,
                                    created_at Timestamp NOT NULL,
                                    author Text,
                                    PRIMARY KEY (id)
                                );
                                """, TxMode.NONE
                ).execute()
        ).join().getStatus().expectSuccess("Can't create table issues");

        // Добавляем колонку link_count и создаем таблицу для связей между тикетами
        retryCtx.supplyResult(
                session -> session.createQuery(
                        """
                                ALTER TABLE issues ADD COLUMN link_count Int64;
                                                                
                                CREATE TABLE IF NOT EXISTS links (
                                    source Int64 NOT NULL,
                                    destination Int64 NOT NULL,
                                    PRIMARY KEY(source, destination)
                                );
                                """, TxMode.NONE
                ).execute()
        ).join().getStatus().expectSuccess("Can't create an author column and index");
    }

    public void dropSchema() {
        retryCtx.supplyResult(
                session -> session.createQuery(
                        """
                                DROP TABLE IF EXISTS issues;
                                DROP TABLE IF EXISTS links;
                                """, TxMode.NONE
                ).execute()
        ).join().getStatus().expectSuccess("Can't drop table issues");
    }
}
