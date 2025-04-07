package tech.ydb.app;

import tech.ydb.common.transaction.TxMode;
import tech.ydb.query.tools.SessionRetryContext;

/**
 * Репозиторий для управления схемой базы данных YDB
 * Отвечает за создание и удаление таблиц и индексов
 * @author Kirill Kurdyukov
 */
public class SchemaYdbRepository {

    // Контекст для автоматических повторных попыток выполнения запросов
    private final SessionRetryContext retryCtx;

    public SchemaYdbRepository(SessionRetryContext retryCtx) {
        this.retryCtx = retryCtx;
    }

    /**
     * Создает таблицы и индексы в базе данных
     */
    public void createSchema() {
        retryCtx.supplyResult(
                session -> session.createQuery(
                        """
                                CREATE TABLE issues (
                                    id Int64 NOT NULL,
                                    title Text NOT NULL,
                                    created_at Timestamp NOT NULL,
                                    author Text,
                                    PRIMARY KEY (id)
                                );
                                """, TxMode.NONE
                ).execute()
        ).join().getStatus().expectSuccess("Can't create table issues");

        // Создаем глобальный вторичный индекс по полю author
        // Этот индекс позволяет эффективно искать тикеты по автору
        retryCtx.supplyResult(
                session -> session.createQuery(
                        """
                                ALTER TABLE issues ADD INDEX authorIndex GLOBAL ON (author);
                                """, TxMode.NONE
                ).execute()
        ).join().getStatus().expectSuccess("Can't create an author column and index");

        // Добавляем колонку link_count и создаем таблицу для связей между тикетами
        retryCtx.supplyResult(
                session -> session.createQuery(
                        """
                                ALTER TABLE issues ADD COLUMN link_count Int64;
                                CREATE TABLE links (
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
