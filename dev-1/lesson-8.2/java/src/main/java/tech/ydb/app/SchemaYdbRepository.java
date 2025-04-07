package tech.ydb.app;

import tech.ydb.common.transaction.TxMode;
import tech.ydb.query.tools.SessionRetryContext;

/**
 * @author Kirill Kurdyukov
 */
public class SchemaYdbRepository {

    private final SessionRetryContext retryCtx;

    public SchemaYdbRepository(SessionRetryContext retryCtx) {
        this.retryCtx = retryCtx;
    }

    public void createSchema() {
        retryCtx.supplyResult(
                session -> session.createQuery(
                        """
                                CREATE TABLE issues (
                                    id Int64 NOT NULL,
                                    title Text NOT NULL,
                                    created_at Timestamp NOT NULL,
                                    author Text NOT NULL,
                                    PRIMARY KEY (id)
                                );
                                """, TxMode.NONE
                ).execute()
        ).join().getStatus().expectSuccess("Can't create table issues");

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

        retryCtx.supplyResult(
                session -> session.createQuery(
                        """
                                ALTER TABLE issues ADD COLUMN status Text;
                                                                
                                ALTER TABLE issues ADD CHANGEFEED updates WITH (
                                    FORMAT = 'JSON',
                                    MODE = 'NEW_AND_OLD_IMAGES',
                                    VIRTUAL_TIMESTAMPS = TRUE,
                                    INITIAL_SCAN = TRUE
                                );
                                """, TxMode.NONE
                ).execute()
        ).join().getStatus().expectSuccess("Can't create schema");

        retryCtx.supplyResult(
                session -> session.createQuery(
                        """
                                ALTER TOPIC `issues/updates` ADD CONSUMER test;
                                """, TxMode.NONE
                ).execute()
        ).join().getStatus().expectSuccess("Can't create schema");
    }

    public void createAuthorIndex() {
        retryCtx.supplyResult(
                session -> session.createQuery(
                        """
                                ALTER TABLE issues ADD INDEX authorIndex GLOBAL ON (author);
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
