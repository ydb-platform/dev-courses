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
                                    id UUID NOT NULL,
                                    title Text,
                                    created_at Timestamp,
                                    author Text,
                                    PRIMARY KEY (id)
                                );
                                """, TxMode.NONE
                ).execute()
        ).join().getStatus().expectSuccess("Can't create table issues");

        retryCtx.supplyResult(
                session -> session.createQuery(
                        """
                                ALTER TABLE issues ADD INDEX authorIndex GLOBAL ON (author);
                                """, TxMode.NONE
                ).execute()
        ).join().getStatus().expectSuccess("Can't create an author column and index");

        retryCtx.supplyResult(
                session -> session.createQuery(
                        """
                                ALTER TABLE issues ADD COLUMN link_count Int64;
                                CREATE TABLE links (
                                    source UUID NOT NULL,
                                    destination UUID NOT NULL,
                                    PRIMARY KEY(source, destination)
                                );
                                """, TxMode.NONE
                ).execute()
        ).join().getStatus().expectSuccess("Can't create an author column and index");

        retryCtx.supplyResult(
                session -> session.createQuery(
                        """
                                CREATE TOPIC task_status(
                                    CONSUMER email
                                ) WITH(
                                    auto_partitioning_strategy='scale_up',
                                    min_active_partitions=2,
                                    max_active_partitions=10,
                                    retention_period = Interval('P3D')
                                );
                                                                    
                                ALTER TABLE issues ADD COLUMN status Text;
                                """, TxMode.NONE
                ).execute()
        ).join().getStatus().expectSuccess("Can't create schema");
    }

    public void dropSchema() {
        retryCtx.supplyResult(
                session -> session.createQuery(
                        """
                                DROP TABLE issues;
                                DROP TABLE links;
                                DROP TOPIC task_status;
                                """, TxMode.NONE
                ).execute()
        ).join().getStatus().expectSuccess("Can't drop table issues");
    }
}
