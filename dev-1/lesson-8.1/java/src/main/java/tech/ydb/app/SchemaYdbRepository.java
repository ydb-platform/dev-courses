package tech.ydb.app;

import tech.ydb.query.tools.SessionRetryContext;

/**
 * @author Kirill Kurdyukov
 */
public class SchemaYdbRepository {

    private final QueryServiceHelper queryServiceHelper;

    public SchemaYdbRepository(SessionRetryContext retryCtx) {
        this.queryServiceHelper = new QueryServiceHelper(retryCtx);
    }

    public void createSchema() {
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

        queryServiceHelper.executeQuery("""
                ALTER TABLE issues ADD INDEX authorIndex GLOBAL ON (author);
                """
        );

        queryServiceHelper.executeQuery("""
                ALTER TABLE issues ADD COLUMN link_count Int64;
                CREATE TABLE IF NOT EXISTS links (
                    source Int64 NOT NULL,
                    destination Int64 NOT NULL,
                    PRIMARY KEY(source, destination)
                );
                """
        );

        queryServiceHelper.executeQuery("""
                ALTER TABLE issues ADD COLUMN status Text;
                                                
                ALTER TABLE issues ADD CHANGEFEED updates WITH (
                    FORMAT = 'JSON',
                    MODE = 'NEW_AND_OLD_IMAGES',
                    VIRTUAL_TIMESTAMPS = TRUE,
                    INITIAL_SCAN = TRUE
                );
                """
        );

        queryServiceHelper.executeQuery("ALTER TOPIC `issues/updates` ADD CONSUMER test;");
    }

    public void dropSchema() {
        queryServiceHelper.executeQuery("""
                DROP TABLE IF EXISTS issues;
                DROP TABLE IF EXISTS links;
                """
        );
    }
}
