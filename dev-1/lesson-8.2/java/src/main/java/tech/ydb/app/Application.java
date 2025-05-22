package tech.ydb.app;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.ydb.core.grpc.GrpcTransport;
import tech.ydb.query.QueryClient;
import tech.ydb.query.tools.SessionRetryContext;
import tech.ydb.table.TableClient;

/**
 * @author Kirill Kurdyukov
 */
public class Application {

    private static final Logger LOGGER = LoggerFactory.getLogger(Application.class);
    private static final String PATH = "/lesson-8.2/java/title_author.csv";
    private static final String CONNECTION_STRING = "grpc://localhost:2136/local";

    public static void main(String[] args) {
        try (GrpcTransport grpcTransport = GrpcTransport
                .forConnectionString(CONNECTION_STRING)
                .withConnectTimeout(Duration.ofSeconds(10))
                .build();
             TableClient tableClient = TableClient.newClient(grpcTransport).build();
             QueryClient queryClient = QueryClient.newClient(grpcTransport).build()
        ) {
            var retryCtx = SessionRetryContext.create(queryClient).build();
            var retryTableCtx = tech.ydb.table.SessionRetryContext.create(tableClient).build();

            var schemaYdbRepository = new SchemaYdbRepository(retryCtx);
            var issueYdbRepository = new IssueYdbRepository(retryCtx);
            var nativeApiYdbRepository = new KeyValueApiYdbRepository(retryTableCtx);

            schemaYdbRepository.dropSchema();
            schemaYdbRepository.createSchema();

            var titleAuthorList = new ArrayList<TitleAuthor>();
            try (CSVReader reader = new CSVReader(Files.newBufferedReader(
                    Path.of(System.getProperty("user.dir"), PATH)))
            ) {
                List<String[]> allRows = reader.readAll();

                for (int i = 1; i < allRows.size(); i++) {
                    var title = allRows.get(i)[0];
                    var author = allRows.get(i)[1];

                    titleAuthorList.add(new TitleAuthor(title, author));
                }
            } catch (IOException | CsvException e) {
                LOGGER.error(e.getMessage());

                throw new RuntimeException(e);
            }

            // Массовое добавление данных через Key-Value API
            nativeApiYdbRepository.bulkUpsert("/local/issues", titleAuthorList);

            Issue lastIssue = null;
            LOGGER.info("Print all issues: ");
            for (var issue : issueYdbRepository.findAll()) {
                printIssue(issue);

                lastIssue = issue;
            }

            // Чтение всех данных через Key-Value API
            LOGGER.info("ReadTable: ");
            for (var issue : nativeApiYdbRepository.readTable("/local/issues")) {
                printIssue(issue);
            }

            // Чтение данных по ключу через Key-Value API
            LOGGER.info("ReadRows: ");
            assert lastIssue != null;
            for (var issue : nativeApiYdbRepository.readRows("/local/issues", lastIssue.id())) {
                printIssue(issue);
            }
        }
    }

    private static void printIssue(Issue issue) {
        LOGGER.info("Issue: {}", issue);
    }
}
