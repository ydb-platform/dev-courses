package tech.ydb.app;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import tech.ydb.core.grpc.GrpcTransport;
import tech.ydb.query.QueryClient;
import tech.ydb.query.tools.SessionRetryContext;
import tech.ydb.table.TableClient;

/**
 * @author Kirill Kurdyukov
 */
public class Application {

    private static final String PATH = "/java/lesson-10/title_author.csv";
    private static final String CONNECTION_STRING = "grpc://localhost:2136/local";

    public static void main(String[] args) {
        try (GrpcTransport grpcTransport = GrpcTransport.forConnectionString(CONNECTION_STRING).build();
             TableClient tableClient = TableClient.newClient(grpcTransport).build();
             QueryClient queryClient = QueryClient.newClient(grpcTransport).build()) {
            var retryCtx = SessionRetryContext.create(queryClient).build();
            var retryTableCtx = tech.ydb.table.SessionRetryContext.create(tableClient).build();

            var schemaYdbRepository = new SchemaYdbRepository(retryCtx);
            var issueYdbRepository = new IssueYdbRepository(retryCtx);
            var nativeApiYdbRepository = new KeyValueApiYdbRepository(retryTableCtx);

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
                System.err.println(e.getMessage());

                throw new RuntimeException(e);
            }

            nativeApiYdbRepository.bulkUpsert("/local/issues", titleAuthorList);

            Issue lastIssue = null;
            System.out.println("Print all issues: ");
            for (var issue : issueYdbRepository.findAll()) {
                printIssue(issue);

                lastIssue = issue;
            }

            System.out.println("ReadTable: ");
            for (var issue : nativeApiYdbRepository.readTable("/local/issues")) {
                printIssue(issue);
            }

            System.out.println("ReadRows: ");
            assert lastIssue != null;
            for (var issue : nativeApiYdbRepository.readRows("/local/issues", lastIssue.id())) {
                printIssue(issue);
            }

            schemaYdbRepository.dropSchema();
        }
    }

    private static void printIssue(Issue issue) {
        System.out.println("Ticket: {id: " + issue.id() + ", title: " + issue.title() + ", timestamp: "
                + issue.now() + ", author: " + issue.author() + ", link_count: "
                + issue.linkCounts() + ", status: " + issue.status() + "}");
    }
}
