package tech.ydb.app;

import java.time.Instant;
import java.util.UUID;

/**
 * @author Kirill Kurdyukov
 */
public record Issue(long id, String title, Instant now, String author, long linkCounts, String status) {
}