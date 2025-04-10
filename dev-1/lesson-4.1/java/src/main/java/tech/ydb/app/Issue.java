package tech.ydb.app;

import java.time.Instant;

/**
 * Модель данных для представления тикета в системе
 *
 * @author Kirill Kurdyukov
 */
public record Issue(
        long id,           // Уникальный идентификатор тикета
        String title,      // Название тикета
        Instant now,       // Время создания тикета
        String author,     // Автор тикета
        long linkCounts    // Количество связей с другими тикетами
) {
}