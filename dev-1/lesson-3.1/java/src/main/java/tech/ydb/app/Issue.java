package tech.ydb.app;

import java.time.Instant;
import java.util.UUID;

/*
 * Модель данных для представления тикета в примере
 * @author Kirill Kurdyukov
 */
public record Issue(
    long id,      // Уникальный идентификатор тикета
    String title, // Название тикета
    Instant now   // Время создания тикета
) {
}