package tech.ydb.app;

import java.time.Instant;

/**
 * Запись, представляющая тикет в системе.
 * Все изменения полей этой записи будут отслеживаться через changefeed.
 *
 * @author Kirill Kurdyukov
 */
public record Issue(
        /*
         * Уникальный идентификатор тикета
         */
        long id,

        /*
         * Заголовок тикета
         */
        String title,

        /*
         * Время создания тикета
         */
        Instant now,

        /*
         * Автор тикета
         */
        String author,

        /*
         * Количество связанных тикетов
         */
        long linkCounts,

        /*
         * Текущий статус тикета (NEW, IN_PROGRESS, DONE)
         */
        String status
) {
}