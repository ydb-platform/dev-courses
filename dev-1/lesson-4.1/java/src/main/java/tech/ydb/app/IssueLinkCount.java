package tech.ydb.app;

/**
 * Модель данных для представления результата операции связывания тикетов
 *
 * @author Kirill Kurdyukov
 */
public record IssueLinkCount(
        long id,        // Идентификатор тикета
        long linkCount  // Количество связей тикета после операции
) {
}
