package main

import (
	"context"
	"log"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
)

// author: Egor Danilov
func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Строка подключения к локальной базе данных YDB
	// Формат: grpc://<хост>:<порт>/<путь к базе данных>
	dsn := "grpc://localhost:2136/local"

	// Создаем драйвер для подключения к YDB через gRPC
	db, err := ydb.Open(ctx, dsn)
	if (err != nil) {
		log.Fatal(err)
	}
	defer db.Close(ctx)

	// Обращаемся к клиенту для выполнения SQL-запросов
	err = db.Query().Do(
		ctx,
		func(ctx context.Context, s query.Session) error {
			// Выполняем запрос с автоматическими повторными попытками при ошибках
			streamResult, err := s.Query(ctx, `SELECT 1 as id;`)

			if err != nil {
				return err;
			}

			defer func() { _ = streamResult.Close(ctx) }()

			// Итерируемся по набору результатов
			for rs, err := range streamResult.ResultSets(ctx) {
				if err != nil {
					return err
				}

				// Итерируемся по строкам в каждом наборе результате
				for row, err := range rs.Rows(ctx) {
					if err != nil {
						return err
					}

					type myStruct struct {
						Id int32 `sql:"id"`
					}
					var s myStruct;

					// Материализуем результат
					if err = row.ScanStruct(&s); err != nil {
						return err;
					}

					println(s.Id)
				}
			}
			return nil
		},
		query.WithIdempotent(),
	)
	
	if err != nil {
		log.Fatal(err)
	}
}
