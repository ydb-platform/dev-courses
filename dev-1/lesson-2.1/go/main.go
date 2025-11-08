package main

import (
	"context"
	"log"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3"
)

// author: Egor Danilov
func main() {
	connectionCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Строка подключения к локальной базе данных YDB
	// Формат: grpc[s]://<хост>:<порт>/<путь к базе данных>
	dsn := "grpc://localhost:2136/local"

	// Создаем драйвер для подключения к YDB через gRPC
	db, err := ydb.Open(connectionCtx, dsn)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close(connectionCtx)

	type myStruct struct {
		Id int32 `sql:"id"`
	}
	var structId myStruct

	queryCtx, txCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer txCancel()

	// Ниже представлены примеры по обращению к клиенту для выполнения YQL запросов
	//
	// Query(), QueryResultSet() и QueryRow() являются хелперами для неинтерактивных транзакций
	// (т.е. когда транзакция завершается за одно обращение к серверу)
	// Ретраи тут происходят внутри, а наружу возвращается уже материализированный в памяти в результат,
	// Таймаутом транзакции можно управлять через контекст

	result, err := db.Query().Query(queryCtx, "SELECT 1 as id")
	if err != nil {
		log.Fatal(err)
	}

	rs, err := result.NextResultSet(queryCtx)
	if err != nil {
		log.Fatal(err)
	}

	row, err := rs.NextRow(connectionCtx)
	if err != nil {
		log.Fatal(err)
	}

	// Читаем результат
	if err = row.ScanStruct(&structId); err != nil {
		log.Fatal(err)
	}
	println(structId.Id)

	// ======
	
	rs, err = db.Query().QueryResultSet(queryCtx, "SELECT 2 as id")
	if err != nil {
		log.Fatal(err)
	}

	row, err = rs.NextRow(connectionCtx)
	if err != nil {
		log.Fatal(err)
	}

	// Читаем результат
	if err = row.ScanStruct(&structId); err != nil {
		log.Fatal(err)
	}
	println(structId.Id)

	// ======

	row, err = db.Query().QueryRow(queryCtx, "SELECT 3 as id")
	if err != nil {
		log.Fatal(err)
	}

	// Читаем результат
	if err = row.ScanStruct(&structId); err != nil {
		log.Fatal(err)
	}
	println(structId.Id)
}
