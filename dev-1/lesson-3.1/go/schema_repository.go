package main

import (
	"context"
	"log"
)

// Репозиторий для управления схемой базы данных YDB
// Отвечает за создание и удаление таблиц
type SchemaRepository struct {
	query *QueryHelper
}

func NewSchemaRepository(query *QueryHelper) *SchemaRepository {
	return &SchemaRepository{
		query: query,
	}
}

// Создает таблицу issues в базе данных
// Таблица содержит поля:
// - id: уникальный идентификатор тикета
// - title: название тикета
// - created_at: время создания тикета
// Все поля являются обязательными.
func (repo *SchemaRepository) CreateSchema(ctx context.Context) {
	err := repo.query.Execute(`
		CREATE TABLE IF NOT EXISTS issues (
			id Int64 NOT NULL,
			title Text NOT NULL,
			created_at Timestamp NOT NULL,
			PRIMARY KEY (id)
		);
		`,
		ctx,
	)
	if err != nil {
		log.Fatal(err)
	}
}

// Удаляет таблицу issues из базы данных
// Используется для очистки схемы перед созданием новой
func (repo *SchemaRepository) DropSchema(ctx context.Context) {
	err := repo.query.Execute("DROP TABLE IF EXISTS issues;", ctx)
	if err != nil {
		log.Fatal(err)
	}
}
