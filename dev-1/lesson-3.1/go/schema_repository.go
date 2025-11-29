package main

import (
	"log"
)

type SchemaRepository struct {
	query *QueryHelper
}

func NewSchemaRepository(query *QueryHelper) *SchemaRepository {
	return &SchemaRepository{
		query: query,
	}
}

func (repo *SchemaRepository) CreateSchema() {
	err := repo.query.Execute(`
		CREATE TABLE IF NOT EXISTS issues (
			id Int64 NOT NULL,
			title Text NOT NULL,
			created_at Timestamp NOT NULL,
			PRIMARY KEY (id)
		);
		`,
	)
	if err != nil {
		log.Fatal(err)
	}
}

func (repo *SchemaRepository) DropSchema() {
	err := repo.query.Execute("DROP TABLE IF EXISTS issues;")
	if err != nil {
		log.Fatal(err)
	}
}
