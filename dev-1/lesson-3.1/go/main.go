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

	dsn := "grpc://localhost:2136/local"

	db, err := ydb.Open(connectionCtx, dsn)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close(connectionCtx)

	queryHelper := NewQueryHelper(db)

	schemaRepository := NewSchemaRepository(queryHelper)
	issuesRepository := NewIssueRepository(queryHelper)

	schemaRepository.DropSchema()
	schemaRepository.CreateSchema()

	firstIssue, err := issuesRepository.AddIssue("Ticket 1")
	if err != nil {
		log.Fatalf("Some error happened (1): %v\n", err)
	}

	_, err = issuesRepository.AddIssue("Ticket 2")
	if err != nil {
		log.Fatalf("Some error happened (2): %v\n", err)
	}

	_, err = issuesRepository.AddIssue("Ticket 3")
	if err != nil {
		log.Fatalf("Some error happened (3): %v\n", err)
	}

	issues, err := issuesRepository.FindAll()
	if err != nil {
		log.Fatalf("Some error happened while finding all: %v\n", err)
	}
	for _, issue := range issues {
		log.Printf("Issue: %v\n", issue)
	}

	searchFirstIssue, err := issuesRepository.FindById(firstIssue.Id)
	if err != nil {
		log.Fatal(err)
	} else {
		log.Printf("First issue: %v\n", searchFirstIssue)
	}
}
