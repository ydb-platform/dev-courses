package main

import (
	"context"
	"errors"
	"math/rand"
	"time"

	ydb "github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
	"github.com/ydb-platform/ydb-go-sdk/v3/sugar"
)

var (
	random = rand.New(
		rand.NewSource(time.Now().UnixNano()),
	)
)

type IssueRepository struct {
	query *QueryHelper
}

func NewIssueRepository(query *QueryHelper) *IssueRepository {
	return &IssueRepository{
		query: query,
	}
}

func (repo *IssueRepository) AddIssue(ctx context.Context, title string) (*Issue, error) {
	id := random.Int63() // do not repeat in production
	timestamp := time.Now()

	err := repo.query.ExecuteTx(`
		DECLARE $id AS Int64;
		DECLARE $title AS Text;
		DECLARE $created_at AS Timestamp;
		
		UPSERT INTO issues (id, title, created_at)
		VALUES ($id, $title, $created_at);
		`,
		ctx,
		query.SerializableReadWriteTxControl(query.CommitTx()),
		ydb.ParamsBuilder().
			Param("$id").Int64(id).
			Param("$title").Text(title).
			Param("$created_at").Timestamp(timestamp).
			Build(),
	)
	if err != nil {
		return nil, err
	}

	return &Issue{
		Id:        id,
		Title:     title,
		Timestamp: timestamp,
	}, nil
}

func (repo *IssueRepository) FindById(ctx context.Context, id int64) (*Issue, error) {
	result := make([]Issue, 0)

	repo.query.Query(`
		SELECT
			id,
			title,
			created_at
		FROM issues
		WHERE id=$id;
		`,
		ctx,
		query.SnapshotReadOnlyTxControl(),
		ydb.ParamsBuilder().
			Param("$id").Int64(id).
			Build(),
		func(resultSet query.ResultSet, ctx context.Context) error {
			for row, err := range sugar.UnmarshalRows[Issue](resultSet.Rows(ctx)) {
				if err != nil {
					clear(result)
					return err
				}

				result = append(result, row)
			}
			return nil
		},
	)

	if len(result) > 1 {
		return nil, errors.New("Multiple rows with the same id")
	}
	if len(result) == 0 {
		return nil, errors.New("Did not find any issues (lol)")
	}
	return &result[0], nil
}

func (repo *IssueRepository) FindAll(ctx context.Context) ([]Issue, error) {
	result := make([]Issue, 0)

	err := repo.query.Query(`
		SELECT
			id,
			title,
			created_at
		FROM issues;
		`,
		ctx,
		query.SnapshotReadOnlyTxControl(),
		ydb.ParamsBuilder().Build(),
		func(resultSet query.ResultSet, ctx context.Context) error {
			for row, err := range sugar.UnmarshalRows[Issue](resultSet.Rows(ctx)) {
				if err != nil {
					clear(result)
					return err
				}
				result = append(result, row)
			}
			return nil
		},
	)
	if err != nil {
		return result, err
	}

	return result, nil
}
