package main

import (
	"context"
	"errors"
	"io"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
)

type QueryHelper struct {
	driver *ydb.Driver
}

func NewQueryHelper(driver *ydb.Driver) *QueryHelper {
	return &QueryHelper{
		driver: driver,
	}
}

func (q *QueryHelper) Execute(yql string, ctx context.Context) error {
	return q.ExecuteTx(
		yql,
		ctx,
		query.NoTx(),
		ydb.ParamsBuilder().Build(),
	)
}

func (q *QueryHelper) ExecuteTx(
	yql string,
	ctx context.Context,
	txControl *query.TransactionControl,
	params ydb.Params,
) error {
	return q.driver.Query().Do(
		ctx,
		func(ctx context.Context, s query.Session) error {
			err := s.Exec(
				ctx,
				yql,
				query.WithTxControl(txControl),
				query.WithParameters(params),
			)
			return err
		},
	)
}

func (q *QueryHelper) Query(
	yql string,
	ctx context.Context,
	txControl *query.TransactionControl,
	params ydb.Params,
	materializeFunc func(query.ResultSet, context.Context) error,
) error {
	return q.driver.Query().Do(
		ctx,
		func(ctx context.Context, s query.Session) error {
			result, err := s.Query(
				ctx,
				yql,
				query.WithTxControl(txControl),
				query.WithParameters(params),
			)

			if err != nil {
				return err
			}

			defer func() { _ = result.Close(ctx) }()

			for {
				resultSet, err := result.NextResultSet(ctx)
				if err != nil {
					if errors.Is(err, io.EOF) {
						break
					}

					return err
				}

				materializeFunc(resultSet, ctx)
			}

			return nil
		},
	)
}
