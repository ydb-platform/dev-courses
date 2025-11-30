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

// Репозиторий для работы с тикетами в базе данных YDB
// Реализует операции добавления и чтения тикетов
type IssueRepository struct {
	query *QueryHelper
}

func NewIssueRepository(query *QueryHelper) *IssueRepository {
	return &IssueRepository{
		query: query,
	}
}

// Добавление нового тикета в БД
// ctx   [context.Context] - контекст для управления исполнением запроса (например, можно задать таймаут)
// title [string] - название тикета
// Возвращает созданный тикет или ошибку
func (repo *IssueRepository) AddIssue(ctx context.Context, title string) (*Issue, error) {
	// Генерируем случайный id для тикета
	id := random.Int63() // do not repeat in production
	timestamp := time.Now()

	// Выполняем UPSERT запрос для добавления тикета
	// Изменять данные можно только в режиме транзакции SERIALIZABLE_RW, поэтому используем его
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

// Возвращает тикет по заданному id
// ctx [context.Context] - контекст для управления исполнением запроса (например, можно задать таймаут)
// id  [int64] - id тикета
// Возвращает найденный тикет или ошибку
func (repo *IssueRepository) FindById(ctx context.Context, id int64) (*Issue, error) {
	result := make([]Issue, 0)

	// Выполняем SELECT запрос в режиме [Snapshot Read-Only] для чтения данных
    // Этот режим сообщает серверу, что эта транзакция только для чтения.
    // Это позволяет снизить накладные расходы на подготовку к изменениям 
	// и просто читать данные из одного "слепка" базы данных.
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
				// Если во время чтения результата возникла ошибка,
				// то необходимо очистить уже прочитанные результаты, 
				// чтобы избежать дублирования при следующем выполнении функции-ретраера
				// и вернуть ретраеру ошибку
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
		return nil, errors.New("Multiple rows with the same id (lol)")
	}
	if len(result) == 0 {
		return nil, errors.New("Did not find any issues")
	}
	return &result[0], nil
}

// Получает все тикеты из базы данных
// ctx [context.Context] - контекст для управления исполнением запроса (например, можно задать таймаут)
func (repo *IssueRepository) FindAll(ctx context.Context) ([]Issue, error) {
	result := make([]Issue, 0)

	// Выполняем SELECT запрос в режиме [Snapshot Read-Only] для чтения данных
    // Этот режим сообщает серверу, что эта транзакция только для чтения.
    // Это позволяет снизить накладные расходы на подготовку к изменениям 
	// и просто читать данные из одного "слепка" базы данных.
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
				// Если во время чтения результата возникла ошибка,
				// то необходимо очистить уже прочитанные результаты, 
				// чтобы избежать дублирования при следующем выполнении функции-ретраера
				// и вернуть ретраеру ошибку
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
