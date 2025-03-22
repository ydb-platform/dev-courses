/*
Copyright © 2025 NAME HERE <EMAIL ADDRESS>
*/
package cmd

import (
	"context"
	"log"
	"time"

	"github.com/spf13/cobra"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
)

// longTransactionCmd represents the longTransaction command
var longTransactionCmd = &cobra.Command{
	Use:   "longTransaction",
	Short: "Демонстрирует длительные конфликтующие транзакции",
	Long:  ``,
	Run: func(cmd *cobra.Command, args []string) {
		ctx := context.Background()
		db := dbConnect()

		_ = db.Query().Exec(ctx, "DROP TABLE IF EXISTS t")
		if err := db.Query().Exec(ctx, "CREATE TABLE t (id Int64, val Int64, PRIMARY KEY(id));"); err != nil {
			log.Fatal("Ошибка при создании таблицы", err)
		}

		if err := db.Query().Exec(ctx, "INSERT INTO t (id, val) VALUES (1, 0)"); err != nil {
			log.Fatal("Ошибка при добавлении начальной строки таблицы", err)
		}

		i := 0
		for {
			i++
			goroutineNum := i
			go func() {
				attempt := 0
				err := db.Query().Do(ctx, func(ctx context.Context, s query.Session) error {
					attempt++

					// тут я создаю транзакцию явно вместо использования DoTx
					// чтобы иметь возможность явно получить ошибку и вывести её в лог
					tx, err := s.Begin(ctx, query.TxSettings(query.WithDefaultTxMode()))
					if err != nil {
						return err
					}

					res, err := tx.QueryRow(ctx, "SELECT val FROM t WHERE id = 1")
					if err != nil {
						return err
					}

					var old int64
					if err = res.Scan(&old); err != nil {
						return err
					}

					time.Sleep(2 * time.Second)
					newVal := old + 1
					err = tx.Exec(ctx, `
DECLARE $val AS Int64;

UPSERT INTO t (id, val) VALUES (1, $val)
`, query.WithParameters(
						ydb.ParamsFromMap(map[string]any{"$val": newVal}),
					), query.WithCommit())
					var updateResult = "OK"
					if err != nil {
						// Сообщения об ошибках очень длинные и подробные - для диагностики
						// поэтому интересующую в демонстрации ошибку я сокращу - для наглядности
						if ydb.IsOperationErrorTransactionLocksInvalidated(err) {
							updateResult = "TLI"
						} else {
							updateResult = err.Error()
						}
					}
					log.Printf("goroutine: %v, val:%v, result: %v", goroutineNum, newVal, updateResult)
					return err
				})
				log.Printf("retry completed for goroutine: %v, attempts: %v, result: %v", goroutineNum, attempt, err)
			}()
			time.Sleep(time.Second)
		}
	},
}

func init() {
	rootCmd.AddCommand(longTransactionCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// longTransactionCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// longTransactionCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
