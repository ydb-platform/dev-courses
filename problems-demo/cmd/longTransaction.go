/*
Copyright © 2025 NAME HERE <EMAIL ADDRESS>
*/
package cmd

import (
	"context"
	"log"

	"github.com/spf13/cobra"
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
			go func() {
				db.Query().Do(ctx, func(ctx context.Context, s query.Session) error {
					// тут я создаю транзакцию явно вместо использования DoTx
					// чтобы иметь возможность явно получить ошибку и вывести её в лог
					tx, err := s.Begin(ctx, query.TxSettings())
					if err != nil {
						return err
					}

					tx.QueryRow()
				})
			}()
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
