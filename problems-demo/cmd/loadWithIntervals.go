/*
Copyright © 2025 NAME HERE <EMAIL ADDRESS>
*/
package cmd

import (
	"context"
	"log"
	"math/rand/v2"
	"time"

	"github.com/spf13/cobra"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
)

// loadWithIntervalsCmd represents the loadWithIntervals command
var loadWithIntervalsCmd = &cobra.Command{
	Use:   "loadWithIntervals",
	Short: "Создаёт рваную нагрузку - то есть, то - нет",
	Long:  ``,
	Run: func(cmd *cobra.Command, args []string) {

		const loadInterval = time.Minute
		const pauseInterval = time.Minute

		ctx := context.Background()
		db := dbConnect()

		if err := db.Query().Exec(ctx, `DROP TABLE IF EXISTS loadWithIntervals`); err != nil {
			log.Fatalf("Ошибка при удалении таблицы: %v", err)
		}

		if err := db.Query().Exec(ctx, `
CREATE TABLE loadWithIntervals (
	id Int64 NOT NULL,
	val Int64,
	PRIMARY KEY (id)
)
`); err != nil {
			log.Fatalf("Ошибка при создании таблицы: %v", err)
		}

		for {
			log.Println("Нагружаю...")

			start := time.Now()
			for time.Since(start) < loadInterval {
				db.Query().Exec(ctx, `
DECLARE $id Int64;
DECLARE $val Int64;

UPSERT INTO loadWithIntervals VALUES ($id, $val);
`, query.WithParameters(ydb.ParamsFromMap(map[string]any{
					"id":  1,
					"val": rand.Int64(),
				})))
			}

			log.Println("Пауза")
			time.Sleep(pauseInterval)
		}
	},
}

func init() {
	rootCmd.AddCommand(loadWithIntervalsCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// loadWithIntervalsCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// loadWithIntervalsCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
