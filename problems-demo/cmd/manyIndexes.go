/*
Copyright © 2025 NAME HERE <EMAIL ADDRESS>
*/
package cmd

import (
	"context"
	"log"
	"math/rand/v2"

	"github.com/spf13/cobra"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
)

// manyIndexesCmd represents the manyIndexes command
var manyIndexesCmd = &cobra.Command{
	Use:   "manyIndexes",
	Short: "Создаёт таблицу с большим количеством индексов и добавляет туда строки",
	Long:  ``,
	Run: func(cmd *cobra.Command, args []string) {
		ctx := context.Background()
		db := dbConnect()

		if err := db.Query().Exec(ctx, "DROP TABLE IF EXISTS many_indexes;"); err != nil {
			log.Fatalf("Ошибка при удалении таблицы: %v", err)
		}

		if err := db.Query().Exec(ctx, `
CREATE TABLE many_indexes (
	id Int64 NOT NULL,
	val1 Int64,
	val2 Int64,
	val3 Int64,
	PRIMARY KEY (id),
	INDEX i1 GLOBAL ON (val1),
	INDEX i12 GLOBAL ON (val1, val2),
	INDEX i13 GLOBAL ON (val1, val3),
	INDEX i123 GLOBAL ON (val1, val2, val3),
	INDEX i132 GLOBAL ON (val1, val3, val2),
	INDEX i2 GLOBAL ON (val2),
	INDEX i21 GLOBAL ON (val2, val1),
	INDEX i213 GLOBAL ON (val2, val1, val3),
	INDEX i23 GLOBAL ON (val2, val3),
	INDEX i231 GLOBAL ON (val2, val3, val1),
	INDEX i3 GLOBAL ON (val3),
	INDEX i31 GLOBAL ON (val3, val1),
	INDEX i312 GLOBAL ON (val3, val1, val2),
	INDEX i32 GLOBAL ON (val3, val2),
	INDEX i321 GLOBAL ON (val3, val2, val1),
) 
`); err != nil {
			log.Fatalf("Ошибка при создании таблицы: %v", err)
		}

		log.Println("Начинаю заполнять строки")
		for i := 0; ; i++ {
			err := db.Query().Exec(ctx, `
DECLARE $id AS Int64;
DECLARE $val1 AS Int64;
DECLARE $val2 AS Int64;
DECLARE $val3 AS Int64;

UPSERT INTO many_indexes (id, val1, val2, val3)
VALUES ($id, $val1, $val2, $val3);
`, query.WithParameters(ydb.ParamsFromMap(map[string]any{
				"$id":   rand.Int64(),
				"$val1": rand.Int64(),
				"$val2": rand.Int64(),
				"$val3": rand.Int64(),
				"$val4": rand.Int64(),
			})))
			if err != nil {
				log.Fatalf("Ошибка при добавлении строки: %v", err)
			}
		}
	},
}

func init() {
	rootCmd.AddCommand(manyIndexesCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// manyIndexesCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// manyIndexesCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
