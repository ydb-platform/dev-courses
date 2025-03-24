/*
Copyright © 2025 NAME HERE <EMAIL ADDRESS>
*/
package cmd

import (
	"context"
	"log"
	"math/rand/v2"
	"path"

	"github.com/spf13/cobra"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
)

// wholeShardLocksCmd represents the wholeShardLocks command
var wholeShardLocksCmd = &cobra.Command{
	Use:   "wholeShardLocks",
	Short: "Демонстрирует лок партиций целиком",
	Long:  ``,
	Run: func(cmd *cobra.Command, args []string) {
		ctx := context.Background()
		db := dbConnect()

		if err := db.Query().Exec(ctx, "DROP TABLE IF EXISTS wholeShardLocks"); err != nil {
			log.Fatalf("Ошибка при удалении старой таблицы: %v", err)
		}

		if err := db.Query().Exec(ctx, `
CREATE TABLE wholeShardLocks (
	id Int64 NOT NULL,
	val Int64,
	PRIMARY KEY(id)
)
`); err != nil {
			log.Fatalf("Ошибка при создании таблицы: %v", err)
		}

		log.Println("Заполняю таблицу данными")

		var list []types.Value

		for i := int64(0); i < 200000; i++ {
			val := types.StructValue(
				types.StructFieldValue("id", types.Int64Value(i)),
				types.StructFieldValue("val", types.Int64Value(i*100)),
			)
			list = append(list, val)
		}

		tableName := path.Join(db.Name(), "wholeShardLocks")
		if err := db.Table().BulkUpsert(ctx, tableName, table.BulkUpsertDataRows(types.ListValue(list...))); err != nil {
			log.Fatalf("Ошибка при наполнеии таблицы данными: %v", err)
		}

		log.Printf("Начинаю запросы...")

		list = []types.Value{}

		for i := int64(0); i < 100001; i++ {
			val := types.StructValue(
				types.StructFieldValue("id", types.Int64Value(rand.Int64())),
			)
			list = append(list, val)
		}

		for {
			// выборка без условия - на время выполнения берёт локи на все шарды таблицы целиком
			err := db.Query().DoTx(ctx, func(ctx context.Context, tx query.TxActor) error {
				res, err := tx.QueryRow(ctx, `
DECLARE $args AS List<Struct<
	id: Int64,
>>;

SELECT sum(w.id) as val
FROM AS_TABLE($args) t
INNER JOIN wholeShardLocks w ON t.id = w.id
`, query.WithParameters(ydb.ParamsBuilder().Param("$args").Any(types.ListValue(list...)).Build()))
				if err != nil {
					return err
				}

				var val int64
				err = res.Scan(&val)

				return nil
			})

			if err != nil {
				log.Fatal("Ошибка при выполнении транзакции", err)
			}
		}
	},
}

func init() {
	rootCmd.AddCommand(wholeShardLocksCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// wholeShardLocksCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// wholeShardLocksCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
