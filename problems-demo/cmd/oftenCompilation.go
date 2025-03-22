/*
Copyright © 2025 NAME HERE <EMAIL ADDRESS>
*/
package cmd

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/spf13/cobra"
)

// oftenCompilationCmd represents the oftenCompilation command
var oftenCompilationCmd = &cobra.Command{
	Use:   "oftenCompilation",
	Short: "Эта команда отправляет на сервер много разных по тексту запросов",
	Long:  ``,
	Run: func(cmd *cobra.Command, args []string) {
		ctx := context.Background()
		db := dbConnect()

		i := int64(0)
		for range 4 {
			go func() {
				for {
					q := "SELECT " + strconv.FormatInt(atomic.AddInt64(&i, 1), 10)
					_, err := db.Query().QueryRow(ctx, q)
					if err != nil {
						log.Printf("query error: %v", err)
						time.Sleep(time.Second)
					}
				}
			}()
		}

		for {
			time.Sleep(time.Second)
			fmt.Println("Выполнено запросов:", i)
		}
	},
}

func init() {
	rootCmd.AddCommand(oftenCompilationCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// oftenCompilationCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// oftenCompilationCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
