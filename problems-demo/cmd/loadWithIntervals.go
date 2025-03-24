/*
Copyright © 2025 NAME HERE <EMAIL ADDRESS>
*/
package cmd

import (
	"context"
	"log"
	"time"

	"github.com/spf13/cobra"
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

		for {
			log.Println("Нагружаю...")

			start := time.Now()
			for time.Since(start) < loadInterval {
				_ = db.Query().Exec(ctx, `SELECT 1`)
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
