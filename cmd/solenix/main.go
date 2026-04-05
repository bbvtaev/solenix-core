package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "solenix",
	Short: "Solenix — lightweight time-series database",
}

// --addr флаг доступен во всех subcommand-ах кроме serve
var serverAddr string

func init() {
	rootCmd.PersistentFlags().StringVar(&serverAddr, "addr", "localhost:50051", "solenix-core gRPC address")
	rootCmd.AddCommand(serveCmd, writeCmd, queryCmd, healthCmd, metricsCmd)
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
