package main

import (
	"fmt"

	sdk "github.com/bbvtaev/solenix-core/sdk/go"
	"github.com/spf13/cobra"
)

var metricsCmd = &cobra.Command{
	Use:   "metrics",
	Short: "List all metrics stored in the database",
	RunE:  runMetrics,
}

func runMetrics(_ *cobra.Command, _ []string) error {
	client, err := sdk.NewClient(serverAddr)
	if err != nil {
		return err
	}
	defer client.Close()

	metrics, err := client.Metrics()
	if err != nil {
		return err
	}

	if len(metrics) == 0 {
		fmt.Println("no metrics")
		return nil
	}

	for _, m := range metrics {
		fmt.Println(m)
	}
	return nil
}
