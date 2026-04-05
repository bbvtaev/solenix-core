package main

import (
	"fmt"

	sdk "github.com/bbvtaev/solenix-core/sdk/go"
	"github.com/spf13/cobra"
)

var healthCmd = &cobra.Command{
	Use:   "health",
	Short: "Check server health",
	RunE:  runHealth,
}

func runHealth(_ *cobra.Command, _ []string) error {
	client, err := sdk.NewClient(serverAddr)
	if err != nil {
		return err
	}
	defer client.Close()

	status, version, err := client.Health()
	if err != nil {
		return fmt.Errorf("server unreachable: %w", err)
	}

	fmt.Printf("status=%s version=%s addr=%s\n", status, version, serverAddr)
	return nil
}
