package main

import (
	"fmt"
	"strconv"
	"strings"

	sdk "github.com/bbvtaev/solenix-core/sdk/go"
	"github.com/spf13/cobra"
)

var pushCmd = &cobra.Command{
	Use:   "push <metric> <value>",
	Short: "Push a single data point",
	Example: `  solenix push cpu.usage 72.5
  solenix push cpu.usage 72.5 --labels host=srv1,env=prod`,
	Args: cobra.ExactArgs(2),
	RunE: runWrite,
}

var writeLabels []string

func init() {
	pushCmd.Flags().StringArrayVar(&writeLabels, "labels", nil, "labels in key=value format (repeatable or comma-separated)")
}

func runWrite(_ *cobra.Command, args []string) error {
	metric := args[0]
	value, err := strconv.ParseFloat(args[1], 64)
	if err != nil {
		return fmt.Errorf("invalid value %q: %w", args[1], err)
	}

	labels, err := parseLabels(writeLabels)
	if err != nil {
		return err
	}

	client, err := sdk.NewClient(serverAddr)
	if err != nil {
		return err
	}
	defer client.Close()

	if err := client.Push(metric, labels, value); err != nil {
		return err
	}

	fmt.Printf("written: %s{%s} = %g\n", metric, formatLabels(labels), value)
	return nil
}

// parseLabels разбирает ["host=srv1,env=prod"] или ["host=srv1", "env=prod"] → map.
func parseLabels(raw []string) (map[string]string, error) {
	result := make(map[string]string)
	for _, s := range raw {
		for _, pair := range strings.Split(s, ",") {
			pair = strings.TrimSpace(pair)
			if pair == "" {
				continue
			}
			k, v, ok := strings.Cut(pair, "=")
			if !ok {
				return nil, fmt.Errorf("invalid label %q, expected key=value", pair)
			}
			result[k] = v
		}
	}
	return result, nil
}

func formatLabels(labels map[string]string) string {
	if len(labels) == 0 {
		return ""
	}
	parts := make([]string, 0, len(labels))
	for k, v := range labels {
		parts = append(parts, k+"="+v)
	}
	return strings.Join(parts, ", ")
}
