package main

import (
	"fmt"
	"strings"
	"time"

	"github.com/bbvtaev/solenix-core/internal/queryparser"
	sdk "github.com/bbvtaev/solenix-core/sdk/go"
	"github.com/spf13/cobra"
)

var queryCmd = &cobra.Command{
	Use:   "query <metric>",
	Short: "Query data points",
	Example: `  solenix query cpu.usage
  solenix query cpu.usage --from 1h
  solenix query cpu.usage --from 30m --to 10m --labels host=srv1`,
	Args: cobra.ExactArgs(1),
	RunE: runQuery,
}

var (
	queryFrom   string
	queryTo     string
	queryLabels []string
	queryLimit  int
)

func init() {
	queryCmd.Flags().StringVar(&queryFrom, "from", "", "start time: unix nanoseconds or duration ago (e.g. 1h, 30m, 7d)")
	queryCmd.Flags().StringVar(&queryTo, "to", "", "end time: unix nanoseconds or duration ago (e.g. 10m)")
	queryCmd.Flags().StringArrayVar(&queryLabels, "labels", nil, "filter labels in key=value format")
	queryCmd.Flags().IntVar(&queryLimit, "limit", 0, "max points to display per series (0 = all)")
}

func runQuery(_ *cobra.Command, args []string) error {
	var metric string
	var labels map[string]string
	var from, to int64

	if strings.ContainsAny(args[0], "{[") {
		q, err := queryparser.Parse(args[0])
		if err != nil {
			return err
		}
		metric, labels, from, to = q.Metric, q.Labels, q.From, q.To
	} else {
		var err error
		metric = args[0]
		from, err = parseTime(queryFrom)
		if err != nil {
			return fmt.Errorf("--from: %w", err)
		}
		to, err = parseTime(queryTo)
		if err != nil {
			return fmt.Errorf("--to: %w", err)
		}
		labels, err = parseLabels(queryLabels)
		if err != nil {
			return err
		}
	}

	client, err := sdk.NewClient(serverAddr)
	if err != nil {
		return err
	}
	defer client.Close()

	results, err := client.Query(metric, labels, from, to)
	if err != nil {
		return err
	}

	if len(results) == 0 {
		fmt.Println("no data")
		return nil
	}

	for idx, s := range results {
		labelStr := formatLabels(s.Labels)
		if labelStr != "" {
			fmt.Printf("[%d] %s{%s}  (%d points)\n", idx+1, s.Metric, labelStr, len(s.Points))
		} else {
			fmt.Printf("[%d] %s  (%d points)\n", idx+1, s.Metric, len(s.Points))
		}
		points := s.Points
		truncated := 0
		if queryLimit > 0 && len(points) > queryLimit {
			truncated = len(points) - queryLimit
			points = points[len(points)-queryLimit:]
		}

		numWidth := len(fmt.Sprintf("%d", len(s.Points)))
		fmt.Printf("    %-*s  %-26s  %s\n", numWidth, "#", "time", "value")
		fmt.Printf("    %-*s  %-26s  %s\n", numWidth, strings.Repeat("─", numWidth), strings.Repeat("─", 26), strings.Repeat("─", 16))
		for i, p := range points {
			ts := time.Unix(0, p.Timestamp).Format("2006-01-02 15:04:05.000")
			fmt.Printf("    %-*d  %-26s  %g\n", numWidth, i+1, ts, p.Value)
		}
		if truncated > 0 {
			fmt.Printf("\n    %d earlier points not shown (use --limit to adjust)\n", truncated)
		}
		fmt.Println()
	}

	return nil
}

func parseTime(s string) (int64, error) {
	if s == "" {
		return 0, nil
	}

	var n int64
	if _, err := fmt.Sscan(s, &n); err == nil {
		return n, nil
	}

	// Попробовать как duration (1h, 30m, 7d)
	d, err := parseDuration(s)
	if err != nil {
		return 0, fmt.Errorf("expected duration (1h, 30m, 7d) or unix nanoseconds, got %q", s)
	}
	return time.Now().Add(-d).UnixNano(), nil
}

// parseDuration расширяет стандартный time.ParseDuration добавляя "d" (days).
func parseDuration(s string) (time.Duration, error) {
	if strings.HasSuffix(s, "d") {
		var n int
		if _, err := fmt.Sscan(s[:len(s)-1], &n); err != nil {
			return 0, fmt.Errorf("invalid duration %q", s)
		}
		return time.Duration(n) * 24 * time.Hour, nil
	}
	return time.ParseDuration(s)
}
