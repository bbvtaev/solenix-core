package queryparser

import (
	"testing"
	"time"
)

func TestParse(t *testing.T) {
	tests := []struct {
		input       string
		wantMetric  string
		wantLabels  map[string]string
		wantHasFrom bool
		wantErr     bool
	}{
		{
			input:      "cpu.usage",
			wantMetric: "cpu.usage",
		},
		{
			input:      `cpu.usage{host="srv1"}`,
			wantMetric: "cpu.usage",
			wantLabels: map[string]string{"host": "srv1"},
		},
		{
			input:       "cpu.usage[1h]",
			wantMetric:  "cpu.usage",
			wantHasFrom: true,
		},
		{
			input:       `cpu.usage{host="srv1", env="prod"}[30m]`,
			wantMetric:  "cpu.usage",
			wantLabels:  map[string]string{"host": "srv1", "env": "prod"},
			wantHasFrom: true,
		},
		{
			input:   "",
			wantErr: true,
		},
		{
			input:   `cpu.usage{host="srv1"`,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			q, err := Parse(tt.input)
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if q.Metric != tt.wantMetric {
				t.Errorf("metric: got %q, want %q", q.Metric, tt.wantMetric)
			}
			for k, v := range tt.wantLabels {
				if q.Labels[k] != v {
					t.Errorf("label[%s]: got %q, want %q", k, q.Labels[k], v)
				}
			}
			if tt.wantHasFrom && q.From == 0 {
				t.Error("expected From to be set")
			}
			if tt.wantHasFrom && q.From >= time.Now().UnixNano() {
				t.Error("From should be in the past")
			}
		})
	}
}