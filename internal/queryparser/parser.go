// Package queryparser разбирает строки запросов вида:
//
//	cpu.usage{host="srv1", env="prod"}[1h]
//	cpu.usage{host="srv1"}
//	cpu.usage[30m]
//	cpu.usage
package queryparser

import (
	"fmt"
	"strings"
	"time"
)

// Query — результат парсинга строки запроса.
type Query struct {
	Metric string
	Labels map[string]string
	From   int64 // Unix nanoseconds, 0 = без ограничения
	To     int64 // Unix nanoseconds, 0 = без ограничения (всегда now если есть From)
}

// Parse разбирает строку запроса и возвращает Query.
func Parse(s string) (Query, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return Query{}, fmt.Errorf("empty query")
	}

	q := Query{}

	// Извлекаем временной диапазон [1h] в конце
	s, err := parseDuration(s, &q)
	if err != nil {
		return Query{}, err
	}

	// Незакрытая фигурная скобка — ошибка
	if strings.Contains(s, "{") && !strings.Contains(s, "}") {
		return Query{}, fmt.Errorf("unmatched '{'")
	}

	// Извлекаем лейблы {key="value", ...}
	s, err = parseLabels(s, &q)
	if err != nil {
		return Query{}, err
	}

	// Остаток — имя метрики
	metric := strings.TrimSpace(s)
	if metric == "" {
		return Query{}, fmt.Errorf("metric name is required")
	}
	q.Metric = metric

	return q, nil
}

// parseDuration ищет [duration] в конце строки.
func parseDuration(s string, q *Query) (string, error) {
	if !strings.HasSuffix(s, "]") {
		return s, nil
	}

	open := strings.LastIndex(s, "[")
	if open == -1 {
		return "", fmt.Errorf("unmatched ']'")
	}

	durStr := s[open+1 : len(s)-1]
	d, err := time.ParseDuration(durStr)
	if err != nil {
		return "", fmt.Errorf("invalid duration %q: %w", durStr, err)
	}

	q.From = time.Now().Add(-d).UnixNano()
	q.To = 0 // без верхней границы — "до сейчас"

	return s[:open], nil
}

// parseLabels ищет {key="value", ...} в конце строки.
func parseLabels(s string, q *Query) (string, error) {
	if !strings.HasSuffix(s, "}") {
		return s, nil
	}

	open := strings.LastIndex(s, "{")
	if open == -1 {
		return "", fmt.Errorf("unmatched '}'")
	}

	inner := s[open+1 : len(s)-1]
	labels, err := parseLabelsInner(inner)
	if err != nil {
		return "", err
	}
	q.Labels = labels

	return s[:open], nil
}

// parseLabelsInner разбирает содержимое фигурных скобок.
func parseLabelsInner(s string) (map[string]string, error) {
	labels := make(map[string]string)
	s = strings.TrimSpace(s)
	if s == "" {
		return labels, nil
	}

	for _, pair := range strings.Split(s, ",") {
		pair = strings.TrimSpace(pair)
		if pair == "" {
			continue
		}

		k, v, ok := strings.Cut(pair, "=")
		if !ok {
			return nil, fmt.Errorf("invalid label %q, expected key=\"value\"", pair)
		}

		k = strings.TrimSpace(k)
		v = strings.TrimSpace(v)
		v = strings.Trim(v, `"`)

		if k == "" {
			return nil, fmt.Errorf("empty label key in %q", pair)
		}

		labels[k] = v
	}

	return labels, nil
}