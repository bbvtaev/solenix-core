package storage

import (
	"errors"
	"fmt"
	"math"
	"sort"
	"time"

	"github.com/bbvtaev/solenix/internal/chunk"
	"github.com/bbvtaev/solenix/internal/model"
)

// Query возвращает серии по метрике и лейблам в диапазоне [from, to].
// from и to — Unix nanoseconds. 0 означает отсутствие ограничения.
// Если opts != nil и opts.Window > 0, точки агрегируются по временным окнам.
func (db *DB) Query(metric string, labels map[string]string, from, to int64, opts *model.QueryOptions) ([]model.SeriesResult, error) {
	if metric == "" {
		return nil, errors.New("metric is required")
	}

	cold, err := chunk.QueryChunks(db.chunksDir, metric, labels, from, to)
	if err != nil {
		return nil, fmt.Errorf("query chunks: %w", err)
	}

	hot := db.queryMemory(metric, labels, from, to)

	var raw []model.SeriesResult
	if len(cold) == 0 {
		raw = hot
	} else if len(hot) == 0 {
		raw = cold
	} else {
		raw = mergeQueryResults(cold, hot)
	}

	if opts == nil || opts.Window == 0 {
		return raw, nil
	}

	results := make([]model.SeriesResult, 0, len(raw))
	for _, s := range raw {
		pts := aggregatePoints(s.Points, from, opts.Window, opts.Agg)
		if len(pts) == 0 {
			continue
		}
		results = append(results, model.SeriesResult{Metric: s.Metric, Labels: s.Labels, Points: pts})
	}
	return results, nil
}

// queryMemory читает только горячий буфер (точки, не сброшенные в chunks).
func (db *DB) queryMemory(metric string, labels map[string]string, from, to int64) []model.SeriesResult {
	ids := db.metricIdx.lookup(metric)
	if len(ids) == 0 {
		return nil
	}

	// Не возвращаем точки, уже записанные в chunks — они придут из cold-слоя
	effectiveFrom := from
	if ft := db.flushedUpTo.Load(); ft > 0 && (from == 0 || ft+1 > from) {
		effectiveFrom = ft + 1
	}

	var res []model.SeriesResult
	for _, id := range ids {
		sh := db.shardFor(id)
		sh.mu.RLock()
		ser, ok := sh.series[id]
		if !ok {
			sh.mu.RUnlock()
			continue
		}
		if !labelsMatch(labels, ser.labels) {
			sh.mu.RUnlock()
			continue
		}
		points := filterPoints(ser.points, effectiveFrom, to)
		lbls := cloneLabels(ser.labels)
		met := ser.metric
		sh.mu.RUnlock()

		if len(points) == 0 {
			continue
		}
		res = append(res, model.SeriesResult{Metric: met, Labels: lbls, Points: points})
	}
	return res
}

// mergeQueryResults объединяет cold (chunks) и hot (память) результаты.
// Для одной и той же серии cold-точки предшествуют hot-точкам, дублей нет.
func mergeQueryResults(cold, hot []model.SeriesResult) []model.SeriesResult {
	hotByID := make(map[uint64]*model.SeriesResult, len(hot))
	for i := range hot {
		id := model.HashSeries(hot[i].Metric, hot[i].Labels)
		hotByID[id] = &hot[i]
	}

	result := make([]model.SeriesResult, 0, len(cold)+len(hot))
	usedHot := make(map[uint64]bool, len(hot))

	for _, c := range cold {
		id := model.HashSeries(c.Metric, c.Labels)
		if h, ok := hotByID[id]; ok {
			combined := make([]model.Point, len(c.Points)+len(h.Points))
			copy(combined, c.Points)
			copy(combined[len(c.Points):], h.Points)
			result = append(result, model.SeriesResult{Metric: c.Metric, Labels: c.Labels, Points: combined})
			usedHot[id] = true
		} else {
			result = append(result, c)
		}
	}
	for i := range hot {
		id := model.HashSeries(hot[i].Metric, hot[i].Labels)
		if !usedHot[id] {
			result = append(result, hot[i])
		}
	}
	return result
}


func filterPoints(points []model.Point, from, to int64) []model.Point {
	if len(points) == 0 {
		return nil
	}

	start := 0
	if from > 0 {
		start = sort.Search(len(points), func(i int) bool {
			return points[i].Timestamp >= from
		})
	}

	end := len(points)
	if to > 0 {
		end = sort.Search(len(points), func(i int) bool {
			return points[i].Timestamp > to
		})
	}

	if start >= end {
		return nil
	}

	out := make([]model.Point, end-start)
	copy(out, points[start:end])
	return out
}

func aggregatePoints(points []model.Point, from int64, window time.Duration, agg model.AggType) []model.Point {
	if len(points) == 0 || window <= 0 {
		return nil
	}

	winNs := window.Nanoseconds()
	base := from
	if base == 0 {
		base = points[0].Timestamp
	}
	bucketStart := (base / winNs) * winNs

	last := points[len(points)-1].Timestamp
	var result []model.Point

	for bucketStart <= last {
		bucketEnd := bucketStart + winNs

		lo := sort.Search(len(points), func(i int) bool { return points[i].Timestamp >= bucketStart })
		hi := sort.Search(len(points), func(i int) bool { return points[i].Timestamp >= bucketEnd })

		if lo < hi {
			vals := make([]float64, hi-lo)
			for i, p := range points[lo:hi] {
				vals[i] = p.Value
			}
			result = append(result, model.Point{
				Timestamp: bucketStart,
				Value:     applyAgg(vals, agg),
			})
		}
		bucketStart = bucketEnd
	}

	return result
}

func applyAgg(vals []float64, agg model.AggType) float64 {
	switch agg {
	case model.AggAvg:
		sum := 0.0
		for _, v := range vals {
			sum += v
		}
		return sum / float64(len(vals))
	case model.AggMin:
		m := math.MaxFloat64
		for _, v := range vals {
			if v < m {
				m = v
			}
		}
		return m
	case model.AggMax:
		m := -math.MaxFloat64
		for _, v := range vals {
			if v > m {
				m = v
			}
		}
		return m
	case model.AggSum:
		sum := 0.0
		for _, v := range vals {
			sum += v
		}
		return sum
	case model.AggCount:
		return float64(len(vals))
	default:
		return 0
	}
}
