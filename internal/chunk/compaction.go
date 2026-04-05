package chunk

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/bbvtaev/solenix-core/internal/model"
)

// Compact проверяет все поддиректории chunksDir и сливает файлы метрики
// в один если их количество превышает threshold.
func Compact(chunksDir string, threshold int) error {
	entries, err := os.ReadDir(chunksDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		metricDir := filepath.Join(chunksDir, entry.Name())
		if err := compactMetric(chunksDir, metricDir, threshold); err != nil {
			return fmt.Errorf("compact %s: %w", entry.Name(), err)
		}
	}
	return nil
}

// compactMetric сливает все chunk-файлы одной метрики в один если их > threshold.
func compactMetric(chunksDir, metricDir string, threshold int) error {
	files, err := chunkFiles(metricDir)
	if err != nil || len(files) <= threshold {
		return err
	}

	cr := reader{}
	seriesMap := make(map[uint64]*model.SeriesResult)
	metric := filepath.Base(metricDir)

	for _, path := range files {
		recs, err := cr.readFile(path)
		if err != nil {
			return fmt.Errorf("read %s: %w", path, err)
		}
		for _, rec := range recs {
			id := model.HashSeries(metric, rec.Labels)
			if sr, ok := seriesMap[id]; ok {
				sr.Points = append(sr.Points, rec.Points...)
			} else {
				pts := make([]model.Point, len(rec.Points))
				copy(pts, rec.Points)
				lbls := make(map[string]string, len(rec.Labels))
				for k, v := range rec.Labels {
					lbls[k] = v
				}
				seriesMap[id] = &model.SeriesResult{
					Metric: metric,
					Labels: lbls,
					Points: pts,
				}
			}
		}
	}

	// Сортируем точки каждой серии по timestamp
	for _, sr := range seriesMap {
		sort.Slice(sr.Points, func(i, j int) bool {
			return sr.Points[i].Timestamp < sr.Points[j].Timestamp
		})
	}

	series := make([]*model.SeriesResult, 0, len(seriesMap))
	for _, sr := range seriesMap {
		series = append(series, sr)
	}

	// Записываем объединённый chunk через Writer
	cw := NewWriter(chunksDir)
	if err := cw.Write(metric, series); err != nil {
		return fmt.Errorf("write compacted chunk: %w", err)
	}

	// Удаляем старые файлы только после успешной записи нового
	for _, path := range files {
		_ = os.Remove(path)
	}

	return nil
}

func chunkFiles(metricDir string) ([]string, error) {
	entries, err := os.ReadDir(metricDir)
	if err != nil {
		return nil, err
	}
	var files []string
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".chunk") {
			files = append(files, filepath.Join(metricDir, e.Name()))
		}
	}
	sort.Strings(files)
	return files, nil
}