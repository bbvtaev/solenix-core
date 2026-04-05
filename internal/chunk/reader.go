package chunk

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/bbvtaev/solenix-core/internal/model"
)

// ChunkTimeRange reads only the 24-byte header of a chunk file and returns its time range.
// This is a fast O(1) operation used to skip irrelevant files during queries.
func ChunkTimeRange(path string) (minTS, maxTS int64, err error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, 0, err
	}
	defer f.Close()

	hdr := make([]byte, headerSize)
	if _, err := io.ReadFull(f, hdr); err != nil {
		return 0, 0, err
	}
	if binary.LittleEndian.Uint32(hdr[0:4]) != Magic {
		return 0, 0, fmt.Errorf("invalid chunk magic in %s", path)
	}
	minTS = int64(binary.LittleEndian.Uint64(hdr[8:16]))
	maxTS = int64(binary.LittleEndian.Uint64(hdr[16:24]))
	return minTS, maxTS, nil
}

// QueryChunks reads points from chunk files for a metric in the time range [from, to].
// labels is a filter (nil = match any). from/to == 0 means no bound.
func QueryChunks(chunksDir, metric string, labels map[string]string, from, to int64) ([]model.SeriesResult, error) {
	metricDir := filepath.Join(chunksDir, sanitizeMetric(metric))
	entries, err := os.ReadDir(metricDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}

	var files []string
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".chunk") {
			files = append(files, filepath.Join(metricDir, e.Name()))
		}
	}
	sort.Strings(files)

	seriesMap := make(map[uint64]*model.SeriesResult)
	cr := reader{}

	for _, path := range files {
		minTS, maxTS, err := ChunkTimeRange(path)
		if err != nil {
			continue // skip unreadable files
		}
		// Skip files that don't overlap [from, to]
		if from > 0 && maxTS < from {
			continue
		}
		if to > 0 && minTS > to {
			continue
		}

		recs, err := cr.readFile(path)
		if err != nil {
			return nil, fmt.Errorf("read chunk %s: %w", path, err)
		}
		for _, rec := range recs {
			if !chunkLabelsMatch(labels, rec.Labels) {
				continue
			}
			pts := chunkFilterPoints(rec.Points, from, to)
			if len(pts) == 0 {
				continue
			}
			id := model.HashSeries(metric, rec.Labels)
			if sr, ok := seriesMap[id]; ok {
				sr.Points = append(sr.Points, pts...)
			} else {
				lblCopy := make(map[string]string, len(rec.Labels))
				for k, v := range rec.Labels {
					lblCopy[k] = v
				}
				seriesMap[id] = &model.SeriesResult{
					Metric: metric,
					Labels: lblCopy,
					Points: pts,
				}
			}
		}
	}

	result := make([]model.SeriesResult, 0, len(seriesMap))
	for _, sr := range seriesMap {
		result = append(result, *sr)
	}
	return result, nil
}

func chunkLabelsMatch(filter, actual map[string]string) bool {
	if len(filter) == 0 {
		return true
	}
	for k, v := range filter {
		if actual[k] != v {
			return false
		}
	}
	return true
}

func chunkFilterPoints(points []model.Point, from, to int64) []model.Point {
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

const versionRaw byte = 0x01

type reader struct{}

// ReadAllChunks читает все *.chunk файлы из data/chunks/*/
// и возвращает model.Record-ы в порядке: метрика за метрикой, внутри — по номеру chunk.
func ReadAllChunks(chunksDir string) ([]model.Record, error) {
	entries, err := os.ReadDir(chunksDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}

	cr := reader{}
	var all []model.Record

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		metricDir := filepath.Join(chunksDir, entry.Name())
		chunkFiles, err := os.ReadDir(metricDir)
		if err != nil {
			return nil, fmt.Errorf("read chunk dir %s: %w", metricDir, err)
		}

		sort.Slice(chunkFiles, func(i, j int) bool {
			return chunkFiles[i].Name() < chunkFiles[j].Name()
		})

		for _, cf := range chunkFiles {
			if cf.IsDir() || !strings.HasSuffix(cf.Name(), ".chunk") {
				continue
			}
			p := filepath.Join(metricDir, cf.Name())
			recs, err := cr.readFile(p)
			if err != nil {
				return nil, fmt.Errorf("read chunk %s: %w", p, err)
			}
			all = append(all, recs...)
		}
	}

	return all, nil
}

// readFile читает один chunk файл и возвращает []model.Record.
// Имя метрики берётся из имени родительской директории.
func (cr reader) readFile(path string) ([]model.Record, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	minSize := headerSize + footerSize
	if len(data) < minSize {
		return nil, fmt.Errorf("chunk file too small: %s (%d bytes)", path, len(data))
	}

	magic := binary.LittleEndian.Uint32(data[0:4])
	if magic != Magic {
		return nil, fmt.Errorf("invalid chunk magic in %s: got %08x", path, magic)
	}

	ver := data[4]

	footerOffset := len(data) - footerSize
	footer := data[footerOffset:]
	seriesCount := int(binary.LittleEndian.Uint32(footer[0:4]))
	expectedCRC := binary.LittleEndian.Uint32(footer[4:8])

	actualCRC := crc32.ChecksumIEEE(data[:footerOffset])
	if actualCRC != expectedCRC {
		return nil, fmt.Errorf("chunk CRC mismatch in %s: expected %d, got %d", path, expectedCRC, actualCRC)
	}

	metric := filepath.Base(filepath.Dir(path))

	records := make([]model.Record, 0, seriesCount)
	offset := headerSize

	for i := 0; i < seriesCount; i++ {
		// series_id (пропускаем — пересчитается из labels)
		if offset+8 > footerOffset {
			return nil, fmt.Errorf("unexpected EOF reading series_id in %s (series %d)", path, i)
		}
		offset += 8

		if offset+2 > footerOffset {
			return nil, fmt.Errorf("unexpected EOF reading labels_len in %s (series %d)", path, i)
		}
		labelsLen := int(binary.LittleEndian.Uint16(data[offset:]))
		offset += 2

		if offset+labelsLen > footerOffset {
			return nil, fmt.Errorf("unexpected EOF reading labels in %s (series %d)", path, i)
		}
		labels, err := decodeLabels(data[offset : offset+labelsLen])
		if err != nil {
			return nil, fmt.Errorf("decode labels in %s (series %d): %w", path, i, err)
		}
		offset += labelsLen

		if offset+4 > footerOffset {
			return nil, fmt.Errorf("unexpected EOF reading points_count in %s (series %d)", path, i)
		}
		pointsCount := int(binary.LittleEndian.Uint32(data[offset:]))
		offset += 4

		var points []model.Point
		switch ver {
		case versionRaw:
			if offset+pointsCount*16 > footerOffset {
				return nil, fmt.Errorf("unexpected EOF reading points in %s (series %d)", path, i)
			}
			points = make([]model.Point, pointsCount)
			for j := 0; j < pointsCount; j++ {
				ts := int64(binary.LittleEndian.Uint64(data[offset:]))
				offset += 8
				val := math.Float64frombits(binary.LittleEndian.Uint64(data[offset:]))
				offset += 8
				points[j] = model.Point{Timestamp: ts, Value: val}
			}
		case versionGorilla:
			if offset+4 > footerOffset {
				return nil, fmt.Errorf("unexpected EOF reading compressed_len in %s (series %d)", path, i)
			}
			compressedLen := int(binary.LittleEndian.Uint32(data[offset:]))
			offset += 4
			if offset+compressedLen > footerOffset {
				return nil, fmt.Errorf("unexpected EOF reading gorilla data in %s (series %d)", path, i)
			}
			points, err = DecodePoints(data[offset:offset+compressedLen], pointsCount)
			if err != nil {
				return nil, fmt.Errorf("decode gorilla points in %s (series %d): %w", path, i, err)
			}
			offset += compressedLen
		default:
			return nil, fmt.Errorf("unknown chunk version %d in %s", ver, path)
		}

		records = append(records, model.Record{
			Metric: metric,
			Labels: labels,
			Points: points,
		})
	}

	return records, nil
}

// decodeLabels декодирует labels block: [count:uint16]([key_len:uint16][key][val_len:uint16][val])*N
func decodeLabels(data []byte) (map[string]string, error) {
	if len(data) < 2 {
		return nil, io.ErrUnexpectedEOF
	}
	count := int(binary.LittleEndian.Uint16(data[0:2]))
	offset := 2
	labels := make(map[string]string, count)

	for i := 0; i < count; i++ {
		if offset+2 > len(data) {
			return nil, io.ErrUnexpectedEOF
		}
		kLen := int(binary.LittleEndian.Uint16(data[offset:]))
		offset += 2
		if offset+kLen > len(data) {
			return nil, io.ErrUnexpectedEOF
		}
		key := string(data[offset : offset+kLen])
		offset += kLen

		if offset+2 > len(data) {
			return nil, io.ErrUnexpectedEOF
		}
		vLen := int(binary.LittleEndian.Uint16(data[offset:]))
		offset += 2
		if offset+vLen > len(data) {
			return nil, io.ErrUnexpectedEOF
		}
		labels[key] = string(data[offset : offset+vLen])
		offset += vLen
	}

	return labels, nil
}
