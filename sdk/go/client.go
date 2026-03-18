// Package pulse предоставляет Go SDK для pulse-core.
//
// Пример использования:
//
//	client, err := pulse.NewClient("localhost:50051")
//	if err != nil { log.Fatal(err) }
//	defer client.Close()
//
//	client.Write("cpu.usage", pulse.Labels{"host": "srv1"}, 72.5)
//
//	results, _ := client.Query("cpu.usage", nil, 0, 0)
//	for _, s := range results {
//	    fmt.Println(s.Metric, s.Points)
//	}
package pulse

import (
	"context"
	"fmt"
	"time"

	pb "github.com/bbvtaev/pulse-core/api/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Labels — псевдоним для удобства.
type Labels = map[string]string

// Point — одна точка временного ряда.
type Point struct {
	Timestamp int64
	Value     float64
}

// SeriesResult — результат Query для одной серии.
type SeriesResult struct {
	Metric string
	Labels Labels
	Points []Point
}

// Client — gRPC клиент для pulse-core.
type Client struct {
	conn    *grpc.ClientConn
	rpc     pb.PulseDBClient
	timeout time.Duration
}

// NewClient подключается к серверу pulse-core по адресу addr (например "localhost:50051").
func NewClient(addr string) (*Client, error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("pulse: connect to %s: %w", addr, err)
	}
	return &Client{
		conn:    conn,
		rpc:     pb.NewPulseDBClient(conn),
		timeout: 5 * time.Second,
	}, nil
}

// Close закрывает соединение.
func (c *Client) Close() error {
	return c.conn.Close()
}

// Write записывает одно значение с текущим временем (UnixNano).
func (c *Client) Write(metric string, labels Labels, value float64) error {
	return c.WriteBatch(metric, labels, []Point{
		{Timestamp: time.Now().UnixNano(), Value: value},
	})
}

// WriteBatch записывает несколько точек с произвольными timestamp.
func (c *Client) WriteBatch(metric string, labels Labels, points []Point) error {
	pbPoints := make([]*pb.DataPoint, len(points))
	for i, p := range points {
		pbPoints[i] = &pb.DataPoint{Timestamp: p.Timestamp, Value: p.Value}
	}

	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	_, err := c.rpc.Write(ctx, &pb.WriteRequest{
		Series: []*pb.Series{{
			Metric: metric,
			Labels: labels,
			Points: pbPoints,
		}},
	})
	return err
}

// Query запрашивает данные. from/to в Unix nanoseconds; 0 означает без ограничения.
func (c *Client) Query(metric string, labels Labels, from, to int64) ([]SeriesResult, error) {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	resp, err := c.rpc.Query(ctx, &pb.QueryRequest{
		Metric: metric,
		Labels: labels,
		From:   from,
		To:     to,
	})
	if err != nil {
		return nil, err
	}

	results := make([]SeriesResult, len(resp.Series))
	for i, s := range resp.Series {
		pts := make([]Point, len(s.Points))
		for j, p := range s.Points {
			pts[j] = Point{Timestamp: p.Timestamp, Value: p.Value}
		}
		results[i] = SeriesResult{Metric: s.Metric, Labels: s.Labels, Points: pts}
	}
	return results, nil
}

// Delete удаляет точки по временному диапазону.
func (c *Client) Delete(metric string, labels Labels, from, to int64) error {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	_, err := c.rpc.Delete(ctx, &pb.DeleteRequest{
		Metric: metric,
		Labels: labels,
		From:   from,
		To:     to,
	})
	return err
}

// Subscribe возвращает канал с новыми точками в реальном времени.
// Подписка активна пока ctx не отменён.
func (c *Client) Subscribe(ctx context.Context, metric string, labels Labels) (<-chan Point, error) {
	stream, err := c.rpc.Subscribe(ctx, &pb.SubscribeRequest{
		Metric: metric,
		Labels: labels,
	})
	if err != nil {
		return nil, err
	}

	ch := make(chan Point, 256)
	go func() {
		defer close(ch)
		for {
			p, err := stream.Recv()
			if err != nil {
				return
			}
			select {
			case ch <- Point{Timestamp: p.Timestamp, Value: p.Value}:
			case <-ctx.Done():
				return
			}
		}
	}()

	return ch, nil
}

// Health возвращает статус и версию сервера.
func (c *Client) Health() (status, version string, err error) {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	resp, err := c.rpc.Health(ctx, &pb.HealthRequest{})
	if err != nil {
		return "", "", err
	}
	return resp.Status, resp.Version, nil
}
