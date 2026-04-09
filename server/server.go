package server

import (
	"context"
	"fmt"
	"net"
	"time"

	solenix "github.com/bbvtaev/solenix"
	pb "github.com/bbvtaev/solenix/api/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Server реализует gRPC-интерфейс SolenixDB поверх storage engine.
type Server struct {
	pb.UnimplementedSolenixDBServer
	db *solenix.DB
}

func New(db *solenix.DB) *Server {
	return &Server{db: db}
}

// Listen запускает gRPC-сервер на заданном адресе (например, ":50051").
func (s *Server) Listen(addr string) error {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", addr, err)
	}

	grpcSrv := grpc.NewServer()
	pb.RegisterSolenixDBServer(grpcSrv, s)

	return grpcSrv.Serve(lis)
}

func (s *Server) Push(_ context.Context, req *pb.PushRequest) (*pb.PushResponse, error) {
	var written int32

	for _, ser := range req.Series {
		points := make([]solenix.Point, len(ser.Points))
		for i, p := range ser.Points {
			points[i] = solenix.Point{Timestamp: p.Timestamp, Value: p.Value}
		}
		if err := s.db.PushBatch(ser.Metric, ser.Labels, points); err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "push: %v", err)
		}
		written++
	}

	return &pb.PushResponse{Written: written}, nil
}

func (s *Server) Query(_ context.Context, req *pb.QueryRequest) (*pb.QueryResponse, error) {
	var opts *solenix.QueryOptions
	if req.Window != "" {
		window, err := time.ParseDuration(req.Window)
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "invalid window %q: %v", req.Window, err)
		}
		agg, err := solenix.ParseAggType(req.Agg)
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "%v", err)
		}
		opts = &solenix.QueryOptions{Window: window, Agg: agg}
	}

	results, err := s.db.Query(req.Metric, req.Labels, req.From, req.To, opts)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%v", err)
	}

	pbSeries := make([]*pb.Series, 0, len(results))
	for _, r := range results {
		pts := make([]*pb.DataPoint, len(r.Points))
		for i, p := range r.Points {
			pts[i] = &pb.DataPoint{Timestamp: p.Timestamp, Value: p.Value}
		}
		pbSeries = append(pbSeries, &pb.Series{
			Metric: r.Metric,
			Labels: r.Labels,
			Points: pts,
		})
	}

	return &pb.QueryResponse{Series: pbSeries}, nil
}

// Subscribe — server-side streaming: шлёт DataPoint в реальном времени.
func (s *Server) Subscribe(req *pb.SubscribeRequest, stream pb.SolenixDB_SubscribeServer) error {
	id, ch := s.db.Subscribe(req.Metric, req.Labels)
	defer s.db.Unsubscribe(id)

	for {
		select {
		case <-stream.Context().Done():
			return nil
		case p, ok := <-ch:
			if !ok {
				return nil
			}
			if err := stream.Send(&pb.DataPoint{
				Timestamp: p.Timestamp,
				Value:     p.Value,
			}); err != nil {
				return err
			}
		}
	}
}

func (s *Server) Health(_ context.Context, _ *pb.HealthRequest) (*pb.HealthResponse, error) {
	return &pb.HealthResponse{
		Status:  "ok",
		Version: solenix.Version,
	}, nil
}

func (s *Server) Metrics(_ context.Context, _ *pb.MetricsRequest) (*pb.MetricsResponse, error) {
	return &pb.MetricsResponse{Metrics: s.db.Metrics()}, nil
}
