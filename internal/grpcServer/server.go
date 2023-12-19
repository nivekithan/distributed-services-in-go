package grpcServer

import (
	// "distributed-services-in-go/internal/log"
	"context"
	api "distributed-services-in-go/api/v1"

	"google.golang.org/grpc"
	// "google.golang.org/grpc"
)

type CommitLog interface {
	Append(*api.Record) (uint64, error)
	Read(uint64) (*api.Record, error)
}

type Config struct {
	commitLog CommitLog
}

type grpcServer struct {
	api.UnimplementedLogServiceServer
	*Config
}

var _ api.LogServiceServer = (*grpcServer)(nil)

func NewGrpcServer(config *Config) (*grpc.Server, error) {
	server := grpc.NewServer()

	logServer, err := newGrpcServer(config)

	if err != nil {
		return nil, err
	}

	api.RegisterLogServiceServer(server, logServer)

	return server, nil
}

func newGrpcServer(config *Config) (*grpcServer, error) {
	server := &grpcServer{Config: config}

	return server, nil
}

func (s *grpcServer) Produce(ctx context.Context, req *api.ProduceRequest) (*api.ProduceResponse, error) {
	record := api.Record{Value: req.Value}
	offset, err := s.commitLog.Append(&record)

	if err != nil {
		return nil, err
	}

	response := &api.ProduceResponse{Offset: offset}

	return response, nil
}

func (s *grpcServer) Consume(ctx context.Context, req *api.ConsumeRequest) (*api.ConsumeResponse, error) {
	record, err := s.commitLog.Read(req.Offset)

	if err != nil {
		return nil, err
	}

	response := &api.ConsumeResponse{Record: record}

	return response, nil
}

func (s *grpcServer) ProduceStream(stream api.LogService_ProduceStreamServer) error {
	for {
		req, err := stream.Recv()

		if err != nil {
			return err
		}

		res, err := s.Produce(stream.Context(), req)

		if err != nil {
			return err
		}

		if err := stream.Send(res); err != nil {
			return err
		}
	}
}

func (s *grpcServer) ConsumeStream(req *api.ConsumeRequest, stream api.LogService_ConsumeStreamServer) error {
	for {
		select {
		case <-stream.Context().Done():
			return nil

		default:
			res, err := s.Consume(stream.Context(), req)
			switch err.(type) {
			case nil:
			case api.ErrOffsetOutOfRange:
				continue
			default:
				return err
			}

			if err := stream.Send(res); err != nil {
				return err
			}

			req.Offset++
		}
	}
}
