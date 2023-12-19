package grpcServer

import (
	"context"
	"net"
	"os"
	"testing"

	api "distributed-services-in-go/api/v1"
	"distributed-services-in-go/internal/log"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

func TestServer(t *testing.T) {
	scenarios := map[string]func(t *testing.T, client api.LogServiceClient, config *Config){
		"produce/consume a message to/from the log": testProduceConsume,
		"consume past log boundary fails":           testConsumePastBoundary,
	}

	for scenario, fn := range scenarios {
		t.Run(scenario, func(t *testing.T) {
			client, config, teardown := setupTest(t, nil)

			defer teardown()

			fn(t, client, config)
		})
	}

}

func setupTest(t *testing.T, fn func(*Config)) (api.LogServiceClient, *Config, func()) {
	t.Helper()

	l, err := net.Listen("tcp", ":0")
	require.NoError(t, err)

	clientOptions := []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}

	cc, err := grpc.Dial(l.Addr().String(), clientOptions...)

	require.NoError(t, err)

	dir, err := os.MkdirTemp("", "server-test")

	require.NoError(t, err)

	clog, err := log.NewLog(dir, log.Config{})

	require.NoError(t, err)

	cfg := &Config{commitLog: clog}

	if fn != nil {
		fn(cfg)
	}

	server, err := NewGrpcServer(cfg)

	require.NoError(t, err)

	go func() {
		server.Serve(l)
	}()

	client := api.NewLogServiceClient(cc)

	return client, cfg, func() {
		server.Stop()
		cc.Close()
		l.Close()
		clog.Remove()
	}

}

func testProduceConsume(t *testing.T, client api.LogServiceClient, config *Config) {
	ctx := context.Background()

	want := &api.Record{Value: []byte("Hello World")}

	produce, err := client.Produce(ctx, &api.ProduceRequest{Value: want.Value})

	require.NoError(t, err)

	consume, err := client.Consume(ctx, &api.ConsumeRequest{Offset: produce.Offset})

	require.NoError(t, err)

	require.Equal(t, want.Value, consume.Record.Value)
	require.Equal(t, want.Offset, consume.Record.Offset)
}

func testConsumePastBoundary(t *testing.T, client api.LogServiceClient, config *Config) {
	ctx := context.Background()

	produce, err := client.Produce(ctx, &api.ProduceRequest{
		Value: []byte("Hello World"),
	})

	require.NoError(t, err)

	consume, err := client.Consume(ctx, &api.ConsumeRequest{
		Offset: produce.Offset + 1,
	})

	require.Nil(t, consume)

	got := status.Code(err)

	want := status.Code(api.ErrOffsetOutOfRange{}.GRPCStatus().Err())

	require.Equal(t, want, got)
}

func testProduceConsumeStream(t *testing.T, client api.LogServiceClient, config *Config) {
	ctx := context.Background()

	records := []*api.Record{
		{
			Value:  []byte("First Message"),
			Offset: 0,
		},
		{
			Value:  []byte("Second message"),
			Offset: 1,
		},
	}

	stream, err := client.ProduceStream(ctx)

	require.NoError(t, err)

	for offset, record := range records {
		err := stream.Send(&api.ProduceRequest{Value: record.Value})

		require.NoError(t, err)

		res, err := stream.Recv()

		require.NoError(t, err)

		require.Equal(t, uint64(offset), res.Offset)
	}

	consumeStream, err := client.ConsumeStream(ctx, &api.ConsumeRequest{Offset: 0})

	require.NoError(t, err)

	for i, record := range records {
		res, err := consumeStream.Recv()

		require.NoError(t, err)

		require.Equal(t, res.Record, &api.Record{Value: record.Value, Offset: uint64(i)})
	}
}
