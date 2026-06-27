package coordinator

import (
	"context"
	"net"

	"google.golang.org/grpc"

	pb "github.com/Mxs8513/StreamForge/internal/proto"
)

// Server adapts the Coordinator to the gRPC Coordinator service.
type Server struct {
	pb.UnimplementedCoordinatorServer
	c *Coordinator
}

// Serve starts the gRPC coordinator on addr and blocks until ctx is cancelled.
func Serve(ctx context.Context, addr string, c *Coordinator) error {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	gs := grpc.NewServer()
	pb.RegisterCoordinatorServer(gs, &Server{c: c})

	go func() {
		<-ctx.Done()
		gs.GracefulStop()
	}()
	return gs.Serve(lis)
}

func toPB(a Assignment) *pb.Assignment {
	return &pb.Assignment{
		Ready:                   a.Ready,
		KafkaPartitions:         a.KafkaPartitions,
		KeyBuckets:              a.KeyBuckets,
		NumBuckets:              a.NumBuckets,
		BucketOwnerAddr:         a.BucketOwnerAddr,
		LastCompletedCheckpoint: a.LastCompletedCheckpoint,
		Epoch:                   a.Epoch,
	}
}

func (s *Server) Register(_ context.Context, req *pb.RegisterRequest) (*pb.RegisterResponse, error) {
	a := s.c.Register(req.WorkerId, req.Address)
	return &pb.RegisterResponse{Assignment: toPB(a)}, nil
}

func (s *Server) Heartbeat(_ context.Context, req *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error) {
	a := s.c.Heartbeat(req.WorkerId)
	return &pb.HeartbeatResponse{Assignment: toPB(a)}, nil
}

func (s *Server) GetAssignment(_ context.Context, req *pb.GetAssignmentRequest) (*pb.Assignment, error) {
	a := s.c.GetAssignment(req.WorkerId)
	return toPB(a), nil
}
