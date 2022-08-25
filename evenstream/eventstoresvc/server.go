package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"

	"github.com/lamhai1401/even-driven-ex/evenstream/cockroachdb/eventstorerepository"
	"github.com/lamhai1401/even-driven-ex/evenstream/eventstore"
	"github.com/lamhai1401/even-driven-ex/evenstream/sqldb"
	"github.com/lamhai1401/even-driven-ex/pkg/natsutil"
	"github.com/nats-io/nats.go"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	port = flag.Int("port", 50051, "The server port")
)

// publishEvent publishes an event via NATS JetStream server
func publishEvent(component *natsutil.NATSComponent, event *eventstore.Event) {
	// Creates JetStreamContext to publish messages into JetStream Stream
	jetStreamContext, _ := component.JetStreamContext()
	subject := event.EventType
	eventMsg := []byte(event.EventData)
	// Publish message on subject (channel)
	jetStreamContext.Publish(subject, eventMsg)
	log.Println("Published message on subject: " + subject)
}

// server is used to implement eventstore.EventStoreServer interface
type server struct {
	eventstore.UnimplementedEventStoreServer
	repository eventstore.Repository
	nats       *natsutil.NATSComponent
}

// CreateEvent creates a new event into the event store
func (s *server) CreateEvent(ctx context.Context, eventRequest *eventstore.CreateEventRequest) (*eventstore.CreateEventResponse, error) {
	err := s.repository.CreateEvent(ctx, eventRequest.Event)
	if err != nil {
		return nil, status.Error(codes.Internal, "internal error")
	}

	log.Println("Event is created")
	go publishEvent(s.nats, eventRequest.Event)
	return &eventstore.CreateEventResponse{IsSuccess: true, Error: ""}, nil
}

// GetEvents gets all events for the given aggregate and event
func (s *server) GetEvents(ctx context.Context, filter *eventstore.GetEventsRequest) (*eventstore.GetEventsResponse, error) {
	events, err := s.repository.GetEvents(ctx, filter)
	if err != nil {
		return nil, status.Error(codes.Internal, "internal error")
	}
	return &eventstore.GetEventsResponse{Events: events}, nil
}

// GetEventsStream get stream of events for the given event
func (s *server) GetEventsStream(*eventstore.GetEventsRequest, eventstore.EventStore_GetEventsStreamServer) error {
	return nil
}

func getServer() *server {
	eventstoreDB, _ := sqldb.NewEventStoreDB()
	repository, _ := eventstorerepository.New(eventstoreDB.DB)
	natsComponent := natsutil.NewNATSComponent("eventstore-service")
	natsComponent.ConnectToServer(nats.DefaultURL)
	server := &server{
		repository: repository,
		nats:       natsComponent,
	}
	return server
}

func main() {
	flag.Parse()
	lis, err := net.Listen("tcp", fmt.Sprintf("localhost:%d", *port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	grpcServer := grpc.NewServer()
	server := getServer()
	eventstore.RegisterEventStoreServer(grpcServer, server)
	log.Printf("server listening at %v", lis.Addr())
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
