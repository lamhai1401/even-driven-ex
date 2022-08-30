package svc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/google/uuid"
	"github.com/lamhai1401/even-driven-ex/evenstream/eventstore"
	"github.com/lamhai1401/even-driven-ex/evenstream/order"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
)

type Service interface {
	CreateOrder(ctx context.Context, o order.Order) (string, error)
	GetOrderByID(ctx context.Context, id string) (order.Order, error)
}

const (
	event     = "ORDERS.created"
	aggregate = "order"
	grpcUri   = "localhost:50051"
)

type rpcClient interface {
	createOrder(order order.Order) error
}

type grpcClient struct {
}

// createOrder calls the CreateEvent RPC
func (gc grpcClient) createOrder(order order.Order) error {
	conn, err := grpc.Dial(grpcUri, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("Unable to connect: %v", err)
	}
	defer conn.Close()
	client := eventstore.NewEventStoreClient(conn)
	orderJSON, _ := json.Marshal(order)

	eventid, _ := uuid.NewUUID()
	event := &eventstore.Event{
		EventId:       eventid.String(),
		EventType:     event,
		AggregateId:   order.ID,
		AggregateType: aggregate,
		EventData:     string(orderJSON),
		Stream:        "ORDERS",
	}

	createEventRequest := &eventstore.CreateEventRequest{Event: event}
	resp, err := client.CreateEvent(context.Background(), createEventRequest)
	if err != nil {
		if st, ok := status.FromError(err); ok {
			return fmt.Errorf("error from RPC server with: status code:%s message:%s", st.Code().String(), st.Message())
		}
		return fmt.Errorf("error from RPC server: %w", err)
	}
	if resp.IsSuccess {
		return nil
	}
	return errors.New("error from RPC server")
}

type service struct {
	rpc        rpcClient
	repository order.QueryRepository
}

func NewServiceWithRPC(rpc rpcClient, repository order.QueryRepository) Service {
	return &service{
		rpc:        rpc,
		repository: repository,
	}
}
func NewService(repository order.QueryRepository) Service {
	return &service{
		rpc:        grpcClient{},
		repository: repository,
	}
}

func (s *service) CreateOrder(ctx context.Context, order order.Order) (string, error) {
	id, _ := uuid.NewUUID()
	aggregateID := id.String()
	order.ID = aggregateID
	order.Status = "Pending"
	order.CreatedOn = time.Now()
	order.Amount = order.GetAmount()
	err := s.rpc.createOrder(order)
	if err != nil {
		return "", err
	}
	return aggregateID, nil
}

func (s *service) GetOrderByID(ctx context.Context, id string) (order.Order, error) {
	o, err := s.repository.GetOrderByID(ctx, id)
	if err != nil {
		return order.Order{}, err
	}
	return o, nil
}
