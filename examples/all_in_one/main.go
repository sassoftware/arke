//go:build example

package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"strings"
	"time"

	pb "github.com/sassoftware/arke/api"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	// creds := credentials.NewTLS(&tls.Config{InsecureSkipVerify: true}) //nolint:gosec
	creds := insecure.NewCredentials()
	// or insecure.NewCredentials() if not using tls

	arkeAddress := "localhost:50051"
	log.Printf("connecting to arke at %s...", arkeAddress)
	conn, err := grpc.NewClient(arkeAddress, grpc.WithTransportCredentials(creds))
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		log.Printf("closing connection to arke")
		conn.Close()
	}()

	producer := pb.NewProducerClient(conn)
	consumer := pb.NewConsumerClient(conn)

	connCfg := &pb.ConnectionConfiguration{
		Host:       "rabbitmq",
		Port:       5672,
		Provider:   "amqp091",
		ClientName: "example-all-in-one",
		Credentials: &pb.Credentials{
			Username: "guest",
			Password: "guest",
		},
	}

	// Connect producer and consumer
	if resp, err := producer.Connect(ctx, connCfg); err != nil ||
		!resp.GetSuccess() {
		log.Fatalf("producer connect failed: %v %v", err, resp.GetError())
	}
	defer func() {
		_, _ = producer.Disconnect(ctx, &pb.Empty{})
	}()

	// Get a stream for consuming messages
	consumeStream, err := consumer.Consume(ctx)
	if err != nil {
		log.Printf("exiting because consume failed: %v", err)
		return
	}

	source := &pb.Source{
		Name: "orders-worker",
		Address: &pb.Address{
			Name:     "orders.exchange",
			Subjects: []string{"orders.created"},
			Type:     pb.Address_TOPIC,
		},
		Type:          pb.Source_QUEUE,
		PrefetchCount: 5,
	}

	// Send the source to create the exchange/queue/bindings and start consuming messages.
	if err := consumeStream.Send(&pb.Consume{
		Msg: &pb.Consume_Src{Src: source},
	}); err != nil {
		log.Fatal(err)
	}

	// Start a goroutine to receive messages and send acks
	go func() {
		for {
			resp, err := consumeStream.Recv()
			if err == io.EOF {
				return
			}
			if err != nil {
				if strings.Contains(err.Error(), "client connection is closing") { // this is normal
					return
				}
				log.Printf("consume recv failed: %v", err)
				return
			}

			if resp.GetMsg() == nil {
				continue
			}

			msg := resp.GetMsg()
			fmt.Printf("received: %s\n", string(msg.GetBody()))

			ack := &pb.MessageConsumed{Uuid: msg.GetUuid()}
			if err := consumeStream.Send(&pb.Consume{
				Msg: &pb.Consume_Ack{Ack: ack},
			}); err != nil {
				log.Printf("ack send failed: %v", err)
				return
			}
		}
	}()

	// ensure queue is created before publishing
	time.Sleep(1 * time.Second)

	// After consuming is set up, publish a message to the exchange.
	pubResp, err := producer.PublishOne(ctx, &pb.Message{
		Body:       []byte(`{"id":"12345"}`),
		Persistent: true,
		Confirm:    true,
		Address: &pb.Address{
			Name:     "orders.exchange",
			Subjects: []string{"orders.created"},
			Type:     pb.Address_TOPIC,
		},
	})
	if err != nil {
		log.Fatal(err)
	}

	if pubResp.GetSuccess() {
		log.Println("message published successfully")
	} else {
		log.Fatalf("publish failed: %s", pubResp.GetError().GetMessage())
	}

	// Wait just a bit to ensure the message is received before exiting
	time.Sleep(2 * time.Second)
}
