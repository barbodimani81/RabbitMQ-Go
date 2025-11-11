package main

import (
	"final/rabbit"
	"fmt"
	"log"
)

func main() {
	client, err := rabbit.NewRabbitMQClient("amqp://guest:guest@rabbitmq:5672/")
	if err != nil {
		log.Fatalf("Failed to connect to RabbitMQ: %s", err)
	}
	defer client.Close()

	exchangeName := "logs"
	msgs, err := client.Consume(exchangeName)
	if err != nil {
		log.Fatalf("Failed to consume messages: %s", err)
	}

	// Process incoming messages
	for msg := range msgs {
		fmt.Printf("Received: %s\n", msg.Body)
	}
}
