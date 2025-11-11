package main

import (
	"final/rabbit"
	"fmt"
	"log"
	"time"
)

func main() {
	client, err := rabbit.NewRabbitMQClient("amqp://guest:guest@rabbitmq:5672/")
	if err != nil {
		log.Fatalf("Failed to connect to RabbitMQ: %s", err)
	}
	defer client.Close()

	exchangeName := "logs"
	for {
		msgs, err := client.Consume(exchangeName)
		if err != nil {
			log.Printf("Failed to start consuming, will retry: %s", err)
			time.Sleep(5 * time.Second)
			continue
		}
		// Process incoming messages until the channel closes (e.g., broker restart)
		for msg := range msgs {
			fmt.Printf("Received: %s\n", msg.Body)
		}
		// If we reach here, the msgs channel closed. Loop will attempt to re-consume.
		log.Println("Message channel closed, attempting to re-establish consumer...")
		time.Sleep(2 * time.Second)
	}
}
