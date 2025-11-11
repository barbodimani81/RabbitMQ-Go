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
	for i := 1; i <= 100; i++ {
		message := fmt.Sprintf("Message #%d", i)
		err := client.Publish(exchangeName, message)
		if err != nil {
			log.Fatalf("Failed to publish message: %s", err)
		}
		fmt.Printf("Sent: %s\n", message)
		time.Sleep(1 * time.Second)
	}
}
