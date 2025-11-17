package main

import (
	"final/rabbit"
	"fmt"
	"log"
	"time"
)

func main() {
	var client *rabbit.RabbitMQClient
	var err error
	// Retry creating client until RabbitMQ is available
	for {
		client, err = rabbit.NewRabbitMQClient("amqp://guest:guest@localhost:5672/")
		if err == nil {
			break
		}
		log.Printf("Failed to connect to RabbitMQ, retrying in 5s: %s", err)
		time.Sleep(5 * time.Second)
	}
	defer client.Close()

	exchangeName := "logs"
	for i := 1; i <= 10; i++ {
		message := fmt.Sprintf("Task #%d", i)
		// Retry publish of the same message until it succeeds
		for {
			err := client.Publish(exchangeName, message)
			if err != nil {
				log.Printf("Failed to publish message, will retry in 2s: %s", err)
				time.Sleep(2 * time.Second)
				continue
			}
			break
		}

		fmt.Printf("Sent: %s\n", message)
		// time.Sleep(1*time.Second)
	}
}
