# RabbitMQ-based Messaging System in Go

This project implements a **message publishing and consuming system** using **RabbitMQ** as the message broker. It includes a **publisher** and a **consumer** running in separate Docker containers, connected via a shared Docker network. The system is built using **Go** and is designed to be **scalable**, **reliable**, and **production-ready**.

## Prerequisites

- **Docker** and **Docker Compose**: For building and running the containers.
- **Go 1.24+**: For building and running the Go applications locally (if not using Docker).
- **RabbitMQ**: The system uses RabbitMQ as a message broker, which is managed within the Docker Compose setup.

# Summary

### *Scanrio A* -> Auto-Ack + Sequential
- Each message 1s -> 10 message 10s

### *Scanrio B* -> Manual-Ack + Sequential
- Each message 2s -> 10 message 20s

### *Scanrio C* -> Manual-Ack + Concurrency
- Each message 3s -> 10 message 10s
- 3 workers + 3 prefetch

## *NOTE*: All task can be done with 2 consumers

### Sample commands for consumer:
`go run main.go --scenario=A --consumer-id=1`

`go run main.go --scenario=C --consumer-id=2`

