# RabbitMQ-based Messaging System in Go

This project implements a **message publishing and consuming system** using **RabbitMQ** as the message broker. It includes a **publisher** and a **consumer** running in separate Docker containers, connected via a shared Docker network. The system is built using **Go** and is designed to be **scalable**, **reliable**, and **production-ready**.

## Features

- **Reliable Messaging**: Durable queues, persistent messages, and exchange durability ensure that messages are not lost in case of RabbitMQ restarts or failures.
- **Scalability**: The consumer implements a worker pool pattern to process messages concurrently, allowing for high throughput and better utilization of system resources.
- **Batch Publishing**: The publisher can send messages in batches, improving performance by reducing network overhead.
- **Metrics Collection**: Integration with **Prometheus** for tracking message throughput, success rates, and processing latency.
- **Structured Logging**: Uses **Logrus** for structured and efficient logging in production environments.



## Prerequisites

- **Docker** and **Docker Compose**: For building and running the containers.
- **Go 1.24+**: For building and running the Go applications locally (if not using Docker).
- **RabbitMQ**: The system uses RabbitMQ as a message broker, which is managed within the Docker Compose setup.

