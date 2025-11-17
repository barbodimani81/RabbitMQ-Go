package rabbit

import (
	"fmt"
	"log"
	"time"

	mq "github.com/rabbitmq/amqp091-go"
)

// MessageQueue defines the methods for publishing and consuming messages.
type MessageQueue interface {
	Publish(exchangeName string, message string) error
	Consume(exchangeName string) (<-chan mq.Delivery, error)
}

// RabbitMQClient holds the connection and channel to RabbitMQ.
type RabbitMQClient struct {
	conn    *mq.Connection
	channel *mq.Channel
	url     string
}

func (c *RabbitMQClient) SetQos(prefetch int) error {
	return c.channel.Qos(prefetch, 0, false)
}

// NewRabbitMQClient creates a new RabbitMQ client and establishes a connection.
func NewRabbitMQClient(url string) (*RabbitMQClient, error) {
	var conn *mq.Connection
	var err error
	backoff := 1 * time.Second

	// Retry connection with exponential backoff
	for retries := 0; retries < 5; retries++ {
		conn, err = mq.Dial(url)
		if err == nil {
			break
		}
		log.Printf("Failed to connect to RabbitMQ, retrying in %s... (attempt %d/5)", backoff, retries+1)
		time.Sleep(backoff)
		backoff *= 2 // Exponential backoff
	}

	if err != nil {
		return nil, fmt.Errorf("could not connect to RabbitMQ after 5 attempts: %w", err)
	}

	// Open a channel
	channel, err := conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("failed to open channel: %w", err)
	}

	return &RabbitMQClient{
		conn:    conn,
		channel: channel,
		url:     url,
	}, nil
}

// Publish sends a persistent message to a durable exchange.
func (r *RabbitMQClient) Publish(exchangeName string, message string) error {
	// Retry logic in case of failure during publishing
	for {
		// Ensure the connection and channel are still valid
		if err := r.ensureConnection(); err != nil {
			log.Println("Connection lost, retrying publish:", err)
			time.Sleep(5 * time.Second) // Sleep before retrying
			continue
		}

		// Declare the durable exchange
		err := r.channel.ExchangeDeclare(
			exchangeName, // Exchange name
			"fanout",     // Exchange type (fanout in this case)
			true,         // Durable (survives restarts)
			false,        // Auto-deleted (false means it won't be deleted when not in use)
			false,        // Internal
			false,        // No-wait
			nil,          // Arguments
		)
		if err != nil {
			log.Println("Failed to declare exchange:", err)
			continue
		}

		// Publish a persistent message
		err = r.channel.Publish(
			exchangeName, // Exchange name
			"",           // Routing key (not used for fanout)
			false,        // Mandatory
			false,        // Immediate
			mq.Publishing{
				ContentType:  "text/plain",    // Message content type
				DeliveryMode: mq.Persistent,   // Persistent message
				Body:         []byte(message), // Message body
			},
		)
		if err != nil {
			log.Println("Failed to publish message:", err)
			continue
		}

		log.Printf("Message sent: %s", message)
		return nil
	}
}

func (r *RabbitMQClient) EnsureExchangeAndQueue(exchangeName, queueName string) error {
    if err := r.ensureConnection(); err != nil {
        return fmt.Errorf("ensure connection: %w", err)
    }

    // 1) Declare durable fanout exchange
    if err := r.channel.ExchangeDeclare(
        exchangeName,
        "fanout",
        true,  // durable
        false, // auto-delete
        false,
        false,
        nil,
    ); err != nil {
        return fmt.Errorf("declare exchange: %w", err)
    }

    // 2) Declare durable, non-auto-delete queue with a FIXED name
    _, err := r.channel.QueueDeclare(
        queueName,
        true,  // durable
        false, // auto-delete (false → stays)
        false, // exclusive
        false, // no-wait
        nil,
    )
    if err != nil {
        return fmt.Errorf("declare queue: %w", err)
    }

    // 3) Bind queue to exchange
    if err := r.channel.QueueBind(
        queueName,
        "",
        exchangeName,
        false,
        nil,
    ); err != nil {
        return fmt.Errorf("bind queue: %w", err)
    }

    return nil
}

// Consume receives messages from the specified exchange.

// Consume receives messages from a specific, named queue.
func (r *RabbitMQClient) Consume(queueName string, autoAck bool) (<-chan mq.Delivery, error) {
    var msgs <-chan mq.Delivery
    var err error

    for {
        if err := r.ensureConnection(); err != nil {
            log.Println("Connection lost, retrying consume:", err)
            time.Sleep(5 * time.Second)
            continue
        }

        // Idempotent: queue may already exist (declared by EnsureExchangeAndQueue)
        _, err = r.channel.QueueDeclare(
            queueName,
            true,  // durable
            false, // auto-delete
            false, // exclusive
            false, // no-wait
            nil,
        )
        if err != nil {
            log.Println("Failed to declare queue:", err)
            time.Sleep(5 * time.Second)
            continue
        }

        msgs, err = r.channel.Consume(
            queueName, // SAME QUEUE NAME FOR ALL CONSUMERS
            "",        // consumer tag
            autoAck,
            false,
            false,
            false,
            nil,
        )
        if err != nil {
            log.Println("Failed to start consuming:", err)
            time.Sleep(5 * time.Second)
            continue
        }

        break
    }

    return msgs, nil
}

// ensureConnection checks if the connection and channel are still valid.
// ensureConnection checks if the connection and channel are still valid.
func (r *RabbitMQClient) ensureConnection() error {
	// Check if the connection is closed or nil
	if r.conn == nil || r.conn.IsClosed() {
		log.Println("RabbitMQ connection is closed or nil, reconnecting...")
		// Reconnect logic with backoff using the stored URL
		r.conn = nil
		r.channel = nil
		backoff := 1 * time.Second
		for {
			conn, err := mq.Dial(r.url)
			if err != nil {
				log.Printf("Reconnect failed, retrying in %s: %v", backoff, err)
				time.Sleep(backoff)
				if backoff < 30*time.Second {
					backoff *= 2
				}
				continue
			}
			ch, err := conn.Channel()
			if err != nil {
				log.Printf("Channel open failed, retrying in %s: %v", backoff, err)
				_ = conn.Close()
				time.Sleep(backoff)
				if backoff < 30*time.Second {
					backoff *= 2
				}
				continue
			}
			r.conn = conn
			r.channel = ch
			break
		}
	}

	// Check if the channel is closed or nil
	if r.channel == nil || r.channel.IsClosed() {
		log.Println("RabbitMQ channel is closed or nil, reopening...")
		var err error
		r.channel, err = r.conn.Channel()
		return err
	}

	return nil
}

// Close gracefully shuts down the RabbitMQ connection and channel.
func (r *RabbitMQClient) Close() {
	// Close the channel and connection, logging any errors
	if r.channel != nil {
		if err := r.channel.Close(); err != nil {
			log.Printf("Error closing RabbitMQ channel: %v", err)
		}
	}
	if r.conn != nil {
		if err := r.conn.Close(); err != nil {
			log.Printf("Error closing RabbitMQ connection: %v", err)
		}
	}
}
