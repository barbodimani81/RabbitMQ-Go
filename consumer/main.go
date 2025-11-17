package main

import (
	"final/rabbit"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type ConsumerConfig struct {
	Scenario    string
	AutoAck     bool
	Prefetch    int
	WorkerCount int
	ConsumerID  string
}

func loadConfig() ConsumerConfig {
	scenarioFlag := flag.String("scenario", "A", "Scenarios: A, B, C")
	consumerIDFlag := flag.String("consumer-id", "1", "Identifier for consumer")

	flag.Parse()

	scenario := strings.ToUpper(strings.TrimSpace(*scenarioFlag))
	consumerID := strings.TrimSpace(*consumerIDFlag)

	cfg := ConsumerConfig{
		Scenario:   scenario,
		ConsumerID: consumerID,
	}

	switch scenario {
	case "A":
		// Auto-ack, sequential
		cfg.AutoAck = true
		cfg.Prefetch = 1
		cfg.WorkerCount = 1

	case "B":
		// Manual ack, sequential
		cfg.AutoAck = false
		cfg.Prefetch = 1
		cfg.WorkerCount = 1

	case "C":
		// Manual ack, concurrent
		// 3 workers, prefetch=3 -> matches our “3 tasks in ~5s” narrative
		cfg.AutoAck = false
		cfg.Prefetch = 3
		cfg.WorkerCount = 3

	default:
		log.Fatalf("unknown scenario: %q (use A, B, or C)", scenario)
	}

	return cfg
}

// setupLogger configures the global logger to write into a scenario- and consumer-specific file.
func setupLogger(cfg ConsumerConfig) (*os.File, error) {
	// Ensure logs directory exists
	if err := os.MkdirAll("logs", 0o755); err != nil {
		return nil, fmt.Errorf("failed to create logs directory: %w", err)
	}

	filename := fmt.Sprintf(
		"scenario_%s_consumer%s.log",
		cfg.Scenario,
		cfg.ConsumerID,
	)
	path := filepath.Join("logs", filename)

	f, err := os.Create(path)
	if err != nil {
		return nil, fmt.Errorf("failed to create log file %q: %w", path, err)
	}

	// Log to both file and stdout so you see something in the terminal too
	mw := io.MultiWriter(os.Stdout, f)
	log.SetOutput(mw)

	// date + time + microseconds
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds)

	log.Printf("Logging to file %s", path)

	return f, nil
}

func main() {
	cfg := loadConfig()

	logFile, err := setupLogger(cfg)
	if err != nil {
		log.Fatalf("Failed to set up logger: %s", err)
	}
	defer logFile.Close()

	log.Printf(
		"Starting: Scenario=%s ConsumerID=%s AutoAck=%t Prefetch=%d Workers=%d",
		cfg.Scenario, cfg.ConsumerID, cfg.AutoAck, cfg.Prefetch, cfg.WorkerCount,
	)

	client, err := rabbit.NewRabbitMQClient("amqp://guest:guest@localhost:5672/")
	if err != nil {
		log.Fatalf("Failed to connect to RabbitMQ: %s", err)
	}
	defer client.Close()

	if err := client.SetQos(cfg.Prefetch); err != nil {
		log.Fatalf("Failed to set QoS (prefetch=%d): %s", cfg.Prefetch, err)
	}

	queueName := "logs" // must match your queue

	switch cfg.Scenario {
	case "A", "B":
		runScenarioAB(cfg, client, queueName)
	case "C":
		runScenarioC(cfg, client, queueName)
	default:
		log.Fatalf("Unsupported scenario: %s", cfg.Scenario)
	}
}

// Scenario A & B: sequential consumer
func runScenarioAB(cfg ConsumerConfig, client *rabbit.RabbitMQClient, queueName string) {
	msgs, err := client.Consume(queueName, cfg.AutoAck)
	if err != nil {
		log.Fatalf("Failed to start consuming from queue %q: %s", queueName, err)
	}

	var (
		firstMessageTime time.Time
		messageCount     int
	)

	log.Println("Consumer is now waiting for messages (sequential)...")

	// Different work durations per scenario just for clearer comparison.
	// workDuration := 1 * time.Second
	// if cfg.Scenario == "B" {
	// 	workDuration = 2 * time.Second
	// }

	for msg := range msgs {
		if firstMessageTime.IsZero() {
			firstMessageTime = time.Now()
			log.Printf("[Scenario=%s Consumer=%s] First message received at %s",
				cfg.Scenario, cfg.ConsumerID, firstMessageTime.Format(time.RFC3339Nano))
		}

		messageCount++
		now := time.Now()
		elapsed := now.Sub(firstMessageTime)

		log.Printf(
			"[Scenario=%s Consumer=%s] Message #%d body=%q elapsed_since_first=%s",
			cfg.Scenario,
			cfg.ConsumerID,
			messageCount,
			string(msg.Body),
			elapsed,
		)

		// Simulate work
		// time.Sleep(workDuration)

		// Manual ack if auto-ack is disabled (Scenario B)
		if !cfg.AutoAck {
			if err := msg.Ack(false); err != nil {
				log.Printf(
					"[Scenario=%s Consumer=%s] FAILED to ack message #%d: %s",
					cfg.Scenario,
					cfg.ConsumerID,
					messageCount,
					err,
				)
			} else {
				log.Printf(
					"[Scenario=%s Consumer=%s] Acked message #%d",
					cfg.Scenario,
					cfg.ConsumerID,
					messageCount,
				)
			}
		}
	}

	// Channel closed -> log total elapsed
	if !firstMessageTime.IsZero() {
		totalElapsed := time.Since(firstMessageTime)
		log.Printf("[Scenario=%s Consumer=%s] Message stream ended. Total elapsed since first message: %s (messages=%d)",
			cfg.Scenario, cfg.ConsumerID, totalElapsed, messageCount)
	} else {
		log.Printf("[Scenario=%s Consumer=%s] No messages received.",
			cfg.Scenario, cfg.ConsumerID)
	}
}

// Scenario C: worker pool with manual ack
func runScenarioC(cfg ConsumerConfig, client *rabbit.RabbitMQClient, queueName string) {
	if cfg.AutoAck {
		log.Fatalf("Scenario C must use manual ack (AutoAck=false)")
	}

	msgs, err := client.Consume(queueName, cfg.AutoAck)
	if err != nil {
		log.Fatalf("Failed to start consuming from queue %q: %s", queueName, err)
	}

	log.Println("Consumer is now waiting for messages (concurrent worker pool)...")

	// Work duration in C: heavier, to make concurrency obvious
	// workDuration := 3 * time.Second

	// Jobs channel for workers
	jobs := make(chan amqp.Delivery, cfg.WorkerCount)

	var (
		firstMessageTime time.Time
		messageCount     int
		mu               sync.Mutex
	)

	// Start workers
	var wg sync.WaitGroup
	for i := 1; i <= cfg.WorkerCount; i++ {
		wg.Add(1)
		workerID := i

		go func(id int) {
			defer wg.Done()
			for msg := range jobs {
				start := time.Now()
				log.Printf(
					"[Scenario=%s Consumer=%s Worker=%d] START processing body=%q at %s",
					cfg.Scenario,
					cfg.ConsumerID,
					id,
					string(msg.Body),
					start.Format(time.RFC3339Nano),
				)

				// time.Sleep(workDuration)

				if err := msg.Ack(false); err != nil {
					log.Printf(
						"[Scenario=%s Consumer=%s Worker=%d] FAILED to ack message: %s",
						cfg.Scenario,
						cfg.ConsumerID,
						id,
						err,
					)
				} else {
					end := time.Now()
					log.Printf(
						"[Scenario=%s Consumer=%s Worker=%d] FINISH processing body=%q at %s (duration=%s)",
						cfg.Scenario,
						cfg.ConsumerID,
						id,
						string(msg.Body),
						end.Format(time.RFC3339Nano),
						end.Sub(start),
					)
				}
			}
		}(workerID)
	}

	// Dispatcher: read from msgs, log receipt, push into jobs
	for msg := range msgs {
		now := time.Now()

		mu.Lock()
		if firstMessageTime.IsZero() {
			firstMessageTime = now
			log.Printf("[Scenario=%s Consumer=%s] First message received at %s",
				cfg.Scenario, cfg.ConsumerID, firstMessageTime.Format(time.RFC3339Nano))
		}
		messageCount++
		elapsed := now.Sub(firstMessageTime)
		mu.Unlock()

		log.Printf(
			"[Scenario=%s Consumer=%s] DISPATCH message #%d body=%q elapsed_since_first=%s",
			cfg.Scenario,
			cfg.ConsumerID,
			messageCount,
			string(msg.Body),
			elapsed,
		)

		jobs <- msg
	}

	// No more messages from RabbitMQ, close the jobs channel, wait for workers
	close(jobs)
	wg.Wait()

	if !firstMessageTime.IsZero() {
		totalElapsed := time.Since(firstMessageTime)
		log.Printf("[Scenario=%s Consumer=%s] All workers done. Total elapsed since first message: %s (messages=%d, workers=%d)",
			cfg.Scenario, cfg.ConsumerID, totalElapsed, messageCount, cfg.WorkerCount)
	} else {
		log.Printf("[Scenario=%s Consumer=%s] No messages received.",
			cfg.Scenario, cfg.ConsumerID)
	}
}
