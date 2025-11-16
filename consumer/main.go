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
	"time"
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
		cfg.AutoAck = true
		cfg.Prefetch = 1
		cfg.WorkerCount = 1

	case "B":
		cfg.AutoAck = false
		cfg.Prefetch = 1
		cfg.WorkerCount = 1

	case "C":
		cfg.AutoAck = false
		cfg.Prefetch = 3
		cfg.WorkerCount = 6

	default:
		log.Fatalf("unknown scanrio: %q", scenario)
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

	// Optional: tweak flags – add date/time + microseconds + short file
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds)

	log.Printf("Logging to file %s", path)

	return f, nil
}

func main() {

	cfg := loadConfig()

	logFile, err := setupLogger(cfg)
	if err != nil {
		// If we can't log to file, that's serious enough to stop
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
			log.Printf(
				"[Scenario=%s Consumer=%s] Received message: %s",
				cfg.Scenario,
				cfg.ConsumerID,
				string(msg.Body),
			)
		}
		// If we reach here, the msgs channel closed. Loop will attempt to re-consume.
		log.Println("Message channel closed, attempting to re-establish consumer...")
		time.Sleep(2 * time.Second)
	}
}
