package main

import (
	"context"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
)

var kafkaBrokers []string

// Kafka topics
const (
	TopicMovieEvents   = "movie-events"
	TopicUserEvents    = "user-events"
	TopicPaymentEvents = "payment-events"
)

func main() {
	brokersEnv := os.Getenv("KAFKA_BROKERS")
	if brokersEnv == "" {
		brokersEnv = "localhost:9092"
	}
	kafkaBrokers = strings.Split(brokersEnv, ",")

	port := os.Getenv("PORT")
	if port == "" {
		port = "8082"
	}

	// Start Kafka consumers in background goroutines
	go startConsumer(TopicMovieEvents)
	go startConsumer(TopicUserEvents)
	go startConsumer(TopicPaymentEvents)

	// HTTP routes
	http.HandleFunc("/api/events/health", handleHealth)
	http.HandleFunc("/api/events/movie", handleMovieEvent)
	http.HandleFunc("/api/events/user", handleUserEvent)
	http.HandleFunc("/api/events/payment", handlePaymentEvent)

	log.Printf("Events service starting on port %s", port)
	log.Printf("Kafka brokers: %v", kafkaBrokers)
	log.Fatal(http.ListenAndServe(":"+port, nil))
}

func handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]bool{"status": true})
}

func handleMovieEvent(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	handleEvent(w, r, TopicMovieEvents)
}

func handleUserEvent(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	handleEvent(w, r, TopicUserEvents)
}

func handlePaymentEvent(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	handleEvent(w, r, TopicPaymentEvents)
}

// handleEvent reads request body, publishes to Kafka topic, returns 201
func handleEvent(w http.ResponseWriter, r *http.Request, topic string) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Failed to read request body", http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	log.Printf("[PRODUCER] Publishing to topic %s: %s", topic, string(body))

	err = publishEvent(topic, body)
	if err != nil {
		log.Printf("[PRODUCER] Error publishing to %s: %v", topic, err)
		http.Error(w, "Failed to publish event", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(map[string]string{"status": "success"})
}

// publishEvent writes a message to the specified Kafka topic
func publishEvent(topic string, value []byte) error {
	writer := &kafka.Writer{
		Addr:         kafka.TCP(kafkaBrokers...),
		Topic:        topic,
		Balancer:     &kafka.LeastBytes{},
		BatchTimeout: 10 * time.Millisecond,
	}
	defer writer.Close()

	return writer.WriteMessages(context.Background(),
		kafka.Message{
			Value: value,
		},
	)
}

// startConsumer reads messages from a Kafka topic and logs them
func startConsumer(topic string) {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  kafkaBrokers,
		Topic:    topic,
		GroupID:  "events-service-" + topic,
		MinBytes: 1,
		MaxBytes: 10e6,
	})
	defer reader.Close()

	log.Printf("[CONSUMER] Started consumer for topic: %s", topic)

	for {
		msg, err := reader.ReadMessage(context.Background())
		if err != nil {
			log.Printf("[CONSUMER] Error reading from %s: %v", topic, err)
			time.Sleep(time.Second)
			continue
		}
		log.Printf("[CONSUMER] Topic: %s | Partition: %d | Offset: %d | Value: %s",
			msg.Topic, msg.Partition, msg.Offset, string(msg.Value))
	}
}
