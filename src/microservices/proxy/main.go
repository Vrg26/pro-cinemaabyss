package main

import (
	"encoding/json"
	"log"
	"math/rand"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"strconv"
	"strings"
)

// Config holds proxy configuration from environment variables
type Config struct {
	Port                   string
	MonolithURL            *url.URL
	MoviesServiceURL       *url.URL
	EventsServiceURL       *url.URL
	GradualMigration       bool
	MoviesMigrationPercent int
}

func main() {
	cfg := loadConfig()

	http.HandleFunc("/health", handleHealth)
	http.HandleFunc("/api/movies", handleMovies(cfg))
	http.HandleFunc("/api/movies/", handleMovies(cfg))
	http.HandleFunc("/api/events/", proxyTo(cfg.EventsServiceURL))
	http.HandleFunc("/api/users", proxyTo(cfg.MonolithURL))
	http.HandleFunc("/api/users/", proxyTo(cfg.MonolithURL))
	http.HandleFunc("/api/payments", proxyTo(cfg.MonolithURL))
	http.HandleFunc("/api/payments/", proxyTo(cfg.MonolithURL))
	http.HandleFunc("/api/subscriptions", proxyTo(cfg.MonolithURL))
	http.HandleFunc("/api/subscriptions/", proxyTo(cfg.MonolithURL))

	log.Printf("Proxy service starting on port %s", cfg.Port)
	log.Printf("Monolith: %s, Movies: %s, Events: %s", cfg.MonolithURL, cfg.MoviesServiceURL, cfg.EventsServiceURL)
	log.Printf("Gradual migration: %v, Movies migration percent: %d%%", cfg.GradualMigration, cfg.MoviesMigrationPercent)
	log.Fatal(http.ListenAndServe(":"+cfg.Port, nil))
}

func loadConfig() Config {
	port := os.Getenv("PORT")
	if port == "" {
		port = "8000"
	}

	monolithURL := parseURL(os.Getenv("MONOLITH_URL"), "http://localhost:8080")
	moviesURL := parseURL(os.Getenv("MOVIES_SERVICE_URL"), "http://localhost:8081")
	eventsURL := parseURL(os.Getenv("EVENTS_SERVICE_URL"), "http://localhost:8082")

	gradual := strings.ToLower(os.Getenv("GRADUAL_MIGRATION")) == "true"

	percent := 0
	if p, err := strconv.Atoi(os.Getenv("MOVIES_MIGRATION_PERCENT")); err == nil {
		percent = p
	}

	return Config{
		Port:                   port,
		MonolithURL:            monolithURL,
		MoviesServiceURL:       moviesURL,
		EventsServiceURL:       eventsURL,
		GradualMigration:       gradual,
		MoviesMigrationPercent: percent,
	}
}

func parseURL(envValue, fallback string) *url.URL {
	raw := envValue
	if raw == "" {
		raw = fallback
	}
	u, err := url.Parse(raw)
	if err != nil {
		log.Fatalf("Invalid URL: %s — %v", raw, err)
	}
	return u
}

func handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]bool{"status": true})
}

// handleMovies implements Strangler Fig pattern: routes /api/movies
// to movies-service or monolith based on MOVIES_MIGRATION_PERCENT
func handleMovies(cfg Config) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var target *url.URL

		if cfg.GradualMigration && rand.Intn(100) < cfg.MoviesMigrationPercent {
			target = cfg.MoviesServiceURL
			log.Printf("[PROXY] %s %s → movies-service", r.Method, r.URL.Path)
		} else {
			target = cfg.MonolithURL
			log.Printf("[PROXY] %s %s → monolith", r.Method, r.URL.Path)
		}

		proxy := httputil.NewSingleHostReverseProxy(target)
		proxy.ServeHTTP(w, r)
	}
}

// proxyTo creates a handler that forwards all requests to the given target
func proxyTo(target *url.URL) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		log.Printf("[PROXY] %s %s → %s", r.Method, r.URL.Path, target.Host)
		proxy := httputil.NewSingleHostReverseProxy(target)
		proxy.ServeHTTP(w, r)
	}
}
