package main

import (
	"context"
	"log"
	"os"
	"strings"

	adapter "carry_sprint/p2/adapter/zmq"
	"carry_sprint/p2/application/usecase"
	"carry_sprint/p2/infrastructure/sqlite"
)

func envOr(key, fallback string) string {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return fallback
	}
	return v
}

func main() {
	dataDir := envOr("CARRY_SPRINT_DATA_DIR", "data")
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		log.Fatal(err)
	}
	mgr, err := sqlite.NewManager(dataDir)
	if err != nil {
		log.Fatal(err)
	}
	defer mgr.Close()

	svc := &usecase.Service{DB: mgr}
	d := &adapter.Dispatcher{Service: svc}
	server := &adapter.Server{Endpoint: envOr("CARRY_SPRINT_ZMQ_ENDPOINT", "tcp://127.0.0.1:5557"), Dispatcher: d}
	log.Printf("P2 listening: %s", server.Endpoint)
	if err := server.Run(context.Background()); err != nil {
		log.Fatal(err)
	}
}
