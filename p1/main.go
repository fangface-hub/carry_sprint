package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"strings"

	gateway "carry_sprint/p1/gateway/zmq"
	"carry_sprint/p1/transport/http/handler"
	httptransport "carry_sprint/p1/transport/http"
)

func envOr(key, fallback string) string {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return fallback
	}
	return v
}

func main() {
	client, err := gateway.NewClient(context.Background(), envOr("CARRY_SPRINT_ZMQ_ENDPOINT", "tcp://127.0.0.1:5557"))
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	h := &handler.Handler{Client: client}
	router := httptransport.NewRouter(h)
	addr := envOr("CARRY_SPRINT_ADDR", ":8080")
	log.Printf("P1 listening: %s", addr)
	if err := http.ListenAndServe(addr, router); err != nil {
		log.Fatal(err)
	}
}
