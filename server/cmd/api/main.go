package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"time"

	"server/internal/api"
	mongostore "server/internal/mongo"
)

func main() {
	addr := getenv("API_ADDR", ":8080")
	mongoURI := getenv("MONGO_URI", "mongodb://localhost:27017")
	database := getenv("MONGO_DATABASE", "financial_anomaly")
	corsOrigin := getenv("API_CORS_ORIGIN", "http://localhost:5173")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	store, err := mongostore.New(ctx, mongoURI, database)
	if err != nil {
		log.Fatalf("connect to MongoDB: %v", err)
	}
	defer func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer closeCancel()
		if closeErr := store.Close(closeCtx); closeErr != nil {
			log.Printf("close MongoDB: %v", closeErr)
		}
	}()

	server := &http.Server{
		Addr:              addr,
		Handler:           api.NewHandler(store, corsOrigin),
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	log.Printf("financial anomaly API listening on %s", addr)
	log.Fatal(server.ListenAndServe())
}

func getenv(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
