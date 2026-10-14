package mongo

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestTransactionPipeline(t *testing.T) {
	for _, anomaliesOnly := range []bool{false, true} {
		status := ""
		if anomaliesOnly {
			status = "anomaly"
		}
		pipeline := transactionPipeline(12, 0, status)
		if !anomaliesOnly {
			if pipeline[0][0].Key != "$sort" || pipeline[1][0].Key != "$skip" || pipeline[2][0].Key != "$limit" || pipeline[3][0].Key != "$lookup" {
				t.Fatal("normal list must sort and limit before lookup")
			}
		} else {
			if pipeline[0][0].Key != "$lookup" || pipeline[2][0].Key != "$match" || pipeline[3][0].Key != "$sort" || pipeline[4][0].Key != "$skip" || pipeline[5][0].Key != "$limit" {
				t.Fatal("anomaly list must filter joined predictions before paging")
			}
		}
		sortIndex := 0
		if anomaliesOnly {
			sortIndex = 3
		}
		sort := pipeline[sortIndex][0].Value.(bson.D)
		if len(sort) != 3 || sort[1].Key != "sourceRowIndex" || sort[2].Key != "transactionId" {
			t.Fatal("transaction ordering must include stable tie-breakers")
		}
	}
}

func TestTransactionStatus(t *testing.T) {
	for _, test := range []struct {
		name       string
		prediction *predictionDocument
		want       string
	}{
		{"unscored", nil, "pending"},
		{"scored normal", &predictionDocument{IsAnomaly: false}, "normal"},
		{"scored anomaly", &predictionDocument{IsAnomaly: true}, "anomaly"},
	} {
		t.Run(test.name, func(t *testing.T) {
			result := (transactionWithPrediction{TransactionID: "x", Prediction: test.prediction}).toAPI()
			if result.Status != test.want {
				t.Fatalf("status = %q; want %q", result.Status, test.want)
			}
			if test.prediction == nil && result.AnomalyScore != nil {
				t.Fatal("unscored transaction must not have a score")
			}
		})
	}
}

// Opt-in integration test uses a unique database and only deletes its own data.
func TestMongoTransactionQueries(t *testing.T) {
	uri := os.Getenv("MONGO_TEST_URI")
	if uri == "" {
		t.Skip("set MONGO_TEST_URI to run MongoDB integration coverage")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	name := fmt.Sprintf("phase6_p2_test_%d", time.Now().UnixNano())
	store, err := New(ctx, uri, name)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := store.client.Database(name).Drop(cleanupCtx); err != nil {
			t.Errorf("drop test database: %v", err)
		}
		_ = store.Close(cleanupCtx)
	})
	_, err = store.transactions.InsertMany(ctx, []any{
		bson.M{"transactionId": "anomaly", "step": 10, "sourceRowIndex": 1, "oldbalanceOrg": 100.0},
		bson.M{"transactionId": "normal", "step": 10, "sourceRowIndex": 2},
		bson.M{"transactionId": "pending", "step": 10, "sourceRowIndex": 3},
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = store.predictions.InsertMany(ctx, []any{
		bson.M{"transactionId": "anomaly", "isAnomaly": true, "anomalyScore": .95, "modelVersion": "v1", "processedAt": time.Now()},
		bson.M{"transactionId": "normal", "isAnomaly": false, "anomalyScore": .1, "modelVersion": "v1", "processedAt": time.Now()},
	})
	if err != nil {
		t.Fatal(err)
	}
	items, err := store.Transactions(ctx, 1, 0, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 1 || items[0].TransactionID != "pending" || items[0].Status != "pending" {
		t.Fatalf("latest row = %+v; want pending", items)
	}
	items, err = store.Transactions(ctx, 1, 0, "anomaly")
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 1 || items[0].TransactionID != "anomaly" || items[0].Status != "anomaly" || items[0].OldBalanceOrg != 100 {
		t.Fatalf("filtered rows = %+v; want older anomaly with balance", items)
	}
	items, err = store.Transactions(ctx, 1, 0, "normal")
	if err != nil || len(items) != 1 || items[0].TransactionID != "normal" || items[0].Status != "normal" {
		t.Fatalf("normal filtered rows = %+v; err=%v", items, err)
	}
	items, err = store.Transactions(ctx, 1, 0, "pending")
	if err != nil || len(items) != 1 || items[0].TransactionID != "pending" || items[0].Status != "pending" {
		t.Fatalf("pending filtered rows = %+v; err=%v", items, err)
	}
}
