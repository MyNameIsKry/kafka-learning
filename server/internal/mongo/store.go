package mongo

import (
	"context"
	"errors"
	"time"

	"server/internal/api"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

type Store struct {
	client       *mongo.Client
	transactions *mongo.Collection
	predictions  *mongo.Collection
	alerts       *mongo.Collection
}

func New(ctx context.Context, uri, database string) (*Store, error) {
	client, err := mongo.Connect(options.Client().ApplyURI(uri).SetServerSelectionTimeout(5 * time.Second))
	if err != nil {
		return nil, err
	}
	store := &Store{
		client:       client,
		transactions: client.Database(database).Collection("transactions"),
		predictions:  client.Database(database).Collection("transaction_predictions"),
		alerts:       client.Database(database).Collection("transaction_alerts"),
	}
	if err := store.Ping(ctx); err != nil {
		_ = client.Disconnect(context.Background())
		return nil, err
	}
	return store, nil
}

func (s *Store) Ping(ctx context.Context) error {
	return s.client.Ping(ctx, nil)
}

func (s *Store) Close(ctx context.Context) error {
	return s.client.Disconnect(ctx)
}

func (s *Store) Summary(ctx context.Context) (api.Summary, error) {
	transactionCount, err := s.transactions.CountDocuments(ctx, bson.D{})
	if err != nil {
		return api.Summary{}, err
	}
	predictionCount, err := s.predictions.CountDocuments(ctx, bson.D{})
	if err != nil {
		return api.Summary{}, err
	}
	anomalyCount, err := s.predictions.CountDocuments(ctx, bson.D{{Key: "isAnomaly", Value: true}})
	if err != nil {
		return api.Summary{}, err
	}

	result := api.Summary{
		TotalTransactions: transactionCount,
		TotalPredictions:  predictionCount,
		TotalAnomalies:    anomalyCount,
		GeneratedAt:       time.Now().UTC(),
	}
	if predictionCount > 0 {
		result.AnomalyRate = float64(anomalyCount) / float64(predictionCount)
	}

	var latestTransaction transactionDocument
	err = s.transactions.FindOne(ctx, bson.D{}, options.FindOne().SetSort(bson.D{{Key: "step", Value: -1}})).Decode(&latestTransaction)
	if err == nil {
		result.LatestStep = latestTransaction.Step
	} else if !errors.Is(err, mongo.ErrNoDocuments) {
		return api.Summary{}, err
	}

	var latestPrediction predictionDocument
	err = s.predictions.FindOne(ctx, bson.D{}, options.FindOne().SetSort(bson.D{{Key: "processedAt", Value: -1}})).Decode(&latestPrediction)
	if err == nil {
		result.LatestModelVersion = latestPrediction.ModelVersion
	} else if !errors.Is(err, mongo.ErrNoDocuments) {
		return api.Summary{}, err
	}
	return result, nil
}

func (s *Store) Transactions(ctx context.Context, limit int, anomaliesOnly bool) ([]api.Transaction, error) {
	pipeline := transactionPipeline(limit, anomaliesOnly)
	cursor, err := s.transactions.Aggregate(ctx, pipeline)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)
	var documents []transactionWithPrediction
	if err := cursor.All(ctx, &documents); err != nil {
		return nil, err
	}
	items := make([]api.Transaction, 0, len(documents))
	for _, document := range documents {
		items = append(items, document.toAPI())
	}
	return items, nil
}

func transactionPipeline(limit int, anomaliesOnly bool) mongo.Pipeline {
	sort := bson.D{{Key: "step", Value: -1}, {Key: "sourceRowIndex", Value: -1}, {Key: "transactionId", Value: -1}}
	pipeline := mongo.Pipeline{
		bson.D{{Key: "$lookup", Value: bson.D{
			{Key: "from", Value: "transaction_predictions"},
			{Key: "let", Value: bson.D{{Key: "transactionId", Value: "$transactionId"}}},
			{Key: "pipeline", Value: mongo.Pipeline{
				bson.D{{Key: "$match", Value: bson.D{{Key: "$expr", Value: bson.D{{Key: "$eq", Value: bson.A{"$transactionId", "$$transactionId"}}}}}}},
				bson.D{{Key: "$sort", Value: bson.D{{Key: "processedAt", Value: -1}, {Key: "modelVersion", Value: -1}}}},
				bson.D{{Key: "$limit", Value: 1}},
			}},
			{Key: "as", Value: "prediction"},
		}}},
		bson.D{{Key: "$unwind", Value: bson.D{
			{Key: "path", Value: "$prediction"},
			{Key: "preserveNullAndEmptyArrays", Value: true},
		}}},
	}
	if anomaliesOnly {
		pipeline = append(pipeline, bson.D{{Key: "$match", Value: bson.D{{Key: "prediction.isAnomaly", Value: true}}}})
		// Keep the raw index order and stop after enough matching rows. Limiting
		// raw rows before the anomaly filter would omit older matching records.
		pipeline = append(pipeline, bson.D{{Key: "$limit", Value: limit}})
		return append(mongo.Pipeline{bson.D{{Key: "$sort", Value: sort}}}, pipeline...)
	}
	// Bound the raw rows before joining predictions on the common list path.
	return append(mongo.Pipeline{
		bson.D{{Key: "$sort", Value: sort}},
		bson.D{{Key: "$limit", Value: limit}},
	}, pipeline...)
}

func (s *Store) Alerts(ctx context.Context, limit int) ([]api.Alert, error) {
	options := options.Find().SetSort(bson.D{{Key: "processedAt", Value: -1}}).SetLimit(int64(limit))
	cursor, err := s.alerts.Find(ctx, bson.D{{Key: "isAnomaly", Value: true}}, options)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)
	var documents []alertDocument
	if err := cursor.All(ctx, &documents); err != nil {
		return nil, err
	}
	items := make([]api.Alert, 0, len(documents))
	for _, document := range documents {
		items = append(items, document.toAPI())
	}
	return items, nil
}

type transactionDocument struct {
	TransactionID  string  `bson:"transactionId"`
	SourceRowIndex int64   `bson:"sourceRowIndex"`
	Step           int64   `bson:"step"`
	Type           string  `bson:"type"`
	Amount         float64 `bson:"amount"`
	NameOrig       string  `bson:"nameOrig"`
	NameDest       string  `bson:"nameDest"`
	OldBalanceOrg  float64 `bson:"oldbalanceOrg"`
	NewBalanceOrig float64 `bson:"newbalanceOrig"`
	OldBalanceDest float64 `bson:"oldbalanceDest"`
	NewBalanceDest float64 `bson:"newbalanceDest"`
}

type predictionDocument struct {
	AnomalyScore float64    `bson:"anomalyScore"`
	IsAnomaly    bool       `bson:"isAnomaly"`
	ModelVersion string     `bson:"modelVersion"`
	ProcessedAt  *time.Time `bson:"processedAt"`
}

type transactionWithPrediction struct {
	TransactionID  string              `bson:"transactionId"`
	SourceRowIndex int64               `bson:"sourceRowIndex"`
	Step           int64               `bson:"step"`
	Type           string              `bson:"type"`
	Amount         float64             `bson:"amount"`
	NameOrig       string              `bson:"nameOrig"`
	NameDest       string              `bson:"nameDest"`
	OldBalanceOrg  float64             `bson:"oldbalanceOrg"`
	NewBalanceOrig float64             `bson:"newbalanceOrig"`
	OldBalanceDest float64             `bson:"oldbalanceDest"`
	NewBalanceDest float64             `bson:"newbalanceDest"`
	Prediction     *predictionDocument `bson:"prediction"`
}

func (document transactionWithPrediction) toAPI() api.Transaction {
	result := api.Transaction{
		Status:         "pending",
		TransactionID:  document.TransactionID,
		SourceRowIndex: document.SourceRowIndex,
		Step:           document.Step,
		Type:           document.Type,
		Amount:         document.Amount,
		NameOrig:       document.NameOrig,
		NameDest:       document.NameDest,
		OldBalanceOrg:  document.OldBalanceOrg,
		NewBalanceOrig: document.NewBalanceOrig,
		OldBalanceDest: document.OldBalanceDest,
		NewBalanceDest: document.NewBalanceDest,
	}
	if document.Prediction != nil {
		result.Status = "normal"
		if document.Prediction.IsAnomaly {
			result.Status = "anomaly"
		}
		result.AnomalyScore = &document.Prediction.AnomalyScore
		result.IsAnomaly = document.Prediction.IsAnomaly
		result.ModelVersion = document.Prediction.ModelVersion
		result.ProcessedAt = document.Prediction.ProcessedAt
	}
	return result
}

type alertDocument struct {
	TransactionID string    `bson:"transactionId"`
	Step          int64     `bson:"step"`
	AnomalyScore  float64   `bson:"anomalyScore"`
	ModelVersion  string    `bson:"modelVersion"`
	ProcessedAt   time.Time `bson:"processedAt"`
	ReasonCodes   []string  `bson:"reasonCodes"`
}

func (document alertDocument) toAPI() api.Alert {
	if document.ReasonCodes == nil {
		document.ReasonCodes = []string{}
	}
	return api.Alert{
		TransactionID: document.TransactionID,
		Step:          document.Step,
		AnomalyScore:  document.AnomalyScore,
		ModelVersion:  document.ModelVersion,
		ProcessedAt:   document.ProcessedAt,
		ReasonCodes:   document.ReasonCodes,
	}
}
