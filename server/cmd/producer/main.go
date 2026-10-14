package main

import (
	"container/heap"
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"

	"server/internal/events"
	"server/internal/kafkaio"
	pipelinemetrics "server/internal/metrics"
)

var paysimColumns = []string{
	"step", "type", "amount", "nameOrig", "oldbalanceOrg", "newbalanceOrig",
	"nameDest", "oldbalanceDest", "newbalanceDest", "isFraud", "isFlaggedFraud",
}

type prepared struct {
	Raw   events.RawTransactionEvent `json:"raw"`
	Truth events.GroundTruthDTO      `json:"truth"`
}

type preparedSource struct {
	tempDir string
	chunks  []string
	count   int
}

func (source *preparedSource) Close() { _ = os.RemoveAll(source.tempDir) }

type mergeItem struct {
	record prepared
	chunk  int
}

type mergeHeap []mergeItem

func (items mergeHeap) Len() int { return len(items) }
func (items mergeHeap) Less(left, right int) bool {
	if items[left].record.Raw.Step == items[right].record.Raw.Step {
		if items[left].record.Raw.SourceRowIndex == items[right].record.Raw.SourceRowIndex {
			return items[left].chunk < items[right].chunk
		}
		return items[left].record.Raw.SourceRowIndex < items[right].record.Raw.SourceRowIndex
	}
	return items[left].record.Raw.Step < items[right].record.Raw.Step
}
func (items mergeHeap) Swap(left, right int) { items[left], items[right] = items[right], items[left] }
func (items *mergeHeap) Push(value any)      { *items = append(*items, value.(mergeItem)) }
func (items *mergeHeap) Pop() any {
	old := *items
	last := old[len(old)-1]
	*items = old[:len(old)-1]
	return last
}

func (source *preparedSource) ForEach(visit func(prepared) error) error {
	files := make([]*os.File, len(source.chunks))
	decoders := make([]*json.Decoder, len(source.chunks))
	defer func() {
		for _, file := range files {
			if file != nil {
				_ = file.Close()
			}
		}
	}()
	items := &mergeHeap{}
	for index, path := range source.chunks {
		file, err := os.Open(path)
		if err != nil {
			return err
		}
		files[index] = file
		decoders[index] = json.NewDecoder(file)
		var record prepared
		if err := decoders[index].Decode(&record); err != nil {
			return err
		}
		heap.Push(items, mergeItem{record: record, chunk: index})
	}
	heap.Init(items)
	for items.Len() > 0 {
		item := heap.Pop(items).(mergeItem)
		if err := visit(item.record); err != nil {
			return err
		}
		var next prepared
		if err := decoders[item.chunk].Decode(&next); err != nil {
			if errors.Is(err, io.EOF) {
				continue
			}
			return err
		}
		heap.Push(items, mergeItem{record: next, chunk: item.chunk})
	}
	return nil
}

func main() {
	var (
		csvPath         = flag.String("csv", "", "PaySim CSV path")
		groundTruthPath = flag.String("ground-truth-path", "data/ground_truth.csv", "offline ground truth output")
		brokers         = flag.String("bootstrap-servers", "localhost:9092", "Kafka bootstrap servers")
		topic           = flag.String("topic", "transactions.raw", "raw Kafka topic")
		eventsPerSecond = flag.Float64("events-per-second", 0, "replay rate; zero means unthrottled")
		datasetVersion  = flag.String("dataset-version", "", "optional dataset ID segment")
		maxEvents       = flag.Int("max-events", 0, "maximum rows for a demo; zero means all rows")
		chunkSize       = flag.Int("sort-chunk-size", 10000, "rows held in memory per external-sort chunk")
		dryRun          = flag.Bool("dry-run", false, "validate and write ground truth without Kafka")
		metricsPort     = flag.Int("metrics-port", 0, "Prometheus port; zero disables metrics")
	)
	flag.Parse()
	if *csvPath == "" {
		log.Fatal("-csv is required")
	}
	if *eventsPerSecond < 0 || *maxEvents < 0 {
		log.Fatal("events-per-second and max-events must be non-negative")
	}
	source, err := readPaySim(*csvPath, *datasetVersion, *maxEvents, *chunkSize)
	if err != nil {
		log.Fatal(err)
	}
	defer source.Close()
	if source.count == 0 {
		log.Fatal("PaySim CSV contains no records")
	}
	if *dryRun {
		if err := writeGroundTruth(source, *groundTruthPath); err != nil {
			log.Fatal(err)
		}
		log.Printf("validated %d records; no Kafka publish", source.count)
		return
	}

	writer := kafkaio.NewWriter(kafkaio.Brokers(*brokers), *topic)
	defer writer.Close()
	metrics, err := pipelinemetrics.New("producer", *metricsPort)
	if err != nil {
		log.Fatal(err)
	}
	defer metrics.Close(context.Background())
	if err := replay(context.Background(), source, writer, *topic, *groundTruthPath, *eventsPerSecond, metrics); err != nil {
		log.Fatal(err)
	}
	log.Printf("replay finished: published=%d", source.count)
}

func readPaySim(path, datasetVersion string, maxEvents, chunkSize int) (*preparedSource, error) {
	if chunkSize <= 0 {
		return nil, errors.New("sort-chunk-size must be positive")
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open PaySim CSV: %w", err)
	}
	defer file.Close()
	reader := csv.NewReader(file)
	reader.FieldsPerRecord = -1
	header, err := reader.Read()
	if err != nil {
		return nil, fmt.Errorf("read PaySim header: %w", err)
	}
	if len(header) != len(paysimColumns) {
		return nil, fmt.Errorf("invalid PaySim header: expected %d columns", len(paysimColumns))
	}
	seenHeaders := map[string]bool{}
	for index, value := range header {
		name := strings.TrimSpace(value)
		if name != paysimColumns[index] || seenHeaders[name] {
			return nil, fmt.Errorf("invalid PaySim header at column %d: %q", index, value)
		}
		seenHeaders[name] = true
	}

	tempDir, err := os.MkdirTemp("", "go-paysim-sort-")
	if err != nil {
		return nil, fmt.Errorf("create sort temp directory: %w", err)
	}
	source := &preparedSource{tempDir: tempDir}
	pending := make([]prepared, 0, chunkSize)
	flush := func() error {
		if len(pending) == 0 {
			return nil
		}
		sort.SliceStable(pending, func(i, j int) bool {
			if pending[i].Raw.Step == pending[j].Raw.Step {
				return pending[i].Raw.SourceRowIndex < pending[j].Raw.SourceRowIndex
			}
			return pending[i].Raw.Step < pending[j].Raw.Step
		})
		path := filepath.Join(tempDir, fmt.Sprintf("chunk-%08d.jsonl", len(source.chunks)))
		file, createErr := os.Create(path)
		if createErr != nil {
			return createErr
		}
		encoder := json.NewEncoder(file)
		for _, record := range pending {
			if encodeErr := encoder.Encode(record); encodeErr != nil {
				_ = file.Close()
				return encodeErr
			}
		}
		if closeErr := file.Close(); closeErr != nil {
			return closeErr
		}
		source.chunks = append(source.chunks, path)
		pending = pending[:0]
		return nil
	}
	for sourceRowIndex := 0; ; sourceRowIndex++ {
		if maxEvents > 0 && sourceRowIndex >= maxEvents {
			break
		}
		row, readErr := reader.Read()
		if readErr != nil {
			if errors.Is(readErr, io.EOF) {
				break
			}
			source.Close()
			return nil, fmt.Errorf("read PaySim row %d: %w", sourceRowIndex, readErr)
		}
		if len(row) != len(paysimColumns) {
			source.Close()
			return nil, fmt.Errorf("row %d has %d columns; expected %d", sourceRowIndex, len(row), len(paysimColumns))
		}
		item, parseErr := parseRow(row, sourceRowIndex, datasetVersion)
		if parseErr != nil {
			source.Close()
			return nil, parseErr
		}
		pending = append(pending, item)
		source.count++
		if len(pending) >= chunkSize {
			if err := flush(); err != nil {
				source.Close()
				return nil, err
			}
		}
	}
	if err := flush(); err != nil {
		source.Close()
		return nil, err
	}
	return source, nil
}

func parseRow(row []string, sourceRowIndex int, datasetVersion string) (prepared, error) {
	step, err := parseInt(row[0], "step", sourceRowIndex)
	if err != nil {
		return prepared{}, err
	}
	isFraud, err := parseInt(row[9], "isFraud", sourceRowIndex)
	if err != nil {
		return prepared{}, err
	}
	isFlagged, err := parseInt(row[10], "isFlaggedFraud", sourceRowIndex)
	if err != nil {
		return prepared{}, err
	}
	if isFraud < 0 || isFraud > 1 || isFlagged < 0 || isFlagged > 1 {
		return prepared{}, fmt.Errorf("row %d: labels must be 0 or 1", sourceRowIndex)
	}
	amount, err := parseMoney(row[2], "amount", sourceRowIndex)
	if err != nil {
		return prepared{}, err
	}
	oldOrg, err := parseMoney(row[4], "oldbalanceOrg", sourceRowIndex)
	if err != nil {
		return prepared{}, err
	}
	newOrg, err := parseMoney(row[5], "newbalanceOrig", sourceRowIndex)
	if err != nil {
		return prepared{}, err
	}
	oldDest, err := parseMoney(row[7], "oldbalanceDest", sourceRowIndex)
	if err != nil {
		return prepared{}, err
	}
	newDest, err := parseMoney(row[8], "newbalanceDest", sourceRowIndex)
	if err != nil {
		return prepared{}, err
	}
	transactionID := "paysim-" + strconv.Itoa(sourceRowIndex)
	if strings.TrimSpace(datasetVersion) != "" {
		transactionID = "paysim-" + strings.TrimSpace(datasetVersion) + "-" + strconv.Itoa(sourceRowIndex)
	}
	item := prepared{
		Raw: events.RawTransactionEvent{
			SchemaVersion: events.RawSchemaVersion, TransactionID: transactionID,
			SourceRowIndex: int64(sourceRowIndex), Step: step, Type: strings.TrimSpace(row[1]),
			Amount: amount, NameOrig: strings.TrimSpace(row[3]), OldBalanceOrg: oldOrg,
			NewBalanceOrig: newOrg, NameDest: strings.TrimSpace(row[6]), OldBalanceDest: oldDest,
			NewBalanceDest: newDest,
		},
		Truth: events.GroundTruthDTO{TransactionID: transactionID, Step: step, IsFraud: int(isFraud), IsFlaggedFraud: int(isFlagged)},
	}
	if err := item.Raw.Validate(); err != nil {
		return prepared{}, fmt.Errorf("row %d: %w", sourceRowIndex, err)
	}
	return item, nil
}

func parseInt(value, field string, row int) (int64, error) {
	parsed, err := strconv.ParseInt(strings.TrimSpace(value), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("row %d: %s must be an integer", row, field)
	}
	return parsed, nil
}

func parseMoney(value, field string, row int) (float64, error) {
	parsed, err := strconv.ParseFloat(strings.TrimSpace(value), 64)
	if err != nil || math.IsNaN(parsed) || math.IsInf(parsed, 0) || parsed < 0 {
		return 0, fmt.Errorf("row %d: %s must be finite and non-negative", row, field)
	}
	return parsed, nil
}

func writeGroundTruth(source *preparedSource, path string) error {
	return withAtomicFile(path, func(writer *csv.Writer) error {
		if err := writer.Write([]string{"transactionId", "step", "isFraud", "isFlaggedFraud"}); err != nil {
			return err
		}
		return source.ForEach(func(record prepared) error {
			return writer.Write([]string{record.Truth.TransactionID, strconv.FormatInt(record.Truth.Step, 10), strconv.Itoa(record.Truth.IsFraud), strconv.Itoa(record.Truth.IsFlaggedFraud)})
		})
	})
}

func replay(ctx context.Context, source *preparedSource, writer *kafka.Writer, topic, groundTruthPath string, rate float64, metrics *pipelinemetrics.Metrics) error {
	return withAtomicFile(groundTruthPath, func(csvWriter *csv.Writer) error {
		if err := csvWriter.Write([]string{"transactionId", "step", "isFraud", "isFlaggedFraud"}); err != nil {
			return err
		}
		interval := time.Duration(0)
		if rate > 0 {
			interval = time.Duration(float64(time.Second) / rate)
		}
		nextSend := time.Now()
		return source.ForEach(func(record prepared) error {
			if interval > 0 {
				if delay := time.Until(nextSend); delay > 0 {
					time.Sleep(delay)
				}
				nextSend = nextSend.Add(interval)
			}
			payload, err := events.Encode(record.Raw)
			if err != nil {
				return err
			}
			started := time.Now()
			if err := kafkaio.Publish(ctx, writer, record.Raw.NameOrig, payload); err != nil {
				metrics.Failure("publish")
				return fmt.Errorf("publish %s: %w", record.Raw.TransactionID, err)
			}
			metrics.Delivery(time.Since(started).Seconds())
			metrics.Published(topic)
			if err := csvWriter.Write([]string{
				record.Truth.TransactionID, strconv.FormatInt(record.Truth.Step, 10),
				strconv.Itoa(record.Truth.IsFraud), strconv.Itoa(record.Truth.IsFlaggedFraud),
			}); err != nil {
				return err
			}
			return nil
		})
	})
}

func withAtomicFile(path string, write func(*csv.Writer) error) error {
	directory := filepath.Dir(path)
	if err := os.MkdirAll(directory, 0o755); err != nil {
		return err
	}
	temporary, err := os.CreateTemp(directory, ".ground-truth-*.tmp")
	if err != nil {
		return err
	}
	temporaryPath := temporary.Name()
	cleanup := func() { _ = temporary.Close(); _ = os.Remove(temporaryPath) }
	writer := csv.NewWriter(temporary)
	if err := write(writer); err != nil {
		cleanup()
		return err
	}
	writer.Flush()
	if err := writer.Error(); err != nil {
		cleanup()
		return err
	}
	if err := temporary.Sync(); err != nil {
		cleanup()
		return err
	}
	if err := temporary.Close(); err != nil {
		_ = os.Remove(temporaryPath)
		return err
	}
	return os.Rename(temporaryPath, path)
}
