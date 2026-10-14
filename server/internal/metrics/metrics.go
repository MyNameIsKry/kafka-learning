package metrics

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type Metrics struct {
	component string
	registry  *prometheus.Registry
	server    *http.Server

	published *prometheus.CounterVec
	stored    prometheus.Counter
	committed prometheus.Counter
	failures  *prometheus.CounterVec
	retries   prometheus.Counter
	inference prometheus.Histogram
	batch     prometheus.Histogram
	delivery  prometheus.Histogram
	recordAge prometheus.Histogram
	lag       prometheus.Gauge
	mu        sync.Mutex
}

func New(component string, port int) (*Metrics, error) {
	if port < 0 || port > 65535 {
		return nil, fmt.Errorf("metrics port must be between 0 and 65535")
	}
	registry := prometheus.NewRegistry()
	result := &Metrics{
		component: component,
		registry:  registry,
		published: prometheus.NewCounterVec(prometheus.CounterOpts{Name: "pipeline_published_records_total", Help: "Acknowledged Kafka publishes", ConstLabels: prometheus.Labels{"component": component}}, []string{"topic"}),
		stored:    prometheus.NewCounter(prometheus.CounterOpts{Name: "pipeline_stored_records_total", Help: "Acknowledged MongoDB projections", ConstLabels: prometheus.Labels{"component": component}}),
		committed: prometheus.NewCounter(prometheus.CounterOpts{Name: "pipeline_committed_records_total", Help: "Kafka input records committed", ConstLabels: prometheus.Labels{"component": component}}),
		failures:  prometheus.NewCounterVec(prometheus.CounterOpts{Name: "pipeline_failures_total", Help: "Failed processing attempts", ConstLabels: prometheus.Labels{"component": component}}, []string{"stage"}),
		retries:   prometheus.NewCounter(prometheus.CounterOpts{Name: "pipeline_retries_total", Help: "Scheduled retries", ConstLabels: prometheus.Labels{"component": component}}),
		inference: prometheus.NewHistogram(prometheus.HistogramOpts{Name: "pipeline_inference_seconds", Help: "Inference duration", ConstLabels: prometheus.Labels{"component": component}}),
		batch:     prometheus.NewHistogram(prometheus.HistogramOpts{Name: "pipeline_batch_seconds", Help: "Batch or message processing duration", ConstLabels: prometheus.Labels{"component": component}}),
		delivery:  prometheus.NewHistogram(prometheus.HistogramOpts{Name: "pipeline_delivery_seconds", Help: "Kafka publish acknowledgement duration", ConstLabels: prometheus.Labels{"component": component}}),
		recordAge: prometheus.NewHistogram(prometheus.HistogramOpts{Name: "pipeline_input_to_completion_seconds", Help: "Kafka input timestamp to committed projection", ConstLabels: prometheus.Labels{"component": component}}),
		lag:       prometheus.NewGauge(prometheus.GaugeOpts{Name: "pipeline_consumer_lag", Help: "Kafka reader lag", ConstLabels: prometheus.Labels{"component": component}}),
	}
	for _, collector := range []prometheus.Collector{result.published, result.stored, result.committed, result.failures, result.retries, result.inference, result.batch, result.delivery, result.recordAge, result.lag} {
		if err := registry.Register(collector); err != nil {
			return nil, err
		}
	}
	if port != 0 {
		result.server = &http.Server{Addr: ":" + strconv.Itoa(port), Handler: promhttp.HandlerFor(registry, promhttp.HandlerOpts{}), ReadHeaderTimeout: 5 * time.Second}
		go func() { _ = result.server.ListenAndServe() }()
	}
	return result, nil
}

func (m *Metrics) Published(topic string)    { m.published.WithLabelValues(topic).Inc() }
func (m *Metrics) Stored()                   { m.stored.Inc() }
func (m *Metrics) Committed(count int)       { m.committed.Add(float64(count)) }
func (m *Metrics) Failure(stage string)      { m.failures.WithLabelValues(stage).Inc() }
func (m *Metrics) Retry()                    { m.retries.Inc() }
func (m *Metrics) Inference(seconds float64) { m.inference.Observe(seconds) }
func (m *Metrics) Batch(seconds float64)     { m.batch.Observe(seconds) }
func (m *Metrics) Delivery(seconds float64)  { m.delivery.Observe(seconds) }
func (m *Metrics) Completion(inputTime time.Time) {
	if !inputTime.IsZero() && !inputTime.After(time.Now().UTC()) {
		m.recordAge.Observe(time.Since(inputTime).Seconds())
	}
}
func (m *Metrics) ConsumerLag(value int64) { m.lag.Set(float64(value)) }

func (m *Metrics) Close(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.server == nil {
		return nil
	}
	err := m.server.Shutdown(ctx)
	m.server = nil
	return err
}
