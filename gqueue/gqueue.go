package gqueue

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type Request struct {
	ID       string
	Topic    string
	Data     interface{}
	Response chan Result
}

type Result struct {
	Success bool
	Message string
}

type TopicQueue struct {
	requests     chan Request
	topics       map[string]chan Request
	mu           sync.RWMutex
	wg           sync.WaitGroup
	maxTopics    int
	topicTimeout time.Duration
	Metrics      *Metrics
}

func (tq *TopicQueue) GetActiveTopics() []string {
	tq.mu.RLock()
	defer tq.mu.RUnlock()

	topics := make([]string, 0, len(tq.topics))
	for topic := range tq.topics {
		topics = append(topics, topic)
	}
	return topics
}

// method to get topic queue lengths
func (tq *TopicQueue) GetTopicQueueLengths() map[string]int {
	tq.mu.RLock()
	defer tq.mu.RUnlock()

	lengths := make(map[string]int)
	for topic, ch := range tq.topics {
		lengths[topic] = len(ch)
	}
	return lengths
}

type Metrics struct {
	queueLength    *prometheus.GaugeVec
	processingTime *prometheus.HistogramVec
	errorCount     *prometheus.CounterVec
}

func NewMetrics() *Metrics {
	return &Metrics{
		queueLength: promauto.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "queue_length",
				Help: "The number of items in the queue",
			},
			[]string{"topic"},
		),
		processingTime: promauto.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "processing_time_seconds",
				Help:    "Time taken to process a request",
				Buckets: prometheus.DefBuckets,
			},
			[]string{"topic"},
		),
		errorCount: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Name: "error_count",
				Help: "Number of errors encountered",
			},
			[]string{"topic", "error_type"},
		),
	}
}

func NewTopicQueue(bufferSize, maxTopics int, topicTimeout time.Duration) *TopicQueue {
	return &TopicQueue{
		requests:     make(chan Request, bufferSize),
		topics:       make(map[string]chan Request),
		maxTopics:    maxTopics,
		topicTimeout: topicTimeout,
		Metrics:      NewMetrics(),
	}
}

func (tq *TopicQueue) EnqueueRequest(req Request) error {
	tq.mu.Lock()
	defer tq.mu.Unlock()

	if len(tq.topics) >= tq.maxTopics && tq.topics[req.Topic] == nil {
		return fmt.Errorf("max number of topics reached")
	}

	if _, exists := tq.topics[req.Topic]; !exists {
		tq.topics[req.Topic] = make(chan Request, 100)
		go tq.topicWorker(req.Topic)
	}

	select {
	case tq.topics[req.Topic] <- req:
		tq.Metrics.queueLength.With(prometheus.Labels{"topic": req.Topic}).Inc()
		return nil
	default:
		return fmt.Errorf("topic queue is full")
	}
}

// go tq.distributor()
func (tq *TopicQueue) Start(workerCount int, processFunc func(Request) Result) {
	for i := 0; i < workerCount; i++ {
		go tq.worker(processFunc)
	}
}

//	func (tq *TopicQueue) distributor() {
//		for req := range tq.requests {
//			tq.mu.Lock()
//			// if worker topic is not exist create it
//			if _, exists := tq.topics[req.Topic]; !exists {
//				tq.topics[req.Topic] = make(chan Request, 100)
//				go tq.topicWorker(req.Topic)
//			}
//			// sned a request to each of the topic
//			tq.mu.Unlock()
//			tq.topics[req.Topic] <- req
//		}
//	}
func (tq *TopicQueue) topicWorker(topic string) {
	timeout := time.NewTimer(tq.topicTimeout)
	defer timeout.Stop()

	for {
		select {
		case req, ok := <-tq.topics[topic]:
			if !ok {
				return
			}
			tq.requests <- req
			timeout.Reset(tq.topicTimeout)
		case <-timeout.C:
			tq.mu.Lock()
			close(tq.topics[topic])
			delete(tq.topics, topic)
			tq.mu.Unlock()
			log.Printf("Cleaned up inactive topic: %s", topic)
			return
		}
	}
}

func (tq *TopicQueue) worker(processFunc func(Request) Result) {
	for req := range tq.requests {
		start := time.Now()
		result := processFunc(req)
		duration := time.Since(start)

		tq.Metrics.processingTime.With(prometheus.Labels{"topic": req.Topic}).Observe(duration.Seconds())
		tq.Metrics.queueLength.With(prometheus.Labels{"topic": req.Topic}).Dec()

		if !result.Success {
			tq.Metrics.errorCount.With(prometheus.Labels{
				"topic":      req.Topic,
				"error_type": "processing_error",
			}).Inc()
		}

		req.Response <- result
		close(req.Response)
	}
}

func (tq *TopicQueue) Shutdown(ctx context.Context) error {
	close(tq.requests)
	done := make(chan struct{})
	go func() {
		tq.wg.Wait()
		close(done)
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-done:
		return nil
	}
}
