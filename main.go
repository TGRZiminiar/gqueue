package main

import (
	"log"
	"math/rand"
	"os"
	"os/signal"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/tgrziminiar/gqueue/gqueue"
)

func main() {

	// Create a topic queue with a buffer size of 1000, max 100 topics, and 5 minute topic timeout
	tq := gqueue.NewTopicQueue(1000, 100, 5*time.Minute)

	// var wg sync.WaitGroup
	// maxConcurrent := 2
	// wg.Add(maxConcurrent)

	// Start 10 worker goroutines
	tq.Start(10, func(req gqueue.Request) gqueue.Result {
		// Acquire a slot in the wait group
		// wg.Wait()
		// defer wg.Done()

		// Simulate work
		time.Sleep(2 * time.Second)
		log.Printf("Successfully processed request: %s (Topic: %s)\n", req.ID, req.Topic)
		return gqueue.Result{Success: true, Message: "Request processed successfully"}
	})

	// Set up Fiber app
	app := fiber.New()

	// Set up Prometheus handler
	// go func() {
	// 	http.Handle("/metrics", promhttp.Handler())
	// 	http.ListenAndServe(":2112", nil)
	// }()

	// Define a route that enqueues requests and waits for the response
	app.Post("/process", func(c *fiber.Ctx) error {
		var reqData struct {
			ID    string      `json:"id"`
			Topic string      `json:"topic"`
			Data  interface{} `json:"data"`
		}
		if err := c.BodyParser(&reqData); err != nil {
			return c.Status(fiber.StatusBadRequest).SendString(err.Error())
		}

		responseChan := make(chan gqueue.Result)
		req := gqueue.Request{
			ID:       reqData.ID,
			Topic:    reqData.Topic,
			Data:     reqData.Data,
			Response: responseChan,
		}

		if err := tq.EnqueueRequest(req); err != nil {
			return c.Status(fiber.StatusServiceUnavailable).SendString(err.Error())
		}

		select {
		case result := <-responseChan:
			if result.Success {
				return c.Status(fiber.StatusOK).JSON(fiber.Map{
					"response": result,
					"id":       rand.Int(),
				})
			} else {
				return c.Status(fiber.StatusInternalServerError).JSON(result)
			}
		case <-time.After(10 * time.Second):
			// tq.Metrics.errorCount.With(prometheus.Labels{
			// 	"topic":      req.Topic,
			// 	"error_type": "timeout",
			// }).Inc()
			return c.Status(fiber.StatusRequestTimeout).SendString("Request timed out")
		}
	})

	app.Get("/topics", func(c *fiber.Ctx) error {
		activeTopics := tq.GetActiveTopics()
		queueLengths := tq.GetTopicQueueLengths()

		type TopicInfo struct {
			Name        string `json:"name"`
			QueueLength int    `json:"queueLength"`
		}

		topicInfos := make([]TopicInfo, 0, len(activeTopics))
		for _, topic := range activeTopics {
			topicInfos = append(topicInfos, TopicInfo{
				Name:        topic,
				QueueLength: queueLengths[topic],
			})
		}

		return c.JSON(fiber.Map{
			"activeTopics": topicInfos,
			"totalTopics":  len(activeTopics),
		})
	})

	// Graceful shutdown
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func() {
		<-c
		log.Println("Gracefully shutting down...")
		app.Shutdown()
	}()

	// Start the Fiber app
	if err := app.Listen(":3000"); err != nil {
		log.Panic(err)
	}

	// Ensure all requests are processed before shutting down
	// shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
	// defer shutdownCancel()
	// if err := tq.Shutdown(shutdownCtx); err != nil {
	// 	log.Printf("Error during shutdown: %v", err)
	// }
}
