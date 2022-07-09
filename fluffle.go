package fluffle

import (
	"fmt"
	"github.com/streadway/amqp"
	"time"
)

// this holds a channel in turn, a single transaction
type rabbitChannel struct {
	amqpChan    *amqp.Channel
	isConnected bool
}

const (
	Regular    RetryType = "regular"
	Responsive RetryType = "responsive"
)

type RetryType string

type QueueProperties struct {
	Name          string
	RetryType     RetryType
	Retriable     bool
	PrefetchCount int
}

// publish publishes messages to a reconnecting session to a fanout exchange.
// It receives from the application specific source of messages.
func publish(channels chan *rabbitChannel, qName string, qProperties QueueProperties) {
	for channel := range channels {
		logger.Trace("publishing...", nil)
		for {
			if pErr := CreateQueue(channel.amqpChan, qProperties); pErr != nil {
				logger.Error("failed to initialize queue while publishing", pErr, map[string]interface{}{
					"message":    "cannot declare queue while publishing",
					"queue_name": qName,
					"error":      pErr,
				})
				time.Sleep(reConnectTime)
			} else {
				break
			}
		}
	}
}

// subscribe consumes deliveries from an exclusive queue from a fanout exchange and sends to the application specific messages chan.
func subscribe(messages chan<- amqp.Delivery, qName string, queuePrefetchCount int, qProperties QueueProperties) {
	for {
		channel := getChannel().amqpChan
		if pErr := CreateQueue(channel, qProperties); pErr != nil {
			logger.Error("failed to initialize queue while publishing", pErr, nil)
			return
		}
		if queuePrefetchCount == 0 {
			queuePrefetchCount = PrefetchCount
		}

		if err := channel.Qos(queuePrefetchCount, 0, false); err != nil {
			logger.Error("RabbitMQ", err, map[string]interface{}{
				"message":    "cannot set QOS",
				"queue_name": qName,
				"error":      err,
			})
			return
		}

		deliveries, err := channel.Consume(qName, "", false, false, false, false, nil)
		if err != nil {
			logger.Error("RabbitMQ", err, map[string]interface{}{
				"message":    fmt.Sprintf("cannot consume from: %q, %v", qName, err),
				"queue_name": qName,
				"error":      err,
			})
			return
		}

		logger.Trace("subscribed...", nil)

		for msg := range deliveries {
			messages <- msg
		}
	}
}
