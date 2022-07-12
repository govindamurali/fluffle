package fluffle

import (
	"fmt"
	"github.com/rabbitmq/amqp091-go"
	"time"
)

// this holds a channel in turn, a single transaction
type rabbitChannel struct {
	amqpChan    *amqp091.Channel
	isConnected bool
}

type RetryType string

type QueueProperties struct {
	Name          string
	PrefetchCount int
}

// publish publishes messages to a reconnecting session to a fanout exchange.
// It receives from the application specific source of messages.
func publish(channels chan *rabbitChannel, qProperties QueueProperties) {
	for channel := range channels {
		logger.Trace("publishing...", nil)
		for {
			if pErr := CreateQueue(channel.amqpChan, qProperties); pErr != nil {
				logger.Error("failed to initialize queue while publishing", pErr, map[string]interface{}{
					"message":    "cannot declare queue while publishing",
					"queue_name": qProperties.Name,
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
func subscribe(messages chan<- amqp091.Delivery, qProperties QueueProperties) {
	for {
		channel := getChannel().amqpChan
		if pErr := CreateQueue(channel, qProperties); pErr != nil {
			logger.Error("failed to initialize queue while publishing", pErr, nil)
			return
		}

		if err := channel.Qos(qProperties.PrefetchCount, 0, false); err != nil {
			logger.Error("RabbitMQ", err, map[string]interface{}{
				"message":    "cannot set QOS",
				"queue_name": qProperties.Name,
				"error":      err,
			})
			return
		}

		deliveries, err := channel.Consume(qProperties.Name, "", false, false, false, false, nil)
		if err != nil {
			logger.Error("RabbitMQ", err, map[string]interface{}{
				"message":    fmt.Sprintf("cannot consume from: %q, %v", qProperties.Name, err),
				"queue_name": qProperties.Name,
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
