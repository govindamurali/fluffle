package fluffle

import (
	"github.com/rabbitmq/amqp091-go"
)

type MQ struct {
	QueueProperties
	name            string
	prefetchCount   int
	channels        chan *rabbitChannel
	deliveryChannel chan amqp091.Delivery
}

type IRabbitTrans interface {
	Commit() error
	Rollback() error
	Publish(bty []byte) error
}

func (b *MQ) PublishMsg(parentIdempKey, idempotencyKey string, bty []byte) error {
	return b.publish(bty, amqp091.Table{
		commons.IdempotencyKey:       idempotencyKey,
		commons.ParentIdempotencyKey: parentIdempKey,
	})
}

func (b *MQ) Publish(bty []byte) error {
	return b.publish(bty, nil)
}

func (b *MQ) publish(bty []byte, publishingHeader amqp091.Table) error {
	ch := getChannel()
	defer ch.amqpChan.Close()

	if err := CreateQueue(ch.amqpChan, b.QueueProperties); err != nil {
		logger.Error("failed to initialize queue while publishing", err, nil)
		return err
	}

	err := ch.amqpChan.Publish(
		"",     // exchange
		b.name, // routing key
		false,  // mandatory
		false,  // immediate
		amqp091.Publishing{
			Headers:      publishingHeader,
			ContentType:  "application/json",
			Body:         bty,
			DeliveryMode: amqp091.Persistent,
		})
	if err != nil {
		logger.Error("RabbitMQ", err, map[string]interface{}{"message": "Error while sending message"})
	}
	return err
}

func (b MQ) Retry(delivery amqp091.Delivery) {

	defer func() {
		recover()
	}()

	err := delivery.Nack(false, false)
	if err != nil {
		logger.Error("MQ Retry", err, map[string]interface{}{
			"message": "Error while nack ",
		})
	}
}

func (b *MQ) Consume() <-chan amqp091.Delivery {
	// lazy loading. delivery channel won't be created if there are no consumers
	if b.deliveryChannel == nil {
		b.deliveryChannel = make(chan amqp091.Delivery)
	}
	go subscribe(b.deliveryChannel, b.name, b.prefetchCount, b.QueueProperties)
	return b.deliveryChannel
}
