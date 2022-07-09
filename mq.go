package fluffle

import (
	"github.com/streadway/amqp"
)

type MQ struct {
	QueueProperties
	name            string
	prefetchCount   int
	channels        chan *rabbitChannel
	deliveryChannel chan amqp.Delivery
}

type IRabbitTrans interface {
	Commit() error
	Rollback() error
	Publish(bty []byte) error
}

func (b *MQ) PublishMsg(parentIdempKey, idempotencyKey string, bty []byte) error {
	return b.publish(bty, amqp.Table{
		commons.IdempotencyKey:       idempotencyKey,
		commons.ParentIdempotencyKey: parentIdempKey,
	})
}

func (b *MQ) Publish(bty []byte) error {
	return b.publish(bty, nil)
}

func (b *MQ) publish(bty []byte, publishingHeader amqp.Table) error {
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
		amqp.Publishing{
			Headers:      publishingHeader,
			ContentType:  "application/json",
			Body:         bty,
			DeliveryMode: amqp.Persistent,
		})
	if err != nil {
		logger.Error("RabbitMQ", err, map[string]interface{}{"message": "Error while sending message"})
	}
	return err
}

func (b MQ) Retry(delivery amqp.Delivery) {

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

func (b *MQ) Consume() <-chan amqp.Delivery {
	// lazy loading. delivery channel won't be created if there are no consumers
	if b.deliveryChannel == nil {
		b.deliveryChannel = make(chan amqp.Delivery)
	}
	go subscribe(b.deliveryChannel, b.name, b.prefetchCount, b.QueueProperties)
	return b.deliveryChannel
}
