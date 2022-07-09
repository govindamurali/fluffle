package fluffle

import (
	"fmt"
	"github.com/streadway/amqp"
)

type Exchange interface {
	Push(message []byte)
	Receive() <-chan DeliveryMessage
}

type demuxQueue struct {
	Config       *Config
	Name         string
	ExchangeName string
	RoutingKey   string
	Input        chan message
	Output       chan DeliveryMessage
}

type message []byte
type DeliveryMessage struct {
	amqp.Delivery
}

type Config struct {
	Prefetch int
	Username string
	Password string
	Host     string
	Port     int
}

func (q *demuxQueue) Push(message []byte) {
	q.Input <- message
}

func (q *demuxQueue) Receive() <-chan DeliveryMessage {
	go q.subscribe()
	return q.Output
}

func NewDemux(queueName string, exchangeName string, routingKey string) Exchange {
	input := make(chan message, 1)
	output := make(chan DeliveryMessage, 1)
	q := &demuxQueue{
		Name:         queueName,
		ExchangeName: exchangeName,
		RoutingKey:   routingKey,
		Config: &Config{
			Prefetch: 10,
		},
		Input:  input,
		Output: output,
	}
	go q.publish()

	return q
}

func (q *demuxQueue) publish() {
	var (
		pending = make(chan message, 1)
	)

	pub := getChannel().amqpChan
	logger.Info("declaring exchange "+q.ExchangeName, nil)

	err := pub.ExchangeDeclare(
		q.ExchangeName,
		"direct",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		logger.Error(fmt.Sprintf("cannot declare exchange while publishing: %q", q.ExchangeName), err, nil)
		return
	}

loop:
	for {
		select {
		case msg := <-q.Input:
			err := pub.Publish(
				q.ExchangeName,
				q.RoutingKey,
				false,
				false,
				amqp.Publishing{
					ContentType:  "application/json",
					Body:         msg,
					DeliveryMode: amqp.Persistent,
				})
			if err != nil {
				logger.Error("Error while sending message", err, nil)
				pending <- msg
				pub.Close()
				break loop
			}
		case msg := <-pending:
			err := pub.Publish(
				q.ExchangeName,
				q.RoutingKey,
				false,
				false,
				amqp.Publishing{
					ContentType:  "application/json",
					Body:         msg,
					DeliveryMode: amqp.Persistent,
				})
			if err != nil {
				logger.Error("Error while retry send message", err, nil)
				pending <- msg
				pub.Close()
				break loop
			}
		}
	}
}

func (q *demuxQueue) subscribe() {
	sub := getChannel().amqpChan

	args := amqp.Table{}

	err := sub.ExchangeDeclare(
		q.ExchangeName,
		"direct",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		logger.Error(fmt.Sprintf("cannot declare exchange while subscribing: %q", q.ExchangeName), err, nil)
		return
	}

	if err := sub.Qos(q.Config.Prefetch, 0, false); err != nil {
		logger.Error(fmt.Sprintf("cannot set QOS: %q", q.ExchangeName), err, nil)
		return
	}

	if _, err := sub.QueueDeclare(q.Name, true, false, false, false, args); err != nil {
		logger.Error(fmt.Sprintf("cannot set QOS: Queue: %q Exchange: %q", q.Name, q.ExchangeName), err, nil)
		return
	}

	err = sub.QueueBind(q.Name, q.RoutingKey, q.ExchangeName, false, nil)
	if err != nil {
		logger.Error(fmt.Sprintf("queue binding failed for exchange: %q queue: %q", q.ExchangeName, q.Name), err, nil)
		return
	}

	deliveries, err := sub.Consume(q.Name, "", false, false, false, false, nil)
	if err != nil {
		logger.Error(fmt.Sprintf("cannot consume from exchange: %q queue: %q", q.ExchangeName, q.Name), err, nil)
		return
	}

	logger.Info(fmt.Sprintf("subscribed by queue %v from exchange with routing key... %v : %v", q.Name, q.ExchangeName, q.RoutingKey), nil)

	for msg := range deliveries {
		q.Output <- DeliveryMessage{msg}
	}
}
