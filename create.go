package fluffle

import "github.com/rabbitmq/amqp091-go"

func CreateQueue(pub *amqp091.Channel, qProperties QueueProperties) (err error) {

	if _, err := pub.QueueDeclare(qProperties.Name, true, false, false, false, nil); err != nil {
		logger.Error("RabbitMQ", err, map[string]interface{}{
			"message":    "failed to declare queue while publishing",
			"queue_name": qProperties.Name,
			"error":      err,
		})
	}
	return
}
