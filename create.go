package fluffle

import "github.com/streadway/amqp"

const (
	responsiveRetryQueue      = "responsive_retry_queue"
	L1RetryDeadletterExchange = "dlx.l1.RetryExchange"
	L2RetryDeadletterExchange = "dlx.l2.RetryExchange"
)

func CreateQueue(pub *amqp.Channel, qProperties QueueProperties) (err error) {

	if !qProperties.Retriable {
		if _, err := pub.QueueDeclare(qProperties.Name, true, false, false, false, nil); err != nil {
			logger.Error("RabbitMQ", err, map[string]interface{}{
				"message":    "failed to declare queue while publishing",
				"queue_name": qProperties.Name,
				"error":      err,
			})
		}
		return
	}

	err = createDeadLetterQueue(pub, qProperties)
	return
}

func createDeadLetterQueue(pub *amqp.Channel, qProperties QueueProperties) (err error) {
	err = setupDeadletteringAndRetries(pub, qProperties)
	if err != nil {
		return
	}
	args := amqp.Table{}
	args["x-dead-letter-exchange"] = L1RetryDeadletterExchange
	if _, err := pub.QueueDeclare(qProperties.Name, true, false, false, false, args); err != nil {
		logger.Error("RabbitMQ", err, map[string]interface{}{
			"message":    "failed to declare queue while publishing",
			"queue_name": qProperties.Name,
			"error":      err,
		})
		return
	}

	if err := pub.QueueBind(qProperties.Name, qProperties.Name, L2RetryDeadletterExchange, false, nil); err != nil {
		logger.Error("setupDeadletteringAndRetries", err, map[string]interface{}{
			"queueName":    qProperties.Name,
			"exchangeName": L2RetryDeadletterExchange,
		})
		return
	}

	return
}

func setupDeadletteringAndRetries(session *amqp.Channel, qProperties QueueProperties) (err error) {
	if err := session.ExchangeDeclare(
		L1RetryDeadletterExchange,
		"direct",
		true,
		false,
		false,
		false,
		nil); err != nil {
		logger.Error("setupDeadletteringAndRetries", err, map[string]interface{}{
			"exchangeName": L1RetryDeadletterExchange,
			"message":      "failed to setup the dead letter exchange",
		})
		return
	}

	if err := session.ExchangeDeclare(
		L2RetryDeadletterExchange,
		"direct",
		true,
		false,
		false,
		false,
		nil); err != nil {
		pErr = plerrors.NewAppError("setupDeadletteringAndRetries", "", "failed to settup the dead letter exchange", plerrors.InternalServiceError, err.Error(), plog.Params{"exchangeName": L2RetryDeadletterExchange})
		return
	}

	/*
		declare regular wait retry queue
	*/
	if _, err := session.QueueDeclare(
		retryWaitQueue,
		true,
		false,
		false,
		false,
		amqp.Table{
			"x-message-ttl":          waitQueueTtl,
			"x-dead-letter-exchange": L2RetryDeadletterExchange,
		}); err != nil {
		pErr = plerrors.NewAppError("setupDeadletteringAndRetries", "", "failed to createDeadLetterQueue retry queue with TTL", plerrors.InternalServiceError, err.Error(), plog.Params{"queueName": retryWaitQueue})
		return
	}

	/*
		declare regular responsive wait retry queue
	*/
	if _, err := session.QueueDeclare(
		responsiveRetryQueue,
		true,
		false,
		false,
		false,
		amqp.Table{
			"x-message-ttl":          responsiveWaitQueueTtl,
			"x-dead-letter-exchange": L2RetryDeadletterExchange,
		}); err != nil {
		pErr = plerrors.NewAppError("setupDeadletteringAndRetries", "", "failed to createDeadLetterQueue retry queue with TTL", plerrors.InternalServiceError, err.Error(), plog.Params{"queueName": responsiveRetryQueue})
		return
	}

	switch qProperties.RetryType {
	case Regular:
		if err := session.QueueBind(
			retryWaitQueue,
			qProperties.Name,
			L1RetryDeadletterExchange,
			false,
			nil); err != nil {
			pErr = plerrors.NewAppError("setupDeadletteringAndRetries", "", "cannot bind queue with deadletter exchange", plerrors.InternalServiceError, err.Error(), plog.Params{"queueName": retryWaitQueue, "exchangeName": L1RetryDeadletterExchange})
			return
		}
	case Responsive:
		if err := session.QueueBind(
			responsiveRetryQueue,
			qProperties.Name,
			L1RetryDeadletterExchange,
			false,
			nil); err != nil {
			pErr = plerrors.NewAppError("setupDeadletteringAndRetries", "", "cannot bind queue with deadletter exchange", plerrors.InternalServiceError, err.Error(), plog.Params{"queueName": responsiveRetryQueue, "exchangeName": L1RetryDeadletterExchange})
			return
		}
	}

	return
}
