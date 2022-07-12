package fluffle

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/rabbitmq/amqp091-go"
)

type IMQ interface {
	Publish(bty []byte) error
	PublishIdempotent(idempotencyKey, idempotencyValue string, bty []byte) error
	Consume() <-chan amqp091.Delivery
	Retry(delivery amqp091.Delivery)
}

func Start(externalConfig RabbitConfig, externalLogger iLogger) {
	logger = externalLogger
	config = externalConfig

	connections := make(chan *connection)
	go initiateConnections(connections)
	connectionPool = connections
	poolStarted = true
}

func New(name string, prefetch int) IMQ {
	return new(name, prefetch)
}

type QueueStatus struct {
	Name     string `json:"name"`
	Messages int    `json:"messages"`
	Error    string `json:"error", omitempty`
}

// GetQueueStats gets stats API
func GetQueueStats(queueName string, rabbitConfig RabbitConfig) (qStatus QueueStatus, err error) {
	req, _ := http.NewRequest("GET", getApiBaseUrl(rabbitConfig.Host, queueName), nil)
	req.SetBasicAuth(rabbitConfig.UserName, rabbitConfig.Password)
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return
	}
	defer res.Body.Close()
	err = json.NewDecoder(res.Body).Decode(&qStatus)

	if qStatus.Error != "" {
		return qStatus, errors.New(qStatus.Error)
	}
	return
}

func getApiBaseUrl(hostName, queueName string) string {
	return "https://" + hostName + ":15672" + "/api/queues/%2f/" + queueName
}
