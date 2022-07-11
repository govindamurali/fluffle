package fluffle

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/rabbitmq/amqp091-go"
)

type IMQ interface {
	Publish(bty []byte) error
	PublishMsg(parentIdempKey, idempotencyKey string, bty []byte) error
	Consume() <-chan amqp091.Delivery
	Retry(delivery amqp091.Delivery)
}

var rabbitBaseApiUrl = "http://" + config.Host + ":15672" + "/api/queues/%2f/"

type QueueStatus struct {
	Name     string `json:"name"`
	Messages int    `json:"messages"`
	Error    string `json:"error", omitempty`
}

// gets stats
func GetQueueStats(queueName string) (qStatus QueueStatus, err error) {
	req, _ := http.NewRequest("GET", rabbitBaseApiUrl+queueName, nil)
	req.SetBasicAuth(config.UserName, config.Password)
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

func New(name string) IMQ {

	if !config.IsConnectionPoolEnabled {
		return New(name)
	}
	return new(name)
}

func NewQueueWithProperties(properties QueueProperties) IMQ {
	if !config.IsConnectionPoolEnabled {
		return NewQueueWithProperties(properties)
	}

	return newQueueWithProperties(properties)
}

func new(name string) *MQ {
	b := MQ{name: name, prefetchCount: 0, channels: make(chan *rabbitChannel)}
	b.Name = name
	b.RetryType = Regular

	go publish(b.channels, b.name, b.QueueProperties)

	return &b
}

func newQueueWithProperties(properties QueueProperties) *MQ {
	b := MQ{
		QueueProperties: properties, name: properties.Name, prefetchCount: 0, channels: make(chan *rabbitChannel),
	}

	go publish(b.channels, b.name, b.QueueProperties)

	return &b
}
