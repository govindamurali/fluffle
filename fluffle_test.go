package fluffle

import (
	"github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"strconv"
	"testing"
)

func TestMQ_Publish(t *testing.T) {
	tq := new("publish-test-q")
	tStr := "thisisatestmessage"
	for i := 0; i < 10; i++ {
		err := tq.Publish([]byte(tStr + strconv.Itoa(i)))
		assert.Nil(t, err)
	}
	var str string
	i := 0
	for msg := range tq.Consume() {
		str = string(msg.Body)
		assert.Equal(t, str, tStr+strconv.Itoa(i))
		msg.Ack(true)
		i++
		if i == 10 {
			break
		}
	}
}

func TestMQ_PublishMsg(t *testing.T) {
	tq := new("publishmsg-test-q")
	tStr := "thisisatestmessage"
	tKey := "thisisatestkey"
	pKey := "thisisparentKey"
	err := tq.PublishMsg(pKey, tKey, []byte(tStr))
	assert.Nil(t, err)
	var str string
	var key, pK string
	msg := <-tq.Consume()
	str = string(msg.Body)
	key = msg.Headers[commons.IdempotencyKey].(string)
	pK = msg.Headers[commons.ParentIdempotencyKey].(string)
	msg.Ack(true)

	assert.Equal(t, str, tStr)
	assert.Equal(t, key, tKey)
	assert.Equal(t, pK, pKey)
}

func TestDeadLettering(t *testing.T) {
	tq := new("test-dead")
	key := "testMessage"
	body := "this is a message string"
	err := tq.PublishMsg("", key, []byte(body))
	assert.Nil(t, err)

	msg := <-tq.Consume()
	assert.Equal(t, string(msg.Body), body)
	msg.Nack(false, false)

	msg = <-tq.Consume()
	assert.Equal(t, string(msg.Body), body)
	msg.Ack(true)
}

func Test_deadletting_x_death(t *testing.T) {
	tq := new("test-x-death")
	key := "test-x-death"
	message := "thanos/2"

	tq.PublishMsg("", key, []byte(message))

	for i := 0; i < 3; i++ {
		msg := <-tq.Consume()
		msg.Nack(false, false)
	}

	msg := <-tq.Consume()
	data := msg.Headers["x-death"].([]interface{})
	assert.Equal(t, 2, len(data))

	row := data[0].(amqp091.Table)
	count, ok := row["count"].(int64)
	assert.True(t, ok)
	assert.Equal(t, int64(3), count)

	msg.Ack(true)
}
