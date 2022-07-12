package fluffle

import (
	"github.com/stretchr/testify/assert"
	"strconv"
	"testing"
)

func TestMQ_Publish(t *testing.T) {
	rConf := RabbitConfig{
		UserName:            "",
		Password:            "",
		Host:                "",
		Port:                0,
		Prefetch:            "",
		ChannelLimitPerConn: 0,
	}
	Start(rConf, fakeLogger{})
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
	message := "thisisatestmessage"
	iKey := "thisisatestkey"
	iVal := "thisisparentKey"
	err := tq.PublishIdempotent(iKey, iVal, []byte(message))
	assert.Nil(t, err)
	var str string
	var key string
	msg := <-tq.Consume()
	str = string(msg.Body)
	key = msg.Headers[iKey].(string)
	msg.Ack(true)

	assert.Equal(t, str, msg)
	assert.Equal(t, key, iVal)
}

type fakeLogger struct {
}

func (f fakeLogger) Fatal(message2 string, err error, params map[string]interface{}) {
}

func (f fakeLogger) Error(message2 string, err error, params map[string]interface{}) {
}

func (f fakeLogger) Info(message2 string, params map[string]interface{}) {
}

func (f fakeLogger) Trace(message2 string, params map[string]interface{}) {
}
