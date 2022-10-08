package fluffle

import (
	"github.com/stretchr/testify/assert"
	"strconv"
	"testing"
)

func TestMQ_Publish(t *testing.T) {
	rConf := RabbitConfig{
		UserName:            "guest",
		Password:            "guest",
		Host:                "localhost",
		Port:                5672,
		ChannelLimitPerConn: 0,
	} // default connection params

	fakeLogger := getFakeLogger()

	Start(rConf, fakeLogger)
	tq := new("publish-test-q", 100)
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
	assert.Equal(t, 0, len(fakeLogger.errors))
	assert.Equal(t, 0, len(fakeLogger.fatal))
}

func TestMQ_PublishMsg(t *testing.T) {
	tq := new("publishmsg-test-q", 100)
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

	assert.Equal(t, str, message)
	assert.Equal(t, key, iVal)
}

type fakeLogger struct {
	fatal  []error
	errors []error
	info   []string
	trace  []string
}

func getFakeLogger() fakeLogger {
	return fakeLogger{
		fatal:  make([]error, 0),
		errors: make([]error, 0),
		info:   make([]string, 0),
		trace:  make([]string, 0),
	}
}

func (f fakeLogger) Fatal(message2 string, err error, params map[string]interface{}) {
	f.errors = append(f.errors, err)
}

func (f fakeLogger) Error(message2 string, err error, params map[string]interface{}) {
	f.errors = append(f.errors, err)
}

func (f fakeLogger) Info(message2 string, params map[string]interface{}) {
	f.info = append(f.info, message2)
}

func (f fakeLogger) Trace(message2 string, params map[string]interface{}) {
	f.trace = append(f.trace, message2)
}
