package fluffle

import (
	"github.com/streadway/amqp"
	"strconv"
	"sync"
	"time"
)

var PrefetchCount int

var connectionPool chan *connection

type RabbitConfig struct {
	UserName                string
	Password                string
	Host                    string
	Port                    string
	Prefetch                string
	ChannelLimitPerConn     int
	IsConnectionPoolEnabled bool
}

const (
	reConnectTime = time.Second
)

type connection struct {
	*amqp.Connection
	channelCount int
	isPrimary    bool
	chanClose    chan bool
	syncMutex    sync.Mutex
}

type iLogger interface {
	Fatal(message2 string, err error, params map[string]interface{})
	Error(message2 string, err error, params map[string]interface{})
	Info(message2 string, params map[string]interface{})
	Trace(message2 string, params map[string]interface{})
}

var logger iLogger
var config RabbitConfig

func Start(externalConfig RabbitConfig, externalLogger iLogger) {
	logger = externalLogger
	config = externalConfig

	var err error
	if PrefetchCount, err = strconv.Atoi(config.Prefetch); err != nil {
		logger.Fatal("rabbitmq init failure. invalid prefetch config", err, nil)
		panic(err)
	}

	connections := make(chan *connection)
	go initiateConnections(connections)
	connectionPool = connections
}

func startAmqpConnection() (conn *amqp.Connection) {
	var err error
	for {
		conn, err = amqp.DialConfig("amqp://"+config.UserName+":"+config.Password+"@"+config.Host+":"+config.Port+"/", amqp.Config{
			Heartbeat: 20 * time.Second,
			Locale:    "en_US",
		})
		if err != nil {
			logger.Error("cannot (re)dial: %v: %q", err, nil)
			time.Sleep(reConnectTime)
			continue
		}
		break
	}
	return conn
}

func initiateConnections(connections chan *connection) {
	conn := startAmqpConnection()
	go func() {
		closeErr := <-conn.NotifyClose(make(chan *amqp.Error))
		if closeErr != nil {
			logger.Error("CONNECTION_CLOSE_NOTIf", closeErr, map[string]interface{}{
				"initiating_server": closeErr.Server,
				"recoverable":       closeErr.Recover,
			})

		}
		initiateConnections(connections)
	}()
	go func() {
		conn := &connection{
			Connection:   conn,
			channelCount: 0,
			isPrimary:    true,
			chanClose:    make(chan bool),
			syncMutex:    sync.Mutex{},
		}
		go conn.listenToChannelClose()
		connections <- conn
	}()
}

// gets a connection from the connection pool, opens a channel and pushes the connection back to the pool
// also handles cases where connection is no longer open
func getChannel() (rChan *rabbitChannel) {
	var err error
	var ch *amqp.Channel
	var conn *connection
	for {
		conn = <-connectionPool
		conn.syncMutex.Lock()
		if conn.IsClosed() {
			if conn.isPrimary {
				initiateConnections(connectionPool)
			}
			conn.syncMutex.Unlock()
			continue
		}
		if config.ChannelLimitPerConn > 0 && conn.channelCount < config.ChannelLimitPerConn {
			ch, err = conn.Channel()
			if err != nil {
				if conn.isPrimary {
					initiateConnections(connectionPool)
					conn.isPrimary = false
				}
				go func() { connectionPool <- conn }()
				conn.syncMutex.Unlock()
				continue
			} else {
				rChan = &rabbitChannel{
					amqpChan:    ch,
					isConnected: true,
				}
				go rChan.listenToClose(conn.chanClose)
				conn.channelCount++
				conn.syncMutex.Unlock()
			}
		} else {
			if conn.isPrimary {
				initiateConnections(connectionPool)
				conn.isPrimary = false
			}
			go func() { connectionPool <- conn }()
			conn.syncMutex.Unlock()
			continue
		}
		break
	}
	go func() { connectionPool <- conn }()
	return
}

func (c *connection) listenToChannelClose() {
	for <-c.chanClose {
		c.syncMutex.Lock()
		c.channelCount--
		if c.channelCount == 0 && !c.isPrimary {
			c.Close()
			close(c.chanClose)
			c.syncMutex.Unlock()
			return
		}
		c.syncMutex.Unlock()
	}
}

func (t *rabbitChannel) listenToClose(chanClose chan bool) {
	closeErr := <-t.amqpChan.NotifyClose(make(chan *amqp.Error))
	if closeErr != nil {
		logger.Error("CHANNEL_CLOSE_NOTIf", closeErr, map[string]interface{}{
			"initiating_server": closeErr.Server,
			"recoverable":       closeErr.Recover,
		})
	}
	t.isConnected = false
	chanClose <- true
}
