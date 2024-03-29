package fluffle

import (
	"fmt"
	"github.com/rabbitmq/amqp091-go"
	"sync"
	"time"
)

var connectionPool chan *connection

type RabbitConfig struct {
	UserName            string
	Password            string
	Host                string
	Port                int
	ChannelLimitPerConn int
}

const (
	reConnectTime = time.Second
)

type connection struct {
	*amqp091.Connection
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
var poolStarted bool

//"amqp://"+config.UserName+":"+config.Password+"@"+config.Host+":"+config.Port+"/"
func getConnectionString(config RabbitConfig) string {
	return fmt.Sprintf("amqp://%s:%s@%s:%d/", config.UserName, config.Password, config.Host, config.Port)
}

func startAmqpConnection() (conn *amqp091.Connection) {
	var err error
	for {
		conn, err = amqp091.DialConfig(getConnectionString(config), amqp091.Config{
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

func newConnection(connectionsChan chan *connection) {
	conn := startAmqpConnection()
	go func() {
		closeErr := <-conn.NotifyClose(make(chan *amqp091.Error))
		if closeErr != nil {
			logger.Error("CONNECTION_CLOSE_NOTIf", closeErr, map[string]interface{}{
				"initiating_server": closeErr.Server,
				"recoverable":       closeErr.Recover,
			})

		}
		newConnection(connectionsChan)
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
		connectionsChan <- conn
	}()
}

// gets a connection from the connection pool, opens a channel and pushes the connection back to the pool
// also handles cases where connection is no longer open
func getChannel() (rChan *rabbitChannel) {
	var err error
	var ch *amqp091.Channel
	var conn *connection
	for {
		conn = <-connectionPool
		conn.syncMutex.Lock()
		if conn.IsClosed() { // discard this connection if it's closed
			if conn.isPrimary { //  if it's the only connection, then start a new one
				newConnection(connectionPool)
			}
			conn.syncMutex.Unlock()
			continue
		}

		usableConnection := config.ChannelLimitPerConn == 0 || conn.channelCount < config.ChannelLimitPerConn

		if usableConnection {
			ch, err = conn.Channel()
			if err != nil {
				usableConnection = false
			} else {
				rChan = &rabbitChannel{
					amqpChan:    ch,
					isConnected: true,
				}
				go rChan.listenToClose(conn.chanClose)
				conn.channelCount++
				conn.syncMutex.Unlock()
			}
		}
		if !usableConnection { // spawn a new connection as the primary connection, push the current one back to pool to be used later and retry the loop
			if conn.isPrimary {
				newConnection(connectionPool)
				conn.isPrimary = false
			}
			go func() { connectionPool <- conn }()
			conn.syncMutex.Unlock()
			continue
		}
		break
	}
	go func() { connectionPool <- conn }() // puts the connection back to pool after use
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
	closeErr := <-t.amqpChan.NotifyClose(make(chan *amqp091.Error))
	if closeErr != nil {
		logger.Error("CHANNEL_CLOSE_NOTIf", closeErr, map[string]interface{}{
			"initiating_server": closeErr.Server,
			"recoverable":       closeErr.Recover,
		})
	}
	t.isConnected = false
	chanClose <- true
}
