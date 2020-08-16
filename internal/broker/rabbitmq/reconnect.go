package rabbitmq

import (
	"sync/atomic"
	"time"

	"github.com/streadway/amqp"
)

//
// see: https://github.com/isayme/go-amqp-reconnect
//

// Dial wrap amqp.Dial, dial and get a reconnect connection
func dial(cfg config) (*Connection, error) {
	conn, err := amqp.Dial(cfg.AmqpUrl)
	if err != nil {
		return nil, err
	}

	connection := &Connection{
		Connection: conn,
		cfg:        cfg,
	}

	go func() {
		for {
			reason, ok := <-connection.Connection.NotifyClose(make(chan *amqp.Error))
			// exit this goroutine if closed by developer
			if !ok {
				cfg.Log.Infof("connection closed")
				break
			}
			cfg.Log.Infof("connection closed: %s", reason)

			// reconnect if not closed by developer
			for {
				// wait 1s for reconnect
				time.Sleep(cfg.ReconnectDelay)

				conn, err := amqp.Dial(cfg.AmqpUrl)
				if err == nil {
					connection.Connection = conn
					cfg.Log.Infof("reconnect success")
					break
				}
				cfg.Log.Infof("reconnect failed: %v", err)
			}
		}
	}()

	return connection, nil
}

type Connection struct {
	*amqp.Connection
	cfg config
}

// Channel wrap amqp.Connection.Channel, get a auto reconnect channel
func (c *Connection) Channel() (*Channel, error) {
	ch, err := c.Connection.Channel()
	if err != nil {
		return nil, err
	}

	channel := &Channel{
		Channel: ch,
	}

	go func() {
		for {
			reason, ok := <-channel.Channel.NotifyClose(make(chan *amqp.Error))
			// exit this goroutine if closed by graceful shutdown
			if !ok || channel.IsClosed() {
				c.cfg.Log.Infof("channel closed")
				channel.Close() // close again, ensure closed flag set when connection closed
				break
			}
			c.cfg.Log.Errorf("channel closed: %v", reason)

			// reconnect if not closed by graceful shutdown
			for {
				// wait 1s for connection reconnect
				time.Sleep(c.cfg.ReconnectDelay)

				ch, err := c.Connection.Channel()
				if err == nil {
					c.cfg.Log.Infof("channel recreated")
					channel.Channel = ch
					break
				}

				c.cfg.Log.Infof("channel recreate failed: %v", err)
			}
		}

	}()
	return channel, nil
}

type Channel struct {
	*amqp.Channel
	cfg    config
	closed int32
}

func (ch *Channel) IsClosed() bool {
	return atomic.LoadInt32(&ch.closed) == 1
}

func (ch *Channel) Close() error {
	if ch.IsClosed() {
		return amqp.ErrClosed
	}

	atomic.StoreInt32(&ch.closed, 1)

	return ch.Channel.Close()
}

func (ch *Channel) Consume(queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table) (<-chan amqp.Delivery, error) {
	deliveries := make(chan amqp.Delivery)

	go func() {
		for {
			d, err := ch.Channel.Consume(queue, consumer, autoAck, exclusive, noLocal, noWait, args)
			if err != nil {
				ch.cfg.Log.Infof("consume failed: %v", err)
				time.Sleep(ch.cfg.ReconnectDelay)
				continue
			}

			for msg := range d {
				deliveries <- msg
			}

			time.Sleep(ch.cfg.ReconnectDelay)

			if ch.IsClosed() {
				break
			}
		}
	}()

	return deliveries, nil
}
