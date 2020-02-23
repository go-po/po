package rabbitmq

import "github.com/streadway/amqp"

func newGroupNumberAssigner(broker *Broker) *GroupNumberAssigner {
	return &GroupNumberAssigner{broker: broker}
}

// mechanism to assign group numbers to messages
type GroupNumberAssigner struct {
	broker  *Broker
	channel *amqp.Channel
}

func (assigner *GroupNumberAssigner) connect() error {
	var err error
	assigner.channel, err = pub.broker.connect()
	return err
}
