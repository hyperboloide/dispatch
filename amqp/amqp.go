package amqp

import (
	"github.com/hyperboloide/dispatch"
	"github.com/streadway/amqp"
)

// AMQP defines a RabbitMQ queue.
type AMQP struct {
	Name string
	host string
}

// NewAMQP creates a new AMQP queue name to host
func NewAMQP(name, host string) (*AMQP, error) {
	queue := &AMQP{name, host}

	conn, err := amqp.Dial(queue.host)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		return nil, err
	}
	defer ch.Close()

	_, err = ch.QueueDeclare(
		queue.Name, // name
		true,       // durable
		false,      // delete when unused
		false,      // exclusive
		false,      // no-wait
		nil,        // arguments
	)
	return queue, err
}

// Purge purges the queue
func (queue *AMQP) Purge() error {
	conn, err := amqp.Dial(queue.host)
	if err != nil {
		return err
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		return err
	}
	defer ch.Close()

	_, err = ch.QueuePurge(queue.Name, false)
	return err
}

// SendBytes sends a []byte on the queue
func (queue *AMQP) SendBytes(bytes []byte) error {

	conn, err := amqp.Dial(queue.host)
	if err != nil {
		return err
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		return err
	}
	defer ch.Close()

	err = ch.Publish(
		"",         // exchange
		queue.Name, // routing key
		false,      // mandatory
		false,
		amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			ContentType:  "application/octet-stream",
			Body:         bytes,
		})

	return err
}

// Listen fetch messages from the queue and then call fn
// with a []byte.
func (queue *AMQP) Listen(fn dispatch.Listenner) error {
	conn, err := amqp.Dial(queue.host)
	if err != nil {
		return err
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		return err
	}
	defer ch.Close()

	err = ch.Qos(
		1,     // prefetch count
		0,     // prefetch size
		false, // global
	)
	if err != nil {
		return err
	}

	msgs, err := ch.Consume(
		queue.Name, // queue
		"",         // consumer
		false,      // auto-ack
		false,      // exclusive
		false,      // no-local
		false,      // no-wait
		nil,        // args
	)
	if err != nil {
		return err
	}
	errChan := make(chan error)

	go func() {
		for d := range msgs {
			if err := fn(d.Body); err != nil {
				defer d.Reject(true)
				errChan <- err
				return
			}
			d.Ack(false)
		}
	}()

	return <-errChan
}
