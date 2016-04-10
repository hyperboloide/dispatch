package dispatch

import (
	log "github.com/Sirupsen/logrus"
	"github.com/streadway/amqp"
)

// AMQPQueue defines a RabbitMQ queue.
type AMQPQueue struct {
	Name string
	host string
}

// NewAMQPQueue creates a new queue name to host
func NewAMQPQueue(name, host string) (*AMQPQueue, error) {
	queue := &AMQPQueue{name, host}

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
func (queue *AMQPQueue) Purge() error {
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
func (queue *AMQPQueue) SendBytes(bytes []byte) error {

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

	if err != nil {
		log.WithFields(log.Fields{
			"queue": queue.Name,
			"error": err,
		}).Error("Error sending message.")
	} else {
		log.WithField("queue", queue.Name).Info("New message sent")
	}

	return err
}

// ListenBytes fetch messages from the queue and then call fn
// with a []byte.
func (queue *AMQPQueue) ListenBytes(fn ListennerBytes) error {
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
			log.WithField("queue", queue.Name).Info("New message received.")

			if err := fn(d.Body); err != nil {
				defer d.Reject(true)

				log.WithFields(log.Fields{
					"queue": queue.Name,
					"error": err,
				}).Error("Error while processing message.")
				errChan <- err
				return
			}
			d.Ack(false)
		}
	}()

	return <-errChan
}
