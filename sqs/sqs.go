package sqs

import (
	"encoding/base64"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/hyperboloide/dispatch"
)

// SQS is a queue hosted on amazon AWS SQS.
type SQS struct {
	session  *session.Session
	queueURL *string
}

func (s *SQS) client() *sqs.SQS {
	return sqs.New(s.session)
}

// New creates a new SQS Queue object.
func New(queueName string, sess *session.Session) (*SQS, error) {
	s := &SQS{
		session:  sess,
		queueURL: &queueName,
	}

	resp, err := s.client().GetQueueUrl(&sqs.GetQueueUrlInput{
		QueueName: aws.String(queueName),
	})
	if err != nil {
		return nil, err

	}

	s.queueURL = resp.QueueUrl
	return s, nil
}

// Purge the queue, removing all elements.
func (s *SQS) Purge() error {

	params := &sqs.PurgeQueueInput{
		QueueUrl: s.queueURL,
	}
	_, err := s.client().PurgeQueue(params)
	return err
}

// SendBytes to aws servers.
func (s *SQS) SendBytes(bytes []byte) error {

	encoded := base64.StdEncoding.EncodeToString(bytes)
	params := &sqs.SendMessageInput{
		MessageBody: aws.String(encoded),
		QueueUrl:    s.queueURL,
	}

	_, err := s.client().SendMessage(params)
	return err
}

// Listen to incomming messages form SQS.
func (s *SQS) Listen(fn dispatch.Listenner) error {

	errChan := make(chan error)

	go func() {

		for true {
			params := &sqs.ReceiveMessageInput{
				QueueUrl:            s.queueURL,
				MaxNumberOfMessages: aws.Int64(1),
			}

			if resp, err := s.client().ReceiveMessage(params); err != nil {
				errChan <- err
				break

			} else {
				for _, m := range resp.Messages {

					if m.Body != nil {
						if decoded, err := base64.StdEncoding.DecodeString(*m.Body); err != nil {
							errChan <- err
							return

						} else if err := fn(decoded); err != nil {
							errChan <- err
							return

						} else if err := s.ack(m); err != nil {
							errChan <- err
							return

						}
					}
				}
			}
		}
	}()

	return <-errChan

}

func (s *SQS) ack(msg *sqs.Message) error {
	params := &sqs.DeleteMessageInput{
		QueueUrl:      s.queueURL,
		ReceiptHandle: msg.ReceiptHandle,
	}

	_, err := s.client().DeleteMessage(params)
	return err
}
