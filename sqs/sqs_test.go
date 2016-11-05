package sqs_test

import (
	"encoding/json"
	"errors"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/dchest/uniuri"
	"github.com/hyperboloide/dispatch"
	"github.com/hyperboloide/dispatch/sqs"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"os"
)

var _ = Describe("Sqs", func() {

	defer GinkgoRecover()

	if os.Getenv("AWS_ACCESS_KEY_ID") == "" || os.Getenv("AWS_SECRET_ACCESS_KEY") == "" {
		Skip("no access key to aws")
	}

	var queue dispatch.Queue
	var result = make(chan bool, 1)

	content := uniuri.New()

	It("should create a new queue and purge it", func() {

		value := credentials.Value{
			AccessKeyID:     os.Getenv("AWS_ACCESS_KEY_ID"),
			SecretAccessKey: os.Getenv("AWS_SECRET_ACCESS_KEY"),
		}
		credentials := credentials.NewStaticCredentialsFromCreds(value)
		config := aws.NewConfig().WithCredentials(credentials)
		config.Region = aws.String("eu-west-1")

		sess, err := session.NewSession(config)
		Ω(err).To(BeNil())
		Ω(sess).ToNot(BeNil())

		q, err := sqs.New("testDispatch", sess)
		Ω(err).To(BeNil())

		queue = q
	}, 3)

	It("should send some random messages", func() {
		for i := 0; i < 10; i++ {
			msg, err := json.Marshal(struct {
				Msg string `json:"msg"`
			}{"a random message"})
			Ω(err).NotTo(HaveOccurred())

			err = queue.SendBytes(msg)
			Ω(err).NotTo(HaveOccurred())
		}
	}, 10)

	It("should send a message with the special content", func() {
		msg, err := json.Marshal(struct {
			Msg string `json:"msg"`
		}{content})
		Ω(err).To(BeNil())

		Ω(queue.SendBytes(msg)).To(BeNil())
	}, 3)

	It("should send some random messages", func() {
		for i := 0; i < 10; i++ {
			msg, err := json.Marshal(struct {
				Msg string `json:"msg"`
			}{"a random message"})
			Ω(err).NotTo(HaveOccurred())

			err = queue.SendBytes(msg)
			Ω(err).NotTo(HaveOccurred())
		}
	}, 10)

	It("should Listen for messages", func() {

		var listenner = func(b []byte) error {
			var data = struct {
				Msg string `json:"msg"`
			}{""}

			if err := json.Unmarshal(b, &data); err != nil {
				return err

			}

			if data.Msg == content {
				result <- true
				return errors.New("some error")

			}
			return nil
		}

		Ω(queue.Listen(listenner)).ToNot(BeNil())
		Ω(<-result).To(BeTrue())
		close(result)
	}, 20)

})
