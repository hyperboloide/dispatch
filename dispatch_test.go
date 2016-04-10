package dispatch_test

import (
	"encoding/json"
	"errors"

	. "github.com/hyperboloide/dispatch"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Dispatch", func() {

	var queue Queue
	var result = make(chan bool, 1)

	It("should create a new queue and purge it", func() {
		q, err := NewAMQPQueue("test", "amqp://guest:guest@localhost:5672/")
		Ω(err).To(BeNil())
		Ω(q.Purge()).To(BeNil())

		queue = q
	})

	It("should send a message", func() {
		msg, err := json.Marshal(struct {
			Msg string `json:"msg"`
		}{"ok"})
		Ω(err).To(BeNil())

		Ω(queue.SendBytes(msg)).To(BeNil())

		// send message to stop
		msg, err = json.Marshal(struct {
			Msg string `json:"msg"`
		}{"ko"})
		Ω(err).To(BeNil())

		Ω(queue.SendBytes(msg)).To(BeNil())
	})

	It("should Listen for messages", func() {

		var listenner = func(b []byte) error {
			var data = struct {
				Msg string `json:"msg"`
			}{}

			if err := json.Unmarshal(b, &data); err != nil {
				return err
			} else if data.Msg == "ok" {
				result <- true
			} else {
				return errors.New("Test Error")
			}
			return nil
		}

		Ω(queue.ListenBytes(listenner)).ToNot(BeNil())
		Ω(<-result).To(BeTrue())
		close(result)
	})

})
