package sqs_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"testing"
)

func TestSqs(t *testing.T) {

	RegisterFailHandler(Fail)
	RunSpecs(t, "Sqs Suite")
}
