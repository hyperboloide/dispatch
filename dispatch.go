package dispatch

// Listenner is a type of function that received a []byte from the queue
type Listenner func([]byte) error

// Queue represents a generic queue
type Queue interface {
	// Send(interface{}) error
	SendBytes([]byte) error
	Listen(fn Listenner) error
}

// PersistantQueue is a queue that persist messages.
type PersistantQueue interface {
	Queue
	Purge() error
}
