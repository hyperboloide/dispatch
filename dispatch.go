package dispatch

// ListennerBytes is a type of function that received a []byte from the queue
type ListennerBytes func([]byte) error

// Queue represents a generic queue
type Queue interface {
	// Send(interface{}) error
	SendBytes([]byte) error
	ListenBytes(fn ListennerBytes) error
}

// PersistantQueue is a queue saved on disk
type PersistantQueue interface {
	Queue
	Purge() error
}
