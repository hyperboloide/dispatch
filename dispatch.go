package dispatch

type ListennerBytes func([]byte) error

type Queue interface {
	SendBytes([]byte) error
	ListenBytes(fn ListennerBytes) error
}

type PersistantQueue interface {
	Queue
	Purge() error
}
