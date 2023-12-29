package broker

import "errors"

type SimpleBroker struct {
	Queue         map[string][][]byte
	IsMasterQueue map[string]bool
}

var (
	ErrQueueAlreadyExists = errors.New("queue already exists")
	ErrQueueNotExists     = errors.New("queue not exists")
	ErrQueueIsEmpty       = errors.New("queue is empty")
	ErrNoKeyFound         = errors.New("no key found")
)

func NewBroker() Broker {
	return &SimpleBroker{
		Queue:         make(map[string][][]byte),
		IsMasterQueue: make(map[string]bool),
	}
}

func (b *SimpleBroker) AddQueue(name string, isMaster bool) error {
	if _, ok := b.Queue[name]; !ok {
		b.Queue[name] = [][]byte{}
		b.IsMasterQueue[name] = isMaster
	} else {
		return ErrQueueAlreadyExists
	}
	return nil
}

func (b *SimpleBroker) SetQueueMaster(name string, masterStatus bool) error {
	if _, ok := b.Queue[name]; ok {
		b.IsMasterQueue[name] = masterStatus
	} else {
		return ErrQueueNotExists
	}
	return nil
}

func (b *SimpleBroker) QueuePush(name string, value []byte) error {
	if _, ok := b.Queue[name]; ok {
		b.Queue[name] = append(b.Queue[name], value)
	} else {
		return ErrQueueNotExists
	}
	return nil
}

func (b *SimpleBroker) QueuePop(name string) ([]byte, error) {
	if queue, ok := b.Queue[name]; ok {
		if len(queue) == 0 {
			return nil, ErrQueueIsEmpty
		}
		head := queue[0]
		b.Queue[name] = queue[1:]
		return head, nil
	} else {
		return nil, ErrQueueNotExists
	}
}

func (b *SimpleBroker) Pop() (string, []byte, error) {
	for name, _ := range b.Queue {
		head, err := b.QueuePop(name)
		if err == nil {
			return name, head, nil
		}
	}
	return "", nil, ErrNoKeyFound
}
