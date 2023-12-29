package broker

type SimpleBroker struct {
}

func NewBroker() Broker {
	return &SimpleBroker{}
}

func (b *SimpleBroker) AddQueue(name string, isMaster bool) error {
	return nil
}

func (b *SimpleBroker) SetQueueMaster(name string, masterStatus bool) error {
	return nil
}

func (b *SimpleBroker) QueuePush(name string, value []byte) error {
	return nil
}

func (b *SimpleBroker) QueuePop(name string) ([]byte, error) {
	return nil, nil
}

func (b *SimpleBroker) Pop() (string, []byte, error) {
	return "", nil, nil
}
