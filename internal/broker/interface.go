package broker

type Broker interface {
	AddQueue(queueName string, isMaster bool) error
	SetQueueMaster(queueName string, masterStatus bool) error
	QueuePush(queueName string, value []byte) error
	QueuePop(queueName string) ([]byte, error)
	Pop() (string, []byte, error)
}
