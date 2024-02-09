package broker

import (
	"database/sql"
	"errors"
	_ "github.com/lib/pq"
	"log"
)

type Broker struct {
	Database *sql.DB
}

func NewBroker() *Broker {
	db, err := sql.Open("postgres", "user=postgres password=postgres dbname=postgres sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}
	return &Broker{
		Database: db,
	}
}

var (
	ErrQueueAlreadyExists = errors.New("queue already exists")
	ErrQueueNotExists     = errors.New("queue not exists")
	ErrQueueIsEmpty       = errors.New("queue is empty")
	ErrNoKeyFound         = errors.New("no key found")
)

// AddQueue: Add a new queue name to the broker
func (b *Broker) AddQueue(name string, isMaster bool) error {
	return nil
}

// SetQueueMaster: Set the master status of a queue
func (b *Broker) SetQueueMaster(name string, masterStatus bool) error {
	return nil
}

// QueuePush: Push a value to a queue
func (b *Broker) QueuePush(name string, value []byte) error {
	return nil
}

// QueuePop: Pop a value from a queue
func (b *Broker) QueuePop(name string) ([]byte, error) {
	return nil, nil
}

// Front: Get the front value of any queue that is a master and not empty
func (b *Broker) Front() (string, []byte, error) {
	return "", nil, nil
}
