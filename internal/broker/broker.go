package broker

import (
	"database/sql"
	"errors"
	"log"

	_ "github.com/lib/pq"
)

type Broker struct {
	Database *sql.DB
}

func NewBroker() *Broker {
	db, err := sql.Open("postgres", "user=postgres password=postgres dbname=postgres sslmode=disable port=5433")
	if err != nil {
		log.Fatal(err)
	}
	_, err = db.Exec("CREATE TABLE IF NOT EXISTS queues (name TEXT PRIMARY KEY, is_master BOOLEAN)")
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
	queueName := ""
	err := b.Database.QueryRow("SELECT name FROM queues WHERE name = $1", name).Scan(&queueName)
	if err != sql.ErrNoRows {
		return ErrQueueAlreadyExists
	}

	_, err = b.Database.Exec("INSERT INTO queues (name, is_master) VALUES ($1, $2)", name, isMaster)
	if err != nil {
		return err
	}

	_, err = b.Database.Exec("CREATE TABLE IF NOT EXISTS " + name + " (id SERIAL PRIMARY KEY, value BYTEA)")
	if err != nil {
		return err
	}
	return nil
}

// SetQueueMaster: Set the master status of a queue
func (b *Broker) SetQueueMaster(name string, masterStatus bool) error {
	queueName := ""
	err := b.Database.QueryRow("SELECT name FROM queues WHERE name = $1", name).Scan(&queueName)
	if err == sql.ErrNoRows {
		return ErrQueueNotExists
	}

	_, err = b.Database.Exec("UPDATE queues SET is_master = $1 WHERE name = $2", masterStatus, name)
	if err != nil {
		return err
	}
	return nil
}

// QueuePush: Push a value to a queue
func (b *Broker) QueuePush(name string, value []byte) error {
	// check queue not exists
	queueName := ""
	err := b.Database.QueryRow("SELECT name FROM queues WHERE name = $1", name).Scan(&queueName)
	if err == sql.ErrNoRows {
		return ErrQueueNotExists
	}

	_, err = b.Database.Exec("INSERT INTO "+name+" (value) VALUES ($1)", value)
	if err != nil {
		return err
	}
	return nil
}

// QueuePop: Pop a value from a queue
func (b *Broker) QueuePop(name string) ([]byte, error) {
	// check queue exist
	queueName := ""
	err := b.Database.QueryRow("SELECT name FROM queues WHERE name = $1", name).Scan(&queueName)
	if err == sql.ErrNoRows {
		return nil, ErrQueueNotExists
	}

	// check queue is empty
	var id int
	err = b.Database.QueryRow("SELECT id FROM " + name + " ORDER BY id LIMIT 1").Scan(&id)
	if err == sql.ErrNoRows {
		return nil, ErrQueueIsEmpty
	}

	// pop value
	var value []byte
	err = b.Database.QueryRow("DELETE FROM "+name+" WHERE id = $1 RETURNING value", id).Scan(&value)
	if err != nil {
		return nil, err
	}
	return value, nil
}

// Front: Get the front value of any queue that is a master and not empty
func (b *Broker) Front() (string, []byte, error) {
	//find the master queue
	var name string
	err := b.Database.QueryRow("SELECT name FROM queues WHERE is_master = true").Scan(&name)
	if err == sql.ErrNoRows {
		return "", nil, ErrNoKeyFound
	}

	// check queue is empty
	var id int
	err = b.Database.QueryRow("SELECT id FROM " + name + " ORDER BY id LIMIT 1").Scan(&id)
	if err == sql.ErrNoRows {
		return "", nil, ErrQueueIsEmpty
	}

	// get value
	var value []byte
	err = b.Database.QueryRow("SELECT value FROM "+name+" WHERE id = $1", id).Scan(&value)
	if err != nil {
		return "", nil, err
	}
	return name, value, nil
}

// import: add new queue with elements to queues
func (b *Broker) Import(name string, isMaster bool, values [][]byte) error {
	err := b.AddQueue(name, isMaster)
	if err != nil {
		return err
	}
	for _, value := range values {
		err = b.QueuePush(name, value)
		if err != nil {
			return err
		}
	}
	return nil
}

// export: get all elements from a queue
func (b *Broker) Export(name string) ([][]byte, error) {
	// check queue exist
	queueName := ""
	err := b.Database.QueryRow("SELECT name FROM queues WHERE name = $1", name).Scan(&queueName)
	if err == sql.ErrNoRows {
		return nil, ErrQueueNotExists
	}

	// get all values in order of id
	rows, err := b.Database.Query("SELECT value FROM " + name + " ORDER BY id")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	// get all values
	var values [][]byte
	for rows.Next() {
		var value []byte
		err = rows.Scan(&value)
		if err != nil {
			return nil, err
		}
		values = append(values, value)
	}
	return values, nil
}
