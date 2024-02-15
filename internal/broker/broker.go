package broker

import (
	"database/sql"
	"errors"
	_ "github.com/lib/pq"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"sync"
)

type Broker struct {
	Database *sql.DB
	Mutex    *sync.Mutex
}

func NewBroker() *Broker {
	type postgresConfig struct {
		Host     string `yaml:"host" binding:"required"`
		Port     string `yaml:"port" binding:"required"`
		User     string `yaml:"user" binding:"required"`
		Password string `yaml:"password" binding:"required"`
		Dbname   string `yaml:"dbname" binding:"required"`
	}
	var postgres postgresConfig
	if err := viper.UnmarshalKey("postgres", &postgres); err != nil {
		log.WithFields(log.Fields{
			"host":   postgres.Host,
			"port":   postgres.Port,
			"dbname": postgres.Dbname,
		}).Fatal(err.Error())
	}
	log.WithFields(log.Fields{
		"host":   postgres.Host,
		"port":   postgres.Port,
		"dbname": postgres.Dbname,
		"user":   postgres.User,
	}).Debug("Read postgres configuration successfully")

	conninfo := "host=" + postgres.Host + " port=" + postgres.Port + " user=" + postgres.User + " password=" + postgres.Password + " dbname=" + postgres.Dbname + " sslmode=disable"
	db, err := sql.Open("postgres", conninfo)
	if err != nil {
		log.WithFields(log.Fields{
			"host":   postgres.Host,
			"port":   postgres.Port,
			"dbname": postgres.Dbname,
		}).Fatal(err.Error())
	}
	err = db.Ping()
	if err != nil {
		log.WithFields(log.Fields{
			"host":   postgres.Host,
			"port":   postgres.Port,
			"dbname": postgres.Dbname,
		}).Fatal(err.Error())
	}
	log.WithFields(log.Fields{
		"host":   postgres.Host,
		"port":   postgres.Port,
		"dbname": postgres.Dbname,
	}).Debugf("Connected to database successfully")

	_, err = db.Exec("CREATE TABLE IF NOT EXISTS queues (name TEXT PRIMARY KEY, is_master BOOLEAN)")
	if err != nil {
		log.Fatal(err)
	}
	return &Broker{
		Database: db,
		Mutex:    &sync.Mutex{},
	}
}

var (
	ErrKeyAlreadyExists = errors.New("key already exists")
	ErrKeyNotExists     = errors.New("key not exists")
	ErrKeyIsEmpty       = errors.New("key is empty")
	ErrNoKeyFound       = errors.New("no key found")
)

func (b *Broker) Ping() error {
	return b.Database.Ping()
}

// AddKey: Add a new queue name to the broker
func (b *Broker) AddKey(name string, isMaster bool) error {
	b.Mutex.Lock()
	defer b.Mutex.Unlock()

	log.WithFields(log.Fields{
		"key":    name,
		"master": isMaster,
	}).Info("Add a new key to the broker")
	key := ""
	err := b.Database.QueryRow("SELECT name FROM queues WHERE name = $1", name).Scan(&key)
	if !errors.Is(err, sql.ErrNoRows) {
		return ErrKeyAlreadyExists
	}
	log.WithFields(log.Fields{
		"key":    name,
		"master": isMaster,
	}).Info("Key is going to be added to the broker")
	_, err = b.Database.Exec("INSERT INTO queues (name, is_master) VALUES ($1, $2)", name, isMaster)
	if err != nil {
		return err
	}
	log.WithFields(log.Fields{
		"key":    name,
		"master": isMaster,
	}).Info("Key added to the broker successfully")

	_, err = b.Database.Exec("CREATE TABLE IF NOT EXISTS " + name + " (id SERIAL PRIMARY KEY, value BYTEA)")
	if err != nil {
		return err
	}

	log.WithFields(log.Fields{
		"key":    name,
		"master": isMaster,
	}).Info("Key added to the database successfully")
	return nil
}

// SetKeyMaster: Set the master status of a queue
func (b *Broker) SetKeyMaster(name string, masterStatus bool) error {
	b.Mutex.Lock()
	defer b.Mutex.Unlock()

	key := ""
	err := b.Database.QueryRow("SELECT name FROM queues WHERE name = $1", name).Scan(&key)
	if err == sql.ErrNoRows {
		return ErrKeyNotExists
	}

	_, err = b.Database.Exec("UPDATE queues SET is_master = $1 WHERE name = $2", masterStatus, name)
	if err != nil {
		return err
	}
	return nil
}

// KeyPush: Push a value to a queue
func (b *Broker) KeyPush(name string, value []byte) error {
	b.Mutex.Lock()
	defer b.Mutex.Unlock()

	key := ""
	err := b.Database.QueryRow("SELECT name FROM queues WHERE name = $1", name).Scan(&key)
	if err == sql.ErrNoRows {
		log.WithFields(log.Fields{
			"name":  name,
			"value": value,
		}).Errorf("couldn't push value to the queue: %s", err)
		return ErrKeyNotExists
	}

	_, err = b.Database.Exec("INSERT INTO "+name+" (value) VALUES ($1)", value)
	if err != nil {
		log.WithFields(log.Fields{
			"name":  name,
			"value": value,
		}).Errorf("couldn't push value to the queue: %s", err)
		return err
	}
	return nil
}

// KeyPop: Pop a value from a queue
func (b *Broker) KeyPop(name string) ([]byte, error) {
	b.Mutex.Lock()
	defer b.Mutex.Unlock()
	// check queue exist
	key := ""
	err := b.Database.QueryRow("SELECT name FROM queues WHERE name = $1", name).Scan(&key)
	if err == sql.ErrNoRows {
		return nil, ErrKeyNotExists
	}

	// check queue is empty
	var id int
	err = b.Database.QueryRow("SELECT id FROM " + name + " ORDER BY id LIMIT 1").Scan(&id)
	if err == sql.ErrNoRows {
		return nil, ErrKeyIsEmpty
	}

	// pop value
	var value []byte
	err = b.Database.QueryRow("DELETE FROM "+name+" WHERE id = $1 RETURNING value", id).Scan(&value)
	if err != nil {
		return nil, err
	}
	return value, nil
}

// Front: Get the front value of any key that is a master and not empty
func (b *Broker) Front() (string, []byte, error) {
	b.Mutex.Lock()
	defer b.Mutex.Unlock()

	rows, err := b.Database.Query("SELECT name FROM queues WHERE is_master = true")
	defer rows.Close()

	if errors.Is(err, sql.ErrNoRows) {
		log.WithFields(log.Fields{
			"master": true,
		}).Infof("No master key found: %s", err.Error())
		return "", nil, nil
	}
	if err != nil {
		return "", nil, err
	}

	for rows.Next() {
		var name string
		err = rows.Scan(&name)
		if err != nil {
			continue
		}
		var id int
		err = b.Database.QueryRow("SELECT id FROM " + name + " ORDER BY id LIMIT 1").Scan(&id)
		if errors.Is(err, sql.ErrNoRows) {
			continue
		}

		var value []byte
		err = b.Database.QueryRow("SELECT value FROM "+name+" WHERE id = $1", id).Scan(&value)
		if err != nil {
			continue
		}
		_, err = b.Database.Exec("DELETE FROM "+name+" WHERE id = $1", id)
		if err != nil {
			return "", nil, err
		}
		return name, value, nil
	}
	return "", nil, ErrNoKeyFound
}

// Import: Add a key and push values to it
func (b *Broker) Import(name string, isMaster bool, values [][]byte) error {
	log.WithFields(log.Fields{
		"key":    name,
		"master": isMaster,
		"values": values,
	}).Info("Import a new key to the broker")
	err := b.AddKey(name, isMaster)
	if err != nil {
		return err
	}
	for _, value := range values {
		err = b.KeyPush(name, value)
		if err != nil {
			return err
		}
	}
	return nil
}

// Export: Get all elements from a key
func (b *Broker) Export(name string) ([][]byte, error) {
	b.Mutex.Lock()
	defer b.Mutex.Unlock()

	// check queue exist
	key := ""
	err := b.Database.QueryRow("SELECT name FROM queues WHERE name = $1", name).Scan(&key)
	if err == sql.ErrNoRows {
		return nil, ErrKeyNotExists
	}

	// get all values in order of id
	rows, err := b.Database.Query("SELECT value FROM " + name + " ORDER BY id")
	defer rows.Close()

	if err != nil {
		return nil, err
	}

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

	// delete all values
	b.Database.Exec("DROP TABLE " + name)
	b.Database.Exec("DELETE FROM queues WHERE name = $1", name)

	if err != nil {
		return nil, err
	}
	return values, nil
}
