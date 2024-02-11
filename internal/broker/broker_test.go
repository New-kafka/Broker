package broker

//ToDO: Add tests for ci/cd pipeline

// run with sudo
import (
	"os/exec"
	"testing"

	"github.com/stretchr/testify/assert"
)

func startDatabase() {
	cmd := exec.Command("/bin/sh", "start-database.sh")
	err := cmd.Run()
	if err != nil {
		panic(err)
	}
}

func TestSimpleBroker_AddQueue(t *testing.T) {
	startDatabase()
	broker := NewBroker()
	err := broker.AddQueue("test", true)
	assert.NoError(t, err)
}

func TestSimpleBroker_AddQueueDuplicateName(t *testing.T) {
	startDatabase()
	broker := NewBroker()
	err := broker.AddQueue("test", true)
	assert.NoError(t, err)
	err = broker.AddQueue("test", true)
	assert.ErrorIs(t, err, ErrQueueAlreadyExists)
}

func TestSimpleBroker_Front(t *testing.T) {
	startDatabase()
	broker := NewBroker()
	err := broker.AddQueue("test", true)
	assert.NoError(t, err)

	err = broker.QueuePush("test", []byte("test-message"))
	assert.NoError(t, err)

	queueName, value, err := broker.Front()
	assert.NoError(t, err)
	assert.Equal(t, "test", queueName)
	assert.Equal(t, []byte("test-message"), value)
}

func TestSimpleBroker_Export(t *testing.T) {
	startDatabase()
	broker := NewBroker()
	err := broker.AddQueue("test", true)
	assert.NoError(t, err)
	err = broker.QueuePush("test", []byte("test-message1"))
	assert.NoError(t, err)
	err = broker.QueuePush("test", []byte("test-message2"))
	assert.NoError(t, err)

	values, err := broker.Export("test")
	assert.NoError(t, err)
	assert.Equal(t, [][]byte{[]byte("test-message1"), []byte("test-message2")}, values)
}

func TestSimpleBroker_Import(t *testing.T) {
	startDatabase()
	broker := NewBroker()

	err = broker.Import("test", true, [][]byte{[]byte("test-message1"), []byte("test-message2")})
	assert.NoError(t, err)

	values, err := broker.Export("test")
	assert.NoError(t, err)
	assert.Equal(t, [][]byte{[]byte("test-message1"), []byte("test-message2")}, values)
}
