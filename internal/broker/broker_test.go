package broker

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestSimpleBroker_AddQueue(t *testing.T) {
	broker := NewBroker()
	err := broker.AddQueue("test", true)
	assert.NoError(t, err)
}

func TestSimpleBroker_AddQueueDuplicateName(t *testing.T) {
	broker := NewBroker()
	err := broker.AddQueue("test", true)
	assert.NoError(t, err)
	err = broker.AddQueue("test", true)
	assert.ErrorIs(t, err, ErrQueueAlreadyExists)
}

func TestSimpleBroker_Front(t *testing.T) {
	broker := NewBroker()
	err := broker.AddQueue("test", true)
	assert.NoError(t, err)

	err = broker.QueuePush("test", []byte("test"))
	assert.NoError(t, err)

	queueName, value, err := broker.Front()
	assert.NoError(t, err)
	assert.Equal(t, "test", queueName)
	assert.Equal(t, []byte("test"), value)
}
