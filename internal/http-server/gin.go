package http_server

import (
	"github.com/gin-gonic/gin"
	"mai/internal/broker"
)

type GinServer struct {
	broker *broker.Broker
	gin    *gin.Engine
}

func NewGinServer(broker *broker.Broker) *GinServer {
	gs := &GinServer{
		broker: broker,
		gin:    gin.Default(),
	}
	gs.registerRoutes()
	return gs
}

func (s *GinServer) registerRoutes() {
	s.gin.POST("/queue", s.AddQueue)
	s.gin.POST("/queue/:queue_name/set_master", s.QueueSetMaster)
	s.gin.POST("/queue/:queue_name/push", s.QueuePush)
	s.gin.POST("/queue/:queue_name/pop", s.QueuePop)
	s.gin.GET("/front", s.Front)
}

func (s *GinServer) Run() {
	s.gin.Run()
}

type AddQueueRequest struct {
	QueueName string `json:"queue_name"`
	IsMaster  bool   `json:"is_master"`
}

type QueuePushRequest struct {
	Value []byte `json:"value"`
}

type QueueSetMasterRequest struct {
	MasterStatus bool `json:"master_status"`
}

func (s *GinServer) AddQueue(c *gin.Context) {
	req := &AddQueueRequest{}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(400, gin.H{"error": err.Error()})
		return
	}
	if err := (*s.broker).AddQueue(req.QueueName, req.IsMaster); err != nil {
		c.JSON(400, gin.H{"error": err.Error()})
		return
	}
	c.JSON(200, gin.H{"message": "ok", "queue_name": req.QueueName})
}

func (s *GinServer) QueuePush(c *gin.Context) {
	queueName := c.Param("queue_name")
	data := &QueuePushRequest{}
	if err := c.ShouldBindJSON(data); err != nil {
		c.JSON(400, gin.H{"error": err.Error()})
		return
	}
	if err := (*s.broker).QueuePush(queueName, data.Value); err != nil {
		c.JSON(400, gin.H{"error": err.Error(), "queue_name": queueName})
		return
	}
	c.JSON(200, gin.H{"message": "ok"})
}

func (s *GinServer) QueuePop(c *gin.Context) {
	queueName := c.Param("queue_name")
	value, err := (*s.broker).QueuePop(queueName)
	if err != nil {
		c.JSON(400, gin.H{"error": err.Error()})
		return
	}
	c.JSON(200, gin.H{"message": "ok", "value": value})
}

func (s *GinServer) QueueSetMaster(c *gin.Context) {
	queueName := c.Param("queue_name")
	data := &QueueSetMasterRequest{}
	if err := c.ShouldBindJSON(&data); err != nil {
		c.JSON(400, gin.H{"error": err.Error()})
		return
	}
	if err := (*s.broker).SetQueueMaster(queueName, data.MasterStatus); err != nil {
		c.JSON(400, gin.H{"error": err.Error()})
		return
	}
	c.JSON(200, gin.H{"message": "ok"})
}

func (s *GinServer) Front(c *gin.Context) {
	queueName, value, err := (*s.broker).Front()
	if err != nil {
		c.JSON(400, gin.H{"error": err.Error()})
		return
	}
	c.JSON(200, gin.H{"message": "ok", "queue_name": queueName, "value": value})
}
