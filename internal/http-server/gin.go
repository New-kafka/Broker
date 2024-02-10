package http_server

import (
	"github.com/gin-gonic/gin"
	"github.com/spf13/viper"
	"github.com/new-kafka/broker/internal/broker"
	"github.com/new-kafka/broker/internal/types"
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
	gs.registerMiddlewares()
	gs.registerRoutes()
	return gs
}

func (s *GinServer) registerMiddlewares() {
	s.gin.Use(gin.Logger())
	s.gin.Use(gin.Recovery())
}

func (s *GinServer) registerRoutes() {
	s.gin.POST("/queue", s.AddQueue)
	s.gin.POST("/queue/:queue_name/set_master", s.QueueSetMaster)
	s.gin.POST("/queue/:queue_name/push", s.QueuePush)
	s.gin.POST("/queue/:queue_name/pop", s.QueuePop)
	s.gin.GET("/front", s.Front)
	s.gin.GET(viper.GetString("health_check_path"), func(c *gin.Context) {
		c.JSON(200, gin.H{"message": "ok"})
	})
}

func (s *GinServer) Run() {
	s.gin.Run("localhost:" + viper.GetString("port"))
}

func (s *GinServer) AddQueue(c *gin.Context) {
	req := &types.AddQueueRequest{}
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
	data := &types.QueuePushRequest{}
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
	data := &types.QueueSetMasterRequest{}
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
