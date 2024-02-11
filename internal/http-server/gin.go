package http_server

import (
	"github.com/gin-gonic/gin"
	"github.com/new-kafka/broker/internal/broker"
	"github.com/new-kafka/broker/internal/types"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
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
	s.gin.POST("/key", s.AddKey)
	s.gin.POST("/key/:key/set_master", s.KeySetMaster)
	s.gin.POST("/key/:key/push", s.KeyPush)
	s.gin.POST("/key/:key/pop", s.KeyPop)
	s.gin.POST("/export", s.Export)
	s.gin.GET("/import", s.Import)
	s.gin.GET("/front", s.Front)

	s.gin.GET(viper.GetString("health_check_path"), func(c *gin.Context) {
		c.JSON(200, gin.H{"message": "ok"})
	})
}

func (s *GinServer) Run() {
	s.gin.Run("0.0.0.0:" + viper.GetString("port"))
}

func (s *GinServer) AddKey(c *gin.Context) {
	req := &types.AddKeyRequest{}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(400, gin.H{"error": err.Error()})
		return
	}
	log.WithFields(log.Fields{
		"key":    req.Key,
		"master": req.IsMaster,
	}).Debug("Add a new key to the broker")
	if err := (*s.broker).AddKey(req.Key, req.IsMaster); err != nil {
		log.WithFields(log.Fields{
			"key":    req.Key,
			"master": req.IsMaster,
		}).Warnf("Couldn't add key to the broker: %s", err.Error())
		c.JSON(400, gin.H{"error": err.Error()})
		return
	}
	log.WithFields(log.Fields{
		"key":    req.Key,
		"master": req.IsMaster,
	}).Info("Key added to the broker successfully")
	c.JSON(200, gin.H{"message": "ok", "key": req.Key})
}

func (s *GinServer) KeyPush(c *gin.Context) {
	key := c.Param("key")
	data := &types.KeyPushRequest{}
	if err := c.ShouldBindJSON(data); err != nil {
		c.JSON(400, gin.H{"error": err.Error()})
		return
	}
	if err := (*s.broker).KeyPush(key, data.Value); err != nil {
		c.JSON(400, gin.H{"error": err.Error(), "key": key})
		return
	}
	c.JSON(200, gin.H{"message": "ok"})
}

func (s *GinServer) KeyPop(c *gin.Context) {
	key := c.Param("key")
	value, err := (*s.broker).KeyPop(key)
	if err != nil {
		c.JSON(400, gin.H{"error": err.Error()})
		return
	}
	c.JSON(200, gin.H{"message": "ok", "value": value})
}

func (s *GinServer) KeySetMaster(c *gin.Context) {
	key := c.Param("key")
	data := &types.KeySetMasterRequest{}
	if err := c.ShouldBindJSON(&data); err != nil {
		c.JSON(400, gin.H{"error": err.Error()})
		return
	}
	if err := (*s.broker).SetKeyMaster(key, data.MasterStatus); err != nil {
		c.JSON(400, gin.H{"error": err.Error()})
		return
	}
	c.JSON(200, gin.H{"message": "ok"})
}

func (s *GinServer) Import(c *gin.Context) {
	req := &types.ImportRequest{}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(400, gin.H{"error": err.Error()})
		return
	}
	if err := (*s.broker).Import(req.Key, req.IsMaster, req.Data); err != nil {
		c.JSON(400, gin.H{"error": err.Error()})
		return
	}
	c.JSON(200, gin.H{"message": "ok"})
}

func (s *GinServer) Export(c *gin.Context) {
	req := &types.ExportRequest{}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(400, gin.H{"error": err.Error()})
		return
	}
	res, err := (*s.broker).Export(req.Key)
	if err != nil {
		c.JSON(400, gin.H{"error": err.Error()})
		return
	}
	c.JSON(200, gin.H{"message": "ok", "value": res})
}

func (s *GinServer) Front(c *gin.Context) {
	key, value, err := (*s.broker).Front()
	if err != nil {
		c.JSON(400, gin.H{"error": err.Error()})
		return
	}
	c.JSON(200, gin.H{"message": "ok", "key": key, "value": value})
}
