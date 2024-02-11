package types

type AddQueueRequest struct {
	QueueName string `json:"queue_name" binding:"required" validate:"min=1,max=100"`
	IsMaster  bool   `json:"is_master" binding:"required" validate:"oneof=true false"`
}

type QueuePushRequest struct {
	Value []byte `json:"value" binding:"required"`
}

type QueueSetMasterRequest struct {
	MasterStatus bool `json:"master_status" binding:"required" validate:"oneof=true false"`
}
