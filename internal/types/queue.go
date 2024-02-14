package types

type AddKeyRequest struct {
	Key      string `json:"key"`
	IsMaster bool   `json:"isMaster"`
}

type KeyPushRequest struct {
	Value []byte `json:"value"`
}

type KeySetMasterRequest struct {
	MasterStatus bool `json:"masterStatus"`
}

type ExportRequest struct {
	Key string `json:"key" binding:"required"`
}

type ImportRequest struct {
	Key      string   `json:"key" binding:"required"`
	Values   [][]byte `json:"values" binding:"required"`
	IsMaster bool     `json:"isMaster" binding:"required"`
}
