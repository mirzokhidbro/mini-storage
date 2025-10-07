package models

type InsertRecordRequest struct {
	Name   string         `json:"name" binding:"required"`
	Values map[string]any `json:"values" binding:"required"`
}

type GetAllRecordsRequest struct {
	Name string `json:"name" binding:"required"`
}
