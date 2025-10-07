package handlers

import (
	"rdbms/api/http"
	"rdbms/src"

	"github.com/gin-gonic/gin"
)

type Handler struct {
	Stg src.StorageI
}

func NewHandler(stg src.StorageI) Handler {
	return Handler{
		Stg: stg,
	}
}

func (h *Handler) handleResponse(c *gin.Context, status http.Status, data interface{}) {
	c.JSON(status.Code, http.Response{
		Status:      status.Status,
		Description: status.Description,
		Data:        data,
	})
}
