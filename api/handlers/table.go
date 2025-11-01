package handlers

import (
	"rdbms/api/http"
	"rdbms/api/models"
	"rdbms/utils"

	"github.com/gin-gonic/gin"
)

func (h *Handler) CreateTable(c *gin.Context) {
	var req models.CreateTableRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		h.handleResponse(c, http.BadRequest, err.Error())
		return
	}

	schema, err := utils.ToStorageSchema(req)
	if err != nil {
		h.handleResponse(c, http.InvalidArgument, err.Error())
		return
	}

	if err := h.Stg.Table().CreateTable(req.Name, &schema); err != nil {
		h.handleResponse(c, http.BadRequest, err.Error())
		return
	}

	h.handleResponse(c, http.Created, "Table created successfully!")
}
