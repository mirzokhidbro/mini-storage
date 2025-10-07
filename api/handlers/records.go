package handlers

import (
	"rdbms/api/http"
	"rdbms/api/models"
	"rdbms/src/storage"

	"github.com/gin-gonic/gin"
)

func (h *Handler) InsertRecord(c *gin.Context) {
	var req models.InsertRecordRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		h.handleResponse(c, http.BadRequest, err.Error())
		return
	}

	schema, err := h.Stg.Table().GetTableSchema(req.Name + ".schema")
	if err != nil {
		h.handleResponse(c, http.NOT_FOUND, err.Error())
		return
	}

	items := make([]storage.Item, 0, len(schema.Columns))
	for _, col := range schema.Columns {
		v, ok := req.Values[col.Name]
		if !ok {
			h.handleResponse(c, http.InvalidArgument, "missing column: "+col.Name)
			return
		}

		switch col.Type {
		case storage.TypeInt:
			n, ok := v.(float64)
			if !ok || n != float64(int(n)) {
				h.handleResponse(c, http.InvalidArgument, "column "+col.Name+" must be integer")
				return
			}
			items = append(items, storage.Item{Literal: int(n)})
		case storage.TypeVarchar:
			s, ok := v.(string)
			if !ok {
				h.handleResponse(c, http.InvalidArgument, "column "+col.Name+" must be string")
				return
			}
			if len(s) > col.Length {
				h.handleResponse(c, http.InvalidArgument, "column "+col.Name+" exceeds length")
				return
			}
			items = append(items, storage.Item{Literal: s})
		}
	}

	if err := h.Stg.Table().Insert(req.Name, storage.Record{Items: items}); err != nil {
		h.handleResponse(c, http.InternalServerError, err.Error())
		return
	}

	h.handleResponse(c, http.Created, "Record inserted")
}

func (h *Handler) GetAllRecords(c *gin.Context) {
	var req models.GetAllRecordsRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		h.handleResponse(c, http.BadRequest, err.Error())
		return
	}

	schema, err := h.Stg.Table().GetTableSchema(req.Name + ".schema")
	if err != nil {
		h.handleResponse(c, http.NOT_FOUND, err.Error())
		return
	}

	records, err := h.Stg.Table().GetAllData(req.Name)
	if err != nil {
		h.handleResponse(c, http.InternalServerError, err.Error())
		return
	}

	data := make([]map[string]any, 0, len(records))
	for _, r := range records {
		row := make(map[string]any, len(schema.Columns))
		for i, col := range schema.Columns {
			row[col.Name] = r.Items[i].Literal
		}
		data = append(data, row)
	}

	h.handleResponse(c, http.OK, data)
}
