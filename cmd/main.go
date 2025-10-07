package main

import (
	"net/http"
	"rdbms/api"
	"rdbms/api/handlers"
	"rdbms/src"
)

func main() {

	var stg src.StorageI
	stg, err := src.NewStorage("data")

	if err != nil {
		panic(err)
	}

	h := handlers.NewHandler(stg)

	r := api.SetUpRouter(h)
	server := &http.Server{
		Addr:    ":8000",
		Handler: r,
	}
	server.ListenAndServe()
}
