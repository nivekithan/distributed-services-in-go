package main

import (
	"distributed-services-in-go/internal/server"
	"log"
)

func main() {
	server := server.NewHttpServer(":8080")

	log.Println(`Running server at: 8080`)
	log.Fatal(server.ListenAndServe())
}
