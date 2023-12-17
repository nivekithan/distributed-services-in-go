package server

import (
	"encoding/json"
	"net/http"
)

type httpServer struct {
	log *Log
}

type ProduceRequest struct {
	Value []byte `json:"value"`
}

type ProduceResponse struct {
	Offsert int `json:"offset"`
}

func (server *httpServer) handleProduce(res http.ResponseWriter, req *http.Request) {
	var produceRequest ProduceRequest

	if err := json.NewDecoder(req.Body).Decode(&produceRequest); err != nil {
		http.Error(res, err.Error(), http.StatusBadRequest)
		return
	}

	offset, err := server.log.Append(produceRequest.Value)

	if err != nil {
		http.Error(res, err.Error(), http.StatusInternalServerError)
		return
	}

	produceResponse := ProduceResponse{Offsert: offset}

	if err := json.NewEncoder(res).Encode(produceResponse); err != nil {
		http.Error(res, err.Error(), http.StatusInternalServerError)
		return
	}
}

type ConsumeRequest struct {
	Offset int `json:"offset"`
}

type ConsumeResponse struct {
	Value []byte `json:"value"`
}

func (server *httpServer) handleConsume(res http.ResponseWriter, req *http.Request) {
	var consumeRequest ConsumeRequest

	if err := json.NewDecoder(req.Body).Decode(&consumeRequest); err != nil {
		http.Error(res, err.Error(), http.StatusBadRequest)
		return
	}

	record, err := server.log.Read(consumeRequest.Offset)

	if err != nil {
		http.Error(res, err.Error(), http.StatusNotFound)
		return
	}

	consumeResponse := ConsumeResponse{Value: record.Value}

	if err := json.NewEncoder(res).Encode(consumeResponse); err != nil {
		http.Error(res, err.Error(), http.StatusInternalServerError)
		return
	}

}

func NewHttpServer(addr string) *http.Server {
	httpServer := httpServer{log: NewLog()}

	http.HandleFunc("/", func(res http.ResponseWriter, req *http.Request) {
		method := req.Method

		if method == "GET" {
			httpServer.handleConsume(res, req)
		} else if method == "POST" {
			httpServer.handleProduce(res, req)
		} else {
			http.Error(res, "Method not allowed", http.StatusMethodNotAllowed)
		}
	})

	server := &http.Server{
		Addr:    addr,
		Handler: nil,
	}

	return server
}
