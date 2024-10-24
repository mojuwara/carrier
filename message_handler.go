package main

import (
	"log"
	"net/http"
	"time"

	"github.com/google/uuid"
)

func HandleMessage(w http.ResponseWriter, r *http.Request) {
	msg, err := UnmarshalMsg(r)
	if err != nil {
		log.Println(err.Error())
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	c := GetTopic(msg.TopicName)
	if c == nil {
		log.Printf("Received Message for non-existant Topic: '%s'\n", msg.TopicName)
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	msg.TSCreated = time.Now()
	msg.ID = uuid.New().String()
	msg.processed = make(chan bool)
	if msg.Type == PUB {
		SaveMessage(msg)
	}
	c <- msg

	// Wait for Message to be processed
	// TODO: Should we wait for the Msg to be processed or should we be optimistic?
	if success := <-msg.processed; !success {
		log.Printf("WARNING: Failed to process Message '%s'\n", msg.ID)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
}
