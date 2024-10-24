package main

import (
	"encoding/json"
	"log"
	"net/http"
	"time"
)

func HandleTopicCreate(w http.ResponseWriter, r *http.Request) {
	m, err := UnmarshalMsg(r)
	if err != nil {
		log.Println(err.Error())
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	if m.TopicName == "" {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]string{"msg": "'topic' name must be provided"})
		return
	}

	sender := m.CreatorAddr
	if sender == "" {
		sender = r.RemoteAddr
	}

	topic := &Topic{Name: m.TopicName, CreatorAddr: sender, TSCreated: time.Now()}
	SaveTopic(topic)
	CreateTopic(topic)
}
