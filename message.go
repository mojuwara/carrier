package main

import "time"

type MessageChan chan *Message

type Message struct {
	ID             string
	Type           string
	TopicName      string
	Payload        []byte
	PayloadType    string
	CreatorAddr    string
	TSCreated      time.Time
	SubscriberAddr string

	// Set to true if this was already saved in DB
	Persisted bool

	// Handler-provided and will receive a bool if this message was successfully processed
	processed chan bool
}
