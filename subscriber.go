package main

import (
	"bytes"
	"encoding/json"
	"log"
	"net/http"
	"time"
)

const (
	// If a response is not received in this time, assume failure. Don't want
	// to wait indefinitely and possibly block channel and other Subscriber goroutines
	// on this Topic
	REQUEST_TIMEOUT = time.Second * 5

	// Periodically check if there are any pending messages to be sent to Subscribers
	RETRY_INTERVAL = time.Second * 10
)

type Subscriber struct {
	Addr         string
	Topic        string
	TSSubscribed time.Time
	Chan         chan *Message
}

// TODO: One unresponsive subscriber could be the reason for many Messages being in memory.
// We should identify this, free space in pendingMsgs and just add pending Messages to the DB. When the Subscriber is responsive again, pick them up from the DB
func StartSubscriberRoutine(sub *Subscriber, c MessageChan) {
	pendingMsgs := &List{}
	ticker := time.NewTicker(RETRY_INTERVAL)

	// TODO: Handle chan being closed
	for {
		select {
		case msg := <-c:
			if msg == nil {
				return // This person unsubscribed
			}

			// Deliver message to Subscriber, add to pendingMsgs if failed
			if !pendingMsgs.Empty() || !deliverMsg(msg, sub.Addr) {
				SavePendingMessage(sub, msg)
				pendingMsgs.Push(msg)
			}
		case <-ticker.C:
			for !pendingMsgs.Empty() {
				// Attempt to deliver the next pending message, or try later if fail
				msg := pendingMsgs.Head.Msg
				if deliverMsg(msg, sub.Addr) {
					DeletePendingMessage(sub, msg)
					pendingMsgs.Pop()
				} else {
					break
				}
			}
		}
	}
}

var client = &http.Client{Timeout: REQUEST_TIMEOUT}

func deliverMsg(msg *Message, recipient string) bool {
	marshaledMsg, err := json.Marshal(msg)
	if err != nil {
		log.Printf("WARNING: Error while delivering message to '%s': %s\n", recipient, err.Error())
		return false
	}

	body := bytes.NewBuffer(marshaledMsg)
	resp, err := client.Post(recipient, msg.PayloadType, body)
	if err != nil {
		log.Printf("WARNING: Error while delivering message '%s' to '%s': %s\n", msg.ID, recipient, err.Error())
		return false
	} else if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		log.Printf("WARNING: Received status code %d while delivering message '%s' to '%s'\n", resp.StatusCode, msg.ID, recipient)
		return false
	}

	log.Printf("INFO: Delivered message '%s' to '%s' on Topic '%s'\n", msg.ID, recipient, msg.TopicName)
	return true
}
