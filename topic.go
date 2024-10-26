package main

import (
	"log"
	"sync"
	"time"
)

const (
	PUB   = "Pub"
	SUB   = "Sub"
	UNSUB = "Unsub"

	// The number of Subscribe, Unsubscribe and Publish messages that can be queued for a Topic
	TOPIC_CHAN_SIZE = 64

	// The number of messages that can be queued for each Topic that a client is subscribed to.
	// Should be fine is this is a large number, we're only storing pointers to the original Message.
	// Helps account for bursts in messages published to a single Topic
	SUB_CHAN_SIZE = 256
)

type Topic struct {
	CreatorAddr string
	Name        string
	TSCreated   time.Time
}

// Used to get the channel that is being monitored by the Topic goroutine
var TopicMap = make(map[string]MessageChan)
var TopicMapLock sync.RWMutex

func CreateTopic(t *Topic) MessageChan {
	TopicMapLock.Lock()
	defer TopicMapLock.Unlock()

	// If Topic already exists
	if c, ok := TopicMap[t.Name]; ok {
		return c
	}

	c := make(MessageChan, TOPIC_CHAN_SIZE)
	go StartTopicRoutine(t, c)
	TopicMap[t.Name] = c
	return c
}

func GetTopic(name string) MessageChan {
	TopicMapLock.Lock()
	defer TopicMapLock.Unlock()

	return TopicMap[name]
}

func StartTopicRoutine(t *Topic, topicChan MessageChan) {
	log.Printf("INFO: Starting Topic Goroutine for Topic '%s'\n", t.Name)

	// Maps subscriber addrs to Subscriber
	subs := make(map[string]*Subscriber)

	for msg := range topicChan {
		processTopicMsg(msg, subs)
		msg.processed <- true // Let handler know Message was processed
	}
}

func processTopicMsg(msg *Message, subs map[string]*Subscriber) {
	switch msg.Type {
	case SUB:
		// log.Printf("INFO: Subscribing '%s' to Topic '%s'\n", msg.CreatorAddr, msg.TopicName)
		if _, ok := subs[msg.CreatorAddr]; !ok {
			c := make(MessageChan, SUB_CHAN_SIZE)
			sub := &Subscriber{Addr: msg.CreatorAddr, Topic: msg.TopicName, TSSubscribed: time.Now(), Chan: c}
			subs[msg.CreatorAddr] = sub
			go StartSubscriberRoutine(sub, c)

			if !msg.Persisted {
				SaveSubscriber(sub)
			}
		}
	case UNSUB:
		// log.Printf("INFO: Unsubscribing '%s' to Topic '%s'\n", msg.CreatorAddr, msg.TopicName)
		if sub, ok := subs[msg.CreatorAddr]; ok {
			DeleteSubscriber(sub)
			close(sub.Chan)

			if !msg.Persisted {
				delete(subs, msg.CreatorAddr)
			}
		}
	case PUB:
		// If this Message is intended for one person. i.e. loaded from DB after a restart
		if msg.SubscriberAddr != "" {
			log.Printf("INFO: Publishing message '%s' from DB to Subscriber '%s' for Topic '%s'\n", msg.ID, msg.SubscriberAddr, msg.TopicName)
			subs[msg.SubscriberAddr].Chan <- msg
			return
		}

		log.Printf("INFO: Processing 'Pub' message '%s' for subscribers of Topic '%s'\n", msg.ID, msg.TopicName)
		SaveBulkPendingMessage(subs, msg)
		for _, sub := range subs {
			sub.Chan <- msg
		}
	}
}
