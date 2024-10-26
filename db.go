package main

import (
	"database/sql"
	"log"
	"os"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

var db *sql.DB

// TODO: Should call db.Close() before program ends?
func init() {
	// TODO: Remove when done testing
	// Ideally use dependency injection when running tests
	os.Remove("./data.db")

	log.Println("INFO: Connecting to database")
	conn, err := sql.Open("sqlite3", "./data.db")
	if err != nil {
		log.Fatal(err)
	}
	db = conn

	log.Println("INFO: Initializing database")
	if err = createDB(); err != nil {
		log.Fatal(err)
	}

	log.Println("INFO: Loading state from database")
	if err = loadState(); err != nil {
		log.Fatal(err)
	}
}

// Load state from DB if Carrier was restarted
func loadState() error {
	msgMap := make(map[string]*Message)

	// Load all topics
	topics := LoadTopics()
	for _, t := range topics {
		log.Printf("INFO: Loading Topic '%s' from database\n", t.Name)
		topicChan := CreateTopic(t)

		// Load all subscribers for current Topic
		subs := LoadSubscribers(t.Name)
		for _, sub := range subs {
			log.Printf("Loading Subscriber '%s' for Topic '%s' from database\n", sub.Addr, t.Name)
			topicChan <- &Message{Type: SUB, TSCreated: sub.TSSubscribed, TopicName: t.Name, CreatorAddr: sub.Addr, Persisted: true}
		}

		// Put pending Messages in Topic channel
		msgs := LoadPendingMessages(t.Name)
		for _, msg := range msgs {
			// Attempt to optimize memory: It's possible for the same Message to
			// be loaded multiple times if multiple subscribers did not receive it.
			// Only one instance of the Payload is needed - point all instances of
			// this Message to the one Payload. Could do for other fields but probably won't gain much
			log.Printf("Loading pending Message '%s' for Subscriber '%s' from database\n", msg.ID, msg.SubscriberAddr)
			if originalMsg, ok := msgMap[msg.ID]; ok {
				msg.Payload = originalMsg.Payload
			} else {
				msgMap[msg.ID] = msg
			}

			topicChan <- msg
		}
	}

	return nil
}

func SaveTopic(t *Topic) error {
	q := `insert into topic(topic_name, creator_addr, ts_created) values(?, ?, ?)`
	if _, err := db.Exec(q, t.Name, t.CreatorAddr, t.TSCreated); err != nil {
		log.Println("ERROR:", err)
		return err
	}
	log.Printf("INFO: Saved Topic '%s' in database\n", t.Name)
	return nil
}

func LoadTopics() []*Topic {
	results := []*Topic{}
	rows, err := db.Query(`SELECT topic_name, creator_addr, ts_created FROM topic`)
	if err != nil {
		return nil
	}

	for rows.Next() {
		t := &Topic{}
		err := rows.Scan(&t.Name, &t.CreatorAddr, &t.TSCreated)
		if err != nil {
			log.Fatal(err)
		}
		results = append(results, t)
	}
	return results
}

func SaveMessage(m *Message) error {
	q := `insert into msg(id, msg_type, topic_name, payload, payload_type, creator_addr, ts_created) values(?, ?, ?, ?, ?, ?, ?)`
	if _, err := db.Exec(q, m.ID, m.Type, m.TopicName, m.Payload, m.PayloadType, m.CreatorAddr, m.TSCreated); err != nil {
		log.Println(m.Payload)
		log.Println("ERROR:", err)
		return err
	}
	m.Persisted = true
	log.Printf("INFO: Saved '%s' Message '%s' for Topic '%s' in database\n", m.Type, m.ID, m.TopicName)
	return nil
}

func SavePendingMessage(sub *Subscriber, m *Message) error {
	q := `insert into pending_msg(msg_id, subscriber_addr, ts_inserted) values(?, ?, ?)`
	if _, err := db.Exec(q, m.ID, sub.Addr, time.Now()); err != nil {
		log.Println("ERROR:", err)
		return err
	}
	log.Printf("INFO: Saved pending Message '%s' for Topic '%s' and subscriber '%s' in database\n", m.ID, m.TopicName, sub.Addr)
	return nil
}

func SaveBulkPendingMessage(subs map[string]*Subscriber, m *Message) error {
	tx, err := db.Begin()
	if err != nil {
		log.Println("ERROR: Beginning transaction", err)
		return err
	}

	q := `insert into pending_msg(msg_id, subscriber_addr, ts_inserted) values(?, ?, ?)`
	stmt, err := tx.Prepare(q)
	if err != nil {
		log.Println("ERROR: Preparing statement", err)
		tx.Rollback()
		return err
	}

	ts := time.Now()
	for _, sub := range subs {
		if _, err := stmt.Exec(m.ID, sub.Addr, ts); err != nil {
			log.Println("ERROR: Inserting pending message", err)
			tx.Rollback()
			return err
		}
	}

	if err = tx.Commit(); err != nil {
		log.Println("ERROR: committing Transaction", err)
		return err
	}
	log.Printf("INFO: Saved bulk pending Message '%s' to all subscribers of Topic '%s' in database\n", m.ID, m.TopicName)
	return nil
}

func DeletePendingMessage(sub *Subscriber, m *Message) error {
	q := `delete from pending_msg where msg_id = ? and subscriber_addr = ?`
	if _, err := db.Exec(q, m.ID, sub.Addr, time.Now()); err != nil {
		log.Println("ERROR:", err)
		return err
	}
	log.Printf("INFO: Deleted pending Message for Topic '%s' and subscriber '%s' in database\n", m.TopicName, sub.Addr)
	return nil
}

func LoadPendingMessages(topic string) []*Message {
	q := `
	select msg.id, subscriber_addr, msg_type, topic_name, payload, payload_type, creator_addr, ts_created
	from pending_msg left join msg on pending_msg.msg_id = msg.id
	where topic_name = ?
	order by ts_created asc
	`
	rows, err := db.Query(q, topic)
	if err != nil {
		log.Fatal(err)
	}

	results := []*Message{}
	for rows.Next() {
		m := &Message{Persisted: true}
		err := rows.Scan(&m.ID, &m.SubscriberAddr, &m.Type, &m.TopicName, &m.Payload, &m.PayloadType, &m.CreatorAddr, &m.TSCreated)
		if err != nil {
			log.Fatal(err)
		}
		results = append(results, m)
	}
	return results
}

func SaveSubscriber(s *Subscriber) error {
	q := `insert into subscriber(subscriber_addr, topic_name, ts_subscribed) values(?, ?, ?)`
	if _, err := db.Exec(q, s.Addr, s.Topic, s.TSSubscribed); err != nil {
		log.Println("ERROR:", err)
		return err
	}
	log.Printf("INFO: Saved Subscriber '%s' for Topic '%s' in database\n", s.Addr, s.Topic)
	return nil
}

func DeleteSubscriber(s *Subscriber) error {
	q := `delete from subscriber where subscriber_addr = ? and topic_name = ?`
	if _, err := db.Exec(q, s.Addr, s.Topic); err != nil {
		log.Println("ERROR:", err)
		return err
	}
	log.Printf("INFO: Deleted Subscriber '%s' for Topic '%s' in database\n", s.Addr, s.Topic)
	return nil
}

func LoadSubscribers(topic string) []*Subscriber {
	rows, err := db.Query(`select subscriber_addr, ts_subscribed from subscriber where topic_name = ?`, topic)
	if err != nil {
		log.Fatal(err)
	}

	results := []*Subscriber{}
	for rows.Next() {
		s := &Subscriber{Topic: topic}
		err := rows.Scan(&s.Addr, &s.TSSubscribed)
		if err != nil {
			log.Fatal(err)
		}
		results = append(results, s)
	}
	return results
}

// Create DB tables
func createDB() error {
	_, err := db.Exec(`
	CREATE TABLE IF NOT EXISTS topic(
		topic_name TEXT PRIMARY KEY NOT NULL,
		creator_addr TEXT NOT NULL,
		ts_created DATETIME NOT NULL
	);

	CREATE TABLE IF NOT EXISTS subscriber(
		subscriber_addr TEXT NOT NULL,
		topic_name TEXT NOT NULL,
		ts_subscribed DATETIME NOT NULL,
		PRIMARY KEY(subscriber_addr, topic_name)
		FOREIGN KEY(topic_name) REFERENCES topic(topic_name)
		-- TODO: Create index on subscriber_addr? Probably rarely used
	);

	CREATE TABLE IF NOT EXISTS msg(
		id TEXT PRIMARY KEY NOT NULL,
		msg_type TEXT NOT NULL,
		topic_name TEXT NOT NULL,
		payload BLOB, -- Can be null if msg_type is SUB/UNSUB
		payload_type TEXT,
		creator_addr TEXT NOT NULL,
		ts_created DATETIME NOT NULL,
		FOREIGN KEY(topic_name) REFERENCES topic(topic_name)
	);

	CREATE TABLE IF NOT EXISTS pending_msg(
		msg_id TEXT NOT NULL,
		subscriber_addr TEXT NOT NULL,
		ts_inserted DATETIME NOT NULL,
		PRIMARY KEY(subscriber_addr, msg_id),
		FOREIGN KEY(msg_id) REFERENCES msg(id),
		FOREIGN KEY(subscriber_addr) REFERENCES subscriber(addr) on delete cascade
	);
	`)
	return err
}
