package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// TODO: Test if pending Messages are retried
// TODO: Test if pending Messages are loaded and retried if Carrier restarts
// TODO: Find max Topics + subscribers + messages we can handle

func TestPublish(t *testing.T) {
	N := 3
	topic := "NYSE"
	clients := MakeTestClients(N, t)
	for i := 0; i < N; i++ {
		defer clients[i].Server.Close()
	}

	// Create new Topic from the first client
	m := &Message{TopicName: topic, CreatorAddr: clients[0].Server.URL}
	_, err := MakeReq(HandleTopicCreate, "POST", "http://localhost:8080/create", m)
	if err != nil {
		t.Fatal(err.Error())
	}

	// Have remaining clients subscribe to Topic
	for i := 1; i < N; i++ {
		m = &Message{Type: SUB, TopicName: topic, CreatorAddr: clients[i].Server.URL}
		_, err = MakeReq(HandleMessage, "POST", "http://localhost:8080/msg", m)
		if err != nil {
			t.Fatalf("While client index %d was subscribing to Topic '%s'", i, err.Error())
		}
	}

	// Create and publish message to topic from the first client
	data := Stock{Ticker: "DUDE", Price: 100}
	payload, err := json.Marshal(data)
	if err != nil {
		t.Fatal(err.Error())
	}

	m = &Message{Type: PUB, TopicName: topic, CreatorAddr: clients[0].Server.URL, Payload: payload, PayloadType: "application/json"}
	_, err = MakeReq(HandleMessage, "POST", "http://localhost:8080/msg", m)
	if err != nil {
		t.Fatal(err.Error())
	}

	// Give Carrier a chance to deliver Messages
	time.Sleep(time.Millisecond * 500)

	// Validate Messages were received by subscribers
	for i := 1; i < N; i++ {
		msgs := clients[i].Msgs
		if len(msgs) != 1 || msgs[len(msgs)-1].Payload == nil {
			t.Fatal("Did not receive message on client index", i)
		}
	}

	// Have remaining clients unsubscribe to Topic
	for i := 1; i < N; i++ {
		m = &Message{Type: UNSUB, TopicName: topic, CreatorAddr: clients[i].Server.URL}
		_, err = MakeReq(HandleMessage, "POST", "http://localhost:8080/msg", m)
		if err != nil {
			t.Fatalf("While client index %d was subscribing to Topic '%s'", i, err.Error())
		}
	}

	// Give Carrier a chance to handle unsubscriptions
	time.Sleep(time.Millisecond * 500)
}

func MakeReq(handler http.HandlerFunc, method string, url string, msg *Message) (*http.Response, error) {
	body, err := MakeReqBody(msg)
	if err != nil {
		return nil, err
	}

	req := httptest.NewRequest(method, url, body)
	w := httptest.NewRecorder()
	handler(w, req)
	resp := w.Result()

	if resp.StatusCode != 200 {
		return resp, fmt.Errorf("Received StatusCode: %d", resp.StatusCode)
	}

	return resp, nil
}

func MakeReqBody(m *Message) (io.Reader, error) {
	if m == nil {
		return nil, nil
	}

	data, err := json.Marshal(m)
	if err != nil {
		return nil, err
	}

	return bytes.NewBuffer(data), nil
}

type TestClient struct {
	Alive  bool // Toggle true/false to simulate unresponsive client
	Msgs   []*Message
	Server *httptest.Server
}

func (c *TestClient) SetAlive(val bool) {
	c.Alive = val
}

// Caller should "defer .Close()" on all returned Clients.Server
func MakeTestClients(n int, t *testing.T) []*TestClient {
	clients := make([]*TestClient, n)
	for i := 0; i < n; i++ {
		clients[i] = &TestClient{Alive: true}
		clients[i].Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if !clients[i].Alive {
				t.Logf("INFO: Client %d (%s) is ignoring requests\n", i, clients[i].Server.URL)
				w.WriteHeader(http.StatusInternalServerError)
				return
			}

			msg, err := UnmarshalMsg(r)
			if err != nil {
				clients[i].Msgs = nil
				t.Logf("ERROR on client %d: '%s'\n", i, err.Error())
				return
			}
			clients[i].Msgs = append(clients[i].Msgs, msg)
		}))
	}
	return clients
}

type Stock struct {
	Ticker string
	Price  float64
}
