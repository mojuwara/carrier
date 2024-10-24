# Carrier - A PubSub System

### Features
- (DONE) Create a Topic
- (DONE) Subscribe to a Topic
- (DONE) Publish to a Topic
- (DONE) Retry delivery to subscribers until success
- (TODO) Durable - Carrier picks up where it left off upon restart

### Concepts
- Topic: Logical groupings of Messages

### Why?
- It's interesting
- Learning experience

### Nice-to-haves
- SSL
- Authentication
- Make Topics private
- Alerting when issues are detected
- Golang client for easy use by developers
- Distributed Carrier architecture (Partition by Topic and use a proxy to route request to the partition hanling that Topic?)

### Design
- Server is responsible for handling client requests and delivering messages to clients
	- A `Topic goroutine` is created when a Topic is created
		- The goroutine will have a channel to receive requests from Carrier clients (to subscribe, publish, etc.)
		- A global map of Topic => channel can be accessed to get a reference to the topic's channel
	- A `subscriber goroutine` is created when a client subscribes to a Topic
		- The subscriber goroutine will have a channel for receiving messages from the Topic goroutine
		- The subscriber goroutine is responsible for delivering messages to the subscriber, handling retries, tracking undelivered messages, etc.
		- To prevent blocking when messages can't be delivered to a subscriber, messages will be kept in a linked list
	- A `storage goroutine` and global channel is created to occassionally persist:
		- Topics: Persist when Topic is created
		- Subscribers: Persist when a client subscribes to a Topic
		- Messages: Persist when a message is received
		- Pending messages: Persist when a message fails to deliver to a subscriber, remove when the subscriber receives the message
		- Requires high write concurrency if many Messages are published while many subscribers are unresponsive at that time
		- Load state from DB at start up
- Clients interacts with the server via HTTP requests
	- POST `/create`
		```js
		// Request body
		{ "topic": "string", "addr": "string" }
		```
	- POST `/subscribe`
		```js
		// Request body
		{ "topic": "string", "addr": "string" }
		```
	- POST `/publish`
		```javascript
		// Request body
		{ "topic": "string", "addr": "string", "payloadType": "string", "payload": "byte[]" }
		```
- Clients will receive the published Message
	- The `Payload` field contains the raw bytes from the sender
	- The `PayloadType` field should is set by the sender and used by the subscriber to deserialize the bytes
- Database schema: See `db.go`
- Testing:
	- (DONE) Initialize multiple clients, each listening on a port
	- (DONE) Confirm a client can create a Topic
	- (DONE) Confirm clients can subscribe to the Topic
	- (DONE) Confirm a client can publish a message to that Topic
	- (DONE) Confirm subscribers receive the message
	- Simulate unresponsive server
	- Stress test