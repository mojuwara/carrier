package main

import (
	"log"
	"net/http"
)

// Need to keep track of subscribers for given Topic
// Need to keep track of messages received for each topic
func main() {
	http.HandleFunc("POST /create", HandleTopicCreate)
	http.HandleFunc("POST /msg", HandleMessage)
	// http.HandleFunc("POST /subscribe", HandleMessage)

	log.Println("Starting Carrier")
	log.Fatal(http.ListenAndServe(":8080", nil))
}
