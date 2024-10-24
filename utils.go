package main

import (
	"encoding/json"
	"io"
	"net/http"
)

func UnmarshalMsg(r *http.Request) (*Message, error) {
	var m *Message
	bytes, err := io.ReadAll(r.Body)
	if err != nil {
		return nil, err
	}

	err = json.Unmarshal(bytes, &m)
	if err != nil {
		return nil, err
	}
	return m, nil
}
