package client

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
)

type ErrType string

const (
	requestErr  ErrType = "req"
	responseErr ErrType = "resp"
)

type APIError struct {
	Type              ErrType
	Reason, RequestID string
}

var ErrNoMessage = &APIError{requestErr, "[204]no message in brokers", ""}

func (e *APIError) Error() string {
	if e.RequestID != "" {
		return fmt.Sprintf("type:%s; reason:%s; rid:%s", e.Type, e.Reason, e.RequestID)
	}
	return fmt.Sprintf("tpye:%s; reason:%s", e.Type, e.Reason)
}

func parseResponseError(resp *http.Response) string {
	respBytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return fmt.Sprintf("Invalid response: %s", err)
	}
	var errData struct {
		Error string `json:"error"`
	}
	err = json.Unmarshal(respBytes, &errData)
	if err != nil {
		return fmt.Sprintf("Invalid JSON: %s", err)
	}
	return fmt.Sprintf("[%d]%s", resp.StatusCode, errData.Error)
}
