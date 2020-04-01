package functional

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"mime/multipart"
	"net/http"
	"net/url"
	"testing"
	"time"

	"golang.org/x/net/context/ctxhttp"
)

const boundary = "__X_BOUNDARY__"

func batchProduceMessage(data [][2]string, partitioner string) error {
	targetUrl := fmt.Sprintf("%s/topic/%s/batch", hostAndPort, testTopic)
	query := url.Values{}
	query.Add("token", testToken)
	query.Add("partitioner", partitioner)

	// create multipart/form-data body
	requestBody := new(bytes.Buffer)
	multiform := multipart.NewWriter(requestBody)
	multiform.SetBoundary(boundary)
	for _, v := range data {
		multiform.WriteField(v[0], v[1])
	}
	multiform.Close()

	req, err := http.NewRequest("POST", fmt.Sprintf("%s?%s", targetUrl, query.Encode()), requestBody)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", fmt.Sprintf("multipart/form-data; charset=utf-8; boundary=%s", boundary))
	ctx, cancel := context.WithTimeout(context.TODO(), 3*time.Second)
	defer cancel()
	resp, err := ctxhttp.Do(ctx, nil, req)
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	if err != nil {
		return err
	}
	body, err := ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		return err
	}
	if resp.StatusCode != 200 {
		return fmt.Errorf("response is not OK: [%s]\n%s", resp.Status, body)
	}
	return nil
}

func TestProduceBatchMessages(t *testing.T) {
	consumeAllMessages()

	testData := [][2]string{
		{"key3", "val3"},
		{"key3", "val4"}, // same key, should work
		{"key1", "val1"},
		{"key2", "val2"},
	}
	if err := batchProduceMessage(testData, "0"); err != nil {
		t.Errorf("Failed to do batchProduceMessage: %s", err)
	}
	for _, v := range testData {
		_, body, err := consumeMessage(3000)
		if err != nil {
			t.Errorf("Failed to consume the produced message: %s", err)
		}
		var data map[string]interface{}
		if err := json.Unmarshal(body, &data); err != nil {
			t.Errorf("Failed to unmarshal: %s", err)
		}
		if v[0] != data["key"] {
			t.Errorf("Message key mismatched, want %s, got %s", v[0], data["key"])
		}
		if v[1] != data["value"] {
			t.Errorf("Message value mismatched, want %s, got %s", v[1], data["value"])
		}
	}
}
