package functional

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"testing"
	"time"
)

type message struct {
	Key       string `json:"key"`
	Value     string `json:"value"`
	Topic     string `json:"topic"`
	Partition int32  `json:"partition"`
	Offset    int64  `json:"offset"`
}

func consumeMessage(timeout int) (int, []byte, error) {
	url := fmt.Sprintf("%s/group/%s/topic/%s?token=%s&timeout=%d",
		hostAndPort, testPullGroup, testTopic, testToken, timeout)
	resp, err := http.Get(url)
	if err != nil {
		return resp.StatusCode, nil, err
	}
	if resp.StatusCode == http.StatusNoContent {
		return resp.StatusCode, nil, nil
	}
	body, err := ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		return resp.StatusCode, nil, err
	}
	return resp.StatusCode, body, nil
}

func consumeAllMessages() {
	for {
		code, _, _ := consumeMessage(3000)
		if code == http.StatusNoContent {
			break
		}
	}
}

//It is considered equal,if the error is less than 10ms
func isEqualInDuration(a time.Duration, b time.Duration) bool {
	if a-b < 10*time.Millisecond && a-b > -10*time.Millisecond {
		return true
	}
	return false
}

func TestConsumeTopic(t *testing.T) {
	consumeAllMessages()
	value := "testValue"
	key := "testKey"
	times := 100
	stat := make([]int64, 10)
	for i := range stat {
		stat[i] = -1
	}
	for i := 0; i < times; i++ {
		produceMessage(key, value, "")
	}

	for i := 0; i < times; i++ {
		_, respBody, err := consumeMessage(100)
		if err != nil {
			t.Errorf("http request failed: %s", err)
			break
		}
		var msg message
		err = json.Unmarshal(respBody, &msg)
		if err != nil {
			t.Errorf("unmarshal response failed: %s", err)
			break
		}
		if msg.Key != key {
			t.Errorf("expect %s, but got %s", key, msg.Key)
			break
		}
		if msg.Value != value {
			t.Errorf("expect %s, but got %s", value, msg.Value)
			break
		}
		partition := msg.Partition
		offset := msg.Offset
		if stat[partition]+1 != offset && stat[partition] != -1 {
			t.Errorf("expect %d, but got %d", stat[partition]+1, msg.Offset)
			break
		}
		stat[partition] = offset
	}
}

func TestBlockConsumeTopic(t *testing.T) {
	consumeAllMessages()
	timeout := 2000
	start := time.Now()
	consumeMessage(timeout)
	duration := time.Since(start)
	if !isEqualInDuration(duration, time.Duration(timeout)*time.Millisecond) {
		t.Fatalf("expect block %d ms, but %d ms", timeout, duration/time.Millisecond)
	}
	start = time.Now()
	timeout = 1000
	go func() {
		<-time.After(time.Duration(timeout) * time.Millisecond)
		produceMessage("test", "test", "")
	}()
	consumeMessage(timeout)
	duration = time.Since(start)
	if !isEqualInDuration(duration, time.Duration(timeout)*time.Millisecond) {
		t.Fatalf("expect block %d ms, but %d ms", timeout, duration/time.Millisecond)
	}
}

func TestRebalance(t *testing.T) {
	//TODO

}

func TestConsumeFromPeers(t *testing.T) {
	//TODO
}

func TestPush(t *testing.T) {
	//TODO
}

func TestPushInOrder(t *testing.T) {
	//TODO
}
