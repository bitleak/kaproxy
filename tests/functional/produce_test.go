package functional

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"
	"testing"

	"github.com/bitleak/kaproxy/producer"
)

func produceMessage(key, value, partitioner string) ([]byte, error) {
	url := fmt.Sprintf("%s/topic/%s", hostAndPort, testTopic)
	data := fmt.Sprintf("key=%s&value=%s&token=%s", key, value, testToken)
	if partitioner != "" {
		if _, err := strconv.Atoi(partitioner); err != nil {
			data += fmt.Sprintf("&partitioner=%s", partitioner)
		} else {
			url += fmt.Sprintf("/partition/%s", partitioner)
		}
	}
	resp, err := http.Post(url, contentType, strings.NewReader(data))
	if err != nil {
		return nil, err
	}
	body, err := ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		return nil, err
	}
	return body, nil
}

func TestProduceManual(t *testing.T) {
	offset := int64(-1)
	keys := []string{"83aeda576781c1cb8acc0d8d3a554cb4",
		"f6933f009ad01c72d0f04c69425e2a7f",
		"145c5cc64595ea66e8d0ec716aa3af74",
	}
	//随机key去测试是否指定了partition就会落到指定的partition中
	for _, key := range keys {
		respBody, err := produceMessage(key, "test", "1")
		if err != nil {
			t.Fatalf("http request failed: %s", err)
		}
		var body producer.ProduceResponse
		err = json.Unmarshal(respBody, &body)
		if err != nil {
			t.Fatalf("unmarshal response failed: %s", err)
		}
		if body.Partition != 1 {
			t.Fatalf("expect send message to partition 1, but send to partition %d", body.Partition)
		}
		if offset == -1 {
			offset = body.Offset
		} else if body.Offset != offset+1 {
			t.Fatalf("expect offset is %d, but got %d", offset+1, body.Offset)
		} else {
			offset++
		}
	}
}

func TestProduceInvalidPartition(t *testing.T) {
	respBody, _ := produceMessage("test", "test", "11")
	var bodyMap map[string]string
	err := json.Unmarshal(respBody, &bodyMap)
	if err != nil {
		t.Fatalf("unmarshal response failed, %s", err)
	}
	if bodyMap["error"] != "invalid partition index" {
		t.Fatalf("expect invalid partition index error, but got %s", bodyMap["error"])
	}
}

func TestProduceRandom(t *testing.T) {
	stat := make([]int, 10)
	for i := 0; i < 150; i++ {
		respBody, _ := produceMessage("test", "test", "random")
		var body producer.ProduceResponse
		json.Unmarshal(respBody, &body)
		stat[body.Partition]++
	}
	for i := 0; i < 10; i++ {
		if stat[i] == 0 {
			t.Fatal("expect produce random, but not")
		}
	}
}

func TestProduceHash(t *testing.T) {
	key := "test-test-test"
	var partition int32 = -1
	for i := 0; i < 5; i++ {
		respBody, _ := produceMessage(key, "test", "hash")
		var body producer.ProduceResponse
		json.Unmarshal(respBody, &body)
		if partition == -1 {
			partition = body.Partition
		} else if partition != body.Partition {
			t.Fatalf("expect send message to partition %d, but send to partition %d", partition, body.Partition)
		}
	}
}

func TestProduceWithoutPartitioner(t *testing.T) {
	stat := make([]int, 10)
	for i := 0; i < 150; i++ {
		respBody, _ := produceMessage("test", "test", "")
		var body producer.ProduceResponse
		json.Unmarshal(respBody, &body)
		stat[body.Partition]++
	}
	for i := 0; i < 10; i++ {
		if stat[i] == 0 {
			t.Fatal("expect produce random, but not")
		}
	}
}

func TestProduceInvalidPartitioner(t *testing.T) {
	respBody, _ := produceMessage("test", "test", "test")
	var bodyMap map[string]string
	json.Unmarshal(respBody, &bodyMap)
	if bodyMap["error"] != "invalid partition type" {
		t.Fatalf("expect invalid partition type error, but got %s", bodyMap["error"])
	}
}
