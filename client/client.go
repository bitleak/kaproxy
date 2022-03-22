package client

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"mime/multipart"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const rid = "X-Request-ID"

type ProduceResp struct {
	Partition int   `json:"partition"`
	Offset    int64 `json:"offset"`
}

type ConsumeResp struct {
	Partition int    `json:"partition"`
	Offset    int64  `json:"offset"`
	Key       []byte `json:"key"`
	Value     []byte `json:"value"`
}

func (c *ConsumeResp) UnmarshalJSON(data []byte) error {
	var resp struct {
		Topic     string `json:"topic"`
		Partition int    `json:"partition"`
		Encoding  string `json:"encoding"`
		Offset    int64  `json:"offset"`
		Key       string `json:"key"`
		Value     string `json:"value"`
	}
	err := json.Unmarshal(data, &resp)
	if err != nil {
		return err
	}
	if resp.Encoding == "utf8" {
		c.Value = []byte(resp.Value)
	} else {
		c.Value, err = base64.StdEncoding.DecodeString(resp.Value)
		if err != nil {
			return err
		}
	}
	c.Key = []byte(resp.Key)
	c.Partition = resp.Partition
	c.Offset = resp.Offset
	return nil
}

type Message struct {
	Key   []byte
	Value []byte
}

type KaproxyClient struct {
	host  string
	port  int
	token string

	replicate bool // For airbus only

	httpCli         *http.Client
	httpBlockingCli *http.Client //For blocking request only
}

func NewKaproxyClient(host string, port int, token string) *KaproxyClient {
	httpCli := &http.Client{
		Transport: &http.Transport{
			DialContext: (&net.Dialer{
				Timeout: 1500 * time.Millisecond,
			}).DialContext,
		},
		Timeout: 5 * time.Second,
	}
	httpBlockingCli := &http.Client{
		Transport: &http.Transport{
			DialContext: (&net.Dialer{
				Timeout: 1500 * time.Millisecond,
			}).DialContext,
		},
	}

	return &KaproxyClient{
		host:  host,
		port:  port,
		token: token,

		replicate: true,

		httpCli:         httpCli,
		httpBlockingCli: httpBlockingCli,
	}
}

// WithHttpClient was used to allow the user to custom the http client
func (c *KaproxyClient) WithHttpClient(httpCli *http.Client, blockingHttpCli *http.Client) *KaproxyClient {
	if httpCli != nil {
		c.httpCli = httpCli
	}
	if blockingHttpCli != nil {
		c.httpBlockingCli = blockingHttpCli
	}
	return c
}

// WithTimeout return a pointer to kaproxyClient which http client's timeout is specified
func (c *KaproxyClient) WithTimeout(timeout time.Duration) *KaproxyClient {
	c.httpCli.Timeout = timeout
	return c
}

// For airbus only
func (c *KaproxyClient) WithoutReplicate() *KaproxyClient {
	c.replicate = false
	return c
}

func (c *KaproxyClient) buildRequest(method, path string, query url.Values, body io.Reader) (*http.Request, error) {
	if query == nil {
		query = url.Values{}
	}
	query.Add("token", c.token)
	if method == http.MethodPost && !c.replicate {
		query.Add("replicate", "no")
	}
	targetUrl := url.URL{
		Scheme:   "http",
		Host:     fmt.Sprintf("%s:%d", c.host, c.port),
		Path:     path,
		RawQuery: query.Encode(),
	}
	return http.NewRequest(method, targetUrl.String(), body)
}

func (c *KaproxyClient) produce(topic string, partition int, message Message, hash bool) (*ProduceResp, *APIError) {
	var path string
	body := url.Values{}
	body.Add("value", string(message.Value))
	if message.Key != nil {
		body.Add("key", string(message.Key))
	}
	if partition >= 0 {
		path = fmt.Sprintf("/topic/%s/partition/%d", topic, partition)
	} else {
		path = fmt.Sprintf("/topic/%s", topic)
		if hash {
			body.Add("partitioner", "hash")
		}
	}

	req, err := c.buildRequest(http.MethodPost, path, nil, strings.NewReader(body.Encode()))
	if err != nil {
		return nil, &APIError{requestErr, err.Error(), ""}
	}
	req.Header.Add("Content-Type", "application/x-www-form-urlencoded")
	resp, err := c.httpCli.Do(req)
	if err != nil {
		return nil, &APIError{requestErr, err.Error(), ""}
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, &APIError{requestErr, parseResponseError(resp), resp.Header.Get(rid)}
	}

	respBytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, &APIError{responseErr, err.Error(), resp.Header.Get(rid)}
	}

	var respData ProduceResp
	err = json.Unmarshal(respBytes, &respData)
	if err != nil {
		return nil, &APIError{responseErr, err.Error(), resp.Header.Get(rid)}
	}
	return &respData, nil
}

func (c *KaproxyClient) Produce(topic string, message Message) (*ProduceResp, *APIError) {
	return c.produce(topic, -1, message, false)
}

func (c *KaproxyClient) ProduceWithHash(topic string, message Message) (*ProduceResp, *APIError) {
	return c.produce(topic, -1, message, true)
}

func (c *KaproxyClient) ProduceWithPartition(topic string, partition int, message Message) (*ProduceResp, *APIError) {
	return c.produce(topic, partition, message, false)
}

func (c *KaproxyClient) BatchProduce(topic string, messages []Message) (int, *APIError) {
	body := new(bytes.Buffer)
	multiform := multipart.NewWriter(body)
	for _, message := range messages {
		multiform.WriteField(string(message.Key), string(message.Value))
	}
	multiform.Close()
	req, err := c.buildRequest(http.MethodPost, fmt.Sprintf("/topic/%s/batch", topic), nil, body)
	if err != nil {
		return 0, &APIError{requestErr, err.Error(), ""}
	}
	req.Header.Add("Content-Type", fmt.Sprintf("multipart/form-data; charset=utf-8; boundary=%s", multiform.Boundary()))
	resp, err := c.httpCli.Do(req)
	if err != nil {
		return 0, &APIError{requestErr, err.Error(), ""}
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return 0, &APIError{requestErr, parseResponseError(resp), resp.Header.Get(rid)}
	}

	respBytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return 0, &APIError{responseErr, err.Error(), resp.Header.Get(rid)}
	}

	var respData struct {
		Count int `json:"count"`
	}
	err = json.Unmarshal(respBytes, &respData)
	if err != nil {
		return 0, &APIError{responseErr, err.Error(), resp.Header.Get(rid)}
	}
	return respData.Count, nil
}

func (c *KaproxyClient) Consume(group, topic string, durationArg ...time.Duration) (*ConsumeResp, *APIError) {
	timeout := 3000 * time.Millisecond //blocking timeout
	if len(durationArg) > 0 {
		timeout = durationArg[0]
	}
	query := url.Values{}
	query.Add("timeout", strconv.FormatInt(timeout.Nanoseconds()/1000000, 10))
	if len(durationArg) > 1 {
		ttr := durationArg[1]
		query.Add("ttr", strconv.FormatInt(ttr.Nanoseconds()/1000000, 10))
	}
	req, err := c.buildRequest(http.MethodGet, fmt.Sprintf("/group/%s/topic/%s", group, topic), query, nil)
	if err != nil {
		return nil, &APIError{requestErr, err.Error(), ""}
	}
	//http timeout always greater than blocking timeout
	c.httpBlockingCli.Timeout = timeout - 200*time.Millisecond
	resp, err := c.httpBlockingCli.Do(req)
	if err != nil {
		return nil, &APIError{requestErr, err.Error(), ""}
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusOK:
		//continue
	case http.StatusNoContent:
		discardResponseBody(resp.Body)
		return nil, ErrNoMessage
	default:
		return nil, &APIError{requestErr, parseResponseError(resp), resp.Header.Get(rid)}
	}

	respBytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, &APIError{responseErr, err.Error(), resp.Header.Get(rid)}
	}

	//for compatibility with version 0.2.*
	var tmpErr struct {
		Error string `json:"error"`
	}
	json.Unmarshal(respBytes, &tmpErr)
	if tmpErr.Error != "" {
		return nil, ErrNoMessage
	}

	var respData ConsumeResp
	err = json.Unmarshal(respBytes, &respData)
	if err != nil {
		return nil, &APIError{responseErr, err.Error(), resp.Header.Get(rid)}
	}
	return &respData, nil
}

func (c *KaproxyClient) ACK(group, topic string, message *ConsumeResp) *APIError {
	body := url.Values{}
	body.Add("partition", strconv.Itoa(int(message.Partition)))
	body.Add("offset", strconv.FormatInt(message.Offset, 10))
	path := fmt.Sprintf("/group/%s/topic/%s", group, topic)

	req, err := c.buildRequest(http.MethodPost, path, nil, strings.NewReader(body.Encode()))
	if err != nil {
		return &APIError{requestErr, err.Error(), ""}
	}
	req.Header.Add("Content-Type", "application/x-www-form-urlencoded")
	resp, err := c.httpCli.Do(req)
	if err != nil {
		return &APIError{requestErr, err.Error(), ""}
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return &APIError{requestErr, parseResponseError(resp), resp.Header.Get(rid)}
	}
	return nil
}

func discardResponseBody(resp io.ReadCloser) {
	// discard response body, to make this connection reusable in the http connection pool
	ioutil.ReadAll(resp)
}
