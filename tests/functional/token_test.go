package functional

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
	"testing"
)

type Token struct {
	Groups []string `json:"groups"`
	Topics []string `json:"topics"`
	Role   string   `json:"role"`
}

func deleteToken(token string) error {
	targetURL := adminHostPort + "/tokens"
	req, err := http.NewRequest(http.MethodDelete, targetURL+"/"+token, nil)
	if err != nil {
		return fmt.Errorf("failed to build the request, err: %v", err)
	}
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("send the DELETE request, err: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("got the wrong http code: %d", resp.StatusCode)
	}
	_, err = ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	return err
}

func updateToken(token string, isAdd bool, role string, groups, topics []string) error {
	targetURL := adminHostPort + "/tokens"
	if groups == nil {
		groups = []string{}
	}
	if topics == nil {
		topics = []string{}
	}
	data := url.Values{
		"token":  []string{token},
		"role":   []string{role},
		"groups": []string{strings.Join(groups, ",")},
		"topics": []string{strings.Join(topics, ",")},
	}
	if isAdd {
		targetURL = targetURL + "/" + token + "/add"
	} else {
		targetURL = targetURL + "/" + token + "/delete"
	}
	req, err := http.NewRequest(http.MethodPatch, targetURL, strings.NewReader(data.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	if err != nil {
		return fmt.Errorf("failed to build the request, err: %v", err)
	}
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("send the DELETE request, err: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("got the wrong http code: %d", resp.StatusCode)
	}
	_, err = ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	return err
}

func createToken(token, role string, groups, topics []string) error {
	targetURL := adminHostPort + "/tokens"
	if groups == nil {
		groups = []string{}
	}
	if topics == nil {
		topics = []string{}
	}
	data := url.Values{
		"token":  []string{token},
		"role":   []string{role},
		"groups": []string{strings.Join(groups, ",")},
		"topics": []string{strings.Join(topics, ",")},
	}
	resp, err := http.PostForm(targetURL, data)
	if err != nil {
		return fmt.Errorf("failed to send the POST request, err: %v", err)
	}
	if resp.StatusCode != http.StatusCreated {
		return fmt.Errorf("got the wrong http code: %d", resp.StatusCode)
	}
	_, err = ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		return fmt.Errorf("failed to read the response, err: %v", err)
	}
	return nil
}

func getToken(token string) (*Token, error) {
	targetURL := adminHostPort + "/tokens"
	resp, err := http.Get(targetURL + "/" + token + "?sync=true")
	if err != nil {
		return nil, fmt.Errorf("send the POST request, err: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("got the wrong http code: %d", resp.StatusCode)
	}
	body, err := ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		return nil, fmt.Errorf("read response, err: %v", err)
	}

	var tmpToken Token
	json.Unmarshal(body, &tmpToken)
	return &tmpToken, nil
}

func TestGetToken(t *testing.T) {
	token := "test_token"
	groups := []string{"g1", "g2"}
	topics := []string{"t1", "t2", "t3"}
	createToken(token, "admin", groups, topics)
	{
		tmpToken, err := getToken(token)
		if err != nil {
			t.Fatalf(err.Error())
		}
		if len(tmpToken.Groups) != len(groups) {
			t.Fatal("mismatch the num of groups")
		}
		if len(tmpToken.Topics) != len(topics) {
			t.Fatal("mismatch the num of topics")
		}
	}
	deleteToken(token)
}

func TestUpdateToken(t *testing.T) {
	token := "test-update-token"
	groups := []string{"g1", "g2"}
	topics := []string{"t1", "t2"}
	createToken(token, "admin", groups, topics)
	{
		err := updateToken(token, true, "admin", []string{"g4"}, []string{"t4"})
		if err != nil {
			t.Fatalf(err.Error())
		}
		tmpToken, err := getToken(token)
		if err != nil {
			t.Fatalf("Failed to get token, %v", err.Error())
		}
		if len(tmpToken.Groups) != len(groups)+1 {
			t.Fatal("mismatch the num of groups")
		}
		if len(tmpToken.Topics) != len(topics)+1 {
			t.Fatal("mismatch the num of topics")
		}

		err = updateToken(token, false, "admin", []string{"g4"}, []string{"t4"})
		if err != nil {
			t.Fatalf(err.Error())
		}
		tmpToken, err = getToken(token)
		if err != nil {
			t.Fatalf("Failed to get token, %v", err.Error())
		}
		if len(tmpToken.Groups) != len(groups) {
			t.Fatal("mismatch the num of groups")
		}
		if len(tmpToken.Topics) != len(topics) {
			t.Fatal("mismatch the num of topics")
		}
	}
	deleteToken(token)
}

func TestAddAndDeleteToken(t *testing.T) {
	tokens := []string{
		"t1", "t2", "t3", "t4",
	}
	for _, token := range tokens {
		err := createToken(token, "admin", nil, nil)
		if err != nil {
			t.Fatal(err.Error())
		}
	}

	// delete tokens
	for _, token := range tokens {
		err := deleteToken(token)
		if err != nil {
			t.Fatal(err.Error())
		}
	}
}
