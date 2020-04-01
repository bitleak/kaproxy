package auth

import "testing"

const (
	testAdminToken = "test_admin_token"
	testUserToken  = "test_user_token"
)

func mockTokenManager() *TokenManager {
	tm := new(TokenManager)
	tm.tokens = map[string]*Token{
		testAdminToken: {role: "admin"},
		testUserToken: {
			groups: map[string]bool{
				"g1": true,
				"g2": true,
			},
			topics: map[string]bool{
				"t1": true,
				"t2": true,
			},
		},
	}
	return tm
}

func TestTokenManager_IsTokenExists(t *testing.T) {
	tm := mockTokenManager()
	goodCases := []string{testAdminToken, testUserToken}
	badCases := []string{"no-exists-token1", "no-exists-token2"}
	for _, token := range goodCases {
		if !tm.IsTokenExists(token) {
			t.Errorf("expect the token %s should be exists", token)
		}
	}
	for _, token := range badCases {
		if tm.IsTokenExists(token) {
			t.Errorf("expect the token %s shouldn't be exists", token)
		}
	}
}

func TestTokenManager_IsAllowConsume(t *testing.T) {
	tm := mockTokenManager()
	goodCases := []struct {
		token string
		group string
	}{
		{testUserToken, "g1"},
		{testUserToken, "g2"},
		{testAdminToken, "g1"},
		{testUserToken, "g2"},
	}
	badCases := []struct {
		token string
		group string
	}{
		{"no-exists-token", "g1"},
		{"no-exists-token", "g2"},
		{testUserToken, "g3"},
		{testUserToken, "g4"},
	}
	for _, good := range goodCases {
		if !tm.IsAllowConsume(good.token, good.group) {
			t.Errorf("expect the token %s has permission to access the group", good.token)
		}
	}
	for _, bad := range badCases {
		if tm.IsAllowConsume(bad.token, bad.group) {
			t.Errorf("expect the token %s has no permission to access the group", bad.token)
		}
	}
}

func TestTokenManager_IsAllowProduce(t *testing.T) {
	tm := mockTokenManager()
	goodCases := []struct {
		token string
		topic string
	}{
		{testUserToken, "t1"},
		{testUserToken, "t2"},
		{testAdminToken, "t1"},
		{testAdminToken, "t2"},
	}
	badCases := []struct {
		token string
		topic string
	}{
		{"no-exists-token", "t1"},
		{"no-exists-token", "t2"},
		{testUserToken, "t3"},
	}
	for _, good := range goodCases {
		if !tm.IsAllowProduce(good.token, good.topic) {
			t.Errorf("expect the token %s has permission to access the topic", good.token)
		}
	}
	for _, bad := range badCases {
		if tm.IsAllowProduce(bad.token, bad.topic) {
			t.Errorf("expect the token %s has no permission to access the topic", bad.token)
		}
	}
}

func TestToken_isAdminRole(t *testing.T) {
	tm := mockTokenManager()
	if !tm.tokens[testAdminToken].isAdminRole() {
		t.Error("expect the role of the token is to be admin")
	}
	if tm.tokens[testUserToken].isAdminRole() {
		t.Error("expect the role of the token shouldn't be admin")
	}
}

func TestToken_addAndDelTopics(t *testing.T) {
	groups := []string{"g1", "g2", "g3", "g1", "g2"}
	topics := []string{"t1", "t2", "t3", "t1", "t2"}
	token := NewToken()
	expectedCnt := 3
	if cnt := token.addGroups(groups); cnt != expectedCnt {
		t.Errorf("Expect the num of groups added was %d, but got %d", expectedCnt, cnt)
	}
	if cnt := token.addTopics(topics); cnt != expectedCnt {
		t.Errorf("Expect the num of topics added was %d, but got %d", expectedCnt, cnt)
	}
	if cnt := token.delGroups(groups); cnt != expectedCnt {
		t.Errorf("Expect the num of deleted groups was %d, but got %d", expectedCnt, cnt)
	}
	if cnt := token.delTopics(topics); cnt != expectedCnt {
		t.Errorf("Expect the num of deleted topics was %d, but got %d", expectedCnt, cnt)
	}
}
