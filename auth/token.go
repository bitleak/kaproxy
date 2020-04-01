package auth

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/sirupsen/logrus"
	"sync"
	"time"

	"github.com/meitu/go-zookeeper/zk"
	"github.com/meitu/kaproxy/util"
	"github.com/meitu/zk_wrapper"
)

const (
	tokenZKDir  = "/proxy/app/acl"
	tokenZKPath = "/proxy/app/acl/%s"
)

var (
	ErrTokenExists   = errors.New("the token has already exists")
	ErrTokenNotFound = errors.New("the token was not found")
)

type Token struct {
	groups map[string]bool
	topics map[string]bool
	role   string

	mu sync.RWMutex
}

type TokenManager struct {
	tokens map[string]*Token
	zkCli  *zk_wrapper.Conn
	logger *logrus.Logger
}

func (token *Token) isAdminRole() bool {
	return token.role == "admin"
}

func NewToken() *Token {
	return &Token{
		groups: make(map[string]bool, 0),
		topics: make(map[string]bool, 0),
		role:   "",
	}
}

func (token *Token) addTopics(topics []string) int {
	if topics == nil || len(topics) == 0 {
		return 0
	}

	cnt := 0
	token.mu.Lock()
	for _, topic := range topics {
		if _, ok := token.topics[topic]; !ok {
			cnt += 1
		}
		token.topics[topic] = true
	}
	token.mu.Unlock()
	return cnt
}

func (token *Token) delTopics(topics []string) int {
	if topics == nil || len(topics) == 0 {
		return 0
	}

	cnt := 0
	token.mu.Lock()
	for _, topic := range topics {
		if _, ok := token.topics[topic]; ok {
			cnt += 1
			delete(token.topics, topic)
		}
	}
	token.mu.Unlock()
	return cnt
}

func (token *Token) addGroups(groups []string) int {
	if groups == nil || len(groups) == 0 {
		return 0
	}

	cnt := 0
	token.mu.Lock()
	for _, group := range groups {
		if _, ok := token.groups[group]; !ok {
			cnt += 1
		}
		token.groups[group] = true
	}
	token.mu.Unlock()
	return cnt
}

func (token *Token) delGroups(groups []string) int {
	if groups == nil || len(groups) == 0 {
		return 0
	}

	cnt := 0
	token.mu.Lock()
	for _, group := range groups {
		if _, ok := token.groups[group]; ok {
			cnt += 1
			delete(token.groups, group)
		}
	}
	token.mu.Unlock()
	return cnt
}

// Unmarshal would unmarshal the token from JSON format
func (token *Token) Unmarshal(bytes []byte) error {
	var tmpToken struct {
		Groups []string `json:"groups"`
		Topics []string `json:"topics"`
		Role   string   `json:"role"`
	}
	if err := json.Unmarshal(bytes, &tmpToken); err != nil {
		return err
	}
	token.mu.Lock()
	token.role = tmpToken.Role
	for _, topic := range tmpToken.Topics {
		token.topics[topic] = true
	}
	for _, group := range tmpToken.Groups {
		token.groups[group] = true
	}
	token.mu.Unlock()
	return nil
}

// Marshal would marshal the token into JSON format
func (token *Token) Marshal() ([]byte, error) {
	var tmpToken struct {
		Groups []string `json:"groups,omitempty"`
		Topics []string `json:"topics,omitempty"`
		Role   string   `json:"role,omitempty"`
	}

	tmpToken.Role = token.role
	tmpToken.Topics = make([]string, 0)
	tmpToken.Groups = make([]string, 0)
	token.mu.RLock()
	for topic := range token.topics {
		tmpToken.Topics = append(tmpToken.Topics, topic)
	}
	for group := range token.groups {
		tmpToken.Groups = append(tmpToken.Groups, group)
	}
	token.mu.RUnlock()
	return json.Marshal(tmpToken)
}

// NewTokenManager create the token manger and it would sync the tokens from zk
func NewTokenManager(zkCli *zk_wrapper.Conn, logger *logrus.Logger) (*TokenManager, error) {
	tm := &TokenManager{
		zkCli:  zkCli,
		logger: logger,
	}
	exists, _, err := zkCli.Exists(tokenZKDir)
	if err != nil {
		return nil, err
	}
	if !exists {
		if err := util.ZKCreatePersistentPath(zkCli, tokenZKDir, nil); err != nil {
			return nil, err
		}
	}
	go tm.loop()
	return tm, nil
}

func (tm *TokenManager) loop() {
	defer util.WithRecover(tm.logger)

	tm.sync()

	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		_, _, watcher, err := tm.zkCli.ChildrenW(tokenZKDir)
		if err != nil {
			<-ticker.C
			tm.sync()
			tm.logger.WithField("err", err).Error("Failed to watch the tokens")
		} else {
			select {
			case <-watcher.EvCh:
				tm.logger.Info("Sync tokens action was triggered by watch")
				tm.sync()
			case <-ticker.C:
				tm.logger.Info("Sync tokens action was triggered by timer")
				tm.sync()
			}
		}
	}
}

func (tm *TokenManager) sync() {
	tokens, _, err := tm.zkCli.Children(tokenZKDir)
	if err != nil {
		tm.logger.WithField("err", err).Error("Failed to list tokens from ZK")
		return
	}
	newTokens := make(map[string]*Token, 0)
	for _, token := range tokens {
		bytes, _, err := tm.zkCli.Get(fmt.Sprintf(tokenZKPath, token))
		if err != nil {
			tm.logger.WithFields(logrus.Fields{
				"token": token,
				"err":   err,
			}).Warn("Failed to get token from ZK")
			continue
		}
		t := NewToken()
		if err := t.Unmarshal(bytes); err != nil {
			tm.logger.WithFields(logrus.Fields{
				"token": token,
				"err":   err,
			}).Warn("invalid token format")
			continue
		}
		newTokens[token] = t
	}
	tm.tokens = newTokens
}

// IsAllowProduce check whether the token has the permission to send message or not
func (tm *TokenManager) IsAllowProduce(token, topic string) bool {
	if token == "" || topic == "" {
		return false
	}
	if t, ok := tm.tokens[token]; ok {
		t.mu.RLock()
		if t.isAdminRole() {
			t.mu.RUnlock()
			return true
		}
		_, ok := t.topics[topic]
		t.mu.RUnlock()
		return ok
	}
	return false
}

// IsAllowProduce check whether the token has the permission to consume message or not
func (tm *TokenManager) IsAllowConsume(token, group string) bool {
	if token == "" || group == "" {
		return false
	}
	if t, ok := tm.tokens[token]; ok {
		t.mu.RLock()
		if t.isAdminRole() {
			t.mu.RUnlock()
			return true
		}
		_, ok := t.groups[group]
		t.mu.RUnlock()
		return ok
	}
	return false
}

// IsTokenExists check the token was exists or not
func (tm *TokenManager) IsTokenExists(token string) bool {
	if token == "" {
		return false
	}
	_, ok := tm.tokens[token]
	return ok
}

// CreateTokenh create the new token
func (tm *TokenManager) CreateToken(token, role string, topics, groups []string) error {
	t := NewToken()
	t.role = role
	t.addTopics(topics)
	t.addGroups(groups)
	bytes, _ := t.Marshal()
	err := util.ZKCreatePersistentPath(tm.zkCli, fmt.Sprintf(tokenZKPath, token), bytes)
	if err != nil && err == zk.ErrNodeExists {
		return ErrTokenExists
	}
	return err
}

// ListToken return token list in token manager
func (tm *TokenManager) ListToken(sync bool) []string {
	if sync {
		tm.sync()
	}
	tokens := make([]string, 0)
	for token := range tm.tokens {
		tokens = append(tokens, token)
	}
	return tokens
}

// GetToken return token in token manager
func (tm *TokenManager) GetToken(token string, sync bool) *Token {
	if sync {
		tm.sync()
	}
	return tm.tokens[token]
}

func (tm *TokenManager) DeleteToken(token string) error {
	err := tm.zkCli.Delete(fmt.Sprintf(tokenZKPath, token), -1)
	if err != nil && err == zk.ErrNoNode {
		return ErrTokenNotFound
	}
	return err
}

// UpdateToken update the token role or topics/groups
func (tm *TokenManager) UpdateToken(token string, isAdd bool, role string, topics, groups []string) error {
	bytes, stat, err := tm.zkCli.Get(fmt.Sprintf(tokenZKPath, token))
	if err != nil {
		if err == zk.ErrNoNode {
			err = ErrTokenNotFound
		}
		return err
	}

	t := NewToken()
	if err := t.Unmarshal(bytes); err != nil {
		return errors.New("invalid token format in zookeeper")
	}
	if role != "" {
		t.role = role
	}
	if isAdd {
		t.addTopics(topics)
		t.addGroups(groups)
	} else {
		t.delTopics(topics)
		t.delGroups(groups)
	}
	tokenBytes, _ := t.Marshal()
	_, err = tm.zkCli.Set(fmt.Sprintf(tokenZKPath, token), tokenBytes, stat.Version)
	if err == zk.ErrBadVersion {
		err = errors.New("update token conflicts, someone has updated the token")
	}
	return err
}
