package functional

import (
	"log"
	"net"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/Shopify/sarama"
	"github.com/bitleak/kaproxy/config"
	"github.com/bitleak/kaproxy/server"
)

const (
	confFile      = "./test.toml"
	host          = "127.0.0.1"
	port          = "8080"
	contentType   = "application/x-www-form-urlencoded"
	hostAndPort   = "http://" + host + ":" + port
	adminHostPort = "http://127.0.0.1:8081"
	testToken     = "test"
	testTopic     = "test"
	testPullGroup = "pull-test"
)

func init() {
	proxyConf := new(config.Config)
	toml.DecodeFile("test.toml", proxyConf)
	conf := sarama.NewConfig()
	client, _ := sarama.NewClient(proxyConf.Kafka.Brokers, conf)
	for {
		if partitions, _ := client.Partitions(testTopic); len(partitions) == 0 {
			time.Sleep(50 * time.Millisecond)
			continue
		}
		break
	}
	go func() {
		log.Fatal(server.Start(confFile, "./pidFile"))
	}()
	for {
		conn, err := net.Dial("tcp", host+":"+port)
		if err != nil {
			time.Sleep(50 * time.Millisecond)
			continue
		}
		conn.Close()
		//Waiting for group initialization to complete
		time.Sleep(500 * time.Millisecond)
		break
	}
}
