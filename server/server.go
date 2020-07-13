package server

import (
	"bufio"
	"fmt"
	"github.com/sirupsen/logrus"
	"net/http/httputil"
	"os"
	"os/signal"
	"runtime/debug"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/Shopify/sarama"
	"github.com/bitleak/kaproxy/auth"
	"github.com/bitleak/kaproxy/config"
	"github.com/bitleak/kaproxy/consumer"
	"github.com/bitleak/kaproxy/log"
	"github.com/bitleak/kaproxy/metrics"
	"github.com/bitleak/kaproxy/producer"
	"github.com/bitleak/kaproxy/util"
	"github.com/gin-gonic/gin"
	"github.com/meitu/go-zookeeper/zk"
	"github.com/meitu/zk_wrapper"
)

type server struct {
	conf         *config.Config
	producer     *producer.Producer
	consumer     *consumer.Consumer
	tokenManager *auth.TokenManager

	zkCli     *zk_wrapper.Conn
	saramaCli sarama.Client
	stop      chan bool
}

var srv *server

func serverRecovery() gin.HandlerFunc {
	logger := log.ErrorLogger
	return func(c *gin.Context) {
		defer func() {
			if err := recover(); err != nil {
				if logger != nil {
					httprequest, _ := httputil.DumpRequest(c.Request, false)
					logger.Errorf("[Recovery] panic recovered:\n%s\n%s\n%s", string(httprequest), err, string(debug.Stack()))
				}
				c.AbortWithStatus(500)
			}
		}()
		c.Next()
	}
}

func genRequestId(c *gin.Context) {
	reqId := util.GenUniqueID()
	c.Request.Header.Add("X-Request-Id", reqId)
	c.Set("logger", log.ErrorLogger.WithField("req_id", reqId))
	c.Header("X-Request-Id", reqId)
}

func getDefaultGinEngine(accessLogEnable bool, accessLogger *logrus.Logger) *gin.Engine {
	gin.SetMode(gin.ReleaseMode)
	engine := gin.New()
	engine.Use(serverRecovery(), genRequestId, AccessLogMiddleware(accessLogEnable, accessLogger))
	return engine
}

func runEngine(engine *gin.Engine, host string, port int) {
	go func(engine *gin.Engine, host string, port int) {
		err := engine.Run(host + ":" + strconv.Itoa(port))
		if err != nil {
			fmt.Printf("Run engine err, %s\n", err)
			os.Exit(1)
		}
	}(engine, host, port)
}

func writePidFile(path string) error {
	fd, err := os.Create(path)
	if err != nil {
		return err
	}
	defer fd.Close()

	_, err = fd.WriteString(fmt.Sprintf("%d\n", os.Getpid()))
	return err
}

func readPidFile(path string) (int, error) {
	fd, err := os.Open(path)
	if err != nil {
		return -1, err
	}
	defer fd.Close()

	buf := bufio.NewReader(fd)
	line, err := buf.ReadString('\n')
	if err != nil {
		return -1, err
	}
	line = strings.TrimSpace(line)
	return strconv.Atoi(line)
}

func handleSignal(sig os.Signal) {
	switch sig {
	case syscall.SIGINT:
		fallthrough
	case syscall.SIGTERM:
		Stop()
	default:
		// do nothing
	}
}

func registerSignal() {
	go func() {
		var sigs = []os.Signal{
			syscall.SIGHUP,
			syscall.SIGUSR1,
			syscall.SIGUSR2,
			syscall.SIGINT,
			syscall.SIGTERM,
		}
		for {
			c := make(chan os.Signal)
			signal.Notify(c, sigs...)
			sig := <-c //blocked
			handleSignal(sig)
		}
	}()
}

// HandleUserCmd use to stop/reload the proxy service
func HandleUserCmd(cmd, pidFile string) error {
	var sig os.Signal

	switch cmd {
	case "stop":
		sig = syscall.SIGTERM
	default:
		return fmt.Errorf("unknown user command %s", cmd)
	}

	pid, err := readPidFile(pidFile)
	if err != nil {
		return err
	}
	proc := new(os.Process)
	proc.Pid = pid
	return proc.Signal(sig)
}

// Stop proxy
func Stop() {
	srv.consumer.Stop()
	log.ErrorLogger.Info("server release all consumer group")

	srv.stop <- true
}

func initServer(conf *config.Config) error {
	srv = new(server)
	srv.stop = make(chan bool, 0)

	err := log.Init(conf.Server.LogFormat, conf.Server.LogDir)
	if err != nil {
		return err
	}
	zk.DefaultLogger = log.ErrorLogger
	srv.zkCli, _, err = zk_wrapper.Connect(conf.Kafka.Zookeepers, conf.Kafka.ZKSessionTimeout.Duration)
	if err != nil {
		return err
	}
	if conf.Kafka.ZKChroot != "" {
		srv.zkCli.Chroot(conf.Kafka.ZKChroot)
	}

	consumeConf := sarama.NewConfig()
	consumeConf.Net.DialTimeout = 3100 * time.Millisecond
	consumeConf.Net.ReadTimeout = 10 * time.Second
	consumeConf.Net.WriteTimeout = 3 * time.Second
	consumeConf.ChannelBufferSize = conf.Consumer.ChannelBufferSize
	consumeConf.Consumer.Fetch.Default = conf.Consumer.FetchDefault
	consumeConf.Consumer.Return.Errors = true
	consumeConf.Version = conf.Kafka.Version.Version
	srv.saramaCli, err = sarama.NewClient(conf.Kafka.Brokers, consumeConf)
	srv.tokenManager, err = auth.NewTokenManager(srv.zkCli, log.ErrorLogger)
	return err
}

// Start proxy with config file
func Start(confFile, pidFile string) error {
	conf, err := config.Load(confFile)
	if err != nil {
		return err
	}

	err = initServer(conf)
	if err != nil {
		return fmt.Errorf("init server failed, %s", err.Error())
	}
	srv.conf = conf

	srv.producer, err = producer.NewProducer(conf)
	if err != nil {
		return fmt.Errorf("failed to init producer: %s", err)
	}
	log.ErrorLogger.Infof("Init producer succ with brokers %s", conf.Kafka.Brokers)

	srv.consumer, err = consumer.NewConsumer(srv.saramaCli, conf)
	if err != nil {
		return fmt.Errorf("init consumer failed, %s", err.Error())
	}
	err = srv.consumer.Start()
	if err != nil {
		return fmt.Errorf("start consumer failed, %s", err)
	}
	log.ErrorLogger.Infof("Init consumer succ with brokers %s, zookeepers %s", conf.Kafka.Brokers, conf.Kafka.Zookeepers)

	// registe prometheus metrics
	metrics.Init()

	// run proxy engine
	proxyEngine := getDefaultGinEngine(conf.Server.AccessLogEnable, log.AccessLogger)
	setupProxyRouters(proxyEngine)
	runEngine(proxyEngine, conf.Server.Host, conf.Server.Port)
	// run admin engine
	adminEngine := getDefaultGinEngine(conf.Server.AccessLogEnable, log.AccessLogger)
	setupAdminRouter(adminEngine)
	runEngine(adminEngine, conf.Server.AdminHost, conf.Server.AdminPort)

	writePidFile(pidFile)
	registerSignal()

	log.ErrorLogger.Infof("Start server succ, listen on %s:%d and %s:%d",
		conf.Server.Host, conf.Server.Port, conf.Server.AdminHost, conf.Server.AdminPort)

	<-srv.stop
	srv.producer.Close()
	srv.zkCli.Close()
	log.ErrorLogger.Info("server have been stopped")
	logger, ok := log.ErrorLogger.Out.(*os.File)
	if ok {
		logger.Sync()
		logger.Close()
	}
	return nil
}
