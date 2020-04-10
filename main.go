package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/bitleak/kaproxy/server"
)

type options struct {
	showVersion bool
	cmd         string
	confFile    string
	pidFile     string
}

func parseOptions() *options {
	opts := new(options)
	flag.BoolVar(&opts.showVersion, "v", false, "Show Version")
	flag.StringVar(&opts.cmd, "k", "", "status|stop")
	flag.StringVar(&opts.confFile, "c", "conf/kaproxy-default.toml", "conf file path")
	flag.StringVar(&opts.pidFile, "p", "/var/run/kaproxy.pid", "pid file path")
	flag.Parse()
	return opts
}

func main() {
	//use "github.com/pkg/profile" to debug
	//defer profile.Start(profile.TraceProfile, profile.ProfilePath("/tmp/pprof"), profile.NoShutdownHook).Stop()
	opts := parseOptions()
	if opts.showVersion {
		fmt.Printf("%s\ncommit %s\nbuilt on %s\n", server.Version, server.BuildCommit, server.BuildDate)
		os.Exit(0)
	}
	if opts.cmd != "" {
		err := server.HandleUserCmd(opts.cmd, opts.pidFile)
		if err != nil {
			fmt.Printf("Handle user command(%s) error, %s\n", opts.cmd, err)
		} else {
			fmt.Printf("Handle user command(%s) succ\n", opts.cmd)
		}
		os.Exit(0)
	}

	err := server.Start(opts.confFile, opts.pidFile)
	if err != nil {
		fmt.Printf("Server start error, %s\n", err)
		os.Exit(1)
	}
}
