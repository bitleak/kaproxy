package config

import (
	"net"
	"testing"
)

func TestIsLocalAddress(t *testing.T) {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		t.Errorf("get infterface addrs failed, %s", err.Error())
	}
	goodCases := []string{
		"127.0.0.1",
		"0.0.0.0",
	}
	for _, addr := range addrs {
		if ipnet, ok := addr.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				goodCases = append(goodCases, ipnet.IP.To4().String())
			} else if ipnet.IP.To16() != nil {
				goodCases = append(goodCases, ipnet.IP.To16().String())
			}
		}
	}

	badCases := []string{
		"127.0.1.2",
		"1.1.1.1",
		"122",
	}
	for _, good := range goodCases {
		expected, err := isLocalAddress(good)
		if !expected || err != nil {
			t.Error("TestIsLocalAddress failed, expect true and not error")
		}
	}
	for _, bad := range badCases {
		expected, err := isLocalAddress(bad)
		if expected || err == nil {
			t.Error("TestIsLocalAddress failed, expect false and error")
		}
	}
}
