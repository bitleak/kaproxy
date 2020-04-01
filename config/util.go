package config

import (
	"errors"
	"net"
)

func isLocalAddress(ip string) (bool, error) {
	if ip == "" {
		return false, errors.New("ip is null")
	}
	if ip == "0.0.0.0" || ip == "127.0.0.1" {
		return true, nil
	}
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return false, err
	}
	for _, addr := range addrs {
		if ipnet, ok := addr.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				if ipnet.IP.To4().String() == ip {
					return true, nil
				}
			} else if ipnet.IP.To16() != nil {
				if ipnet.IP.To16().String() == ip {
					return true, nil
				}
			}
		}
	}
	return false, errors.New("ip not found")
}
