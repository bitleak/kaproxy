package consumer

import (
	"fmt"
	"net"
	"sort"
	"strconv"
	"time"

	"github.com/meitu/go-zookeeper/zk"
	"github.com/meitu/zk_wrapper"
)

const (
	SemanticAtMostOnce  = "atMostOnce"
	SemanticAtLeastOnce = "atLeastOnce"

	OwnerName = "kaproxy"
)

const (
	consumerGroupsDir  = "/consumers"
	consumerGroupsPath = "/consumers/%s"
	ownerPath          = "/consumers/%s/owners/%s/%d"
	offsetPath         = "/consumers/%s/offsets/%s/%d"
	idDir              = "/consumers/%s/ids"
	idPath             = "/consumers/%s/ids/%s"
	controllerPath     = "/proxy/controller"
	weightPath         = "/consumers/%s/weights/%s"
)

// SliceRemoveDuplicates removes duplicate elements from the slice.
func sliceRemoveDuplicates(slice []string) []string {
	sort.Strings(slice)
	i := 0
	var j int
	for {
		if i >= len(slice)-1 {
			break
		}
		for j = i + 1; j < len(slice) && slice[i] == slice[j]; j++ {
		}
		slice = append(slice[:i+1], slice[j:]...)
		i++
	}
	return slice
}

// GenProxyID generates a consumer ID by local ipv4 address, current time and
// a random number.
func genProxyID(port int) (string, error) {
	ipv4, err := getLocalIPv4()
	if err != nil {
		return "", err
	}
	currentMilliSec := time.Now().UnixNano() / int64(time.Millisecond)
	return fmt.Sprintf("{%s:%d}-{%d}", ipv4.String(), port, currentMilliSec), nil
}

func getLocalIPv4() (net.IP, error) {
	netInterfaces, err := net.Interfaces()
	if err != nil {
		return nil, err
	}

	for _, netInterface := range netInterfaces {
		if (netInterface.Flags & net.FlagUp) == 0 {
			continue
		}
		addrs, _ := netInterface.Addrs()
		for _, addr := range addrs {
			ipnet, ok := addr.(*net.IPNet)
			if !ok || ipnet.IP.IsLoopback() {
				continue
			}
			ipv4 := ipnet.IP.To4()
			if ipv4 == nil {
				continue
			}
			if ipv4[0] == 10 || (ipv4[0] == 172 && ipv4[1] >= 16 && ipv4[1] <= 31) || (ipv4[0] == 192 && ipv4[1] == 168) {
				return ipv4, nil
			}
		}
	}
	return nil, fmt.Errorf("no ip address satisfied requirement")
}

func getConsumerOffset(c *zk_wrapper.Conn, group, topic string, partId int32) (int64, *zk.Stat, error) {
	zkPath := fmt.Sprintf(offsetPath, group, topic, partId)
	offset, stat, err := c.Get(zkPath)
	if err != nil {
		return 0, nil, err
	}
	v, err := strconv.ParseInt(string(offset), 10, 64)
	return v, stat, err
}

func getConsumerOwner(c *zk_wrapper.Conn, group, topic string, partId int32) (string, error) {
	zkPath := fmt.Sprintf(ownerPath, group, topic, partId)
	owner, _, err := c.Get(zkPath)
	if err != nil {
		return "-", err
	}
	return string(owner), err
}
