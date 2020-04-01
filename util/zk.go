package util

import (
	"path"

	"github.com/meitu/go-zookeeper/zk"
	"github.com/meitu/zk_wrapper"
)

func mkdirRecursive(c *zk_wrapper.Conn, zkPath string) error {
	var err error
	parent := path.Dir(zkPath)
	if parent != "/" {
		if err = mkdirRecursive(c, parent); err != nil {
			return err
		}
	}

	_, err = c.Create(zkPath, nil, 0, zk.WorldACL(zk.PermAll))
	if err == zk.ErrNodeExists {
		err = nil
	}
	return err
}

func ZKCreateEphemeralPath(c *zk_wrapper.Conn, zkPath string, data []byte) error {
	return ZKCreateRecursive(c, zkPath, zk.FlagEphemeral, data)
}

func ZKCreatePersistentPath(c *zk_wrapper.Conn, zkPath string, data []byte) error {
	return ZKCreateRecursive(c, zkPath, 0, data)
}

// ZKCreateRecursive creates the zkPath recursively if it does not exist.
func ZKCreateRecursive(c *zk_wrapper.Conn, zkPath string, flags int32, data []byte) error {
	_, err := c.Create(zkPath, data, flags, zk.WorldACL(zk.PermAll))
	if err != nil && err != zk.ErrNoNode {
		return err
	}
	if err == zk.ErrNoNode {
		mkdirRecursive(c, path.Dir(zkPath))
		_, err = c.Create(zkPath, data, flags, zk.WorldACL(zk.PermAll))
	}
	return err
}

// ZKSetPersistentPath writes data to the node specified by zkPath. zkPath
// will be created recursively if it does not exist in zookeeper.
func ZKSetPersistentPath(c *zk_wrapper.Conn, zkPath string, data []byte) error {
	_, err := c.Set(zkPath, data, -1)
	if err != nil && err != zk.ErrNoNode {
		return err
	}
	if err == zk.ErrNoNode {
		mkdirRecursive(c, path.Dir(zkPath))
		_, err = c.Create(zkPath, data, 0, zk.WorldACL(zk.PermAll))
	}
	return err
}
