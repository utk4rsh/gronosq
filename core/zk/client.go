package zk

import (
	"fmt"
	"github.com/samuel/go-zookeeper/zk"
	"strings"
	"time"
)

type Client struct {
}

func (r *Client) Client() *zk.Conn {
	hostString := "localhost:2181"
	hosts := strings.Split(hostString, ",")
	zkConn, _, err := zk.Connect(hosts, 5*time.Second)
	if err != nil {
		fmt.Printf("Error in zk.Connect (%s): %v", hosts, err)
		panic(err)
	}
	return zkConn
}
