package ha_worker

import (
	"github.com/samuel/go-zookeeper/zk"
	log "github.com/sirupsen/logrus"
	"strings"
	"time"
)

type ZKDiscovery struct {
	ZKHosts string
	ZKPath  string
	zk      *zk.Conn
}

func (s *ZKDiscovery) AddListener(onChange func(children []string)) error {
	var err error
	s.zk, _, err = zk.Connect(strings.Split(s.ZKHosts, ","), 5*time.Second)
	if err != nil {
		log.Printf("Error in zk.Connect (%s): %v", s.ZKHosts, err)
		panic(err)
	}
	dumpChildren, stat, _, err := s.zk.ChildrenW(s.ZKPath)
	if err != nil {
		panic(err)
	}
	log.Printf("ZK Childrens : %+v Stat: %+v", dumpChildren, stat)
	go WatchChildren(s.zk, s.ZKPath, onChange)
	return nil
}

func (s *ZKDiscovery) GetChildren() []string {
	dumpChildren, _, _, err := s.zk.ChildrenW(s.ZKPath)
	if err != nil {
		panic(err)
	}
	return dumpChildren
}

func WatchChildren(conn *zk.Conn, zkPath string, onChange func(children []string)) {
	for {
		children, _, ch, err := conn.ChildrenW(zkPath)
		if err != nil {
			log.Printf("Watch children path error, path:%s, err:%v \n", zkPath, err)
			continue
		}
		onChange(children)
		select {
		case <-ch:
		}
	}
}
