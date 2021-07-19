package ha_worker

import (
	"github.com/samuel/go-zookeeper/zk"
	log "github.com/sirupsen/logrus"
	zkClient "gronosq/core/zk"
)

type ZKDiscovery struct {
	zk *zk.Conn
}

func NewZKDiscovery(client zkClient.Client) *ZKDiscovery {
	return &ZKDiscovery{zk: client.Client()}
}

func (s *ZKDiscovery) CreatePersistentEphemeralNode(path string, instanceId string) error {
	var err error
	completePath := path + "/" + instanceId
	create, err := s.zk.Create(completePath, []byte(instanceId), 1, zk.WorldACL(zk.PermAll))
	if err != nil {
		log.Printf("Could not create node for instance id : %+v Stat: %+v", instanceId, create)
		return err
	}
	log.Printf("ZK Created for instance id : %+v Stat: %+v", instanceId, create)
	return nil
}

func (s *ZKDiscovery) AddListener(zkPath string, onChange func(children []string)) error {
	var err error
	dumpChildren, stat, _, err := s.zk.ChildrenW(zkPath)
	if err != nil {
		panic(err)
	}
	log.Printf("ZK Childrens : %+v Stat: %+v", dumpChildren, stat)
	go WatchChildren(s.zk, zkPath, onChange)
	return nil
}

func (s *ZKDiscovery) GetChildren(path string) ([]string, error) {
	dumpChildren, _, _, err := s.zk.ChildrenW(path)
	return dumpChildren, err
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
