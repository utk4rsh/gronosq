package ha_worker

import (
	"fmt"
)

type ZkTaskDistributor struct {
	zkPrefix        string
	totalPartitions int64
	instanceId      string
	tasksToRun      []int
	workerInstances []string
	workerManager   *WorkerManager
	zkDiscovery     *ZKDiscovery
}

func NewZkTaskDistributor(zkPrefix string, totalPartitions int64, instanceId string, zkDiscovery *ZKDiscovery) *ZkTaskDistributor {
	z := &ZkTaskDistributor{zkPrefix: zkPrefix, totalPartitions: totalPartitions, instanceId: instanceId, zkDiscovery: zkDiscovery}
	createNode(zkPrefix, z, instanceId)
	_ = z.zkDiscovery.AddListener(zkPrefix, z.childEvent)
	return z
}

func createNode(zkPrefix string, z *ZkTaskDistributor, instanceId string) {
	err := z.zkDiscovery.CreatePersistentEphemeralNode(zkPrefix, instanceId)
	if err != nil {
		fmt.Println("Panic for CreatePersistentEphemeralNode", zkPrefix)
		panic(err)
	}
}

func (td *ZkTaskDistributor) GetTasks() []int {
	return td.tasksToRun
}

func (td *ZkTaskDistributor) Init() {
	td.workerInstances = td.getWorkerInstances(td.zkPrefix)
	instanceIndex := -1
	i := 0
	for ; i < len(td.workerInstances); i++ {
		instanceName := td.workerInstances[i]
		if instanceName == td.instanceId {
			instanceIndex = i
			break
		}
	}
	if instanceIndex < 0 {
		panic("'" + td.instanceId + "' instanceId is unknown & not configured!")
	}
	td.tasksToRun = td.createTaskIdsForExecution(td.workerInstances, td.totalPartitions, instanceIndex)
}

func (td *ZkTaskDistributor) getWorkerInstances(prefix string) []string {
	children, err := td.zkDiscovery.GetChildren(prefix)
	if err != nil {
		fmt.Println("Panic for prefix", prefix)
		panic(err)
	}
	return children
}

func (td *ZkTaskDistributor) createTaskIdsForExecution(instances []string, totalPartitions int64, instanceIndex int) []int {
	newTasks := []int{}
	instanceSize := len(instances)
	for i := 0; i < int(totalPartitions); i++ {
		if ((i) % instanceSize) == instanceIndex {
			newTasks = append(newTasks, i)
		}
	}
	return newTasks
}

func (td *ZkTaskDistributor) SetRestartAble(workerManager *WorkerManager) {
	td.workerManager = workerManager
}

func (td *ZkTaskDistributor) childEvent(children []string) {
	if td.hasHostListChanged(children) {
		fmt.Println("Host list changed ", children, td.workerInstances)
		td.workerManager.Restart()
	} else {
		fmt.Println("Host list remain unchanged ", children, td.workerInstances)
	}
}

func (td *ZkTaskDistributor) hasHostListChanged(values []string) bool {
	if td.createHash(values) != td.createHash(td.workerInstances) {
		return true
	} else {
		return false
	}
}

func (td *ZkTaskDistributor) createHash(items []string) string {
	var hash string
	for _, item := range items {
		hash = hash + item + ":"
	}
	return hash
}
