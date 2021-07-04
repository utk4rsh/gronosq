package ha_worker

import "fmt"

type ZkTaskDistributor struct {
	zkPrefix        string
	totalPartitions int64
	instanceId      string
	tasksToRun      []int
	workerInstances []string
	zkDiscovery     *ZKDiscovery
}

func NewZkTaskDistributor(zkPrefix string, totalPartitions int64, instanceId string, zkDiscovery *ZKDiscovery) *ZkTaskDistributor {
	return &ZkTaskDistributor{zkPrefix: zkPrefix, totalPartitions: totalPartitions, instanceId: instanceId, zkDiscovery: zkDiscovery}
}

func (td *ZkTaskDistributor) GetTasks() []int {
	return td.tasksToRun
}

func (td *ZkTaskDistributor) Init() {
	err := td.zkDiscovery.CreatePersistentEphemeralNode(td.zkPrefix, td.instanceId)
	if err != nil {
		fmt.Println("Panic for CreatePersistentEphemeralNode", td.zkPrefix)
		panic(err)
	}
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
	td.createTaskIdsForExecution(td.workerInstances, td.totalPartitions, instanceIndex)
}

func (td *ZkTaskDistributor) getWorkerInstances(prefix string) []string {
	children, err := td.zkDiscovery.GetChildren(prefix)
	if err != nil {
		fmt.Println("Panic for prefix", prefix)
		panic(err)
	}
	return children
}

func (td *ZkTaskDistributor) createTaskIdsForExecution(instances []string, totalPartitions int64, instanceIndex int) {
	instanceSize := len(instances)
	for i := 0; i < int(totalPartitions); i++ {
		if ((i) % instanceSize) == instanceIndex {
			td.tasksToRun = append(td.tasksToRun, i)
		}
	}
}
