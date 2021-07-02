package ha_worker

type ZkTaskDistributor struct {
	zkPrefix        string
	tasks           []string
	instanceId      string
	tasksToRun      []string
	workerInstances []string
	zkDiscovery     ZKDiscovery
}

func NewZkTaskDistributor(zkPrefix string, zkDiscovery ZKDiscovery) *ZkTaskDistributor {
	return &ZkTaskDistributor{zkPrefix: zkPrefix, zkDiscovery: zkDiscovery}
}

func (td ZkTaskDistributor) GetTasks() []string {
	return td.tasks
}

func (td ZkTaskDistributor) Init() {
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
	td.tasksToRun = td.createTaskIdsForExecution(td.workerInstances, td.tasks, instanceIndex)

}

func (td ZkTaskDistributor) getWorkerInstances(prefix string) []string {
	children, err := td.zkDiscovery.GetChildren(prefix)
	if err != nil {
		panic(err)
	}
	return children
}

func (td ZkTaskDistributor) createTaskIdsForExecution(instances []string, tasks []string, instanceIndex int) []string {
	instanceSize := len(instances)
	var result []string
	for i := 0; i < len(tasks); i++ {
		if ((i) % instanceSize) == instanceIndex {
			result = append(result, tasks[i])
		}
	}
	return result
}
