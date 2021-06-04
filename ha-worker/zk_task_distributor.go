package ha_worker

type ZKTaskDistributor struct {
	zkDiscovery     ZKDiscovery
	zkPrefix        string
	tasks           []string
	instanceId      string
	tasksToRun      []string
	workerInstances []string
}

func (t *ZKTaskDistributor) init() {
	t.workerInstances = t.zkDiscovery.GetChildren()
	instanceIndex := -1
	for e := range t.workerInstances {
		if t.workerInstances[e] == t.instanceId {
			instanceIndex = e
			break
		}
	}
	if instanceIndex < 0 {
		panic("Instance Id not configured")
	} else {
		t.tasksToRun = t.createTaskIdsForExecution(instanceIndex)
	}
}

func (t *ZKTaskDistributor) createTaskIdsForExecution(instanceIndex int) []string {
	var instances []string
	for idx := range t.tasks {
		if ((idx) % len(t.workerInstances)) == instanceIndex {
			instances = append(instances, t.tasks[idx])
		}
	}
	return instances
}
