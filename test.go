package main

import (
	"fmt"
	"time"
)

type STask struct {
	QuitChan chan bool
}

func NewSTask(quitChan chan bool) *STask {
	return &STask{QuitChan: quitChan}
}

func (w *STask) Start() {
	go func() {
		for {
			select {
			case <-w.QuitChan:
				fmt.Println("worker stopping")
				time.Sleep(time.Duration(2000) * time.Millisecond)
				fmt.Println("worker stopped")
				return
			default:
				w.doWork()
			}
		}
	}()
}

func (w *STask) doWork() {
	fmt.Println("Working ...")
	time.Sleep(time.Duration(1000) * time.Millisecond)

}

func (w *STask) Stop() {
	go func() {
		fmt.Println("stop worker stopping")
		w.QuitChan <- true
	}()
}

func main() {
	task := NewSTask(make(chan bool))
	task.Start()
	time.Sleep(time.Duration(10000) * time.Millisecond)
	task.Stop()
	time.Sleep(time.Duration(2000) * time.Millisecond)
	fmt.Println(" worker starting again")
	task.Start()
	task.Stop()
	time.Sleep(time.Duration(5000) * time.Millisecond)
	select {}
}
