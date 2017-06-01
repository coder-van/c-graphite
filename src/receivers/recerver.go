package receivers

import "fmt"
import (
	"github.com/coder-van/v-carbon/src/common"
	"net"
	"strings"
)

type InterfaceReceiver interface {
	Start()
	Stop()
}

func New() *ReceiverManager {
	return &ReceiverManager{
		receivers:             make([]InterfaceReceiver, 0),
		ChanPointBagsReceived: make(chan common.MetricPoint, 1024*1024),
		exit: make(chan bool, 2),
	}
}

type ReceiverManager struct {
	receivers             []InterfaceReceiver
	ChanPointBagsReceived chan common.MetricPoint
	exit                  chan bool
	CachePB               func(common.MetricPoint)
}


func (rm *ReceiverManager) Run() {
	defer close(rm.exit)
	defer close(rm.ChanPointBagsReceived)
	
	if rm.CachePB == nil {
		panic(" ReceiverManager CachePB func can't be nil")
	}
	for _, r := range rm.receivers {
		r.Start()
	}

	var pb common.MetricPoint
	for {
		select {
		case <-rm.exit:
			fmt.Println("* ReceiveManage stopped")
			return

		case pb = <-rm.ChanPointBagsReceived:
			if strings.Index(pb.Key, "cpu.cpu-total.idle") >= 0 {
				fmt.Println(pb.Key, pb.Value, pb.Timestamp)
			}
			rm.CachePB(pb)
		}
	}
	return
}

func (rm *ReceiverManager) RegisterReceiver(r InterfaceReceiver) {
	rm.receivers = append(rm.receivers, r)
}

func (rm *ReceiverManager) CreateNewReceiver(name, t, port string, isPocket bool) (InterfaceReceiver, error) {
	if t == "tcp" {
		s := NewTCPServer(rm.ChanPointBagsReceived, name)
		if port > "0" && port != "2003" {
			addr, err := net.ResolveTCPAddr("tcp", "localhost:"+string(port))
			if err != nil {
				return nil, err
			}
			s.Addr = addr
		}
		if isPocket == false {
			s.IsPickle = false
		}
		return s, nil
	}
	return nil, fmt.Errorf("%s","server type  from config error")
}

func (rm *ReceiverManager) Start (){
	fmt.Println("* ReceiverManager starting")
	go rm.Run()
}

func (rm *ReceiverManager) Stop() {
	rm.exit <- true
	fmt.Println("* ReceiveManage stopping")
	for _, r := range rm.receivers {
		r.Stop()
	}
}
