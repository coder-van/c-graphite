package receivers

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"time"

	"github.com/coder-van/v-carbon/src/common"
	"strings"
	statsd "github.com/coder-van/v-stats"
	"github.com/coder-van/v-util/log"
)

func NewTCPServer(ch chan common.MetricPoint, name string) *TCPServer {
	s := &TCPServer{
		Name:                  name,
		IsPickle:              true,
		ChanPointBagsReceived: ch,
		stat:                  common.GetStat("tcp_receiver"),
		logger: log.GetLogger("tcp", log.RotateModeMonth),
	}
	addr, err := net.ResolveTCPAddr("tcp", "localhost:2003")
	if err != nil {
		fmt.Println(err)
	}
	s.Addr = addr
	return s
}

// TCP receive metrics from TCP connections
type TCPServer struct {
	Name string

	Addr                  *net.TCPAddr
	IsPickle              bool
	maxPickleMessageSize  uint32
	listener              *net.TCPListener
	ChanPointBagsReceived chan common.MetricPoint // 收到的数据都写入channel
	logger                *log.Vlogger
	stat                  *statsd.BaseStat
}

func (rcv *TCPServer) handleByteArray(conn net.Conn) {

	if conn == nil {
		return
	}
	rcv.stat.GaugeInc("active_conn", 1)
	defer rcv.stat.GaugeDec("active_conn", -1)
	defer conn.Close()

	reader := bufio.NewReader(conn)

	for {
		conn.SetReadDeadline(time.Now().Add(2 * time.Minute))

		line, err := reader.ReadBytes('\n')
		rcv.logger.Debug(
			fmt.Sprintf("tcp received %s \n", string(line)))
		
		if err != nil {
			if err == io.EOF {
				if len(line) > 0 {
					rcv.logger.Printf("Warn unfinished line %s", string(line))
				}
			} else {
				rcv.stat.OnErr("error-tcp-receiver-readline", err)
				rcv.logger.Printf("read error %s", err.Error())
			}
			break
		}

		if len(line) > 0 { // skip empty lines
			if mp, err := common.ParseFromStr(string(line)); err != nil {
				rcv.stat.OnErr("error-tcp-receiver-ParseFromStr", err)
			} else {
				rcv.stat.CounterInc("point-received", 1)
				rcv.pipeOut(*mp)
			}
		}
	}

}

//func (rcv *TCPServer) handlePickle(conn net.Conn) {
//	rcv.stat.GaugeInc("active_conn", 1)
//	defer rcv.stat.GaugeDec("active_conn", -1)
//	defer conn.Close()
//
//	c, _ := NewConn(conn, byte(4), binary.BigEndian)
//	c.MaxFrameSize = uint(rcv.maxPickleMessageSize)
//	for {
//		conn.SetReadDeadline(time.Now().Add(2 * time.Minute))
//		data, err := c.ReadFrame()
//		if err != nil {
//			rcv.stat.OnErr("error-tcp-receiver-ReadFrame", err)
//			return
//		}
//		fmt.Println(data)
//
//		mps, err := common.ParseFromBytes(data)
//
//		if err != nil {
//			rcv.stat.OnErr("error-tcp-receiver-ParseFromBytes", err)
//			return
//		}
//
//		for _, pointBag := range mps {
//			rcv.stat.CounterInc("point-received", 1)
//			rcv.pipeOut(pointBag)
//		}
//	}
//}

func (rcv *TCPServer) pipeOut(mp common.MetricPoint) {
	rcv.ChanPointBagsReceived <- mp
}


func (rcv *TCPServer) Start() {
	fmt.Println("* Tcp receiver starting")
	go rcv.Listen()
}

// Listen 是阻塞的 需要调用时加 go
func (rcv *TCPServer) Listen() error {
	var err error
	rcv.listener, err = net.ListenTCP("tcp", rcv.Addr)
	if err != nil {
		// TODO 直接退出
		return err
	}
	defer rcv.listener.Close()

	handler := rcv.handleByteArray
	//if rcv.IsPickle {
	//	handler = rcv.handlePickle
	//}
	
	fmt.Println("* Tcp receiver started")
	for {

		conn, err := rcv.listener.Accept()
		if err != nil {
			if strings.Contains(err.Error(), "use of closed network connection") {
				break
			}
			// fmt.Fprintf(os.Stdout, "Error: %s", err.Error())
			continue
		}
		go handler(conn)

	}
	return nil
}

func (rcv *TCPServer) Stop() {
	fmt.Println("* Tcp receiver closing")
	rcv.listener.Close()
	rcv.listener = nil
	fmt.Println("* Tcp receiver closed")
}
