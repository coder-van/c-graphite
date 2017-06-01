package receivers

import (
	"fmt"
	"github.com/coder-van/v-util/log"
	"net"
)

const (
	//  means UDP packet limit, see https://en.wikipedia.org/wiki/User_Datagram_Protocol#Packet_structure
	UDPMaxPacketSize int = 64 * 1024
)

func NewUdpReceiver(addr string, ch chan []byte) *UdpReceiver {
	return &UdpReceiver{
		exit:            make(chan bool),
		Addr:            addr,
		packetInChannel: ch,
		logger:          log.GetLogger("statsd.UdpReceiver", log.RotateModeMonth),
	}
}

type UdpReceiver struct {
	exit            chan bool
	Addr            string
	drops           int // drops tracks the number of dropped metrics.
	packetInChannel chan []byte
	logger          *log.Vlogger
}

func (udp *UdpReceiver) listen(shutdown chan bool) {
	defer close(shutdown)

	addr, err := net.ResolveUDPAddr("udp", udp.Addr)
	if err != nil {
		fmt.Println("Can't resolve address:", err)

	}

	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		fmt.Println("Error listening:", err)
	}

	fmt.Println("Statsd listening on:", addr)
	udp.logger.Println("Statsd UdpReceiver Started")
	for {
		select {
		case <-shutdown:

			fmt.Println("Statsd server thread exit")
			if err := conn.Close(); err != nil {
				return // todo log
			}
			return
		default:
			udp.handle(conn)
		}
	}
	udp.logger.Println("Statsd UdpReceiver Stoped")
}

func (udp *UdpReceiver) handle(conn *net.UDPConn) {
	// TODO 不用每次都初始化一个数组
	buf := make([]byte, UDPMaxPacketSize)
	n, _, err := conn.ReadFromUDP(buf)
	if err != nil {
		udp.logger.Error("ERROR: failed to read UDP msg because of ", err.Error())
		return
	}

	bufCopy := make([]byte, n)
	copy(bufCopy, buf[:n])

	select {
	case udp.packetInChannel <- bufCopy:
	default:
		udp.drops++
		if udp.drops >= 1 {
			udp.logger.Printf("ERROR: statsd message queue full. dropped %d messages. ", udp.drops)
		}
	}
}

func (udp *UdpReceiver) Start() {
	udp.logger.Println("Statsd UdpReceiver starting")
	go udp.listen(udp.exit)
}

func (udp *UdpReceiver) Stop() {
	udp.logger.Println("Statsd UdpReceiver stoping")
	udp.exit <- true
}
