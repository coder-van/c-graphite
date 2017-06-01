package graghite

import (
	"bufio"
	"net"
)

type Graphite struct {
	Addr string // Network address to connect to
}

func NewGraphite(addr string) *Graphite {
	return &Graphite{
		Addr: addr,
	}
}

func (g *Graphite) Flush(bs []byte) error {
	conn, err := net.Dial("tcp", g.Addr)
	if nil != err {
		return err
	}
	defer conn.Close()
	w := bufio.NewWriter(conn)
	w.Write(bs)
	w.Flush()
	return nil
}
