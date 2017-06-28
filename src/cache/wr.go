package cache

import (
	"io"
	"bufio"
	"errors"
	"fmt"
	"os"
	
	"github.com/coder-van/v-graphite/src/common"
)

const MB = 1048576

func ReadLines(r io.Reader, callbackCacheAdd func(point common.MetricPoint)) error {
	reader := bufio.NewReaderSize(r, MB)
	
	for {
		line, err := reader.ReadBytes('\n')
		
		if err != nil && err != io.EOF {
			return err
		}
		
		if len(line) == 0 {
			break
		}
		
		if line[len(line)-1] != '\n' {
			return errors.New("unfinished line in file")
		}
		
		p, err := common.ParseFromStr(string(line))
		
		if err == nil {
			callbackCacheAdd(*p)
		}
	}
	
	return nil
}


func ReadFrom(filename string, callbackCacheAdd func(point common.MetricPoint)) error {
	file, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer file.Close()
	
	//if strings.HasSuffix(strings.ToLower(filename), ".bin") {
	//	return ReadBinary(file, callback)
	//}
	
	return ReadLines(file, callbackCacheAdd)
}


func WriteTo(p *common.PointBag, w io.Writer) (n int64, err error) {
	var c int
	for _, d := range p.Data { // every metric point
		c, err = w.Write([]byte(fmt.Sprintf("%s %v %v\n", p.Metric, d.Value, d.Timestamp)))
		n += int64(c)
		if err != nil {
			return
		}
	}
	return
}

//func ReadBinary(r io.Reader, callback func(point *common.PointBag)) error {
//	reader := bufio.NewReaderSize(r, MB)
//	var pb *common.PointBag
//	buf := make([]byte, MB)
//
//	flush := func() {
//		if &pb != nil {
//			callback(pb)
//			pb = nil
//		}
//	}
//	defer flush()
//
//	for {
//		flush()
//
//		l, err := binary.ReadVarint(reader)
//		if l > MB {
//			return fmt.Errorf("metric name too long: %d", l)
//		}
//
//		if err == io.EOF {
//			break
//		}
//
//		if err != nil {
//			return err
//		}
//
//		io.ReadAtLeast(reader, buf[:l], int(l))
//
//		cnt, err := binary.ReadVarint(reader)
//
//		var v, t, v0, t0 int64
//
//		for i := int64(0); i < cnt; i++ {
//			v0, err = binary.ReadVarint(reader)
//			if err != nil {
//				return err
//			}
//			v += v0
//
//			t0, err = binary.ReadVarint(reader)
//			if err != nil {
//				return err
//			}
//			t += t0
//
//			if i == int64(0) {
//				metric := string(buf[:l])
//				pb = common.NewPointsBag(metric)
//			}
//			pb.Append(common.Point{math.Float64frombits(uint64(v)), t})
//		}
//	}
//
//	return nil
//}

//func encodeIntValue(value int64) []byte {
//	var buf [10]byte
//	l := binary.PutVarint(buf[:], value)
//	return buf[:l]
//}

//func WriteBinaryTo(p *common.PointBag, w io.Writer) (n int, err error) {
//	var c int
//
//	writeIntValue := func(value int64) bool {
//
//		c, err = w.Write(encodeIntValue(value))
//		n += c
//		if err != nil {
//			return false
//		}
//		return true
//	}
//
//	if !writeIntValue(int64(len(p.Metric))) {
//		return
//	}
//
//	c, err = io.WriteString(w, p.Metric)
//	n += c
//	if err != nil {
//		return
//	}
//
//	if !writeIntValue(int64(len(p.Data))) {
//		return
//	}
//
//	if len(p.Data) > 0 {
//		if !writeIntValue(int64(math.Float64bits(p.Data[0].Value))) {
//			return
//		}
//		if !writeIntValue(p.Data[0].Timestamp) {
//			return
//		}
//	}
//
//	for i := 1; i < len(p.Data); i++ {
//		if !writeIntValue(int64(math.Float64bits(p.Data[i].Value)) - int64(math.Float64bits(p.Data[i-1].Value))) {
//			return
//		}
//		if !writeIntValue(p.Data[i].Timestamp - p.Data[i-1].Timestamp) {
//			return
//		}
//	}
//
//	return
//}
