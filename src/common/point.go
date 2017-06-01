package common

import (
	// "bytes"
	// "errors"
	"fmt"
	// "github.com/hydrogen18/stalecucumber"
	"math"
	"strconv"
	"strings"
)


type Point struct {
	Value     float64
	Timestamp int64
}

type MetricPoint struct {
	Key       string
	Value     float64
	Timestamp int64
}

func NewPointsBag(metric string) *PointBag {
	return &PointBag{
		Metric: metric,
		Data:make([]Point, 0),
	}
}

type PointBag struct {
	Metric string  // e.x sys.cpu.idle
	Data   []Point
}

func (p *PointBag) Append(onePoint Point) *PointBag {
	p.Data = append(p.Data, onePoint)
	return p
}

func (p *PointBag) Equals(other *PointBag) bool {
	if other == nil {
		return false
	}
	if p.Metric != other.Metric {
		return false
	}
	if p.Data == nil && other.Data == nil {
		return true
	}
	if (p.Data == nil || other.Data == nil) && (p.Data != nil || other.Data != nil) {
		return false
	}
	if len(p.Data) != len(other.Data) {
		return false
	}
	for i := 0; i < len(p.Data); i++ {
		if p.Data[i].Value != other.Data[i].Value {
			return false
		}
		if p.Data[i].Timestamp != other.Data[i].Timestamp {
			return false
		}
	}
	return true
}


func ParseFromStr(line string) (*MetricPoint, error) {

	fields := strings.Split(strings.Trim(line, "\n \t\r"), " ")
	if len(fields) != 3 {
		return nil, fmt.Errorf("bad message: %#v", line)
	}
	
	key := fields[0]
	value, err := strconv.ParseFloat(fields[1], 64)
	timestamp, err := strconv.ParseFloat(fields[2], 64)

	if err != nil || math.IsNaN(value) || math.IsNaN(timestamp){
		return nil, fmt.Errorf("bad message: %#v", line)
	}
	
	return &MetricPoint{Key: key, Value: value, Timestamp: int64(timestamp)}, nil
}

//func ParseFromBytes(pkt []byte) ([]MetricPoint, error) {
//	result, err := stalecucumber.Unpickle(bytes.NewReader(pkt))
//
//	list, err := stalecucumber.ListOrTuple(result, err)
//	if err != nil {
//		return nil, err
//	}
//
//	pointsBagList := []MetricPoint{}
//	for i := 0; i < len(list); i++ {
//
//		metric, err := stalecucumber.ListOrTuple(list[i], nil)
//		if err != nil {
//			return nil, err
//		}
//
//		if len(metric) < 2 {
//			return nil, errors.New("Unexpected array length while unpickling metric")
//		}
//
//		name, err := stalecucumber.String(metric[0], nil)
//		if err != nil {
//			return nil, err
//		}
//
//		var mp MetricPoint
//		for j := 1; j < len(metric); j++ {
//			v, err := stalecucumber.ListOrTuple(metric[j], nil)
//			if err != nil {
//				return nil, err
//			}
//			if len(v) != 2 {
//				return nil, errors.New("Unexpected array length while unpickling data point")
//			}
//			timestamp, err := stalecucumber.Int(v[0], nil)
//			if err != nil {
//				timestampFloat, err := stalecucumber.Float(v[0], nil)
//				if err != nil {
//					return nil, err
//				}
//				timestamp = int64(timestampFloat)
//			}
//			if timestamp > math.MaxUint32 || timestamp < 0 {
//				err = errors.New("Unexpected value for timestamp, cannot be cast to uint32")
//				return nil, err
//			}
//
//			value, err := stalecucumber.Float(v[1], nil)
//			if err != nil {
//				valueInt, err := stalecucumber.Int(v[1], nil)
//				if err != nil {
//					return nil, err
//				}
//				value = float64(valueInt)
//			}
//			mp = MetricPoint{Key: name, Value: value, Timestamp: timestamp}
//
//		}
//		pointsBagList = append(pointsBagList, mp)
//	}
//	return pointsBagList, nil
//}
