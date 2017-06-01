package whisper

/*
Schemas read code from https://github.com/grobian/carbonwriter/
*/

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/alyu/configparser"
)

type AggregationItem struct {
	name                 string
	pattern              *regexp.Regexp
	xFilesFactor         float64
	aggregationMethodStr string
	aggregationMethod    AggregationMethod
}

// WhisperAggregation ...
type WhisperAggregation struct {
	Data    []*AggregationItem
	Default *AggregationItem
}

// NewWhisperAggregation create instance of WhisperAggregation
func NewWhisperAggregation() *WhisperAggregation {
	return &WhisperAggregation{
		Data: make([]*AggregationItem, 0),
		Default: &AggregationItem{
			name:                 "default",
			pattern:              nil,
			xFilesFactor:         0.5,
			aggregationMethodStr: "average",
			aggregationMethod:    Average,
		},
	}
}

// Match find aggregation for metric
func (a *WhisperAggregation) Match(metric string) *AggregationItem {
	for _, s := range a.Data {
		if s.pattern.MatchString(metric) {
			return s
		}
	}
	return a.Default
}

func ReadAggregationConfig(filePath string) (*WhisperAggregation, error) {
	config, err := configparser.Read(filePath)
	if err != nil {
		return nil, err
	}
	// pp.Println(config)
	sections, err := config.AllSections()
	if err != nil {
		return nil, err
	}

	result := NewWhisperAggregation()

	for _, s := range sections {
		item := &AggregationItem{}
		// this is mildly stupid, but I don't feel like forking
		// configparser just for this
		item.name =
			strings.Trim(strings.SplitN(s.String(), "\n", 2)[0], " []")
		if item.name == "" || strings.HasPrefix(item.name, "#") {
			continue
		}

		item.pattern, err = regexp.Compile(s.ValueOf("pattern"))
		if err != nil {
			return nil, fmt.Errorf("failed to parse pattern %#v for [%s]: %s",
				s.ValueOf("pattern"), item.name, err.Error())
		}

		item.xFilesFactor, err = strconv.ParseFloat(s.ValueOf("xFilesFactor"), 64)
		if err != nil {
			return nil, fmt.Errorf("failed to parse xFilesFactor %#v in %s: %s",
				s.ValueOf("xFilesFactor"), item.name, err.Error())
		}

		item.aggregationMethodStr = s.ValueOf("aggregationMethod")
		switch item.aggregationMethodStr {
		case "average", "avg":
			item.aggregationMethod = Average
		case "sum":
			item.aggregationMethod = Sum
		case "last":
			item.aggregationMethod = Last
		case "max":
			item.aggregationMethod = Max
		case "min":
			item.aggregationMethod = Min
		default:
			return nil, fmt.Errorf("unknown aggregation method '%s'",
				s.ValueOf("aggregationMethod"))
		}

		result.Data = append(result.Data, item)
	}

	return result, nil
}
