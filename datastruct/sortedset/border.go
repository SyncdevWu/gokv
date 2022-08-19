package sortedset

import (
	"errors"
	"strconv"
)

const (
	negativeInf int8 = -1
	positiveInf int8 = 1
)

// ScoreBorder 用来表示ZRANGEBYSCORE命令的min和max
type ScoreBorder struct {
	Inf     int8 // 正无穷或者负无穷 0表示不包含正无穷或者负无穷
	Value   float64
	Exclude bool // 是否包含value
}

var (
	negativeBorder    = &ScoreBorder{Inf: negativeInf}
	positiveInfBorder = &ScoreBorder{Inf: positiveInf}
)

func (border *ScoreBorder) greater(value float64) bool {
	if border.Inf == negativeInf {
		//  负无穷
		return false
	} else if border.Inf == positiveInf {
		// 正无穷
		return true
	}
	if border.Exclude {
		// 不包含border.Value
		return border.Value > value
	}
	return border.Value >= value
}

func (border *ScoreBorder) less(value float64) bool {
	if border.Inf == negativeInf {
		return true
	} else if border.Inf == positiveInf {
		return false
	}
	if border.Exclude {
		return border.Value < value
	}
	return border.Value <= value
}

// ParseScoreBorder 解析ZRANGEBYSCORE的一个参数
func ParseScoreBorder(s string) (*ScoreBorder, error) {
	// 正无穷  ZRANGEBYSCORE salary -inf +inf
	if s == "inf" || s == "+inf" {
		return positiveInfBorder, nil
	}
	// 负无穷  ZRANGEBYSCORE salary -inf +inf
	// ZRANGEBYSCORE salary -inf 5000
	if s == "-inf" {
		return negativeBorder, nil
	}
	// ZRANGEBYSCORE salary (5000 400000  5000 < salary <= 400000
	if s[0] == '(' {
		value, err := strconv.ParseFloat(s[1:], 64)
		if err != nil {
			return nil, errors.New("ERR min or max is not a float")
		}
		return &ScoreBorder{
			Inf:     0,
			Value:   value,
			Exclude: true,
		}, nil
	}
	value, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return nil, errors.New("ERR min or max is not a float")
	}
	return &ScoreBorder{
		Inf:     0,
		Value:   value,
		Exclude: false,
	}, nil
}
