package sortedset

type SortedSet struct {
	dict     map[string]*Element
	skipList *SkipList
}

func NewSortedSet() *SortedSet {
	return &SortedSet{
		dict:     make(map[string]*Element),
		skipList: NewSkipList(),
	}
}

// Add 添加一个member到set里 返回是否新增节点
func (s *SortedSet) Add(member string, score float64) bool {
	element, ok := s.dict[member]
	s.dict[member] = &Element{
		Member: member,
		Score:  score,
	}
	if ok {
		if score != element.Score {
			s.skipList.remove(member, element.Score)
			s.skipList.insert(member, score)
		}
		return false
	}
	s.skipList.insert(member, score)
	return true
}

func (s *SortedSet) Len() int64 {
	return int64(len(s.dict))
}

func (s *SortedSet) Get(member string) (element *Element, ok bool) {
	elem, ok := s.dict[member]
	if !ok {
		return nil, false
	}
	return elem, true
}

// GetRank rank从0开始 -1表示没找到key
func (s *SortedSet) GetRank(member string, desc bool) int64 {
	elem, ok := s.dict[member]
	if !ok {
		return -1
	}
	// s.skipList.getRank 是从 1 开始
	rank := s.skipList.getRank(member, elem.Score)
	if desc {
		rank = s.Len() - rank
	} else {
		rank--
	}
	return rank
}

func (s *SortedSet) Foreach(start, stop int64, desc bool, consumer func(element *Element) bool) {
	size := s.Len()
	if start < 0 || start >= size {
		return
	}
	if stop < start || stop > size {
		return
	}
	var node *Node
	if desc {
		// start = 0
		node = s.skipList.tail
		if start > 0 {
			node = s.skipList.getByRank(size - start)
		}
	} else {
		node = s.skipList.header.level[0].forward
		if start > 0 {
			node = s.skipList.getByRank(start + 1)
		}
	}
	sliceSize := stop - start
	for i := 0; i < int(sliceSize); i++ {
		if !consumer(node.Element) {
			break
		}
		if desc {
			node = node.backward
		} else {
			node = node.level[0].forward
		}
	}
}

func (s *SortedSet) Range(start int64, stop int64, desc bool) []*Element {
	size := s.Len()
	if start < 0 || start >= size {
		return make([]*Element, 0)
	}
	if stop < start || stop > size {
		return make([]*Element, 0)
	}
	result := make([]*Element, size)
	i := 0
	s.Foreach(start, stop, desc, func(element *Element) bool {
		result[i] = element
		i++
		return true
	})
	return result
}

func (s *SortedSet) Count(min *ScoreBorder, max *ScoreBorder) int64 {
	var count int64 = 0
	s.Foreach(0, s.Len(), false, func(element *Element) bool {
		if !min.less(element.Score) {
			return true
		}
		if !max.greater(element.Score) {
			return false
		}
		count++
		return true
	})
	return count
}

func (s *SortedSet) ForEachByScore(min *ScoreBorder, max *ScoreBorder, offset int64, limit int64, desc bool, consumer func(element *Element) bool) {
	var node *Node
	// 找到范围内的第一个结点
	if desc {
		node = s.skipList.getLastInScoreRange(min, max)
	} else {
		node = s.skipList.getFirstInScoreRange(min, max)
	}
	// 偏移量 相对于第一个结点的
	for node != nil && offset > 0 {
		if desc {
			node = node.backward
		} else {
			node = node.level[0].forward
		}
		offset--
	}
	// limit < 0 表示为无限制个数
	for i := 0; (i < int(limit) || limit < 0) && node != nil; i++ {
		if !consumer(node.Element) {
			break
		}
		if desc {
			node = node.backward
		} else {
			node = node.level[0].forward
		}
		if node == nil {
			break
		}
		// 不在范围内了 终止循环
		if !min.less(node.Element.Score) || !max.greater(node.Element.Score) {
			break
		}
	}
}

func (s *SortedSet) RangeByScore(min *ScoreBorder, max *ScoreBorder, offset int64, limit int64, desc bool) []*Element {
	if limit == 0 || offset < 0 {
		return make([]*Element, 0)
	}
	slice := make([]*Element, 0)
	s.ForEachByScore(min, max, offset, limit, desc, func(element *Element) bool {
		slice = append(slice, element)
		return true
	})
	return slice
}

func (s *SortedSet) RemoveByScore(min *ScoreBorder, max *ScoreBorder) int64 {
	removed := s.skipList.RemoveRangeByScore(min, max)
	for _, element := range removed {
		delete(s.dict, element.Member)
	}
	return int64(len(removed))
}

func (s *SortedSet) RemoveByRank(start int64, stop int64) int64 {
	removed := s.skipList.RemoveRangeByRank(start+1, stop+1)
	for _, element := range removed {
		delete(s.dict, element.Member)
	}
	return int64(len(removed))
}
