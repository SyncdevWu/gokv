package sortedset

import (
	"fmt"
	"math/rand"
)

/**
forward 表示的是指向同一层中的下一个结点
backward 表示的是最底层前一个结点

# 插入前
header(7)                                                        tail
header                          9(3)                      15     tail
header        1                 9(3)                      15     tail
header        1     3           9(3)                      15     tail
header        1     3     7     9(1)     10(2)            15     tail
header        1     3     7     9(1)     10(1)     14     15     tail

# 插入时
header(7)                                                        tail
header                          9(3)                      15     tail
header        1                 9(3)                      15     tail
header        1     3           9(2)                      15     tail
header        1     3     7     9(1)     10(1)            15     tail
header        1     3     7     9(1)     10(1)     14     15     tail

# 插入时
header(7)                                                             tail
header                          9(3)                           15     tail
header        1                 9(3)                           15     tail
header        1     3           9(2)             13(2)         15     tail
header        1     3     7     9(1)     10(1)   13(2)         15     tail
header        1     3     7     9(1)     10(1)   13(1)  14     15     tail

# 插入后
header(8)                                                             tail
header                          9(4)                           15     tail
header        1                 9(4)                           15     tail
header        1     3           9(2)             13(2)         15     tail
header        1     3     7     9(1)     10(1)   13(2)         15     tail
header        1     3     7     9(1)     10(1)   13(1)  14     15     tail

*/

const (
	maxLevel  = 16
	skipListP = 0.75
)

type Element struct {
	Member string
	Score  float64
}

type Node struct {
	*Element
	backward *Node
	level    []*Level
}

func (node *Node) String() string {
	return fmt.Sprintf("%f", node.Score)
}

type Level struct {
	forward *Node // 指向同一层中的下一个结点
	span    int64 // 到forward跳过的节点数
}

type SkipList struct {
	header *Node
	tail   *Node
	length int64
	level  int16
}

func NewSkipList() *SkipList {
	return &SkipList{
		level:  1,
		header: newNode(maxLevel, "", 0),
	}
}

func newNode(level int16, member string, score float64) *Node {
	node := Node{
		Element: &Element{
			Score:  score,
			Member: member,
		},
		level: make([]*Level, level),
	}
	for i := range node.level {
		node.level[i] = new(Level)
	}
	return &node
}

func randomLevel() int16 {
	level := int16(1)
	for float32(rand.Int31()&0xFFFF) < (skipListP * 0xFFFF) {
		// 取低16位
		level++
	}
	if level < maxLevel {
		return level
	} else {
		return maxLevel
	}
}

func (list *SkipList) insert(member string, score float64) *Node {
	// 寻找新节点的先驱节点，它们的 forward 将指向新节点
	// 因为每层都有一个 forward 指针, 所以每层都会对应一个先驱节点
	// 找到这些先驱节点并保存在 update 数组中
	update := make([]*Node, maxLevel)
	rank := make([]int64, maxLevel)
	node := list.header
	for i := list.level - 1; i >= 0; i-- {
		if i == list.level-1 {
			rank[i] = 0
		} else {
			rank[i] = rank[i+1]
		}
		if node.level[i] != nil {
			// 如果node.forward.Score 大于了 score 则node.span是不计算在rank里的
			for node.level[i].forward != nil &&
				(node.level[i].forward.Score < score ||
					(node.level[i].forward.Score == score && node.level[i].forward.Member < member)) { // same score, different key
				rank[i] += node.level[i].span
				node = node.level[i].forward
			}
		}
		update[i] = node
	}
	// 随机生成某一层 作为即将插入的结点的层数
	level := randomLevel()
	// 新插入的层超过了skipList目前的层 需要扩层
	if level > list.level {
		for i := list.level; i < level; i++ {
			rank[i] = 0
			update[i] = list.header
			// update[i]是头结点
			update[i].level[i].span = list.length
		}
		list.level = level
	}
	// 创建新结点并插入到列表
	node = newNode(level, member, score)
	for i := int16(0); i < level; i++ {
		// 链表的插入操作
		node.level[i].forward = update[i].level[i].forward
		update[i].level[i].forward = node
		// span更新
		// 如case中 第三层 update[i]是9(3) rank[0]是5 rank[i]是4
		// 如果是 14.5 rank[0]是6 rank[i]是4
		// rank[0]就是插入结点的位置 - 1
		// update[i].level[i].span - (rank[0] - rank[i]) 其实就是 整段减去前一段就剩插入结点到下一个节点的那段了
		node.level[i].span = update[i].level[i].span - (rank[0] - rank[i])
		update[i].level[i].span = (rank[0] - rank[i]) + 1
	}

	// 对于没有遍历到的层，先驱节点的 span 会加1 (后面插入了新节点导致span+1)
	// 对应case中的插入后 第四层和第五层 9(3) -> 9(4)
	for i := level; i < list.level; i++ {
		update[i].level[i].span++
	}

	if update[0] == list.header {
		node.backward = nil
	} else {
		node.backward = update[0]
	}
	if node.level[0].forward != nil {
		node.level[0].forward.backward = node
	} else {
		list.tail = node
	}
	list.length++
	return node
}

func (list *SkipList) remove(member string, score float64) bool {
	update := make([]*Node, maxLevel)
	node := list.header
	for i := list.level - 1; i >= 0; i-- {
		for node.level[i].forward != nil &&
			(node.level[i].forward.Score < score ||
				(node.level[i].forward.Score == score && node.level[i].forward.Member < member)) {
			node = node.level[i].forward
		}
		update[i] = node
	}
	// 可能要被删除的节点
	node = node.level[0].forward
	if node.Score == score && node.Member == member {
		list.removeNode(node, update)
		return true
	}
	return false
}

func (list *SkipList) removeNode(node *Node, update []*Node) {
	// 遍历每一层 修改链表关系
	for i := int16(0); i < list.level; i++ {
		if update[i].level[i].forward == node {
			// 到下一个距离变远了 插入的时候node.level[i].span是后半段的距离 update[i].level[i].span是前一段距离 现在要加回来 然后扣掉node这个节点
			update[i].level[i].span += node.level[i].span - 1
			update[i].level[i].forward = node.level[i].forward
		} else {
			update[i].level[i].span--
		}
	}
	// 不是最后一个节点
	if node.level[0].forward != nil {
		node.level[0].forward.backward = node.backward
	} else {
		// 是最后一个节点
		list.tail = node.backward
	}
	// 从上往下删层
	for list.level > 1 && list.header.level[list.level-1].forward == nil {
		list.level--
	}
	list.length--
}

// getRank 返回0则是key没有找到
func (list *SkipList) getRank(member string, score float64) int64 {
	var rank int64 = 0
	node := list.header
	for i := list.level - 1; i >= 0; i-- {
		// 注意这里是 node.level[i].forward.Member <= member 因为要把node的span也加上 此时的node是目标member的backward
		for node.level[i].forward != nil &&
			(node.level[i].forward.Score < score ||
				node.level[i].forward.Score == score && node.level[i].forward.Member <= member) {
			rank += node.level[i].span
			node = node.level[i].forward
		}
		if node.Member == member {
			return rank
		}
	}
	return 0
}

// getByRank 返回nil则是没有找到
func (list *SkipList) getByRank(rank int64) *Node {
	var count int64 = 0
	node := list.header
	for i := list.level - 1; i >= 0; i-- {
		for node.level[i].forward != nil && (count+node.level[i].span) <= rank {
			count += node.level[i].span
			node = node.level[i].forward
		}
		if count == rank {
			return node
		}
	}
	return nil
}

func (list *SkipList) hasInRange(min *ScoreBorder, max *ScoreBorder) bool {
	// 空集
	if min.Value > max.Value || (min.Value == max.Value && (min.Exclude || max.Exclude)) {
		return false
	}
	node := list.tail
	// 链表为空 或者 最小值比链表最大值还大
	if node == nil || !min.less(node.Score) {
		return false
	}
	node = list.header.level[0].forward
	// 链表为空 或者 最大值比链表的最小值还小
	if node == nil || !max.greater(node.Score) {
		return false
	}
	return true
}

// getFirstInScoreRange 返回给定范围内的第一个score
func (list *SkipList) getFirstInScoreRange(min *ScoreBorder, max *ScoreBorder) *Node {
	if !list.hasInRange(min, max) {
		return nil
	}
	node := list.header
	for i := list.level - 1; i >= 0; i-- {
		// forward != nil 且 score比min还小 ==> min比score大 则继续找 目的是为了找到最接近min的
		for node.level[i].forward != nil && !min.less(node.level[i].forward.Score) {
			node = node.level[i].forward
		}
	}
	node = node.level[0].forward
	// 找到的第一个节点的 判断它有没有比max大 没有则是目标节点
	if !max.greater(node.Score) {
		return nil
	}
	return node
}

func (list *SkipList) getLastInScoreRange(min *ScoreBorder, max *ScoreBorder) *Node {
	if !list.hasInRange(min, max) {
		return nil
	}
	node := list.header
	for i := list.level - 1; i >= 0; i-- {
		// forward != nil 且 max 比 score还大 则继续找 目的是为了找到最接近max的
		for node.level[i].forward != nil && max.greater(node.level[i].forward.Score) {
			node = node.level[i].forward
		}
	}
	node = node.level[0].forward
	// 找到的最后一个结点 判断它有没有比min大， 如果有 则是目标节点
	if !min.less(node.Score) {
		return nil
	}
	return node
}

func (list *SkipList) RemoveRangeByScore(min *ScoreBorder, max *ScoreBorder) []*Element {
	// 记录删除结点的backward
	update := make([]*Node, maxLevel)
	// 返回删除结点的值
	remove := make([]*Element, 0)
	node := list.header
	// 从最上层开始找
	for i := list.level - 1; i >= 0; i-- {
		for node.level[i].forward != nil {
			// 如果min已经小于node.level[i].forward.Score 则说明node.level[i].Score是 最接近min的结点 且不在删除的范围内
			if min.less(node.level[i].forward.Score) {
				break
			}
			node = node.level[i].forward
		}
		update[i] = node
	}
	// 第一个要被删的结点
	node = node.level[0].forward
	for node != nil {
		// 已经不在范围内了
		if !max.greater(node.Score) {
			break
		}
		next := node.level[0].forward
		removeElement := node.Element
		remove = append(remove, removeElement)
		list.removeNode(node, update)
		node = next
	}
	return remove
}

// RemoveRangeByRank [start, stop)
func (list *SkipList) RemoveRangeByRank(start, stop int64) []*Element {
	remove := make([]*Element, 0)
	if stop <= start {
		return remove
	}
	var count int64 = 0
	update := make([]*Node, maxLevel)
	node := list.header
	for i := list.level - 1; i >= 0; i-- {
		for node.level[i].forward != nil && count+node.level[i].span < start {
			count += node.level[i].span
			node = node.level[i].forward
		}
		update[i] = node
	}
	// 实际上要开始遍历的rank 是count + 1 == start 因为上面的遍历会找到rank = start的结点的backward
	count++
	node = node.level[0].forward
	for node != nil && count < stop {
		next := node.level[0].forward
		removeElement := node.Element
		remove = append(remove, removeElement)
		list.removeNode(node, update)
		node = next
		count++
	}
	return remove
}
