package timewheel

import (
	"container/list"
	"time"

	"go.uber.org/zap"
)

type TimeWheel struct {
	interval       time.Duration        // 每个时间片间隔时间
	ticker         *time.Ticker         // 定时器
	slots          []*list.List         // 循环数组 每个数组元素是一个链表
	timer          map[string]*location // 保存每个task的位置和封装后的list elem
	currentPos     int
	slotNum        int
	addTaskChan    chan task
	removeTaskChan chan string
	stopChan       chan struct{}
}

type location struct {
	slot  int
	etask *list.Element
}

type task struct {
	delay  time.Duration
	circle int
	key    string
	job    func()
}

func New(interval time.Duration, slotNum int) *TimeWheel {
	if interval <= 0 || slotNum <= 0 {
		return nil
	}
	timeWheel := &TimeWheel{
		interval:       interval,
		slots:          make([]*list.List, slotNum),
		timer:          make(map[string]*location),
		currentPos:     0,
		slotNum:        slotNum,
		addTaskChan:    make(chan task),
		removeTaskChan: make(chan string),
		stopChan:       make(chan struct{}),
	}
	timeWheel.initSlots()
	return timeWheel
}

func (tw *TimeWheel) initSlots() {
	if tw == nil {
		return
	}
	for i := 0; i < tw.slotNum; i++ {
		tw.slots[i] = list.New()
	}
}

func (tw *TimeWheel) Start() {
	if tw == nil {
		return
	}
	tw.ticker = time.NewTicker(tw.interval)
	go tw.start()
}

func (tw *TimeWheel) Stop() {
	if tw == nil {
		return
	}
	tw.stopChan <- struct{}{}
}

func (tw *TimeWheel) AddJob(delay time.Duration, key string, job func()) {
	if tw == nil || delay < 0 {
		return
	}
	tw.addTaskChan <- task{
		delay: delay,
		key:   key,
		job:   job,
	}
}

func (tw *TimeWheel) RemoveJob(key string) {
	if tw == nil {
		return
	}
	tw.removeTaskChan <- key
}

func (tw *TimeWheel) start() {
	if tw == nil {
		return
	}
	for {
		select {
		case <-tw.ticker.C:
			tw.tickHandler()
		case task := <-tw.addTaskChan:
			tw.addTask(&task)
		case key := <-tw.removeTaskChan:
			tw.removeTask(key)
		case <-tw.stopChan:
			tw.ticker.Stop()
			return
		}
	}
}

func (tw *TimeWheel) tickHandler() {
	if tw == nil {
		return
	}
	// 当前槽位
	l := tw.slots[tw.currentPos]
	// 下次定时器到来时的位置
	if tw.currentPos == tw.slotNum-1 {
		tw.currentPos = 0
	} else {
		tw.currentPos++
	}
	// 开启协程扫描到期的key
	go tw.scanAndDoTask(l)
}

func (tw *TimeWheel) scanAndDoTask(l *list.List) {
	for e := l.Front(); e != nil; {
		t, ok := e.Value.(task)
		if !ok {
			continue
		}
		if t.circle > 0 {
			t.circle--
			e = e.Next()
			continue
		}
		go func() {
			defer func() {
				if err := recover(); err != nil {
					zap.L().Error("TimeWheel.scanAndDoTask() failed. ", zap.Error(err.(error)))
				}
			}()
			t.job()
		}()
		next := e.Next()
		l.Remove(e)
		delete(tw.timer, t.key)
		e = next
	}
}

func (tw *TimeWheel) addTask(t *task) {
	pos, circle := tw.getPosAndCircle(t.delay)
	t.circle = circle
	e := tw.slots[pos].PushBack(t)
	loc := &location{
		slot:  pos,
		etask: e,
	}
	// 如果已经存在此key 则需要先删除
	_, ok := tw.timer[t.key]
	if ok {
		tw.removeTask(t.key)
	}
	tw.timer[t.key] = loc
}

func (tw *TimeWheel) removeTask(key string) {
	if tw == nil {
		return
	}
	pos, ok := tw.timer[key]
	if !ok {
		return
	}
	l := tw.slots[pos.slot]
	l.Remove(pos.etask)
	delete(tw.timer, key)
}

func (tw *TimeWheel) getPosAndCircle(delay time.Duration) (int, int) {
	delaySeconds := int(delay.Seconds())
	intervalSecond := int(tw.interval.Seconds())
	// 几圈
	circle := delaySeconds / intervalSecond / tw.slotNum
	// 具体位置
	pos := (tw.currentPos + delaySeconds/intervalSecond) % tw.slotNum
	return pos, circle
}
