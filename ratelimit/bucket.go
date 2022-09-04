package ratelimit

import (
	"math"
	"strconv"
	"sync"
	"time"
)

const infinityDuration time.Duration = 0x7fffffffffffffff

type Bucket struct {
	clock Clock

	startTime time.Time

	lock sync.Mutex

	capacity int64

	perTick int64

	interval time.Duration

	availableTokens int64

	lastTick int64

	batch int64
}

const rateMargin float64 = 0.01

func NewBucketWithRateAndClock(rate float64, capacity int64, clock Clock) *Bucket {
	tb := NewBucketWithPerTickAndClock(1, capacity, 1, clock)
	for perTick := int64(1); perTick < 1<<50; perTick = nextPerTick(perTick) {
		interval := time.Duration(1e9 * float64(perTick) / rate)
		if interval < 0 {
			continue
		}
		tb.interval = interval
		tb.perTick = perTick
		if diff := math.Abs(tb.Rate() - rate); diff/rate <= rateMargin {
			return tb
		}
	}
	panic("cannot find suitable perTick for " + strconv.FormatFloat(rate, 'g', -1, 64))
}

func nextPerTick(pt int64) int64 {
	pt1 := pt * 11 / 10
	if pt1 == pt {
		pt1++
	}
	return pt1
}

func NewBucketWithPerTickAndClock(interval time.Duration, capacity, perTick int64, clock Clock) *Bucket {
	if clock == nil {
		clock = realClock{}
	}
	if interval <= 0 {
		panic("token bucket interval is not > 0")
	}
	if capacity <= 0 {
		panic("token bucket capacity is not > 0")
	}
	if perTick <= 0 {
		panic("token bucket perTick is not > 0")
	}
	return &Bucket{
		clock:           clock,
		startTime:       clock.Now(),
		lastTick:        0,
		interval:        interval,
		capacity:        capacity,
		perTick:         perTick,
		availableTokens: capacity,
	}
}

func (tb *Bucket) Capacity() int64 {
	return tb.capacity
}

func (tb *Bucket) Rate() float64 {
	return 1e9 * float64(tb.perTick) / float64(tb.interval)
}

func (tb *Bucket) SetRate(rate float64) {
	tb.lock.Lock()
	defer tb.lock.Unlock()

	for perTick := int64(1); perTick < 1<<50; perTick = nextPerTick(perTick) {
		interval := time.Duration(1e9 * float64(perTick) / rate)
		if interval < 0 {
			continue
		}
		tb.interval = interval
		tb.perTick = perTick
		if diff := math.Abs(tb.Rate() - rate); diff/rate <= rateMargin {
			return
		}
	}
}

func (tb *Bucket) TakeBatch() int64 {

}

func (tb *Bucket) Take(n int64) (time.Duration, bool) {
	return tb.take(tb.clock.Now(), n, infinityDuration)
}

func (tb *Bucket) TakeMaxWait(n int64, maxWait time.Duration) (time.Duration, bool) {
	return tb.take(tb.clock.Now(), n, maxWait)
}

func (tb *Bucket) take(now time.Time, n int64, maxWait time.Duration) (time.Duration, bool) {
	if n <= 0 {
		return 0, true
	}

	tick := tb.curTick(now)
	tb.updateAvailableTokensForTick(tick)
	remain := tb.availableTokens - n
	if remain >= 0 {
		tb.availableTokens = remain //update atomic
		return 0, true
	}
	endTick := tick + (-remain+tb.perTick-1)/tb.perTick
	endTime := tb.startTime.Add(time.Duration(endTick) * tb.interval)
	waitTime := endTime.Sub(now)
	if waitTime > maxWait {
		return 0, false
	}
	tb.availableTokens = remain
	return waitTime, true
}

func (tb *Bucket) curTick(now time.Time) int64 {
	return int64(now.Sub(tb.startTime) / tb.interval)
}

func (tb *Bucket) updateAvailableTokensForTick(tick int64) {
	lastTick := tb.lastTick
	tb.lastTick = tick
	if tb.availableTokens >= tb.capacity {
		return
	}
	tb.availableTokens += (tick - lastTick) * tb.perTick
	if tb.availableTokens > tb.capacity {
		tb.availableTokens = tb.capacity
	}
	return
}

type Clock interface {
	Now() time.Time
	Sleep(d time.Duration)
}

type realClock struct{}

func (realClock) Now() time.Time {
	return time.Now()
}

func (realClock) Sleep(d time.Duration) {
	time.Sleep(d)
}
