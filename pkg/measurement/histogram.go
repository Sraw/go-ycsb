// Copyright 2018 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package measurement

import (
	"bytes"
	"fmt"
	"math"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/magiconair/properties"
	"github.com/pingcap/go-ycsb/pkg/ycsb"
)

type histogram struct {
	sync.RWMutex
	boundCounts map[int]int64
	count       int64
	sum         int64
	min         int64
	max         int64
	startTime   time.Time
}

// Metric name.
const (
	COUNT     = "COUNT"
	QPS       = "QPS"
	AVG       = "AVG"
	MIN       = "MIN"
	MAX       = "MAX"
	PER99TH   = "PER99TH"
	PER999TH  = "PER999TH"
	PER9999TH = "PER9999TH"
)

func (h *histogram) Info() ycsb.MeasurementInfo {
	// copy from Summary()
	min := atomic.LoadInt64(&h.min)
	max := atomic.LoadInt64(&h.max)
	sum := atomic.LoadInt64(&h.sum)
	count := atomic.LoadInt64(&h.count)

	bounds := make([]int, len(h.boundCounts))
	var i = 0
	h.RLock()
	for bound := range h.boundCounts {
		bounds[i] = bound
		i += 1
	}
	h.RUnlock()
	sort.Ints(bounds)

	avg := int64(float64(sum) / float64(count))
	per99 := 0
	per999 := 0
	per9999 := 0

	opCount := int64(0)
	for _, bound := range bounds {
		h.RLock()
		_opCount := h.boundCounts[bound]
		h.RUnlock()
		opCount += _opCount
		per := float64(opCount) / float64(count)
		if per99 == 0 && per >= 0.99 {
			per99 = (bound + 1) * 1000
		}

		if per999 == 0 && per >= 0.999 {
			per999 = (bound + 1) * 1000
		}

		if per9999 == 0 && per >= 0.9999 {
			per9999 = (bound + 1) * 1000
		}
	}

	elapsed := time.Now().Sub(h.startTime).Seconds()
	qps := float64(count) / elapsed
	res := make(map[string]interface{})
	res[COUNT] = count
	res[QPS] = qps
	res[AVG] = avg
	res[MIN] = min
	res[MAX] = max
	res[PER99TH] = per99
	res[PER999TH] = per999
	res[PER9999TH] = per9999

	return newHistogramInfo(res)
}

func newHistogram(_ *properties.Properties) *histogram {
	h := new(histogram)
	h.startTime = time.Now()
	h.boundCounts = make(map[int]int64)
	h.min = math.MaxInt64
	h.max = math.MinInt64
	return h
}

func (h *histogram) Measure(latency time.Duration) {
	n := int64(latency / time.Microsecond)

	atomic.AddInt64(&h.sum, n)
	atomic.AddInt64(&h.count, 1)
	bound := int(n/1000)
	h.Lock()
	if _, ok := h.boundCounts[bound]; ok {
    	h.boundCounts[bound] += 1
	} else {
		h.boundCounts[bound] = 1
	}
	h.Unlock()

	for {
		oldMin := atomic.LoadInt64(&h.min)
		if n >= oldMin {
			break
		}

		if atomic.CompareAndSwapInt64(&h.min, oldMin, n) {
			break
		}
	}

	for {
		oldMax := atomic.LoadInt64(&h.max)
		if n <= oldMax {
			break
		}

		if atomic.CompareAndSwapInt64(&h.max, oldMax, n) {
			break
		}
	}
}

func (h *histogram) Summary() string {
	min := atomic.LoadInt64(&h.min)
	max := atomic.LoadInt64(&h.max)
	sum := atomic.LoadInt64(&h.sum)
	count := atomic.LoadInt64(&h.count)

	bounds := make([]int, len(h.boundCounts))
	var i = 0
	h.RLock()
	for bound := range h.boundCounts {
		bounds[i] = bound
		i += 1
	}
	h.RUnlock()
	sort.Ints(bounds)

	avg := int64(float64(sum) / float64(count))
	per99 := 0
	per999 := 0
	per9999 := 0

	opCount := int64(0)
	for _, bound := range bounds {
		h.RLock()
		_opCount := h.boundCounts[bound]
		h.RUnlock()
		opCount += _opCount
		per := float64(opCount) / float64(count)
		if per99 == 0 && per >= 0.99 {
			per99 = (bound + 1) * 1000
		}

		if per999 == 0 && per >= 0.999 {
			per999 = (bound + 1) * 1000
		}

		if per9999 == 0 && per >= 0.9999 {
			per9999 = (bound + 1) * 1000
		}
	}

	elapsed := time.Now().Sub(h.startTime).Seconds()
	qps := float64(count) / elapsed

	buf := new(bytes.Buffer)
	buf.WriteString(fmt.Sprintf("Takes(s): %.1f, ", elapsed))
	buf.WriteString(fmt.Sprintf("Count: %d, ", count))
	buf.WriteString(fmt.Sprintf("OPS: %.1f, ", qps))
	buf.WriteString(fmt.Sprintf("Avg(us): %d, ", avg))
	buf.WriteString(fmt.Sprintf("Min(us): %d, ", min))
	buf.WriteString(fmt.Sprintf("Max(us): %d, ", max))
	buf.WriteString(fmt.Sprintf("99th(us): %d, ", per99))
	buf.WriteString(fmt.Sprintf("99.9th(us): %d, ", per999))
	buf.WriteString(fmt.Sprintf("99.99th(us): %d", per9999))

	return buf.String()
}

type histogramInfo struct {
	info map[string]interface{}
}

func newHistogramInfo(info map[string]interface{}) *histogramInfo {
	return &histogramInfo{info: info}
}

func (hi *histogramInfo) Get(metricName string) interface{} {
	if value, ok := hi.info[metricName]; ok {
		return value
	}
	return nil
}
