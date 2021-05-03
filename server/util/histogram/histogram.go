package histogram

import (
	"fmt"
	"math"
	"sort"
	"strings"
)

type Options struct {
	// Function to generate per-bucket label.
	// Defaults to a function that returns "$min-$max".
	BucketLabelFormatter func(min int64, max int64) string
	// Defaults to 10 if not specified
	NumBuckets int
	// Maximum width of any bucket. Buckets are scaled linearly to fit this width if any bucket exceeds this value.
	// Defaults to 100.
	MaxWidth int
}

type Histogram struct {
	opts Options
	data []int64
}

func NewWithOptions(opts Options) *Histogram {
	if opts.NumBuckets == 0 {
		opts.NumBuckets = 10
	}
	if opts.MaxWidth == 0 {
		opts.MaxWidth = 100
	}
	if opts.BucketLabelFormatter == nil {
		opts.BucketLabelFormatter = func(min int64, max int64) string {
			return fmt.Sprintf("%d-%d", min, max)
		}
	}
	return &Histogram{opts: opts}
}

func New() *Histogram {
	return NewWithOptions(Options{})
}

func (h *Histogram) Add(v int64) {
	h.data = append(h.data, v)
}

type Percentiles struct {
	P50 int64
	P95 int64
	P99 int64
}

func (h *Histogram) Percentiles() Percentiles {
	sort.Slice(h.data, func(i, j int) bool { return h.data[i] < h.data[j] })

	percentile := func(p float64) int64 {
		if len(h.data) == 0 {
			return 0
		} else if len(h.data) == 1 {
			return h.data[0]
		}
		return h.data[int(float64(len(h.data))*p)-1]
	}
	return Percentiles{
		P50: percentile(0.50),
		P95: percentile(0.95),
		P99: percentile(0.99),
	}
}

func (h *Histogram) String() string {
	if len(h.data) == 0 {
		return "NO DATA\n"
	}

	min := h.data[0]
	max := h.data[0]
	for _, v := range h.data {
		if v < min {
			min = v
		}
		if v > max {
			max = v
		}
	}

	bucketSize := int64(math.Ceil(float64(max-min+1) / float64(h.opts.NumBuckets)))
	if bucketSize == 0 {
		bucketSize = 1
	}

	type bucket struct {
		label string
		count int
		min   int64
		max   int64
	}

	maxLabelWidth := 0
	maxCount := 0
	var buckets []*bucket
	for i := 0; i < h.opts.NumBuckets; i++ {
		bucketMin := min + bucketSize*int64(i)
		bucketMax := bucketMin + bucketSize - 1
		label := h.opts.BucketLabelFormatter(bucketMin, bucketMax)
		buckets = append(buckets, &bucket{
			min:   bucketMin,
			max:   bucketMax,
			label: label,
		})
		if len(label) > maxLabelWidth {
			maxLabelWidth = len(label)
		}
	}

	for _, v := range h.data {
		bucket := int((v - min) / bucketSize)
		buckets[bucket].count++
		if maxCount < buckets[bucket].count {
			maxCount = buckets[bucket].count
		}
	}

	// Scale the count for all buckets if any bucket has count > MaxWidth.
	if maxCount > h.opts.MaxWidth {
		for _, b := range buckets {
			b.count = (b.count * h.opts.MaxWidth) / maxCount
		}
	}

	sb := strings.Builder{}
	for _, b := range buckets {
		sb.WriteString(fmt.Sprintf("%*s", maxLabelWidth, b.label))
		sb.WriteString(": ")
		sb.WriteString(strings.Repeat("|", b.count))
		sb.WriteString("\n")
	}

	percentiles := h.Percentiles()
	sb.WriteString(fmt.Sprintf("\nP50: %d\tP95: %d\tP99: %d\n", percentiles.P50, percentiles.P95, percentiles.P99))

	return sb.String()
}
