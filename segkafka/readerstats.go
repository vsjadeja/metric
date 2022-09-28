package segkafka

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/segmentio/kafka-go"
)

var (
	_ ReaderStatsGetter    = new(kafka.Reader)
	_ prometheus.Collector = new(readerStatsCollector)
)

type ReaderStatsGetter interface {
	Stats() kafka.ReaderStats
}

type readerStatsCollector struct {
	g ReaderStatsGetter
	m readerStatsMetrics
}

func NewReaderStatsCollectorWithLabels(topic string, g ReaderStatsGetter, kv ...string) prometheus.Collector {
	var l prometheus.Labels
	if topic != `` {
		l = prometheus.Labels{`topic`: topic}
	}

	for i := 0; i <= len(kv)-2; i += 2 {
		l[kv[i]] = kv[i+1]
	}

	return CreateReaderStatsCollector(l, g)
}

func NewReaderStatsCollector(topic string, g ReaderStatsGetter) prometheus.Collector {
	var l prometheus.Labels

	if topic != `` {
		l = prometheus.Labels{`topic`: topic}
	}

	return CreateReaderStatsCollector(l, g)
}

func CreateReaderStatsCollector(l prometheus.Labels, g ReaderStatsGetter) prometheus.Collector {
	return &readerStatsCollector{
		g: g,
		m: readerStatsMetrics{
			{
				prometheus.NewDesc(
					readerprefix+`dial_total`,
					`The total number of dial operations.`,
					nil, l,
				),
				func(s *kafka.ReaderStats) float64 { return float64(s.Dials) },
				prometheus.CounterValue,
			},
			{
				prometheus.NewDesc(
					readerprefix+`fetch_total`,
					`The total number of fetch operations.`,
					nil, l,
				),
				func(s *kafka.ReaderStats) float64 { return float64(s.Fetches) },
				prometheus.CounterValue,
			},
			{
				prometheus.NewDesc(
					readerprefix+`message_total`,
					`The total number of messages received from the kafka server.`,
					nil, l,
				),
				func(s *kafka.ReaderStats) float64 { return float64(s.Messages) },
				prometheus.CounterValue,
			},
			{
				prometheus.NewDesc(
					readerprefix+`message_bytes`,
					`The total size of received messages in bytes.`,
					nil, l,
				),
				func(s *kafka.ReaderStats) float64 { return float64(s.Bytes) },
				prometheus.CounterValue,
			},
			{
				prometheus.NewDesc(
					readerprefix+`rebalance_total`,
					`The total number of rebalance operations.`,
					nil, l,
				),
				func(s *kafka.ReaderStats) float64 { return float64(s.Rebalances) },
				prometheus.CounterValue,
			},
			{
				prometheus.NewDesc(
					readerprefix+`timeout_total`,
					`The total number of timeouts.`,
					nil, l,
				),
				func(s *kafka.ReaderStats) float64 { return float64(s.Timeouts) },
				prometheus.CounterValue,
			},
			{
				prometheus.NewDesc(
					readerprefix+`error_total`,
					`The total number of errors.`,
					nil, l,
				),
				func(s *kafka.ReaderStats) float64 { return float64(s.Errors) },
				prometheus.CounterValue,
			},
			{
				prometheus.NewDesc(
					readerprefix+`min_dial_seconds`,
					``,
					nil, l,
				),
				func(s *kafka.ReaderStats) float64 { return float64(s.DialTime.Min) },
				prometheus.GaugeValue,
			},
			{
				prometheus.NewDesc(
					readerprefix+`avg_dial_seconds`,
					``,
					nil, l,
				),
				func(s *kafka.ReaderStats) float64 { return float64(s.DialTime.Avg) },
				prometheus.GaugeValue,
			},
			{
				prometheus.NewDesc(
					readerprefix+`max_dial_seconds`,
					``,
					nil, l,
				),
				func(s *kafka.ReaderStats) float64 { return float64(s.DialTime.Max) },
				prometheus.GaugeValue,
			},
			{
				prometheus.NewDesc(
					readerprefix+`fetch_min_bytes`,
					`Minimum batch size that the consumer will accept.`,
					nil, l,
				),
				func(s *kafka.ReaderStats) float64 { return float64(s.MinBytes) },
				prometheus.GaugeValue,
			},
			{
				prometheus.NewDesc(
					readerprefix+`fetch_max_bytes`,
					`Maximum batch size that the consumer will accept.`,
					nil, l,
				),
				func(s *kafka.ReaderStats) float64 { return float64(s.MaxBytes) },
				prometheus.GaugeValue,
			},
			{
				prometheus.NewDesc(
					readerprefix+`fetch_max_wait_seconds`,
					`Maximum amount of time to wait for new data to come when fetching batches of messages from kafka server.`,
					nil, l,
				),
				func(s *kafka.ReaderStats) float64 { return float64(s.MaxWait) },
				prometheus.GaugeValue,
			},
			{
				prometheus.NewDesc(
					readerprefix+`queue_length`,
					`Number of messages in the message queue.`,
					nil, l,
				),
				func(s *kafka.ReaderStats) float64 { return float64(s.QueueLength) },
				prometheus.GaugeValue,
			},
			{
				prometheus.NewDesc(
					readerprefix+`queue_capacity`,
					`The capacity of the internal message queue.`,
					nil, l,
				),
				func(s *kafka.ReaderStats) float64 { return float64(s.QueueCapacity) },
				prometheus.GaugeValue,
			},
		},
	}
}

// Describe returns all descriptions of the collector.
func (c *readerStatsCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, m := range c.m {
		ch <- m.desc
	}
}

// Collect returns the current state of all metrics of the collector.
func (c *readerStatsCollector) Collect(ch chan<- prometheus.Metric) {
	s := c.g.Stats()

	for _, m := range c.m {
		ch <- prometheus.MustNewConstMetric(m.desc, m.vtyp, m.eval(&s))
	}
}

// readerStatsMetrics provide description, value, and value type for kafka.ReaderStats metrics.
type readerStatsMetrics []struct {
	desc *prometheus.Desc
	eval func(*kafka.ReaderStats) float64
	vtyp prometheus.ValueType
}

const (
	readerprefix = `kafka_reader_`
)
