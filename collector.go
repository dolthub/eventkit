package eventkit

import (
	"context"
	"runtime"
	"sync"
	"time"
)

const (
	defaultChanBuffer  = 32
	defaultMaxBatch    = 250
	defaultInitBackoff = time.Second
	defaultMaxBackoff  = time.Minute
)

type collectorConfig struct {
	distinctID  string
	appName     string
	appVersion  string
	platform    string
	maxBatch    int
	initBackoff time.Duration
	maxBackoff  time.Duration
	disabled    func() bool
}

type CollectorOption func(*collectorConfig)

func WithDistinctID(id string) CollectorOption {
	return func(c *collectorConfig) { c.distinctID = id }
}

func WithAppName(name string) CollectorOption {
	return func(c *collectorConfig) { c.appName = name }
}

func WithAppVersion(version string) CollectorOption {
	return func(c *collectorConfig) { c.appVersion = version }
}

func WithPlatform(platform string) CollectorOption {
	return func(c *collectorConfig) { c.platform = platform }
}

func WithMaxBatch(n int) CollectorOption {
	return func(c *collectorConfig) { c.maxBatch = n }
}

func WithBackoff(initial, max time.Duration) CollectorOption {
	return func(c *collectorConfig) {
		c.initBackoff = initial
		c.maxBackoff = max
	}
}

func WithDisabled(f func() bool) CollectorOption {
	return func(c *collectorConfig) { c.disabled = f }
}

type Collector struct {
	cfg   collectorConfig
	wg    sync.WaitGroup
	evtCh chan EventRecord
	st    *sendingThread
}

func NewCollector(emitter Emitter, opts ...CollectorOption) *Collector {
	cfg := collectorConfig{
		platform:    runtime.GOOS,
		maxBatch:    defaultMaxBatch,
		initBackoff: defaultInitBackoff,
		maxBackoff:  defaultMaxBackoff,
		disabled:    func() bool { return false },
	}
	for _, o := range opts {
		o(&cfg)
	}

	c := &Collector{
		cfg:   cfg,
		evtCh: make(chan EventRecord, defaultChanBuffer),
		st:    newSendingThread(cfg, emitter),
	}
	c.st.start()
	c.wg.Add(1)
	go c.drain()
	return c
}

func (c *Collector) drain() {
	defer c.wg.Done()
	var events []EventRecord
	for evt := range c.evtCh {
		events = append(events, evt)
		if len(events) >= c.cfg.maxBatch {
			c.st.batchCh <- events
			events = nil
		}
	}
	if len(events) > 0 {
		c.st.batchCh <- events
	}
}

func (c *Collector) CloseEventAndAdd(evt *Event) {
	if c.cfg.disabled() {
		_ = evt.close()
		return
	}
	c.evtCh <- evt.close()
}

func (c *Collector) Close(ctx context.Context) error {
	close(c.evtCh)
	done := make(chan struct{})
	go func() {
		c.wg.Wait()
		c.st.stop()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

var (
	globalMu        sync.Mutex
	globalCollector *Collector
)

func SetGlobal(c *Collector) {
	globalMu.Lock()
	defer globalMu.Unlock()
	globalCollector = c
}

func Global() *Collector {
	globalMu.Lock()
	defer globalMu.Unlock()
	return globalCollector
}

type sendingThread struct {
	cfg     collectorConfig
	emitter Emitter
	batchCh chan []EventRecord
	unsent  []EventRecord
	wg      sync.WaitGroup
	ctx     context.Context
	cancel  context.CancelFunc
}

func newSendingThread(cfg collectorConfig, emitter Emitter) *sendingThread {
	ctx, cancel := context.WithCancel(context.Background())
	return &sendingThread{
		cfg:     cfg,
		emitter: emitter,
		batchCh: make(chan []EventRecord, 8),
		ctx:     ctx,
		cancel:  cancel,
	}
}

func (s *sendingThread) start() {
	s.wg.Add(1)
	go s.run()
}

func (s *sendingThread) stop() {
	close(s.batchCh)
	s.wg.Wait()
	s.cancel()
}

func (s *sendingThread) run() {
	defer s.wg.Done()

	var timer *time.Timer
	curBackoff := s.cfg.initBackoff

	for {
		var timerCh <-chan time.Time
		if timer != nil {
			timerCh = timer.C
		}
		select {
		case batch, ok := <-s.batchCh:
			if !ok {
				if len(s.unsent) > 0 && !s.cfg.disabled() {
					if err := s.send(s.unsent); err == nil {
						s.unsent = nil
					}
				}
				return
			}
			s.unsent = append(s.unsent, batch...)
			if len(s.unsent) > s.cfg.maxBatch {
				s.unsent = s.unsent[len(s.unsent)-s.cfg.maxBatch:]
			}
			if timer != nil && !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timer = time.NewTimer(0)
		case <-timerCh:
			if s.cfg.disabled() {
				s.unsent = nil
				timer = nil
				curBackoff = s.cfg.initBackoff
				continue
			}
			if err := s.send(s.unsent); err == nil {
				s.unsent = nil
				curBackoff = s.cfg.initBackoff
				timer = nil
			} else {
				timer.Reset(curBackoff)
				curBackoff = nextBackoff(curBackoff, s.cfg.maxBackoff)
			}
		}
	}
}

func (s *sendingThread) send(batch []EventRecord) error {
	req := &LogEventsRequest{
		DistinctID: s.cfg.distinctID,
		AppName:    s.cfg.appName,
		AppVersion: s.cfg.appVersion,
		Platform:   s.cfg.platform,
		Events:     batch,
	}
	return s.emitter.Send(s.ctx, req)
}

func nextBackoff(cur, max time.Duration) time.Duration {
	n := cur * 2
	if n > max {
		return max
	}
	return n
}
