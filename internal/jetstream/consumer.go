package jetstream

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/bluesky-social/jetstream/pkg/client"
	"github.com/bluesky-social/jetstream/pkg/client/schedulers/sequential"
)

const (
	// DefaultRewind is the cursor safety buffer used on reconnect. Jetstream
	// cursors are time-based, so reconnecting a few seconds before the saved
	// cursor avoids exact-boundary gaps at the cost of harmless duplicate
	// deliveries for idempotent handlers.
	DefaultRewind = 5 * time.Second

	// DefaultReconnectDelay is the pause between failed websocket reads.
	DefaultReconnectDelay = 2 * time.Second
)

// Config configures a reusable jetstream consumer. Use Processor directly when
// you already have events from another source and do not need websocket setup.
type Config struct {
	// WebsocketURL is the jetstream endpoint used by Consumer.
	WebsocketURL string

	// Collections is the set of record collection NSIDs to request from
	// jetstream and process locally. An empty slice means "all collections",
	// matching the upstream client behavior.
	Collections []string

	CursorStore CursorStore
	Handler     Handler
	Logger      *slog.Logger

	// SchedulerIdent names the upstream sequential scheduler. It defaults to
	// "jetstream".
	SchedulerIdent string

	// Rewind controls how far before a saved cursor the Consumer reconnects.
	// Zero uses DefaultRewind.
	Rewind time.Duration

	// ReconnectDelay controls how long the Consumer waits before reconnecting
	// after a failed read loop. Zero uses DefaultReconnectDelay.
	ReconnectDelay time.Duration
}

// Consumer owns the upstream jetstream client and reconnect loop.
type Consumer struct {
	client         *client.Client
	processor      *Processor
	cursorStore    CursorStore
	logger         *slog.Logger
	rewind         time.Duration
	reconnectDelay time.Duration
}

// NewConsumer builds a Consumer. Call Run to enter the blocking reconnect loop,
// or use Start to run it in a background goroutine.
func NewConsumer(cfg Config) (*Consumer, error) {
	if cfg.WebsocketURL == "" {
		return nil, errors.New("websocket URL is required")
	}

	processor := &Processor{
		Collections: cfg.Collections,
		CursorStore: cfg.CursorStore,
		Handler:     cfg.Handler,
		Logger:      cfg.Logger,
	}
	if err := processor.validate(); err != nil {
		return nil, err
	}

	cfg = withDefaults(cfg)
	clientCfg := client.DefaultClientConfig()
	clientCfg.WebsocketURL = cfg.WebsocketURL
	clientCfg.WantedCollections = append([]string(nil), cfg.Collections...)

	c, err := client.NewClient(
		clientCfg,
		cfg.Logger,
		sequential.NewScheduler(cfg.SchedulerIdent, cfg.Logger, processor.HandleEvent),
	)
	if err != nil {
		return nil, fmt.Errorf("new jetstream client: %w", err)
	}

	return &Consumer{
		client:         c,
		processor:      processor,
		cursorStore:    cfg.CursorStore,
		logger:         cfg.Logger,
		rewind:         cfg.Rewind,
		reconnectDelay: cfg.ReconnectDelay,
	}, nil
}

// Start creates a Consumer and runs it in a background goroutine for the
// lifetime of ctx.
func Start(ctx context.Context, cfg Config) (*Consumer, error) {
	c, err := NewConsumer(cfg)
	if err != nil {
		return nil, err
	}
	go c.Run(ctx)
	return c, nil
}

// Run consumes jetstream until ctx is cancelled. Connection failures are logged
// and retried after ReconnectDelay because the websocket is expected to be
// long-lived but not permanent.
func (c *Consumer) Run(ctx context.Context) {
	for {
		if ctx.Err() != nil {
			return
		}

		cur, err := c.cursorStore.LoadCursor(ctx)
		if err != nil {
			c.logger.Warn("ignoring unreadable cursor; resuming from now", "err", err)
			cur = nil
		}

		cursorForLog := cur
		cursorForRead := c.rewindCursor(cur)
		if cursorForRead != nil {
			c.logger.Info("connecting to jetstream",
				"cursor_us", *cursorForLog,
				"rewound_us", *cursorForRead,
				"rewind", c.rewind,
			)
		} else {
			c.logger.Info("connecting to jetstream from now (no cursor)")
		}

		if err := c.client.ConnectAndRead(ctx, cursorForRead); err != nil {
			if ctx.Err() != nil {
				return
			}
			c.logger.Error("jetstream read loop", "err", err)
			select {
			case <-ctx.Done():
				return
			case <-time.After(c.reconnectDelay):
			}
			continue
		}
		if ctx.Err() != nil {
			return
		}
	}
}

// rewindCursor returns the cursor value to hand to jetstream on reconnect.
// Jetstream cursors are microsecond timestamps and exact-boundary replay is not
// guaranteed, so we intentionally resume slightly before the last saved cursor.
// The duplicate window is expected to be safe for idempotent handlers; clamping
// at zero keeps brand-new or very-early cursors valid.
func (c *Consumer) rewindCursor(cur *int64) *int64 {
	if cur == nil {
		return nil
	}
	rewound := *cur - int64(c.rewind/time.Microsecond)
	if rewound < 0 {
		rewound = 0
	}
	return &rewound
}

func withDefaults(cfg Config) Config {
	cfg.Logger = loggerOrDefault(cfg.Logger)
	if cfg.SchedulerIdent == "" {
		cfg.SchedulerIdent = "jetstream"
	}
	if cfg.Rewind == 0 {
		cfg.Rewind = DefaultRewind
	}
	if cfg.ReconnectDelay == 0 {
		cfg.ReconnectDelay = DefaultReconnectDelay
	}
	return cfg
}

func loggerOrDefault(logger *slog.Logger) *slog.Logger {
	if logger != nil {
		return logger
	}
	return slog.Default()
}
