// Package conductor connects a worker process to RunnerQ Cloud.
//
// The agent dials out over a WebSocket and answers the Cloud's requests from
// the engine's own storage backend; the Cloud never connects in and never
// holds database credentials. It is off the execution path: activities keep
// running if the Cloud or the agent fails, and it reconnects with backoff.
//
//	engine, _ := runnerq.Builder().Backend(backend).Build()
//	agent, err := conductor.Start(ctx, engine, conductor.Config{
//		URL:    "wss://cloud.runnerq.dev",
//		APIKey: os.Getenv("RUNNERQ_CONDUCTOR_KEY"),
//	})
//	if err != nil { ... }
//	defer agent.Close(context.Background())
//	engine.Start(ctx)
//
// Services that hold a backend themselves (RunnerQ Cloud's data plane)
// answer the same requests with a Handler, without a worker or connection.
//
// The wire protocol is runnerq-spec's conductor protocol
// (protocol/conductor in github.com/runnerq/runnerq-spec).
package conductor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"math/rand/v2"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"

	"github.com/runnerq/runnerq-go"
	"github.com/runnerq/runnerq-go/conductor/internal/wire"
	"github.com/runnerq/runnerq-go/executor"
)

const (
	agentPath        = "/v1/agent"
	handshakeTimeout = 10 * time.Second
	pingInterval     = 20 * time.Second
	writeTimeout     = 10 * time.Second
	maxMessageBytes  = 4 << 20
	// defaultReportInterval applies until the Cloud sets one.
	defaultReportInterval = 15 * time.Second
	// reportMinGap rate-limits change-triggered reports.
	reportMinGap = time.Second
)

// Config configures the agent.
type Config struct {
	// URL is the Cloud gateway, e.g. "wss://cloud.runnerq.dev". /v1/agent is
	// appended when missing; http(s) maps to ws(s).
	URL string
	// APIKey authenticates the app. It is sent only in the Authorization
	// header, never in the URL.
	APIKey string
	// AllowControl lets the Cloud run commands (cancel, retry, run now,
	// reschedule, set priority, delete, signal). Off by default: read-only.
	AllowControl bool
	// MetadataOnly withholds payloads, results, errors and event details
	// whatever mode the Cloud asks for; the Cloud cannot relax it.
	MetadataOnly bool
	// Labels are executor tags (region, deploy version) the Cloud filters and
	// groups by, merged over WorkerConfig.Labels. Prefer WorkerConfig.Labels,
	// which every way of reporting to the Cloud carries.
	Labels map[string]string
	// MaxConcurrentRequests bounds requests served at once (default 16);
	// excess requests get "resource_exhausted" rather than queueing.
	MaxConcurrentRequests int
	// RequestTimeout bounds one request (default 30s).
	RequestTimeout time.Duration
	// MinReconnectDelay and MaxReconnectDelay bound the reconnect backoff
	// (defaults 1s and 30s). Each wait is jittered by ±50%.
	MinReconnectDelay time.Duration
	MaxReconnectDelay time.Duration
	Logger            *slog.Logger
}

func (c *Config) validate() (*url.URL, error) {
	if c.APIKey == "" {
		return nil, errors.New("conductor: Config.APIKey is required")
	}
	u, err := url.Parse(c.URL)
	if err != nil || c.URL == "" {
		return nil, fmt.Errorf("conductor: invalid Config.URL %q", c.URL)
	}
	switch u.Scheme {
	case "ws", "wss":
	case "http":
		u.Scheme = "ws"
	case "https":
		u.Scheme = "wss"
	default:
		return nil, fmt.Errorf("conductor: Config.URL scheme must be ws, wss, http or https, got %q", u.Scheme)
	}
	if !strings.HasSuffix(u.Path, agentPath) {
		u.Path = strings.TrimSuffix(u.Path, "/") + agentPath
	}
	if c.MaxConcurrentRequests <= 0 {
		c.MaxConcurrentRequests = 16
	}
	if c.RequestTimeout <= 0 {
		c.RequestTimeout = 30 * time.Second
	}
	if c.MinReconnectDelay <= 0 {
		c.MinReconnectDelay = time.Second
	}
	if c.MaxReconnectDelay <= 0 {
		c.MaxReconnectDelay = 30 * time.Second
	}
	c.MaxReconnectDelay = max(c.MaxReconnectDelay, c.MinReconnectDelay)
	if c.Logger == nil {
		c.Logger = slog.Default()
	}
	return u, nil
}

// Agent is a running connection to RunnerQ Cloud.
type Agent struct {
	cfg    Config
	url    string
	engine *runnerq.WorkerEngine
	h      *handlers
	table  map[string]handlerFunc
	sem    chan struct{}
	log    *slog.Logger
	cancel context.CancelFunc
	done   chan struct{}

	mu        sync.Mutex
	conn      *websocket.Conn
	sessionID string
	closing   bool

	connected      atomic.Bool
	reportEvery    atomic.Int64
	peerFrameLimit atomic.Int64

	// The session's notices, and whether the Cloud wants them; the engine
	// announces to them only while both are set. Guarded by mu.
	notices     *notices
	wantNotices bool
}

// Start validates cfg and connects in the background, so an unreachable
// Cloud never delays the worker. The agent stops when ctx ends or on Close.
func Start(ctx context.Context, engine *runnerq.WorkerEngine, cfg Config) (*Agent, error) {
	if engine == nil {
		return nil, errors.New("conductor: engine is required")
	}
	u, err := cfg.validate()
	if err != nil {
		return nil, err
	}
	h := newHandlers(engine, cfg.MetadataOnly, cfg.AllowControl)
	ctx, cancel := context.WithCancel(ctx)
	a := &Agent{
		cfg:    cfg,
		url:    u.String(),
		engine: engine,
		h:      h,
		table:  h.table(),
		sem:    make(chan struct{}, cfg.MaxConcurrentRequests),
		log:    cfg.Logger.With("component", "runnerq-conductor"),
		cancel: cancel,
		done:   make(chan struct{}),
	}
	a.reportEvery.Store(int64(defaultReportInterval))
	go a.run(ctx)
	return a, nil
}

// Connected reports whether the agent currently has a Cloud session.
func (a *Agent) Connected() bool { return a.connected.Load() }

// Close says goodbye (so the Cloud records a deploy, not a crash), closes
// the connection and waits for the agent to stop or ctx to end. Call it
// before stopping the engine.
func (a *Agent) Close(ctx context.Context) error {
	a.mu.Lock()
	a.closing = true
	a.mu.Unlock()
	a.goodbye(ctx)
	a.cancel()
	select {
	case <-a.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// goodbye runs once per session: whichever of Close and a cancelled Start
// context gets here first takes the connection.
func (a *Agent) goodbye(ctx context.Context) {
	a.mu.Lock()
	a.closing = true
	conn := a.conn
	a.conn = nil
	a.mu.Unlock()
	if conn != nil {
		sayGoodbye(ctx, conn)
	}
}

func sayGoodbye(ctx context.Context, conn *websocket.Conn) {
	if evt, err := newEvent(wire.TypeGoodbye, wire.Goodbye{Reason: "shutdown"}); err == nil {
		wctx, cancel := context.WithTimeout(ctx, writeTimeout)
		_ = wsjson.Write(wctx, conn, evt)
		cancel()
	}
	_ = conn.Close(websocket.StatusNormalClosure, "shutdown")
}

func (a *Agent) run(ctx context.Context) {
	defer close(a.done)
	delay := a.cfg.MinReconnectDelay
	for {
		start := time.Now()
		err := a.session(ctx)
		if ctx.Err() != nil || a.isClosing() {
			return
		}

		wait := delay
		switch {
		case websocket.CloseStatus(err) == websocket.StatusGoingAway:
			// The gateway node is draining; reconnect to another one promptly.
			wait = a.cfg.MinReconnectDelay
			a.log.Info("Cloud gateway is restarting; reconnecting")
		case time.Since(start) > a.cfg.MaxReconnectDelay:
			// The session was healthy for a while; start the backoff over.
			delay, wait = a.cfg.MinReconnectDelay, a.cfg.MinReconnectDelay
			a.log.Warn("Lost connection to RunnerQ Cloud; reconnecting", "error", err)
		default:
			a.log.Warn("Could not connect to RunnerQ Cloud; retrying", "error", err, "retry_in", wait)
		}
		delay = min(delay*2, a.cfg.MaxReconnectDelay)

		t := time.NewTimer(jitter(wait))
		select {
		case <-ctx.Done():
			t.Stop()
			return
		case <-t.C:
		}
	}
}

func (a *Agent) isClosing() bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.closing
}

// jitter spreads d uniformly over [d/2, 3d/2).
func jitter(d time.Duration) time.Duration {
	return d/2 + rand.N(d)
}

func (a *Agent) session(ctx context.Context) error {
	// Close doesn't cut the handshake short: it waits (up to handshakeTimeout)
	// and says goodbye below, so a clean stop is never recorded as a crash.
	dctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), handshakeTimeout)
	defer cancel()
	conn, res, err := websocket.Dial(dctx, a.url, &websocket.DialOptions{
		HTTPHeader: http.Header{"Authorization": {"Bearer " + a.cfg.APIKey}},
	})
	if err != nil {
		if res != nil && res.StatusCode == http.StatusUnauthorized {
			return errors.New("the Cloud rejected the API key (401)")
		}
		return err
	}
	defer conn.CloseNow()
	conn.SetReadLimit(maxMessageBytes)

	w, err := a.handshake(dctx, conn)
	if err != nil {
		return err
	}
	frame := int64(maxMessageBytes)
	if w.Limits.MaxFrameBytes > 0 {
		frame = min(frame, int64(w.Limits.MaxFrameBytes))
	}
	a.peerFrameLimit.Store(frame)

	a.mu.Lock()
	if a.closing {
		// Close ran during the handshake and found no connection to use.
		a.mu.Unlock()
		sayGoodbye(context.WithoutCancel(ctx), conn)
		return nil
	}
	a.conn, a.sessionID = conn, w.SessionID
	info := a.engine.Snapshot().Info
	sessionNotices := &notices{queue: info.Queue, executor: info.ID}
	// A welcome without notices means off, unlike a config.update.
	a.notices, a.wantNotices = sessionNotices, false
	a.mu.Unlock()
	a.applyConfig(w.Config)
	a.connected.Store(true)
	a.log.Info("Connected to RunnerQ Cloud", "app", w.App.Name, "session", w.SessionID, "metadata_only", a.h.metadataOnly())
	defer func() {
		a.connected.Store(false)
		a.mu.Lock()
		a.conn, a.notices = nil, nil
		a.syncNotices()
		a.mu.Unlock()
	}()

	// The session outlives ctx just long enough to say goodbye: a read whose
	// context ends closes the connection at once, which reads as a crash.
	sctx, stop := context.WithCancel(context.WithoutCancel(ctx))
	defer stop()
	go func() {
		select {
		case <-ctx.Done():
			a.goodbye(context.WithoutCancel(ctx))
		case <-sctx.Done():
		}
	}()
	go a.pingLoop(sctx, conn)
	go a.reportLoop(sctx, conn)
	go sessionNotices.run(sctx, conn, a.peerFrameLimit.Load)
	st := newStreams(a, conn)
	defer st.close()
	return a.readLoop(sctx, conn, st)
}

func (a *Agent) handshake(ctx context.Context, conn *websocket.Conn) (wire.Welcome, error) {
	info := a.engine.Snapshot().Info
	labels := info.Labels
	if len(a.cfg.Labels) > 0 {
		labels = maps.Clone(labels)
		if labels == nil {
			labels = map[string]string{}
		}
		maps.Copy(labels, a.cfg.Labels)
	}
	ex := wire.ExecutorInfo{
		ID:             info.ID,
		Hostname:       info.Hostname,
		Queues:         []string{info.Queue},
		ActivityTypes:  info.ActivityTypes,
		MaxConcurrency: info.MaxConcurrency,
		Labels:         labels,
	}
	if !info.StartedAt.IsZero() {
		ex.StartedAt = ts(info.StartedAt)
	}
	req, err := newRequest("hello", wire.TypeHello, wire.Hello{
		ProtocolVersions: []int{wire.Version},
		SDK:              wire.SDKInfo{Name: info.SDK.Name, Version: info.SDK.Version, Language: info.SDK.Language},
		Executor:         ex,
		Capabilities:     a.h.capabilities(),
		Limits:           wire.Limits{MaxFrameBytes: maxMessageBytes, MaxConcurrentRequests: a.cfg.MaxConcurrentRequests},
	})
	if err != nil {
		return wire.Welcome{}, err
	}
	if err := wsjson.Write(ctx, conn, req); err != nil {
		return wire.Welcome{}, err
	}
	var res wire.Envelope
	if err := wsjson.Read(ctx, conn, &res); err != nil {
		return wire.Welcome{}, err
	}
	if res.Error != nil {
		return wire.Welcome{}, fmt.Errorf("the Cloud rejected the handshake: %w", (*wireError)(res.Error))
	}
	var w wire.Welcome
	if res.Kind != wire.KindResponse || res.Type != wire.TypeHello {
		return w, fmt.Errorf("unexpected handshake reply %s/%s", res.Kind, res.Type)
	}
	if err := json.Unmarshal(res.Data, &w); err != nil {
		return w, fmt.Errorf("decode welcome: %w", err)
	}
	if w.Version != wire.Version {
		return w, fmt.Errorf("the Cloud chose protocol version %d; this SDK speaks %d", w.Version, wire.Version)
	}
	return w, nil
}

// pingLoop closes the connection when a ping goes unanswered (dead gateway).
func (a *Agent) pingLoop(ctx context.Context, conn *websocket.Conn) {
	t := time.NewTicker(pingInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			pctx, cancel := context.WithTimeout(ctx, pingInterval)
			err := conn.Ping(pctx)
			cancel()
			if err != nil && ctx.Err() == nil {
				conn.CloseNow()
				return
			}
		}
	}
}

// applyConfig applies the Cloud's session settings; the Cloud can tighten
// data mode but never relax a local MetadataOnly.
func (a *Agent) applyConfig(c wire.SessionConfig) {
	if c.DataMode != "" {
		a.h.cloudMetadataOnly.Store(c.DataMode == wire.DataModeMetadataOnly)
	}
	if c.ReportIntervalMS > 0 {
		a.reportEvery.Store(int64(max(time.Duration(c.ReportIntervalMS)*time.Millisecond, time.Second)))
	}
	if c.Notices != nil {
		a.mu.Lock()
		a.wantNotices = *c.Notices
		a.syncNotices()
		a.mu.Unlock()
	}
}

// syncNotices points the engine's announcements at the session's notices
// while the Cloud wants them. Call with mu held.
func (a *Agent) syncNotices() {
	if a.wantNotices && a.notices != nil {
		a.engine.Announce(a.notices)
	} else {
		a.engine.Announce(nil)
	}
}

// reportLoop pushes executor.report so dashboards don't poll.
func (a *Agent) reportLoop(ctx context.Context, conn *websocket.Conn) {
	every := func() time.Duration { return time.Duration(a.reportEvery.Load()) }
	executor.Report(ctx, a.engine, every, reportMinGap, func() {
		if evt, err := newEvent(wire.TypeExecutorReport, a.h.state(false)); err == nil {
			wctx, cancel := context.WithTimeout(ctx, writeTimeout)
			_ = wsjson.Write(wctx, conn, evt)
			cancel()
		}
	})
}

func (a *Agent) readLoop(ctx context.Context, conn *websocket.Conn, st *streams) error {
	// Stream requests are bound to this session's connection.
	session := map[string]handlerFunc{}
	if a.h.qs != nil {
		session[wire.TypeEventsSubscribe] = st.subscribe
		session[wire.TypeEventsUnsubscribe] = st.unsubscribe
	}
	var wg sync.WaitGroup
	defer wg.Wait()
	// In-flight requests are abandoned with the connection: nobody is left
	// to read their responses.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	for {
		var req wire.Envelope
		if err := wsjson.Read(ctx, conn, &req); err != nil {
			return err
		}
		if req.Kind == wire.KindEvent {
			if req.Type == wire.TypeConfigUpdate {
				var c wire.SessionConfig
				if json.Unmarshal(req.Data, &c) == nil {
					a.applyConfig(c)
				}
			}
			continue // unknown events are ignored
		}
		if req.Kind != wire.KindRequest {
			continue
		}
		select {
		case a.sem <- struct{}{}:
		default:
			a.reply(ctx, conn, errorResponse(req, errorf(wire.CodeResourceExhausted, "agent is at its request limit")))
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-a.sem }()
			a.reply(ctx, conn, a.serve(ctx, req, session))
		}()
	}
}

// serve recovers a panicking handler as an internal error so it never takes
// down the worker.
func (a *Agent) serve(ctx context.Context, req wire.Envelope, session map[string]handlerFunc) (res wire.Envelope) {
	defer func() {
		if p := recover(); p != nil {
			a.log.Error("Conductor request handler panicked", "type", req.Type, "panic", p)
			res = errorResponse(req, errorf(wire.CodeInternal, "handler panicked"))
		}
	}()
	h, ok := a.table[req.Type]
	if !ok {
		h, ok = session[req.Type]
	}
	if !ok {
		return errorResponse(req, errorf(wire.CodeUnsupported, "this agent does not serve %q", req.Type))
	}
	deadline := time.Now().Add(a.cfg.RequestTimeout)
	if req.Meta != nil && req.Meta.Deadline != "" {
		if d, err := time.Parse(time.RFC3339Nano, req.Meta.Deadline); err == nil && d.Before(deadline) {
			deadline = d
		}
	}
	if !time.Now().Before(deadline) {
		return errorResponse(req, errorf(wire.CodeDeadlineExceeded, "the request expired before it started"))
	}
	rctx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	out, err := h(rctx, req.Data)
	if err != nil {
		return errorResponse(req, toWireError(err))
	}
	data, err := json.Marshal(out)
	if err != nil {
		return errorResponse(req, errorf(wire.CodeInternal, "encode response: %v", err))
	}
	if limit := a.peerFrameLimit.Load(); limit > 0 && int64(len(data)) > limit-1024 {
		return errorResponse(req, errorf(wire.CodeResourceExhausted,
			"the reply is %d bytes, over the %d-byte frame limit; ask for fewer rows or fields", len(data), limit))
	}
	return wire.Envelope{V: req.V, Kind: wire.KindResponse, ID: req.ID, Type: req.Type, Data: data}
}

func (a *Agent) reply(ctx context.Context, conn *websocket.Conn, res wire.Envelope) {
	wctx, cancel := context.WithTimeout(ctx, writeTimeout)
	defer cancel()
	if err := wsjson.Write(wctx, conn, res); err != nil && ctx.Err() == nil {
		a.log.Debug("Could not send Conductor response", "type", res.Type, "error", err)
	}
}

func newRequest(id, msgType string, data any) (wire.Envelope, error) {
	raw, err := json.Marshal(data)
	if err != nil {
		return wire.Envelope{}, err
	}
	return wire.Envelope{V: wire.Version, Kind: wire.KindRequest, ID: id, Type: msgType, Data: raw}, nil
}

func newEvent(msgType string, data any) (wire.Envelope, error) {
	raw, err := json.Marshal(data)
	if err != nil {
		return wire.Envelope{}, err
	}
	return wire.Envelope{V: wire.Version, Kind: wire.KindEvent, Type: msgType, Data: raw}, nil
}

func errorResponse(req wire.Envelope, e *wireError) wire.Envelope {
	return wire.Envelope{V: req.V, Kind: wire.KindResponse, ID: req.ID, Type: req.Type, Error: (*wire.Error)(e)}
}

// SessionID is the current Cloud session id, or "" while disconnected.
func (a *Agent) SessionID() string {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.conn == nil {
		return ""
	}
	return a.sessionID
}
