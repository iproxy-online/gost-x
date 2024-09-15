package http

import (
	"bufio"
	"context"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"net"
	"net/http"
	"net/http/httputil"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/asaskevich/govalidator"
	"github.com/go-gost/core/handler"
	"github.com/go-gost/core/limiter"
	"github.com/go-gost/core/limiter/traffic"
	"github.com/go-gost/core/logger"
	md "github.com/go-gost/core/metadata"
	"github.com/go-gost/core/observer/stats"
	"github.com/go-gost/core/recorder"
	xbypass "github.com/go-gost/x/bypass"
	ctxvalue "github.com/go-gost/x/ctx"
	xio "github.com/go-gost/x/internal/io"
	netpkg "github.com/go-gost/x/internal/net"
	limiter_util "github.com/go-gost/x/internal/util/limiter"
	stats_util "github.com/go-gost/x/internal/util/stats"
	rate_limiter "github.com/go-gost/x/limiter/rate"
	traffic_wrapper "github.com/go-gost/x/limiter/traffic/wrapper"
	stats_wrapper "github.com/go-gost/x/observer/stats/wrapper"
	xrecorder "github.com/go-gost/x/recorder"
	"github.com/go-gost/x/registry"
)

func init() {
	registry.HandlerRegistry().Register("http", NewHandler)
}

type httpHandler struct {
	md       metadata
	options  handler.Options
	stats    *stats_util.HandlerStats
	limiter  traffic.TrafficLimiter
	cancel   context.CancelFunc
	recorder recorder.RecorderObject
}

func NewHandler(opts ...handler.Option) handler.Handler {
	options := handler.Options{}
	for _, opt := range opts {
		opt(&options)
	}

	return &httpHandler{
		options: options,
	}
}

func (h *httpHandler) Init(md md.Metadata) error {
	if err := h.parseMetadata(md); err != nil {
		return err
	}

	ctx, cancel := context.WithCancel(context.Background())
	h.cancel = cancel

	if h.options.Observer != nil {
		h.stats = stats_util.NewHandlerStats(h.options.Service)
		go h.observeStats(ctx)
	}

	if limiter := h.options.Limiter; limiter != nil {
		h.limiter = limiter_util.NewCachedTrafficLimiter(limiter, 30*time.Second, 60*time.Second)
	}

	for _, ro := range h.options.Recorders {
		if ro.Record == xrecorder.RecorderServiceHandler {
			h.recorder = ro
			break
		}
	}

	return nil
}

func (h *httpHandler) Handle(ctx context.Context, conn net.Conn, opts ...handler.HandleOption) (err error) {
	defer conn.Close()

	start := time.Now()

	ro := &xrecorder.HandlerRecorderObject{
		Service:    h.options.Service,
		RemoteAddr: conn.RemoteAddr().String(),
		LocalAddr:  conn.LocalAddr().String(),
		Time:       start,
		SID:        string(ctxvalue.SidFromContext(ctx)),
	}

	log := h.options.Logger.WithFields(map[string]any{
		"remote": conn.RemoteAddr().String(),
		"local":  conn.LocalAddr().String(),
		"sid":    ctxvalue.SidFromContext(ctx),
	})
	log.Infof("%s <> %s", conn.RemoteAddr(), conn.LocalAddr())
	defer func() {
		if !ro.Time.IsZero() {
			if err != nil {
				ro.Err = err.Error()
			}
			ro.Duration = time.Since(start)
			ro.Record(ctx, h.recorder.Recorder)
		}

		log.WithFields(map[string]any{
			"duration": time.Since(start),
		}).Infof("%s >< %s", conn.RemoteAddr(), conn.LocalAddr())
	}()

	if !h.checkRateLimit(conn.RemoteAddr()) {
		return rate_limiter.ErrRateLimit
	}

	req, err := http.ReadRequest(bufio.NewReader(conn))
	if err != nil {
		log.Error(err)
		return err
	}
	defer req.Body.Close()

	return h.handleRequest(ctx, conn, req, ro, log)
}

func (h *httpHandler) Close() error {
	if h.cancel != nil {
		h.cancel()
	}
	return nil
}

func (h *httpHandler) handleRequest(ctx context.Context, conn net.Conn, req *http.Request, ro *xrecorder.HandlerRecorderObject, log logger.Logger) error {
	if !req.URL.IsAbs() && govalidator.IsDNSName(req.Host) {
		req.URL.Scheme = "http"
	}

	network := req.Header.Get("X-Gost-Protocol")
	if network != "udp" {
		network = "tcp"
	}
	ro.Network = network

	// Try to get the actual host.
	// Compatible with GOST 2.x.
	if v := req.Header.Get("Gost-Target"); v != "" {
		if h, err := h.decodeServerName(v); err == nil {
			req.Host = h
		}
	}
	if v := req.Header.Get("X-Gost-Target"); v != "" {
		if h, err := h.decodeServerName(v); err == nil {
			req.Host = h
		}
	}

	ro.Host = req.Host
	addr := req.Host
	if _, port, _ := net.SplitHostPort(addr); port == "" {
		addr = net.JoinHostPort(strings.Trim(addr, "[]"), "80")
	}

	fields := map[string]any{
		"dst": addr,
	}

	if u, _, _ := h.basicProxyAuth(req.Header.Get("Proxy-Authorization")); u != "" {
		fields["user"] = u
		ro.Client = u
	}
	log = log.WithFields(fields)

	if log.IsLevelEnabled(logger.TraceLevel) {
		dump, _ := httputil.DumpRequest(req, false)
		log.Trace(string(dump))
	}
	log.Debugf("%s >> %s", conn.RemoteAddr(), addr)

	resp := &http.Response{
		ProtoMajor:    1,
		ProtoMinor:    1,
		Header:        h.md.header,
		ContentLength: -1,
	}
	if resp.Header == nil {
		resp.Header = http.Header{}
	}
	if resp.Header.Get("Proxy-Agent") == "" {
		resp.Header.Set("Proxy-Agent", h.md.proxyAgent)
	}

	ro.HTTP = &xrecorder.HTTPRecorderObject{
		Host:   req.Host,
		Proto:  req.Proto,
		Scheme: req.URL.Scheme,
		Method: req.Method,
		URI:    req.RequestURI,
		Request: xrecorder.HTTPRequestRecorderObject{
			ContentLength: req.ContentLength,
			Header:        req.Header.Clone(),
		},
	}
	defer func() {
		ro.HTTP.StatusCode = resp.StatusCode
		ro.HTTP.Response.Header = resp.Header
	}()

	clientID, ok := h.authenticate(ctx, conn, req, resp, log)
	if !ok {
		return errors.New("authenication failed")
	}
	ctx = ctxvalue.ContextWithClientID(ctx, ctxvalue.ClientID(clientID))

	if h.options.Bypass != nil && h.options.Bypass.Contains(ctx, network, addr) {
		resp.StatusCode = http.StatusForbidden

		if log.IsLevelEnabled(logger.TraceLevel) {
			dump, _ := httputil.DumpResponse(resp, false)
			log.Trace(string(dump))
		}
		log.Debug("bypass: ", addr)
		resp.Write(conn)
		return xbypass.ErrBypass
	}

	if network == "udp" {
		return h.handleUDP(ctx, conn, log)
	}

	if req.Method == "PRI" ||
		(req.Method != http.MethodConnect && req.URL.Scheme != "http") {
		resp.StatusCode = http.StatusBadRequest

		if log.IsLevelEnabled(logger.TraceLevel) {
			dump, _ := httputil.DumpResponse(resp, false)
			log.Trace(string(dump))
		}

		return resp.Write(conn)
	}

	switch h.md.hash {
	case "host":
		ctx = ctxvalue.ContextWithHash(ctx, &ctxvalue.Hash{Source: addr})
	}

	cc, err := h.options.Router.Dial(ctx, network, addr)
	if err != nil {
		resp.StatusCode = http.StatusServiceUnavailable

		if log.IsLevelEnabled(logger.TraceLevel) {
			dump, _ := httputil.DumpResponse(resp, false)
			log.Trace(string(dump))
		}
		resp.Write(conn)
		return err
	}
	defer cc.Close()

	rw := traffic_wrapper.WrapReadWriter(
		h.limiter,
		conn,
		clientID,
		limiter.ScopeOption(limiter.ScopeClient),
		limiter.ServiceOption(h.options.Service),
		limiter.NetworkOption(network),
		limiter.AddrOption(addr),
		limiter.ClientOption(clientID),
		limiter.SrcOption(conn.RemoteAddr().String()),
	)
	if h.options.Observer != nil {
		pstats := h.stats.Stats(clientID)
		pstats.Add(stats.KindTotalConns, 1)
		pstats.Add(stats.KindCurrentConns, 1)
		defer pstats.Add(stats.KindCurrentConns, -1)
		rw = stats_wrapper.WrapReadWriter(rw, pstats)
	}

	if req.Method != http.MethodConnect {
		ro2 := &xrecorder.HandlerRecorderObject{}
		*ro2 = *ro
		ro.Time = time.Time{}

		return h.handleProxy(ctx, xio.NewReadWriteCloser(rw, rw, conn), cc, req, ro2, log)
	}

	resp.StatusCode = http.StatusOK
	resp.Status = "200 Connection established"

	if log.IsLevelEnabled(logger.TraceLevel) {
		dump, _ := httputil.DumpResponse(resp, false)
		log.Trace(string(dump))
	}
	if err = resp.Write(rw); err != nil {
		log.Error(err)
		return err
	}

	start := time.Now()
	log.Infof("%s <-> %s", conn.RemoteAddr(), addr)
	netpkg.Transport(rw, cc)
	log.WithFields(map[string]any{
		"duration": time.Since(start),
	}).Infof("%s >-< %s", conn.RemoteAddr(), addr)

	return nil
}

func (h *httpHandler) handleProxy(ctx context.Context, rw io.ReadWriteCloser, cc io.ReadWriter, req *http.Request, ro *xrecorder.HandlerRecorderObject, log logger.Logger) (err error) {
	roundTrip := func(req *http.Request) error {
		if req == nil {
			return nil
		}

		resp := &http.Response{
			ProtoMajor: req.ProtoMajor,
			ProtoMinor: req.ProtoMinor,
			Header:     http.Header{},
			StatusCode: http.StatusServiceUnavailable,
		}

		start := time.Now()
		ro.Time = start

		if ro.HTTP != nil {
			ro.HTTP = &xrecorder.HTTPRecorderObject{
				Host:       req.Host,
				Proto:      req.Proto,
				Scheme:     req.URL.Scheme,
				Method:     req.Method,
				URI:        req.RequestURI,
				StatusCode: resp.StatusCode,
				Request: xrecorder.HTTPRequestRecorderObject{
					ContentLength: req.ContentLength,
					Header:        req.Header.Clone(),
				},
			}
		}

		// HTTP/1.0
		if req.ProtoMajor == 1 && req.ProtoMinor == 0 {
			if strings.ToLower(req.Header.Get("Connection")) == "keep-alive" {
				req.Header.Del("Connection")
			} else {
				req.Header.Set("Connection", "close")
			}
		}

		req.Header.Del("Proxy-Authorization")
		req.Header.Del("Proxy-Connection")
		req.Header.Del("Gost-Target")
		req.Header.Del("X-Gost-Target")

		if err = req.Write(cc); err != nil {
			resp.Write(rw)

			ro.Duration = time.Since(start)
			ro.Err = err.Error()
			ro.Record(ctx, h.recorder.Recorder)

			return err
		}

		go func() {
			var err error
			var res *http.Response

			defer func() {
				ro.Duration = time.Since(start)
				if err != nil {
					ro.Err = err.Error()
				}
				if res != nil && ro.HTTP != nil {
					ro.HTTP.Response.ContentLength = res.ContentLength
					ro.HTTP.Response.Header = res.Header
					ro.HTTP.StatusCode = res.StatusCode
				}
				ro.Record(ctx, h.recorder.Recorder)
			}()

			res, err = http.ReadResponse(bufio.NewReader(cc), req)
			if err != nil {
				h.options.Logger.Errorf("read response: %v", err)
				resp.Write(rw)
				return
			}
			defer res.Body.Close()

			if log.IsLevelEnabled(logger.TraceLevel) {
				dump, _ := httputil.DumpResponse(res, false)
				log.Trace(string(dump))
			}

			if res.Close {
				defer rw.Close()
			}

			// HTTP/1.0
			if req.ProtoMajor == 1 && req.ProtoMinor == 0 {
				if !res.Close {
					res.Header.Set("Connection", "keep-alive")
				}
				res.ProtoMajor = req.ProtoMajor
				res.ProtoMinor = req.ProtoMinor
			}

			if err = res.Write(rw); err != nil {
				rw.Close()
				log.Errorf("write response: %v", err)
			}
		}()

		return nil
	}

	if err = roundTrip(req); err != nil {
		return err
	}

	for {
		req, err := http.ReadRequest(bufio.NewReader(rw))
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed) {
				return nil
			}
			return err
		}

		if log.IsLevelEnabled(logger.TraceLevel) {
			dump, _ := httputil.DumpRequest(req, false)
			log.Trace(string(dump))
		}

		if err = roundTrip(req); err != nil {
			return err
		}
	}
}

func (h *httpHandler) decodeServerName(s string) (string, error) {
	b, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil {
		return "", err
	}
	if len(b) < 4 {
		return "", errors.New("invalid name")
	}
	v, err := base64.RawURLEncoding.DecodeString(string(b[4:]))
	if err != nil {
		return "", err
	}
	if crc32.ChecksumIEEE(v) != binary.BigEndian.Uint32(b[:4]) {
		return "", errors.New("invalid name")
	}
	return string(v), nil
}

func (h *httpHandler) basicProxyAuth(proxyAuth string) (username, password string, ok bool) {
	if proxyAuth == "" {
		return
	}

	if !strings.HasPrefix(proxyAuth, "Basic ") {
		return
	}
	c, err := base64.StdEncoding.DecodeString(strings.TrimPrefix(proxyAuth, "Basic "))
	if err != nil {
		return
	}
	cs := string(c)
	s := strings.IndexByte(cs, ':')
	if s < 0 {
		return
	}

	return cs[:s], cs[s+1:], true
}

func (h *httpHandler) authenticate(ctx context.Context, conn net.Conn, req *http.Request, resp *http.Response, log logger.Logger) (id string, ok bool) {
	u, p, _ := h.basicProxyAuth(req.Header.Get("Proxy-Authorization"))
	if h.options.Auther == nil {
		return "", true
	}
	if id, ok = h.options.Auther.Authenticate(ctx, u, p); ok {
		return
	}

	pr := h.md.probeResistance
	// probing resistance is enabled, and knocking host is mismatch.
	if pr != nil && (pr.Knock == "" || !strings.EqualFold(req.URL.Hostname(), pr.Knock)) {
		resp.StatusCode = http.StatusServiceUnavailable // default status code

		switch pr.Type {
		case "code":
			resp.StatusCode, _ = strconv.Atoi(pr.Value)
		case "web":
			url := pr.Value
			if !strings.HasPrefix(url, "http") {
				url = "http://" + url
			}
			r, err := http.Get(url)
			if err != nil {
				log.Error(err)
				break
			}
			resp = r
			defer resp.Body.Close()
		case "host":
			cc, err := net.Dial("tcp", pr.Value)
			if err != nil {
				log.Error(err)
				break
			}
			defer cc.Close()

			req.Write(cc)
			netpkg.Transport(conn, cc)
			return
		case "file":
			f, _ := os.Open(pr.Value)
			if f != nil {
				defer f.Close()

				resp.StatusCode = http.StatusOK
				if finfo, _ := f.Stat(); finfo != nil {
					resp.ContentLength = finfo.Size()
				}
				resp.Header.Set("Content-Type", "text/html")
				resp.Body = f
			}
		}
	}

	if resp.Header == nil {
		resp.Header = http.Header{}
	}
	if resp.StatusCode == 0 {
		realm := defaultRealm
		if h.md.authBasicRealm != "" {
			realm = h.md.authBasicRealm
		}
		resp.StatusCode = http.StatusProxyAuthRequired
		resp.Header.Add("Proxy-Authenticate", fmt.Sprintf("Basic realm=\"%s\"", realm))
		if strings.ToLower(req.Header.Get("Proxy-Connection")) == "keep-alive" {
			// XXX libcurl will keep sending auth request in same conn
			// which we don't supported yet.
			resp.Header.Set("Connection", "close")
			resp.Header.Set("Proxy-Connection", "close")
		}

		log.Debug("proxy authentication required")
	} else {
		// resp.Header.Set("Server", "nginx/1.20.1")
		// resp.Header.Set("Date", time.Now().Format(http.TimeFormat))
		if resp.StatusCode == http.StatusOK {
			resp.Header.Set("Connection", "keep-alive")
		}
	}

	if log.IsLevelEnabled(logger.TraceLevel) {
		dump, _ := httputil.DumpResponse(resp, false)
		log.Trace(string(dump))
	}

	resp.Write(conn)
	return
}

func (h *httpHandler) checkRateLimit(addr net.Addr) bool {
	if h.options.RateLimiter == nil {
		return true
	}
	host, _, _ := net.SplitHostPort(addr.String())
	if limiter := h.options.RateLimiter.Limiter(host); limiter != nil {
		return limiter.Allow(1)
	}

	return true
}

func (h *httpHandler) observeStats(ctx context.Context) {
	if h.options.Observer == nil {
		return
	}

	d := h.md.observePeriod
	if d < time.Millisecond {
		d = 5 * time.Second
	}
	ticker := time.NewTicker(d)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			h.options.Observer.Observe(ctx, h.stats.Events())
		case <-ctx.Done():
			return
		}
	}
}
