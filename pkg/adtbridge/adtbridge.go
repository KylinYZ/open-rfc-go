// Package adtbridge is the RFC-to-HTTP bridge as a library: what
// cmd/adt-rfc-bridge is, for a program that wants to mount it beside other
// things — a launcher that brings up the workbench, this bridge and the DIAG
// stub as one command. It is the public face of internal/rfcserver, which
// stays internal: a caller gets a Backend, an Options and Serve, and nothing
// of the record layer leaks.
//
// One connection is one Eclipse session: its own cookie jar against the
// backend, its own dispatcher with the four function modules an ADT client
// asks for (the REST endpoint and the three that describe it), and the
// conscious loop that answers frames until the client goes.
package adtbridge

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/cookiejar"
	"time"

	"github.com/KylinYZ/open-rfc-go/internal/rfcserver"
)

// Backend is the HTTP origin the ADT requests are made against, with its
// credentials and trust; see LoadBackend for the file that describes one.
type Backend = rfcserver.Backend

// LoadBackend reads a backend description from a JSON file; an empty path
// gives an empty backend for the caller to fill in.
func LoadBackend(path string) (Backend, error) { return rfcserver.LoadBackend(path) }

// Options is everything a bridge needs besides a listener.
type Options struct {
	Backend Backend
	// Timeout bounds one backend request; zero means two minutes.
	Timeout time.Duration
	// Verbose reports every frame decision through Log.
	Verbose bool
	// Log receives the bridge's lines; nil is quiet.
	Log func(format string, args ...any)
	// Dump, when set, receives every frame in both directions — the recorder
	// cmd/adt-rfc-bridge switches on with STG_DUMP. nil records nothing.
	Dump func(peer, dir string, frame []byte)
}

func (o Options) logf(format string, args ...any) {
	if o.Log != nil {
		o.Log(format, args...)
	}
}

// Check says whether this backend can be bridged at all, before anything
// listens: a URL, and a REST handler that accepts it.
func Check(o Options) error {
	if o.Backend.URL == "" {
		return errors.New("adtbridge: no backend URL")
	}
	_, err := rfcserver.ADTRestHandler(o.Backend, nil)
	return err
}

// Serve accepts connections on ln until the context ends or the listener
// fails, and serves each on its own goroutine. Closing the listener is how
// the context stops it, so a caller that owns ln should not also close it
// while Serve runs.
func Serve(ctx context.Context, ln net.Listener, o Options) error {
	if err := Check(o); err != nil {
		return err
	}
	go func() {
		<-ctx.Done()
		ln.Close()
	}()
	for {
		conn, err := ln.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		go ServeConn(conn, o)
	}
}

// ServeConn is one client from connect to gone.
func ServeConn(conn net.Conn, o Options) {
	defer conn.Close()
	peer := conn.RemoteAddr().String()
	o.logf("%s connected", peer)
	jar, err := cookiejar.New(nil)
	if err != nil {
		o.logf("%s: %v", peer, err)
		return
	}
	timeout := o.Timeout
	if timeout == 0 {
		timeout = 2 * time.Minute
	}
	client := &http.Client{Timeout: timeout, Jar: jar}
	if tr, terr := o.Backend.Transport(); terr != nil {
		o.logf("%s: %v", peer, terr)
		return
	} else if tr != nil {
		client.Transport = tr
	}
	handler, err := rfcserver.ADTRestHandler(o.Backend, client)
	if err != nil {
		o.logf("%s: %v", peer, err)
		return
	}
	dispatcher := rfcserver.NewDispatcher()
	dispatcher.Handle("SADT_REST_RFC_ENDPOINT", handler)
	dispatcher.Handle("RFC_GET_FUNCTION_INTERFACE", rfcserver.FunctionInterfaceHandler())
	dispatcher.Handle("DDIF_FIELDINFO_GET", rfcserver.FieldInfoHandler())
	dispatcher.Handle("RFC_GET_STRUCTURE_DEFINITION", rfcserver.StructureDefinitionHandler())
	dispatcher.Identity = o.Backend.LogonIdentity()
	logf := func(string) {}
	if o.Verbose {
		logf = func(s string) { o.logf("%s: %s", peer, s) }
	}
	var dump func(dir string, frame []byte)
	if o.Dump != nil {
		dump = func(dir string, frame []byte) { o.Dump(peer, dir, frame) }
	}
	rfcserver.ServeConscious(conn, dispatcher, logf, dump)
	o.logf("%s gone", peer)
}

// User names who the backend is reached as, for a banner.
func User(b Backend) string {
	if b.User == "" {
		return "an anonymous client"
	}
	return b.User
}
