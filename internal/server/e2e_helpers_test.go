package server

import "net/http/httptest"

// newHTTPServer mounts a Server on an httptest listener (shared by the
// e2e build).
func newHTTPServer(srv *Server) *httptest.Server { return httptest.NewServer(srv.Handler()) }
