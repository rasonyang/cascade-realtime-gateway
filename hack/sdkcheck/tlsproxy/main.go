// Command tlsproxy is a self-signed TLS terminator for manual checks: the
// official OpenAI SDKs insist on wss:, Cascade serves plain ws. Listens on
// 127.0.0.1:18443 and forwards to 127.0.0.1:18080.
package main

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"log"
	"math/big"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"time"
)

func main() {
	key, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	tmpl := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "localhost"},
		NotBefore: time.Now(), NotAfter: time.Now().Add(time.Hour), IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}}
	der, _ := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	cert := tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}
	target, _ := url.Parse("http://127.0.0.1:18080")
	srv := &http.Server{Addr: "127.0.0.1:18443", Handler: httputil.NewSingleHostReverseProxy(target),
		TLSConfig: &tls.Config{Certificates: []tls.Certificate{cert}}}
	log.Fatal(srv.ListenAndServeTLS("", ""))
}
