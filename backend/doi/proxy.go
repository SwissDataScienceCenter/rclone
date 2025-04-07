// Utilities to use a caching proxy

package doi

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net/http"
	"net/url"
	"os"

	"github.com/rclone/rclone/fs/fshttp"
)

var (
	caCertFile  = "/Users/leafty/projects/github.com/sdsc/renku-squid-cache/self.cert"
	proxyUrlStr = "http://localhost:3128"
)

func newHttpClient(ctx context.Context) (client *http.Client, err error) {
	client = fshttp.NewClient(ctx)

	proxyUrl, err := url.Parse(proxyUrlStr)
	if err != nil {
		return nil, err
	}

	// Get the SystemCertPool, continue with an empty pool on error
	rootCAs, _ := x509.SystemCertPool()
	if rootCAs == nil {
		rootCAs = x509.NewCertPool()
	}

	// Read in the self-signed certificate
	certs, err := os.ReadFile(caCertFile)
	if err != nil {
		return nil, err
	}

	if ok := rootCAs.AppendCertsFromPEM(certs); !ok {
		return nil, fmt.Errorf("could not append self-signed certificate to the CA pool")
	}

	defaultTransport := http.DefaultTransport.(*http.Transport)
	transport := http.Transport{
		Proxy: func(r *http.Request) (*url.URL, error) {
			return proxyUrl, nil
		},
		DialContext:           defaultTransport.DialContext,
		ForceAttemptHTTP2:     defaultTransport.ForceAttemptHTTP2,
		MaxIdleConns:          defaultTransport.MaxIdleConns,
		IdleConnTimeout:       defaultTransport.IdleConnTimeout,
		TLSHandshakeTimeout:   defaultTransport.TLSHandshakeTimeout,
		ExpectContinueTimeout: defaultTransport.ExpectContinueTimeout,
	}
	if transport.TLSClientConfig == nil {
		transport.TLSClientConfig = &tls.Config{}
	}
	transport.TLSClientConfig.RootCAs = rootCAs
	client.Transport = &transport

	return client, nil
}
