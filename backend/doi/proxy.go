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

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/fshttp"
)

func newHttpClient(ctx context.Context, opt *Options) (client *http.Client, err error) {
	client = fshttp.NewClient(ctx)

	proxyUrl, err := url.Parse(opt.ProxyURL)
	if err != nil {
		return nil, err
	}

	// Get the SystemCertPool, continue with an empty pool on error
	rootCAs, _ := x509.SystemCertPool()
	if rootCAs == nil {
		rootCAs = x509.NewCertPool()
	}

	if opt.ProxyCACert != "" {
		fs.Logf(nil, "Adding to root CAs: %s", opt.ProxyCACert)
		// Read in the self-signed certificate
		certs, err := os.ReadFile(opt.ProxyCACert)
		if err != nil {
			return nil, err
		}
		// Add the certificate to the root CAs
		if ok := rootCAs.AppendCertsFromPEM(certs); !ok {
			return nil, fmt.Errorf("could not append self-signed certificate to the CA pool")
		}
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
