package http

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/ioutil"
	"log"
	"time"
)

type options struct {
	timeout            time.Duration
	maxIdleConns       int
	insecureSkipVerify bool
	tls                *tls.Config
}

var (
	defaultOptions = options{
		timeout:      5 * time.Second,
		maxIdleConns: 100,
	}
)

// Option ...
type Option interface {
	apply(*options)
}

type funcOption struct {
	f func(*options)
}

func (fo *funcOption) apply(o *options) {
	fo.f(o)
}

func newFuncOption(f func(*options)) *funcOption {
	return &funcOption{
		f: f,
	}
}

// WithTimeout ...
func WithTimeout(timeout time.Duration) Option {
	return newFuncOption(func(o *options) {
		o.timeout = timeout
	})
}

// WithMaxIdleConns ...
func WithMaxIdleConns(maxIdleConns int) Option {
	return newFuncOption(func(o *options) {
		o.maxIdleConns = maxIdleConns
	})
}

// WithInsecureSkipVerify ...
func WithInsecureSkipVerify() Option {
	return newFuncOption(func(o *options) {
		o.insecureSkipVerify = true
	})
}

// WithSSL ...
func WithSSL(keyFile, certFile, trustedCAFile string) Option {
	return newFuncOption(func(o *options) {
		tlsCert, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			log.Fatalln(fmt.Errorf("unable to load certificat files, %s, %s, %v", certFile, keyFile, err))
		}

		tlsCfg := &tls.Config{Certificates: []tls.Certificate{tlsCert}}

		if len(trustedCAFile) > 0 {
			caCert, err := ioutil.ReadFile(trustedCAFile)
			if err != nil {
				log.Fatalln(fmt.Errorf("unable to load root CA file, %s, %v", trustedCAFile, err))
			}
			caCertPool := x509.NewCertPool()
			caCertPool.AppendCertsFromPEM(caCert)
			tlsCfg.RootCAs = caCertPool
		}

		o.tls = tlsCfg
	})
}
