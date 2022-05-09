//
// app.go
// Copyright (C) 2018 YanMing <yming0221@gmail.com>
//
// Distributed under terms of the MIT license.
//

package server

import (
	"github.com/apache/thrift/lib/go/thrift"
	"github.com/pingcap/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/tidwall/redcon"
	"go.uber.org/zap"
	"net/http"
	"sharedis/config"
	"sharedis/sharedis"
	"sharedis/thrift/gen-go/sharestore"
	"strconv"
	"time"
)

type App struct {
	conf *config.Config

	// wrapper and manager for db instance
	tdb *sharedis.Sharedis

	quitCh chan bool

	service *CmdHandler

	server *thrift.TSimpleServer

	mux *redcon.ServeMux
}

type CmdHandler struct {
	// wrapper and manager for db instance
	tdb *sharedis.Sharedis
}


var (
	apiMs = prometheus.NewSummaryVec(
		prometheus.SummaryOpts{
			Name:       "api_ms",
			Help:       "api_ms",
			Objectives: map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.99: 0.001, 0.999: 0.0001, 0.9999: 0.00001, 1.0: 0},
			MaxAge: time.Minute,
		},
		[]string{"api_name", "segment"},
	)

	apiCounterVec = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "api",
			Subsystem: "counter",
			Name: "total",
			Help: "api_counter",
		},
		// We will want to monitor the worker ID that processed the
		// job, and the type of job that was processed
		[]string{"api_name", "segment"},
	)

	apiCounterFailedVec = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "api",
			Subsystem: "counter",
			Name: "failed",
			Help: "api_counter",
		},
		// We will want to monitor the worker ID that processed the
		// job, and the type of job that was processed
		[]string{"api_name", "segment"},
	)

	apiCounterNFVec = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "api",
			Subsystem: "counter",
			Name: "not_found",
			Help: "api_counter",
		},
		// We will want to monitor the worker ID that processed the
		// job, and the type of job that was processed
		[]string{"api_name", "segment"},
	)
)

func init()  {
	prometheus.MustRegister(apiMs)
	prometheus.MustRegister(apiCounterVec)
	prometheus.MustRegister(apiCounterFailedVec)
	prometheus.MustRegister(apiCounterNFVec)
}

// initialize an app
func NewApp(conf *config.Config) *App {
	var err error
	app := &App{
		conf: conf,
	}

	app.tdb, err = sharedis.NewSharedis(conf)
	if err != nil {
		log.Fatal(err.Error())
	}

	app.service = &CmdHandler{ app.tdb }
	app.server, err = NewThriftServer(conf.Backend.ThriftPort,
		conf.Backend.ThriftTimeoutMs, app.service)
	if err != nil {
		log.Fatal(err.Error())
	}

	app.mux = NewServeMux(app.tdb)
	return app
}

func NewThriftServer(port int, timeout int, service *CmdHandler) (*thrift.TSimpleServer, error) {
	conf := &thrift.TConfiguration{
		ConnectTimeout: time.Duration(timeout) * time.Millisecond,
	}

	transport, err := thrift.NewTServerSocket("0.0.0.0:" + strconv.Itoa(port))
	if err != nil {
		return nil, err
	}
	protocolFactory := thrift.NewTBinaryProtocolFactoryConf(conf)
	transportFactory := thrift.NewTBufferedTransportFactory(10000)

	processor := sharestore.NewSharestoreProcessor(service)
	server := thrift.NewTSimpleServer4(processor, transport, transportFactory, protocolFactory)

	return server, nil
}

func StartPrometheus(host string) (error) {
	http.Handle("/metrics", promhttp.Handler())
	err := http.ListenAndServe(host, nil)
	if err != nil {
		return err
	}

	return nil
}

func (app *App) GetSharedis() *sharedis.Sharedis {
	return app.tdb
}

func (app *App) Close() error {
	app.server.Stop()
	return app.tdb.Close()
}

func (app *App) Run() {
	go StartPrometheus(":" + strconv.Itoa(app.conf.Backend.PrometheusPort))
	// accept connections
	go func() {
		err := redcon.ListenAndServe("0.0.0.0:" + strconv.Itoa(app.conf.Backend.RedisPort),
			app.mux.ServeRESP,
			func(conn redcon.Conn) bool {
				// use this function to accept or deny the connection.
				// log.Printf("accept: %s", conn.RemoteAddr())
				return true
			},
			func(conn redcon.Conn, err error) {
				// this is called when the connection has been closed
				// log.Printf("closed: %s, err: %v", conn.RemoteAddr(), err)
			},
		)
		if err != nil {
			log.Fatal("redis conn error", zap.Error(err))
		}
	}()

	if err := app.server.Serve(); err != nil {
		log.Fatal("thrift server failed", zap.Error(err))
		return
	}
}
