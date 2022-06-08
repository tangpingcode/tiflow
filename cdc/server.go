// Copyright 2021 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package cdc

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"

	"github.com/prometheus/client_golang/prometheus"
	"go.etcd.io/etcd/client/pkg/v3/logutil"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"golang.org/x/net/netutil"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"

	"github.com/pingcap/tiflow/cdc/capture"
	"github.com/pingcap/tiflow/cdc/kv"
	"github.com/pingcap/tiflow/cdc/sorter/unified"
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/etcd"
	"github.com/pingcap/tiflow/pkg/fsutil"
	"github.com/pingcap/tiflow/pkg/httputil"
	"github.com/pingcap/tiflow/pkg/p2p"
	"github.com/pingcap/tiflow/pkg/tcpserver"
	p2pProto "github.com/pingcap/tiflow/proto/p2p"
)

const (
	defaultDataDir = "/tmp/cdc_data"
	// dataDirThreshold is used to warn if the free space of the specified data-dir is lower than it, unit is GB
	dataDirThreshold = 500
	// maxHTTPConnection is used to limits the max concurrent connections of http server.
	maxHTTPConnection = 1000
	// httpConnectionTimeout is used to limits a connection max alive time of http server.
	httpConnectionTimeout = 10 * time.Minute
)

// Server is the capture server
// TODO: we need to make Server more unit testable and add more test cases.
// Especially we need to decouple the HTTPServer out of Server.
type Server struct {
	// tpr: CDC 核心服务 capture. 注意它是有状态的.
	capture *capture.Capture
	// tpr: 所有的对外, 对内服务接口都通过同一个 TCP 端口暴露.
	tcpServer tcpserver.TCPServer
	// tpr: 内部通信的 gRPC 接口
	grpcService *p2p.ServerWrapper
	// tpr: 对外的 HTTP 接口
	statusServer *http.Server
	// tpr: 这个 etcdClient 除了 capture 使用外, 仅在一处向后兼容 sort dir 使用.
	etcdClient  *etcd.CDCEtcdClient
	pdEndpoints []string
}

// NewServer creates a Server instance.
func NewServer(pdEndpoints []string) (*Server, error) {
	conf := config.GetGlobalServerConfig()
	log.Info("creating CDC server",
		zap.Strings("pd", pdEndpoints),
		zap.Stringer("config", conf),
	)

	// This is to make communication between nodes possible.
	// In other words, the nodes have to trust each other.
	if len(conf.Security.CertAllowedCN) != 0 {
		err := conf.Security.AddSelfCommonName()
		if err != nil {
			log.Error("status server set tls config failed", zap.Error(err))
			return nil, errors.Trace(err)
		}
	}

	// tcpServer is the unified frontend of the CDC server that serves
	// both RESTful APIs and gRPC APIs.
	// Note that we pass the TLS config to the tcpServer, so there is no need to
	// configure TLS elsewhere.
	tcpServer, err := tcpserver.NewTCPServer(conf.Addr, conf.Security)
	if err != nil {
		return nil, errors.Trace(err)
	}

	s := &Server{
		pdEndpoints: pdEndpoints,
		grpcService: p2p.NewServerWrapper(),
		tcpServer:   tcpServer,
	}

	return s, nil
}

// Run runs the server.
func (s *Server) Run(ctx context.Context) error {
	conf := config.GetGlobalServerConfig()

	grpcTLSOption, err := conf.Security.ToGRPCDialOption()
	if err != nil {
		return errors.Trace(err)
	}

	tlsConfig, err := conf.Security.ToTLSConfig()
	if err != nil {
		return errors.Trace(err)
	}

	logConfig := logutil.DefaultZapLoggerConfig
	logConfig.Level = zap.NewAtomicLevelAt(zapcore.ErrorLevel)

	etcdCli, err := clientv3.New(clientv3.Config{
		Endpoints:   s.pdEndpoints,
		TLS:         tlsConfig,
		Context:     ctx,
		LogConfig:   &logConfig,
		DialTimeout: 5 * time.Second,
		DialOptions: []grpc.DialOption{
			grpcTLSOption,
			grpc.WithBlock(),
			grpc.WithConnectParams(grpc.ConnectParams{
				Backoff: backoff.Config{
					BaseDelay:  time.Second,
					Multiplier: 1.1,
					Jitter:     0.1,
					MaxDelay:   3 * time.Second,
				},
				MinConnectTimeout: 3 * time.Second,
			}),
		},
	})
	if err != nil {
		return errors.Annotate(cerror.WrapError(cerror.ErrNewCaptureFailed, err), "new etcd client")
	}

	cdcEtcdClient := etcd.NewCDCEtcdClient(ctx, etcdCli)
	s.etcdClient = &cdcEtcdClient

	// tpr: 初始化 db sorter 文件路径.
	err = s.initDir(ctx)
	if err != nil {
		return errors.Trace(err)
	}

	// tpr: 初始化 kv worker pool 单例.
	kv.InitWorkerPool()

	// tpr: 创建 capture 实例.
	s.capture = capture.NewCapture(s.pdEndpoints, s.etcdClient, s.grpcService)

	// tpr: 启动 HTTP 服务并注册所有的接口.
	// tpr: 主要包括: swagger, status, Open API, pprof, 具体可以看 RegisterRoutes().
	err = s.startStatusHTTP(s.tcpServer.HTTP1Listener())
	if err != nil {
		return err
	}

	// tpr: 让 server 跑起来, 正常运行时会阻塞.
	return s.run(ctx)
}

// startStatusHTTP starts the HTTP server.
// `lis` is a listener that gives us plain-text HTTP requests.
// TODO: can we decouple the HTTP server from the capture server?
func (s *Server) startStatusHTTP(lis net.Listener) error {
	// LimitListener returns a Listener that accepts at most n simultaneous
	// connections from the provided Listener. Connections that exceed the
	// limit will wait in a queue and no new goroutines will be created until
	// a connection is processed.
	// We use it here to limit the max concurrent conections of statusServer.
	lis = netutil.LimitListener(lis, maxHTTPConnection)
	conf := config.GetGlobalServerConfig()

	// discard gin log output
	gin.DefaultWriter = io.Discard
	router := gin.New()
	// Register APIs.
	RegisterRoutes(router, s.capture, registry)

	// No need to configure TLS because it is already handled by `s.tcpServer`.
	// Add ReadTimeout and WriteTimeout to avoid some abnormal connections never close.
	s.statusServer = &http.Server{
		Handler:      router,
		ReadTimeout:  httpConnectionTimeout,
		WriteTimeout: httpConnectionTimeout,
	}

	go func() {
		log.Info("http server is running", zap.String("addr", conf.Addr))
		err := s.statusServer.Serve(lis)
		if err != nil && err != http.ErrServerClosed {
			log.Error("http server error", zap.Error(cerror.WrapError(cerror.ErrServeHTTP, err)))
		}
	}()
	return nil
}

func (s *Server) etcdHealthChecker(ctx context.Context) error {
	ticker := time.NewTicker(time.Second * 3)
	defer ticker.Stop()
	conf := config.GetGlobalServerConfig()

	httpCli, err := httputil.NewClient(conf.Security)
	if err != nil {
		return err
	}
	defer httpCli.CloseIdleConnections()
	metrics := make(map[string]prometheus.Observer)
	for _, pdEndpoint := range s.pdEndpoints {
		metrics[pdEndpoint] = etcdHealthCheckDuration.WithLabelValues(pdEndpoint)
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			for _, pdEndpoint := range s.pdEndpoints {
				start := time.Now()
				ctx, cancel := context.WithTimeout(ctx, time.Second*10)
				// tpr: q: 这里检查的是 pd 的管理端口, 能保证服务端口可用吗?
				resp, err := httpCli.Get(ctx, fmt.Sprintf("%s/pd/api/v1/health", pdEndpoint))
				if err != nil {
					log.Warn("etcd health check error", zap.Error(err))
				} else {
					_, _ = io.Copy(io.Discard, resp.Body)
					_ = resp.Body.Close()
					metrics[pdEndpoint].Observe(float64(time.Since(start)) / float64(time.Second))
				}
				cancel()
			}
		}
	}
}

func (s *Server) run(ctx context.Context) (err error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	wg, cctx := errgroup.WithContext(ctx)

	// tpr: 注意以下各个启动项没有先后顺序.
	// tpr: 有任何一项返回 err, 会导致 cctx 的 cancel() 被调用, 进而使所有通过wg.Go()
	// tpr: 开启的 goroutine 因为 cctx.cancel() 被调用而退出.
	wg.Go(func() error {
		return s.capture.Run(cctx)
	})

	// tpr: etcd 健康检查. 健康检查失败只会打 Warn 日志而不会退出.
	wg.Go(func() error {
		return s.etcdHealthChecker(cctx)
	})

	// tpr: 运行 sorter pool 单例
	// tpr: IOPool 目前有2个实现
	wg.Go(func() error {
		return unified.RunWorkerPool(cctx)
	})

	// tpr: 运行 kv pool 单例
	wg.Go(func() error {
		return kv.RunWorkerPool(cctx)
	})

	// tpr: tcpServer 中的 cmux 开始提供服务
	wg.Go(func() error {
		return s.tcpServer.Run(cctx)
	})

	// tpr: 如果开启新版 scheduler, 则注册一个 CDCPeerToPeerServer 服务
	conf := config.GetGlobalServerConfig()
	if conf.Debug.EnableNewScheduler {
		grpcServer := grpc.NewServer()
		p2pProto.RegisterCDCPeerToPeerServer(grpcServer, s.grpcService)

		wg.Go(func() error {
			return grpcServer.Serve(s.tcpServer.GrpcListener())
		})
		wg.Go(func() error {
			<-cctx.Done()
			grpcServer.Stop()
			return nil
		})
	}

	return wg.Wait()
}

// Close closes the server.
func (s *Server) Close() {
	if s.capture != nil {
		s.capture.AsyncClose()
	}
	if s.statusServer != nil {
		err := s.statusServer.Close()
		if err != nil {
			log.Error("close status server", zap.Error(err))
		}
		s.statusServer = nil
	}
	if s.tcpServer != nil {
		err := s.tcpServer.Close()
		if err != nil {
			log.Error("close tcp server", zap.Error(err))
		}
		s.tcpServer = nil
	}
}

func (s *Server) initDir(ctx context.Context) error {
	if err := s.setUpDir(ctx); err != nil {
		return errors.Trace(err)
	}
	conf := config.GetGlobalServerConfig()
	// Ensure data dir exists and read-writable.
	diskInfo, err := checkDir(conf.DataDir)
	if err != nil {
		return errors.Trace(err)
	}
	log.Info(fmt.Sprintf("%s is set as data-dir (%dGB available), sort-dir=%s. "+
		"It is recommended that the disk for data-dir at least have %dGB available space",
		conf.DataDir, diskInfo.Avail, conf.Sorter.SortDir, dataDirThreshold))

	// Ensure sorter dir exists and read-writable.
	_, err = checkDir(conf.Sorter.SortDir)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (s *Server) setUpDir(ctx context.Context) error {
	conf := config.GetGlobalServerConfig()
	if conf.DataDir != "" {
		conf.Sorter.SortDir = filepath.Join(conf.DataDir, config.DefaultSortDir)
		config.StoreGlobalServerConfig(conf)

		return nil
	}

	// data-dir will be decided by exist changefeed for backward compatibility
	allInfo, err := s.etcdClient.GetAllChangeFeedInfo(ctx)
	if err != nil {
		return errors.Trace(err)
	}

	candidates := make([]string, 0, len(allInfo))
	for _, info := range allInfo {
		if info.SortDir != "" {
			candidates = append(candidates, info.SortDir)
		}
	}

	conf.DataDir = defaultDataDir
	best, ok := findBestDataDir(candidates)
	if ok {
		conf.DataDir = best
	}

	conf.Sorter.SortDir = filepath.Join(conf.DataDir, config.DefaultSortDir)
	config.StoreGlobalServerConfig(conf)
	return nil
}

func checkDir(dir string) (*fsutil.DiskInfo, error) {
	err := os.MkdirAll(dir, 0o700)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if err := fsutil.IsDirReadWritable(dir); err != nil {
		return nil, errors.Trace(err)
	}
	return fsutil.GetDiskInfo(dir)
}

// try to find the best data dir by rules
// at the moment, only consider available disk space
func findBestDataDir(candidates []string) (result string, ok bool) {
	var low uint64 = 0

	for _, dir := range candidates {
		info, err := checkDir(dir)
		if err != nil {
			log.Warn("check the availability of dir", zap.String("dir", dir), zap.Error(err))
			continue
		}
		if info.Avail > low {
			result = dir
			low = info.Avail
			ok = true
		}
	}

	if !ok && len(candidates) != 0 {
		log.Warn("try to find directory for data-dir failed, use `/tmp/cdc_data` as data-dir", zap.Strings("candidates", candidates))
	}

	return result, ok
}
