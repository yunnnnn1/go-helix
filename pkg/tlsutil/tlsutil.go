// Package tlsutil 集中处理内部 gRPC/HTTP 的双向 TLS：加载证书、构造
// *tls.Config，以及为“连接 Agent”的拨号点提供统一的凭据选项。
//
// 默认（config 未启用 TLS）退回到明文，保持向后兼容；启用后强制
// TLS 1.2+ 并校验对端证书。
package tlsutil

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"

	"github.com/yunnnnn1/go-helix/pkg/config"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

// MinTLSVersion 是本项目内部链路允许的最低 TLS 版本。
const MinTLSVersion = tls.VersionTLS12

// loadCA 读取根证书文件并返回证书池。
func loadCA(caFile string) (*x509.CertPool, error) {
	pem, err := os.ReadFile(caFile)
	if err != nil {
		return nil, fmt.Errorf("read ca file %q: %w", caFile, err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		return nil, fmt.Errorf("ca file %q contains no valid certificate", caFile)
	}
	return pool, nil
}

// LoadServerTLS 构造 gRPC/HTTP 服务端的 TLS 配置：加载本端证书与密钥，
// 以 CA 校验客户端证书并要求其出示（mTLS）。
func LoadServerTLS(c config.TLSConfig) (*tls.Config, error) {
	if err := c.Validate(); err != nil {
		return nil, err
	}
	cert, err := tls.LoadX509KeyPair(c.CertFile, c.KeyFile)
	if err != nil {
		return nil, fmt.Errorf("load server key pair: %w", err)
	}
	clientCAs, err := loadCA(c.CAFile)
	if err != nil {
		return nil, err
	}
	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		ClientCAs:    clientCAs,
		ClientAuth:   tls.RequireAndVerifyClientCert,
		MinVersion:   MinTLSVersion,
	}, nil
}

// LoadClientTLS 构造客户端的 TLS 配置：加载本端证书（供服务端校验），
// 并以 CA 校验服务端证书。
func LoadClientTLS(c config.TLSConfig) (*tls.Config, error) {
	if err := c.Validate(); err != nil {
		return nil, err
	}
	cert, err := tls.LoadX509KeyPair(c.CertFile, c.KeyFile)
	if err != nil {
		return nil, fmt.Errorf("load client key pair: %w", err)
	}
	rootCAs, err := loadCA(c.CAFile)
	if err != nil {
		return nil, err
	}
	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		RootCAs:      rootCAs,
		MinVersion:   MinTLSVersion,
	}, nil
}

// ClientDialOption 返回连接 Agent 时使用的 gRPC 传输选项：启用 TLS 时
// 使用 mTLS 凭据，否则使用明文。集中读取 manager.tls 配置，避免每个
// 拨号点各自处理。
func ClientDialOption() (grpc.DialOption, error) {
	cfg := config.Get()
	if cfg == nil || !cfg.Manager.TLS.Enabled {
		return grpc.WithTransportCredentials(insecure.NewCredentials()), nil
	}
	tlsCfg, err := LoadClientTLS(cfg.Manager.TLS)
	if err != nil {
		return nil, fmt.Errorf("build client tls: %w", err)
	}
	return grpc.WithTransportCredentials(credentials.NewTLS(tlsCfg)), nil
}
