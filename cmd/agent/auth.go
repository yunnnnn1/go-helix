package main

import (
	"context"

	"github.com/yunnnnn1/go-helix/pkg/config"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func authInterceptor(cfg *config.Config) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		// 若未配置 token，则放行所有请求
		if cfg == nil || cfg.AuthToken == "" {
			return handler(ctx, req)
		}

		md, ok := metadata.FromIncomingContext(ctx)
		if !ok {
			return nil, status.Errorf(codes.Unauthenticated, "metadata is not provided")
		}

		tokens := md["authorization"]
		if len(tokens) == 0 {
			return nil, status.Errorf(codes.Unauthenticated, "authorization token is not provided")
		}

		token := tokens[0]
		// 若存在 "Bearer <token>" 格式则进行处理，否则直接使用原始 token
		if len(token) > 7 && token[:7] == "Bearer " {
			token = token[7:]
		}

		if token != cfg.AuthToken {
			return nil, status.Errorf(codes.Unauthenticated, "invalid token")
		}

		return handler(ctx, req)
	}
}
