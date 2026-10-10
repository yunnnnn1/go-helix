// Copyright (c) 2026 Go-Helix Authors
//
// This file is part of go-helix.
// Licensed under the MIT License. See the LICENSE file in the project root
// for the full license text.

package main

import (
	"context"
	"strings"

	"github.com/yunnnnn1/go-helix/pkg/config"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// authorize 从入站元数据中校验 Bearer Token。未配置 token 时放行所有
// 请求；unary 与 stream 拦截器共用本逻辑。
func authorize(ctx context.Context, cfg *config.Config) error {
	if cfg == nil || cfg.AuthToken == "" {
		return nil
	}

	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return status.Error(codes.Unauthenticated, "metadata is not provided")
	}

	tokens := md["authorization"]
	if len(tokens) == 0 {
		return status.Error(codes.Unauthenticated, "authorization token is not provided")
	}

	token := tokens[0]
	if strings.HasPrefix(token, "Bearer ") {
		token = token[len("Bearer "):]
	}
	if token != cfg.AuthToken {
		return status.Error(codes.Unauthenticated, "invalid token")
	}
	return nil
}

func unaryAuthInterceptor(cfg *config.Config) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		if err := authorize(ctx, cfg); err != nil {
			return nil, err
		}
		return handler(ctx, req)
	}
}

// streamAuthInterceptor 为 ExecStream/FetchFile 等流式 RPC 补齐服务端
// 鉴权（此前只有 unary 被校验）。
func streamAuthInterceptor(cfg *config.Config) grpc.StreamServerInterceptor {
	return func(srv interface{}, ss grpc.ServerStream, _ *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if err := authorize(ss.Context(), cfg); err != nil {
			return err
		}
		return handler(srv, ss)
	}
}
