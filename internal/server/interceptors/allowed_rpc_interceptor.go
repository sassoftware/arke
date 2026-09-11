package interceptors

import (
	"context"
	"fmt"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var allowed_rpcs = map[string]bool{}

func AllowedRPCUnaryInterceptor(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	if len(allowed_rpcs) > 0 && !allowed_rpcs[info.FullMethod] {
		fmt.Printf("blocking %s\n", info.FullMethod)
		return nil, status.Errorf(codes.PermissionDenied, "RPC method not allowed: %s", info.FullMethod)
	}
	return handler(ctx, req)
}

func AllowedRPCStreamInterceptor(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	if len(allowed_rpcs) > 0 && !allowed_rpcs[info.FullMethod] {
		fmt.Printf("blocking %s\n", info.FullMethod)
		return status.Errorf(codes.PermissionDenied, "RPC method not allowed: %s", info.FullMethod)
	}
	return handler(srv, ss)
}

func SetAllowedRPCs(allowedRPCs []string) {
	allowed_rpcs = make(map[string]bool)
	for _, rpc := range allowedRPCs {
		allowed_rpcs[rpc] = true
	}
}
