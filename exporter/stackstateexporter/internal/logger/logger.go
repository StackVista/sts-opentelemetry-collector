package logger

import (
	"context"

	"go.uber.org/zap"
)

type ctxKey struct{}

func ZapLogger(ctx context.Context, name string) *zap.Logger {
	return ZapFromCtx(ctx).Named(name)
}

func ZapFromCtx(ctx context.Context) *zap.Logger {
	ctxLogger := ctx.Value(ctxKey{}).(*zap.Logger)
	if ctxLogger == nil {
		return zap.NewNop()
	}
	return ctxLogger
}

func ZapToCtx(ctx context.Context, logger *zap.Logger) context.Context {
	return context.WithValue(ctx, ctxKey{}, logger)
}
