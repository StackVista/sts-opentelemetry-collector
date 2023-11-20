package logger

import (
	"context"

	"go.uber.org/zap"
)

type ctxKey struct{}

func ZapLogger(ctx context.Context, name string) *zap.Logger {
	return ZapFromCtx(ctx)
}

func ZapFromCtx(ctx context.Context) *zap.Logger {
	ctxValue := ctx.Value(ctxKey{})
	if logger, ok := ctxValue.(*zap.Logger); ok {
		return logger
	}
	return NewZap()
}

func ZapToCtx(ctx context.Context, logger *zap.Logger) context.Context {
	return context.WithValue(ctx, ctxKey{}, logger)
}

func NewZap() *zap.Logger {
	return zap.Must(zap.NewDevelopment())
	// core := zapcore.NewSamplerWithOptions(
	// 	zap.L().Core(),
	// 	1*time.Second,
	// 	10,
	// 	5,
	// )

	// return zap.New(core)
}
