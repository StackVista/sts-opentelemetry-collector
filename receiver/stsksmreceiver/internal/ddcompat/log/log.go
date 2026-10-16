// Package log provides the subset of the Datadog agent log API used by the
// vendored KSM code, backed by a zap logger.
package log

import (
	"errors"
	"fmt"
	"sync/atomic"

	"go.uber.org/zap"
)

var logger atomic.Pointer[zap.SugaredLogger]

func init() {
	logger.Store(zap.NewNop().Sugar())
}

// SetLogger replaces the process-wide logger used by the vendored code.
func SetLogger(l *zap.Logger) {
	logger.Store(l.Sugar())
}

func get() *zap.SugaredLogger { return logger.Load() }

// Tracef logs at debug level; zap has no trace level.
func Tracef(format string, params ...any) { get().Debugf(format, params...) }

func Debug(v ...any) { get().Debug(v...) }

func Debugf(format string, params ...any) { get().Debugf(format, params...) }

func Infof(format string, params ...any) { get().Infof(format, params...) }

func Warnf(format string, params ...any) { get().Warnf(format, params...) }

// Errorf logs and returns the formatted message as an error, as the agent API does.
func Errorf(format string, params ...any) error {
	msg := fmt.Sprintf(format, params...)
	get().Error(msg)
	return errors.New(msg)
}

// Error logs and returns the message as an error, as the agent API does.
func Error(v ...any) error {
	msg := fmt.Sprint(v...)
	get().Error(msg)
	return errors.New(msg)
}
