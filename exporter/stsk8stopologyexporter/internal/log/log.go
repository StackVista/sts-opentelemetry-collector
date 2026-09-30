// Package log adapts the agent logging calls used by the vendored collectors to zap.
package log

import (
	"errors"
	"fmt"
	"sync/atomic"

	"go.uber.org/zap"
)

var logger atomic.Pointer[zap.SugaredLogger] //nolint:gochecknoglobals // stands in for the agent's global logger

func init() {
	logger.Store(zap.NewNop().Sugar())
}

// SetLogger routes vendored collector logging to l.
func SetLogger(l *zap.Logger) {
	logger.Store(l.Sugar())
}

// Tracef logs at debug level; zap has no trace level.
func Tracef(format string, params ...interface{}) { logger.Load().Debugf(format, params...) }

// Debugf logs at debug level.
func Debugf(format string, params ...interface{}) { logger.Load().Debugf(format, params...) }

// Infof logs at info level.
func Infof(format string, params ...interface{}) { logger.Load().Infof(format, params...) }

// Warnf logs at warn level and returns the message as an error.
func Warnf(format string, params ...interface{}) error {
	msg := fmt.Sprintf(format, params...)
	logger.Load().Warn(msg)
	return errors.New(msg)
}

// Warnc logs at warn level and returns the message as an error.
func Warnc(message string, context ...interface{}) error {
	logger.Load().Warnw(message, context...)
	return errors.New(message)
}

// Errorf logs at error level and returns the message as an error.
func Errorf(format string, params ...interface{}) error {
	msg := fmt.Sprintf(format, params...)
	logger.Load().Error(msg)
	return errors.New(msg)
}

// Error logs at error level and returns the message as an error.
func Error(v ...interface{}) error {
	msg := fmt.Sprint(v...)
	logger.Load().Error(msg)
	return errors.New(msg)
}
