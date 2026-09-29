package stslogsagentextension

import (
	"errors"
	"net"
)

type Config struct {
	HealthEndpoint string `mapstructure:"health_endpoint"`
}

func (c *Config) Validate() error {
	if _, _, err := net.SplitHostPort(c.HealthEndpoint); err != nil {
		return errors.New("health_endpoint must be host:port")
	}
	return nil
}
