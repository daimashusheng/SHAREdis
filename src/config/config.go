//
// config.go
// Copyright (C) 2018 YanMing <yming0221@gmail.com>
//
// Distributed under terms of the MIT license.
//

package config

import (
	"github.com/BurntSushi/toml"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

type Config struct {
	Desc    string
	Sharedis   sharedisConfig   `toml:"sharedis"`
	Backend backendConfig `toml:"backend"`
}

type sharedisConfig struct {
	LogLevel            string `toml:"loglevel"`
	LogPath             string `toml:"logpath"`
	LogFileSizeMB       int    `toml:"logfilesize"`

}

type backendConfig struct {
	ThriftPort int `toml:"thrift_port"`
	ThriftTimeoutMs int `toml:"thrift_timeout_ms"`
	PrometheusPort int `toml:"prometheus_port"`
	RedisPort int `toml:"redis_port"`
	Pds string
}

func LoadConfig(path string) (*Config, error) {
	var c Config
	if _, err := toml.DecodeFile(path, &c); err != nil {
		log.Error("config file parse failed", zap.Error(err))
		return nil, err
	}
	return &c, nil
}

func NewConfig(c *Config, addr string) *Config {
	if c == nil {
		backend := backendConfig{
			Pds: addr,
		}
		sharedis := sharedisConfig{
			LogFileSizeMB: 300,
		}
		c = &Config{
			Desc:    "new config",
			Sharedis:   sharedis,
			Backend: backend,
		}
	} else {
		// update config load previous
		if addr != "" {
			c.Backend.Pds = addr
		}
	}
	return c
}

// update config fields with default value if not filled
func FillWithDefaultConfig(c *Config) {
}
