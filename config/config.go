package config

import (
	"github.com/jinzhu/configor"
)

type Config struct {
	Neo4j struct {
		URI      string `required:"true" env:"NEO4J_URI"`
		Username string `required:"true" env:"NEO4J_USERNAME"`
		Password string `required:"true" env:"NEO4J_PASSWORD"`
	}
}

func Load(file string) *Config {
	c := &Config{}
	configor.New(&configor.Config{Silent: true}).Load(c, file)
	return c
}
