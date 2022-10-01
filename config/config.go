package config

import (
	"fmt"
	"github.com/fsnotify/fsnotify"

	"github.com/spf13/viper"
	_ "github.com/spf13/viper"
)

var Conf = new(ServerConfig)

// ServerConfig KV Server 配置
type ServerConfig struct {
	Bind              string     `mapstructure:"bind"`
	Port              int        `mapstructure:"port"`
	AppendOnly        bool       `mapstructure:"append_only"`
	AppendFilename    string     `mapstructure:"append_filename"`
	AppendMode        string     `mapstructure:"append_mode"`
	MaxClients        int        `mapstructure:"max_clients"`
	RequirePass       string     `mapstructure:"require_pass"`
	Databases         int        `mapstructure:"databases"`
	RDBFilename       string     `mapstructure:"db_filename"`
	MasterAuth        string     `mapstructure:"master_auth"`
	SlaveAnnouncePort int        `mapstructure:"slave_announce_port"`
	SlaveAnnounceIP   string     `mapstructure:"slave_announce_ip"`
	ReplTimeout       int        `mapstructure:"repl_timeout"`
	Peers             []string   `mapstructure:"peers"`
	Self              string     `mapstructure:"self"`
	DataDictSize      int32      `mapstructure:"data_dict_size"`
	TtlDictSize       int32      `mapstructure:"ttl_dict_size"`
	LockerSize        int32      `mapstructure:"locker_size"`
	LogConfig         *LogConfig `mapstructure:"logger"`
}

// LogConfig ZapLogger配置
type LogConfig struct {
	Mode       string `mapstructure:"mode"`
	Level      string `mapstructure:"level"`
	Filename   string `mapstructure:"filename"`
	MaxSize    int    `mapstructure:"max_size"`
	MaxAge     int    `mapstructure:"max_age"`
	MaxBackups int    `mapstructure:"max_backups"`
}

func Init() error {
	viper.SetConfigFile("config.yaml")
	err := viper.ReadInConfig()
	if err != nil {
		panic(fmt.Errorf("ReadInConfig failed, err: %v", err))
	}
	if err := viper.Unmarshal(Conf); err != nil {
		panic(fmt.Errorf("unmarshal to Conf failed, err:%v", err))
	}
	viper.WatchConfig()
	viper.OnConfigChange(func(in fsnotify.Event) {
		_ = viper.Unmarshal(Conf)
	})
	return err
}
