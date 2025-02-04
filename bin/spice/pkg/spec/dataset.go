package spec

import "time"

const (
	REFRESH_MODE_FULL   = "full"
	REFRESH_MODE_APPEND = "append"

	DATA_SOURCE_SPICEAI    = "spice.ai"
	DATA_SOURCE_DREMIO     = "dremio"
	DATA_SOURCE_DATABRICKS = "databricks"
)

type DatasetSpec struct {
	From         string            `json:"from,omitempty" csv:"from" yaml:"from,omitempty"`
	Name         string            `json:"name,omitempty" csv:"name" yaml:"name,omitempty"`
	Description  string            `json:"description,omitempty" csv:"description" yaml:"description,omitempty"`
	Params       map[string]string `json:"params,omitempty" csv:"params" yaml:"params,omitempty"`
	Acceleration *AccelerationSpec `json:"acceleration,omitempty" csv:"acceleration" yaml:"acceleration,omitempty"`
}

type AccelerationSpec struct {
	Enabled         bool              `json:"enabled,omitempty" csv:"enabled" yaml:"enabled,omitempty"`
	Mode            string            `json:"mode,omitempty" csv:"mode" yaml:"mode,omitempty"`
	Engine          string            `json:"engine,omitempty" csv:"engine" yaml:"engine,omitempty"`
	RefreshInterval time.Duration     `json:"refresh_interval,omitempty" csv:"refresh_interval" yaml:"refresh_interval,omitempty"`
	RefreshMode     string            `json:"refresh_mode,omitempty" csv:"refresh_mode" yaml:"refresh_mode,omitempty"`
	Retention       time.Duration     `json:"retention,omitempty" csv:"retention" yaml:"retention,omitempty"`
	Params          map[string]string `json:"params,omitempty" csv:"params" yaml:"params,omitempty"`
	EngineSecret    string            `json:"engine_secret,omitempty" csv:"engine_secret" yaml:"engine_secret,omitempty"`
}
