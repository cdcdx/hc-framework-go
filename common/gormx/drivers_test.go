package gormx

import "testing"

func TestOpenDriver_Supported(t *testing.T) {
	for _, drv := range []string{"sqlite", "mysql", "postgres"} {
		_, err := OpenDriver(drv, "test://localhost")
		if err != nil {
			t.Errorf("driver %s should be supported: %v", drv, err)
		}
	}
}

func TestOpenDriver_Unsupported(t *testing.T) {
	for _, drv := range []string{"mongodb", "clickhouse", "elasticsearch", "unknown"} {
		_, err := OpenDriver(drv, "test://localhost")
		if err == nil {
			t.Errorf("driver %s should return error", drv)
		}
	}
}
