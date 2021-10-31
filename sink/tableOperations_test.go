package sink

import (
	"testing"
	"time"
)

func Test_tempTable(t *testing.T) {
	type args struct {
		base string
		d    time.Time
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{"1", args{"basetable", time.Date(2021, 10, 30, 9, 16, 1, 1, time.UTC)}, "basetable_202110300916"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tempTable(tt.args.base, tt.args.d); got != tt.want {
				t.Errorf("tempTable() = %v, want %v", got, tt.want)
			}
		})
	}
}
