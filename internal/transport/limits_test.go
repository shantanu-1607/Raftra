package transport

import (
	"math"
	"testing"
)

func TestLimitsValidate(t *testing.T) {
	tests := []struct {
		name    string
		l       Limits
		wantErr bool
	}{
		{"zero value", Limits{}, false},
		{"playground values", Limits{MaxKeyBytes: 128, MaxValueBytes: 1024, MaxKeys: 10000, WriteRate: 5, WriteBurst: 10}, false},
		{"negative key bytes", Limits{MaxKeyBytes: -1}, true},
		{"negative value bytes", Limits{MaxValueBytes: -1}, true},
		{"negative max keys", Limits{MaxKeys: -1}, true},
		{"negative burst", Limits{WriteBurst: -1}, true},
		{"negative rate", Limits{WriteRate: -0.5}, true},
		{"NaN rate", Limits{WriteRate: math.NaN()}, true},
		{"+Inf rate", Limits{WriteRate: math.Inf(1)}, true},
		{"-Inf rate", Limits{WriteRate: math.Inf(-1)}, true},
		{"refill time exactly 24h", Limits{WriteRate: 1, WriteBurst: 86400}, false},
		{"refill time over 24h", Limits{WriteRate: 1, WriteBurst: 86401}, true},
		{"tiny rate with default burst", Limits{WriteRate: 1e-9}, true},
		{"fractional rate default burst ok", Limits{WriteRate: 0.5}, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.l.Validate()
			if (err != nil) != tc.wantErr {
				t.Fatalf("Validate() error = %v, wantErr %v", err, tc.wantErr)
			}
		})
	}
}
