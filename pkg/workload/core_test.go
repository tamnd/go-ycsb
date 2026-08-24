/*
 *
 * core_test.go
 * workload
 *
 * Created by lintao on 2023/7/20 10:49
 * Copyright © 2020-2023 LINTAO. All rights reserved.
 *
 */

package workload

import (
	"github.com/magiconair/properties"
	"testing"
)

func Test_core_buildKeyName(t *testing.T) {

	type args struct {
		keyNum int64
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "buildKeyName",
			args: args{keyNum: 227},
			want: "user6284890712318570100",
		},
		{
			name: "buildKeyName1",
			args: args{keyNum: 154},
			want: "user6284898408899967577",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Built the way a run builds it, through the creator, so
			// the fields buildKeyName reads are the fields a run sets.
			// This used to be a bare `core{p: ...}`, which stopped
			// being enough once the key prefix was read once at
			// construction instead of once per key. The path is
			// relative to this file because a package test runs in its
			// own directory, and reading it from the repository root
			// meant this test could not pass under `go test ./...` at
			// all.
			p := properties.MustLoadFiles([]string{"../../workloads/workloadc"}, properties.UTF8, false)
			w, err := coreCreator{}.Create(p)
			if err != nil {
				t.Fatalf("create: %v", err)
			}
			c := w.(*core)
			if got := c.buildKeyName(tt.args.keyNum); got != tt.want {
				t.Errorf("buildKeyName() = %v, want %v", got, tt.want)
			}
		})
	}
}
