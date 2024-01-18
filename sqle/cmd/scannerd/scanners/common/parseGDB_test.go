package common

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseGDB(t *testing.T) {
	testCases := []struct {
		input               string
		expectedFingerPrint string
	}{
		{
			"select * from t1 where id = 100;",
			"SELECT * FROM `t1` WHERE `id`=?",
		},
		// {
		// 	"create table t1 (id int,name varchar(255)) distributed by hash(id)(g1,g2)",
		// 	"CREATE TABLE t1 (id int,name varchar(255)) DISTRIBUTED BY HASH(id)(g1,g2)",
		// },
		{
			"SELECT /**/ * FROM `table`",
			"SELECT * FROM `table`",
		}, // 能够解析 带有空注释
	}

	for _, tc := range testCases {
		ns, err := ParseGDB(context.TODO(), tc.input)
		assert.NoError(t, err)
		if len(ns) > 0 {
			assert.Equal(t, tc.expectedFingerPrint, ns[0].Fingerprint)
		}
		if len(ns) == 0 {
			assert.Equal(t, tc.expectedFingerPrint, ``)
		}
	}
}
