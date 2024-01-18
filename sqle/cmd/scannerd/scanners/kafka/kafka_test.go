package kafkaTopic

import (
	"context"
	"testing"

	"github.com/actiontech/sqle/sqle/cmd/scannerd/scanners"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func TestKafkaTopic(t *testing.T) {
	var kParams = &KafkaTopicParams{
		Host:   "192.168.251.1",
		Port:   "9092",
		Topic:  "topic-demo1",
		Group:  "sqle-group01",
		Offset: "earliest",
	}
	params := &Params{
		KfkParams:         kParams,
		MaxLength:         100,
		MaxTime:           10,
		APName:            "abc",
		SkipErrorKfkMsg:   true,
		SkipErrorKfkTopic: false,
		SkipAudit:         false,
	}
	scanner, err := New(params, logrus.New().WithField("test", "test"), nil)
	assert.NoError(t, err)

	err = scanner.Run(context.TODO())
	assert.Error(t, err)

	var sqlCh = scanner.SQLs()
	sqlBuf := []scanners.SQL{}

	for v := range sqlCh {
		sqlBuf = append(sqlBuf, v)
	}
	assert.Len(t, sqlBuf, 10)

}
