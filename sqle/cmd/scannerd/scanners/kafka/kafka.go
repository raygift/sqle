package kafkaTopic

import (
	"context"
	"encoding/json"
	"errors"
	"regexp"
	"time"

	"github.com/actiontech/sqle/sqle/cmd/scannerd/scanners"
	"github.com/actiontech/sqle/sqle/cmd/scannerd/scanners/common"
	driverV2 "github.com/actiontech/sqle/sqle/driver/v2"
	"github.com/actiontech/sqle/sqle/pkg/scanner"
	"github.com/openark/golib/log"
	kafka_go "github.com/segmentio/kafka-go"
	"github.com/sirupsen/logrus"
)

const (
	MatchedStrLength    = 10
	SqlCostSqlStrIdx    = 9
	SqlCostCostValueIdx = 8
	SqlCostTimeStampIdx = 1
)

type KafkaScanner struct {
	l *logrus.Entry
	c *scanner.Client

	kfkParams *KafkaTopicParams

	sqls []scanners.SQL

	allSQL    []driverV2.Node
	getAll    chan struct{}
	maxLength int
	maxTime   int
	apName    string

	skipErrorKfkMsg   bool
	skipErrorKfkTopic bool

	skipAudit bool
}

type Params struct {
	KfkParams *KafkaTopicParams

	APName            string
	SkipErrorKfkMsg   bool
	SkipErrorKfkTopic bool
	SkipAudit         bool

	MaxLength int
	MaxTime   int
}

type KafkaTopicParams struct {
	Host   string
	Port   string
	Topic  string
	Group  string
	Offset string
}

func New(params *Params, l *logrus.Entry, c *scanner.Client) (*KafkaScanner, error) {
	return &KafkaScanner{
		kfkParams:         params.KfkParams,
		maxLength:         params.MaxLength,
		maxTime:           params.MaxTime,
		apName:            params.APName,
		skipErrorKfkMsg:   params.SkipErrorKfkMsg,
		skipErrorKfkTopic: params.SkipErrorKfkTopic,
		skipAudit:         params.SkipAudit,
		l:                 l,
		c:                 c,
		getAll:            make(chan struct{}),
	}, nil
}

func (kfk *KafkaScanner) Run(ctx context.Context) error {
	cancelCtx, cancel := context.WithTimeout(context.Background(), time.Duration(kfk.maxTime)*time.Second)
	defer cancel()
	logrus.StandardLogger().Infoln("kafkaTopic scanner running")

	sqls, err := GetSQLFromKafkaTopic(cancelCtx, kfk.kfkParams, kfk.skipErrorKfkMsg, kfk.skipErrorKfkTopic, kfk.maxLength)
	if len(sqls) == 0 && err != nil {
		logrus.StandardLogger().Errorf("kafkaTopic scanner GetSQLFromKafkaTopic err:%v\n", err)

		return err
	}

	kfk.allSQL = sqls
	close(kfk.getAll)

	<-ctx.Done()
	return nil
}

func (kfk *KafkaScanner) SQLs() <-chan scanners.SQL {
	// todo: channel size configurable
	sqlCh := make(chan scanners.SQL, kfk.maxLength)
	logrus.StandardLogger().Infoln("kafkaTopic scanner got sqls")

	go func() {
		<-kfk.getAll
		for _, sql := range kfk.allSQL {
			sqlCh <- scanners.SQL{
				Fingerprint: sql.Fingerprint,
				RawText:     sql.Text,
			}
		}
		close(sqlCh)
	}()
	return sqlCh
}

func (kfk *KafkaScanner) Upload(ctx context.Context, sqls []scanners.SQL) error {
	logrus.StandardLogger().Infoln("kafkaTopic scanner upload sqls")

	kfk.sqls = append(kfk.sqls, sqls...)
	err := common.Upload(ctx, kfk.sqls, kfk.c, kfk.apName)
	if err != nil {
		return err
	}

	if kfk.skipAudit {
		return nil
	}

	return common.Audit(kfk.c, kfk.apName)
}

type KafksMsgValue struct {
	Value     string `json:"value"`
	RowNumber int    `json:"rowNumber"`
	Fd        int64  `json:"fd"`
}

type KafkaMsg struct {
	CloudId    int             `json:"cloudId"`
	FilePath   string          `json:"filePath"`
	Id         string          `json:"id"`
	Ip         string          `json:"ip"`
	LogType    string          `json:"logType"`
	Timestring string          `json:"time"`
	Tms        int64           `json:"tms"`
	Value      []KafksMsgValue `json:"value"`
}

// "filePath": "/data/goldendb/pfdbproxy1/log/slow_query.log",
func (km *KafkaMsg) FilterSlowQueryLog() error {
	re, err := regexp.Compile("slow_query.log")
	if err != nil {
		panic(err.Error())
	}
	// 只保留含有"slow_query.log" 关键字的日志条目
	found := re.MatchString(km.FilePath)
	if !found {
		logrus.StandardLogger().Infof("kafka msg is not slow_query log:%s\n", km.FilePath)
		return errors.New("kafka msg is not slow_query log")
	}
	return nil
}

// 由于 kafka msg 经过 json.Unmarshal() 解析时，会丢失日志里的 \n 特殊字符，
// 从而导致 SQLExtract 时正则表达式无法匹配 \nTranID 和 \ndigest 规则
// 因此需要在 unmarshal 之前，将 kafka msg 中 \n 替换为 \\n，达到保留 \n 字符的目的
func FixEscapeCharacterForJsonUnmarshal(originStr string) string {
	reg := regexp.MustCompile(`(\\n)`)
	return reg.ReplaceAllString(originStr, `\$0`)
}

// 根据`TotalExecTime`将原始慢日志过滤后只剩下 CN 上的 slow_query.log 日志内容
func MessageFilter(originStr []byte) ([]byte, error) {
	if len(originStr) == 0 {
		return []byte(""), errors.New("originStr empty")
	}
	re, err := regexp.Compile(`TotalExecTime`)
	if err != nil {
		panic(err.Error())
	}
	// 只保留含有"TotalExecTime" 关键字的日志条目
	found := re.MatchString(string(originStr))
	if !found {
		return []byte(""), errors.New("string not match 'TotalExecTime'")
	}

	return originStr, nil
}

func MatchSQLStr(originStr []byte) ([][]byte, error) {
	re, err := regexp.Compile(`\[(.+)\]\|DEBUG\|\|.+\:Port\[.*\]Session\[.*\]TransSerial\[.*\]LinkIP\[(.+)\]LinkPort\[(\d+)\]UserName\[(.+)\]ProxyName\[(.+)\]ClusterName\[(.+)\]Database\[(.+)\]TotalExecTime\[(\d+)us\]BeginTs\[.*\]EndTs\[.*\]SQL\[(.+)\]\\nTraceID\[.+\]\\ndigest\[.+\]\\nMsgToExecTime\[.+\]`) //
	if err != nil {
		panic(err.Error())
	}
	matchedStr := re.FindSubmatch([]byte(originStr))
	if len(matchedStr) == MatchedStrLength {
		return matchedStr, nil
	} else {
		re, err = regexp.Compile(`\[(.+)\]\|DEBUG\|\|.+\:Port\[.*\]Session\[.*\]TransSerial\[.*\]LinkIP\[(.+)\]LinkPort\[(\d+)\]UserName\[(.+)\]ProxyName\[(.+)\]ClusterName\[(.+)\]Database\[(.+)\]TotalExecTime\[(\d+)us\]BeginTs\[.*\]EndTs\[.*\]SQL\[(.+)\]\\ndigest\[.+\]\\nMsgToExecTime\[.+\]`) //
		if err != nil {
			panic(err.Error())
		}
		matchedStr = re.FindSubmatch([]byte(originStr))
		if len(matchedStr) == MatchedStrLength {
			return matchedStr, nil
		}
	}
	return [][]byte{}, errors.New("Match SQL Failed")
}

func SQLExtract(SlowLogStr []byte) (allSQL []driverV2.Node, err error) {

	matchedStr, err := MatchSQLStr(SlowLogStr)
	if err != nil {
		return nil, err
	}
	// 得到SQL 字符串，替换字符串中的 \n \t
	replaceRegexp := regexp.MustCompile(`\\n|\\t`)
	rawSQLStr := replaceRegexp.ReplaceAllString(string(matchedStr[SqlCostSqlStrIdx]), " ")
	sql, err := common.ParseGDB(context.TODO(), rawSQLStr)

	if err != nil {
		return nil, err
	}

	allSQL = append(allSQL, sql...)

	return allSQL, nil

}

func GetSQLFromKafkaTopic(ctx context.Context,
	p *KafkaTopicParams,
	skipErrorKfkMsg,
	skipErrorKfkTopic bool,
	maxLimit int) (allSQL []driverV2.Node, err error) {
	brokers := make([]string, 0)
	brokers = append(brokers, p.Host+":"+p.Port)
	reader := kafka_go.NewReader(kafka_go.ReaderConfig{
		Brokers: brokers,
		GroupID: p.Group,
		Topic:   p.Topic,
		// MinBytes: 10e3, // 10KB
		MaxBytes: 10e6, // 10MB
	})
	defer reader.Close()
	logrus.StandardLogger().Debugf("GetSQLFromKafkaTopic new reader %v\n", reader)

	// // connect kafka topic
	// // conf := make(map[string]kafka.ConfigValue)
	// conf := &kafka.ConfigMap{
	// 	"bootstrap.servers": p.Host + ":" + p.Port,
	// 	"group.id":          p.Group,
	// 	"auto.offset.reset": p.Offset,
	// }
	// topic := p.Topic //"applog_pt"

	// c, err := kafka.NewConsumer(conf)
	// if err != nil {
	// 	log.Errorf("subscribe kafka topic failed: %s\n", err)
	// 	panic(err.Error())
	// }

	// err = c.SubscribeTopics([]string{topic}, nil)
	// if err != nil {
	// 	log.Errorf("subscribe kafka topic failed: %s\n", err)
	// 	panic(err.Error())
	// }

	// // consume kafka topic message
	// ev := &kafka.Message{}
	run := true
	logCount := 0
	// 读取的sql 达到上限、读取超时、读取到 kafka.Message 之外的消息，
	// 都可能结束消费 kafka
	for run && maxLimit > 0 {
		// select {
		// case <-ctx.Done():
		// 	logrus.StandardLogger().Infof("Kafka Consumer Ctx Done, got kafka msg:%d\n", logCount)

		log.Infof("got kafka msg:%d\n", logCount)
		// 	run = false

		// default:
		m, err := reader.ReadMessage(context.Background())
		if err != nil {
			logrus.StandardLogger().Infof("Kafka reader ReadMessage error:%v\n", err)
			run = false
		}
		// fmt.Printf("message at topic:%v partition:%v offset:%v	%s = %s\n", m.Topic, m.Partition, m.Offset, string(m.Key), string(m.Value))

		// event := c.Poll(500) // * time.Millisecond
		// switch e := event.(type) {
		// case *kafka.Message:
		// 	if e.TopicPartition.Error != nil {
		// 		log.Errorf("Kafka Consumer poll topic partition error : %s\n", e.TopicPartition.Error)
		// 		if skipErrorKfkTopic {
		// 			continue
		// 		} else {
		// 			run = false
		// 		}
		// 	}
		// 	ev = e
		logCount++
		// case kafka.Error:
		// default:
		// 	log.Debugf("Kafka Consumer poll kafka.Error or other: %+v\n", e)
		// 	if skipErrorKfkTopic {
		// 		continue
		// 	} else {
		// 		run = false
		// 	}
		// }
		logrus.StandardLogger().Infof("message at topic:%v partition:%v offset:%v	%s = %s\n", m.Topic, m.Partition, m.Offset, string(m.Key), string(m.Value))
		if len(m.Value) == 0 {
			if skipErrorKfkMsg {
				continue
			} else {
				run = false
			}
		}
		fixMsg := FixEscapeCharacterForJsonUnmarshal(string(m.Value))
		kfkMsg := KafkaMsg{}
		err = json.Unmarshal([]byte(fixMsg), &kfkMsg)
		if err != nil {
			logrus.StandardLogger().Infof("unmarshal kafka msg err, looping continue with skipErrorKfkMsg:%s %v\n", err, skipErrorKfkMsg)

			if skipErrorKfkMsg {
				continue
			} else {
				run = false
			}
		}
		if err = kfkMsg.FilterSlowQueryLog(); err != nil {
			// msg is not entry of slow_query.log
			// 从kafka 读取到了 非 gdb slow_query.log 日志数据
			// log.Debugf("FilterSlowQueryLog err: %s %s\n", err, kfkMsg.FilePath)
			continue
		}

		for _, msg := range kfkMsg.Value {
			slow_query_log, err := MessageFilter([]byte(msg.Value))
			if err != nil {
				if skipErrorKfkMsg {
					logrus.StandardLogger().Infof("other kafka msg, filter out:%s\n", err)
					continue
				} else {
					run = false
				}
			}
			maxLimit--
			sqls, err := SQLExtract(slow_query_log)
			logrus.StandardLogger().Infof("sql extract success:%v\n", sqls)

			allSQL = append(allSQL, sqls...)
		}

	}
	// }
	err = reader.Close()
	return allSQL, err
}
