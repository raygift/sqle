package cmd

import (
	"context"
	"fmt"
	"os"
	"time"

	kafkaTopic "github.com/actiontech/sqle/sqle/cmd/scannerd/scanners/kafka"
	"github.com/actiontech/sqle/sqle/cmd/scannerd/scanners/supervisor"
	"github.com/actiontech/sqle/sqle/pkg/scanner"
	"github.com/fatih/color"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

var (
	kfkHost   string
	kfkPort   string
	kfkTopic  string
	kfkGroup  string
	kfkOffset string

	skipErrorKfkMsg   bool
	skipErrorKfkTopic bool
	maxLength         int
)

var kafkaScannerCmd = &cobra.Command{
	Use:   "kafkaTopic",
	Short: "Filter sql from kafka",
	Run: func(cmd *cobra.Command, args []string) {
		kfkparam := &kafkaTopic.KafkaTopicParams{
			Host:   kfkHost,
			Port:   kfkPort,
			Topic:  kfkTopic,
			Group:  kfkGroup,
			Offset: kfkOffset,
		}
		param := &kafkaTopic.Params{
			KfkParams:         kfkparam,
			APName:            rootCmdFlags.auditPlanName,
			SkipErrorKfkMsg:   skipErrorKfkMsg,
			SkipErrorKfkTopic: skipErrorKfkTopic,
			SkipAudit:         skipAudit,
			MaxLength:         maxLength,
			MaxTime:           rootCmdFlags.timeout,
		}
		log := logrus.WithField("scanner", "kafkaTopic")
		client := scanner.NewSQLEClient(time.Second*time.Duration(rootCmdFlags.timeout), rootCmdFlags.host, rootCmdFlags.port).WithToken(rootCmdFlags.token).WithProject(rootCmdFlags.project)
		scanner, err := kafkaTopic.New(param, log, client)
		if err != nil {
			fmt.Println(color.RedString(err.Error()))
			os.Exit(1)
		}

		err = supervisor.Start(context.TODO(), scanner, 30, 1024)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

	},
}

func init() {
	kafkaScannerCmd.Flags().StringVarP(&kfkHost, "kafka broker host", "B", "127.0.0.1", "kafka host")
	kafkaScannerCmd.Flags().StringVarP(&kfkPort, "kafka port", "R", "9092", "kafka port")
	kafkaScannerCmd.Flags().StringVarP(&kfkTopic, "kafka topic", "C", "", "kafka topic")
	kafkaScannerCmd.Flags().StringVarP(&kfkGroup, "kafka groupid", "G", "sqlegroup1", "kafka groupid")
	kafkaScannerCmd.Flags().StringVarP(&kfkOffset, "kafka offset reset type", "O", "latest", "kafka offset reset")
	kafkaScannerCmd.Flags().IntVarP(&maxLength, "kafka cosume msg max length", "L", 10, "kafka max cosume msg length")
	kafkaScannerCmd.Flags().BoolVarP(&skipErrorKfkMsg, "skip-error-kafka-message", "M", false, "skip the kafka message that failed to parse")
	kafkaScannerCmd.Flags().BoolVarP(&skipErrorKfkTopic, "skip-error-kafka-topic", "S", false, "skip the kafka topic that failed to connect")
	kafkaScannerCmd.Flags().BoolVarP(&skipAudit, "skip-kafka-message-audit", "K", false, "only upload sql to sqle, not audit")

	_ = kafkaScannerCmd.MarkFlagRequired("kfkTopic")

	rootCmd.AddCommand(kafkaScannerCmd)

}
