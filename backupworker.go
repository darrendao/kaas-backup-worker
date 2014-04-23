package main

import (
	"fmt"
	"github.com/benmanns/goworker"
	configfile "github.com/crowdmob/goconfig"
	"github.com/darrendao/kafka-go-have-fun/s3backup"
	"github.com/darrendao/kafka-go-have-fun/s3replay"
	"strings"
	"time"
)

func init() {
	goworker.Register("BackupCluster", backupWorker)
	goworker.Register("ReplayClusterTopicPartition", replayWorker)
}

// args: clusterid, hosts, topics to backup
func backupWorker(queue string, args ...interface{}) error {
	configFilename := "consumer.properties"
	config, err := configfile.ReadConfigFile(configFilename)
	if err != nil {
		fmt.Printf("Couldn't read config file %s because: %#v\n", configFilename, err)
		panic(err)
	}
	println(config)

	hosts := strings.Split(args[1].(string), ",")
	topicsToBackup := strings.Split(args[2].(string), ",")

	s3backup.Backup(config, args[0].(string), hosts, topicsToBackup)
	return nil
}

func replayWorker(queue string, args ...interface{}) error {
	configFilename := "consumer.properties"
	config, err := configfile.ReadConfigFile(configFilename)
	if err != nil {
		fmt.Printf("Couldn't read config file %s because: %#v\n", configFilename, err)
		panic(err)
	}

	for _, arg := range args {
		println(arg)
	}

	targets := strings.Split(args[0].(string), ",")
	clusterId := args[1].(string)
	topic := args[2].(string)
	partition := int(args[3].(float64))
	startDateStr := args[4].(string)
	endDateStr := args[5].(string)

	startDate, _ := time.Parse("2006-01-02", startDateStr)
	endDate, _ := time.Parse("2006-01-02", endDateStr)

	s3replay.Replay(config, targets, clusterId, topic, partition, startDate, endDate)
	return nil
}
