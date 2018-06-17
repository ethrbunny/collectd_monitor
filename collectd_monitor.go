package main

import (
//    "fmt"
    "github.com/confluentinc/confluent-kafka-go/kafka"
    "encoding/json"
    "github.com/gocql/gocql"
    "io"
    "io/ioutil"
    "log"
    "os"
    "github.com/levenlabs/golib/timeutil"
)

// [{"values":[99.5004561070772],"dstypes":["derive"],"dsnames":["value"],"time":1528807932.746,"interval":10.000,"host":"playground.orcatech","plugin":"cpu","plugin_instance":"1","type":"cpu","type_instance":"idle"}]

type Kmsg struct {
    Values	[]float32 `json:"values"`
    Dstypes	[]string  `json:"dstypes"`
    Dsnames	[]string  `json:"dsnames"`
    Time	float64   `json:"time"`
    Host	string `json:"host"`
    Plugin	string	  `json:"plugin"`
    Plugin_instance	string  `json:"plugin_instance"`
    Interval	float32  `json:"interval"`
    Ctype	string	`json:"type"`
    Type_instance	string	`json:"type_instance"`
}


var (
    Trace   *log.Logger
    Info    *log.Logger
    Warning *log.Logger
    Error   *log.Logger
)

func initLog(
    traceHandle io.Writer,
    infoHandle io.Writer,
    warningHandle io.Writer,
    errorHandle io.Writer) {

    Trace = log.New(traceHandle,
        "TRACE: ",
        log.Ldate|log.Ltime|log.Lshortfile)

    Info = log.New(infoHandle,
        "INFO: ",
        log.Ldate|log.Ltime|log.Lshortfile)

    Warning = log.New(warningHandle,
        "WARNING: ",
        log.Ldate|log.Ltime|log.Lshortfile)

    Error = log.New(errorHandle,
        "ERROR: ",
        log.Ldate|log.Ltime|log.Lshortfile)
}

func writeRow(msg Kmsg, session *gocql.Session) {
//    if err := session.Query("insert into collectd(host, plugin, stamp, dsnames, dstypes, interval, plugin_instance, meta, plugin_type, type_instance, values) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", ).exec(); err != nil {
    if err := session.Query("insert into collectd(host, plugin, stamp, dsnames, dstypes, interval, plugin_instance, plugin_type, type_instance, values) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", msg.Host, msg.Plugin, timeutil.TimestampFromFloat64(msg.Time).Time, msg.Dsnames[0], msg.Dstypes[0], msg.Interval, msg.Plugin_instance, msg.Plugin, msg.Ctype, msg.Values).Exec(); err != nil {
        log.Fatal(err)
    }
}

func main() {
    initLog(ioutil.Discard, os.Stdout, os.Stdout, os.Stderr)

    consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
        "bootstrap.servers": "kafka1.orcatech",
	"group.id":          "collectd_group",
	"auto.offset.reset": "earliest",
    })

    if err != nil {
	panic(err)
    }

    defer consumer.Close()

    // connect to the cluster
    cluster := gocql.NewCluster("cass11.orcatech", "cass12.orcatech")
    cluster.Keyspace = "collectd_bits"
    session, _ := cluster.CreateSession()
    defer session.Close()

    consumer.SubscribeTopics([]string{"collectd_test", "^aRegex.*[Tt]opic"}, nil)
    var res []Kmsg
    for {
	msg, err := consumer.ReadMessage(-1)
	if err == nil {
            perr := json.Unmarshal([]byte(msg.Value), &res)
            if(perr != nil) {
                panic(perr)
	    }
//            Info.Printf("time: %f h:%s t:%s  v:%f\n", res[0].Time, res[0].Host, res[0].Ctype, res[0].Values)
       //     Info.Printf(string(msg.Value))
            writeRow(res[0], session)
        } else {
            Error.Printf("Consumer error: %v (%v)\n", err, msg)
            break
        }
    }

}
