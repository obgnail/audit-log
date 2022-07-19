package kafka

import (
	"fmt"
	"github.com/Shopify/sarama"
	"testing"
	"time"
)

func checkErr(err error) {
	if err != nil {
		panic(err)
	}
}
func TestKafka(t *testing.T) {
	_, _, err := SendToBinlog([]byte("ZXCASDQWE"))
	checkErr(err)

	time.Sleep(time.Second)
	go func() {
		fmt.Println("-- start --")
		err = ConsumeBinlog(func(msg *sarama.ConsumerMessage) error {
			fmt.Println(string(msg.Value))
			return nil
		})
		fmt.Println("-- consume --")
		checkErr(err)
	}()
	fmt.Println("--- end ---")
	forever := make(chan struct{})
	<-forever
}
