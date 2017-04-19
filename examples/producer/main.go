package main

import (
	"fmt"
	"strconv"

	"github.com/russmack/stompingophers"
)

func main() {

	producer()

}

func producer() {
	queueIp := "127.0.0.1"
	queuePort := 61613

	client, err := stompingophers.Connect(queueIp, queuePort)
	if err != nil {
		panic("failed connecting: " + err.Error())
	}

	fmt.Printf("Connection response:\n%s\n", client.Response)

	fmt.Println("Sending messages...")

	for i := 0; i < 10; i++ {
		err = client.Send("/queue/nooq", "Well, hello, number "+strconv.Itoa(i)+"!")
		if err != nil {
			panic("failed sending: " + err.Error())
		}
	}

	client.Disconnect()
}
