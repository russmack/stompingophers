package main

import (
	"fmt"

	"github.com/russmack/stompingophers"
)

func main() {

	consumer()

}

func consumer() {
	queueIp := "127.0.0.1"
	queuePort := 61613

	client, err := stompingophers.Connect(queueIp, queuePort)
	if err != nil {
		panic("failed connecting: " + err.Error())
	}

	fmt.Println("Consuming messages...")

	fmt.Println("Subscribing to queue...\n")

	err = client.Subscribe("/queue/nooq", "auto")
	if err != nil {
		panic("failed sending: " + err.Error())
	}

	fmt.Println("\nResponse:", client.Response)

	client.Receive(printMessage)

	<-make(chan bool)

	client.Disconnect()
}

func printMessage(s string) {
	fmt.Println("\n---------------------------")
	fmt.Println("Message arrived:")
	fmt.Println(s)
	fmt.Println("---------------------------")
}
