package main

import (
	"fmt"

	stomper "github.com/russmack/stompingophers"
)

func main() {

	consumer()

}

func consumer() {
	queueIp := "127.0.0.1"
	queuePort := 61613

	client, err := stomper.Connect(queueIp, queuePort)
	if err != nil {
		panic("failed connecting: " + err.Error())
	}

	fmt.Println("Consuming messages...")

	fmt.Println("Subscribing to queue...\n")

	err = client.Subscribe("/queue/nooq", &stomper.AckModeClientIndividual{})
	if err != nil {
		panic("failed sending: " + err.Error())
	}

	fmt.Println("\nResponse:", client.Response)

	done := make(chan int)

	go client.Receive(printMessage, done)

	// For dev purposes.
	//for i := 0; i < 45000; i++ {
	for i := 0; i < 4; i++ {
		<-done
	}

	client.Disconnect()
}

func printMessage(s string, ch chan int) {
	m, err := stomper.ParseResponse(s)
	if err != nil {
		fmt.Println("failed parsing message:", err)
	} else {
		fmt.Printf("Parsed message: \n%+v\n", m)
	}

	ch <- 1

	//fmt.Println("\n---------------------------")
	//fmt.Println("Message arrived:")
	//fmt.Println(s)
	//fmt.Println("---------------------------")
}
