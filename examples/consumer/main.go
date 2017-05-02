package main

import (
	"fmt"

	stomper "github.com/russmack/stompingophers"
)

var (
	printer chan string
	client  stomper.Client
)

func main() {

	printer = make(chan string)

	go func() {
		for {
			msg := <-printer
			_ = msg
		}
	}()
	consumer()

}

func consumer() {
	queueIp := "127.0.0.1"
	queuePort := 61613

	var err error
	client, err = stomper.Connect(queueIp, queuePort)
	if err != nil {
		panic("failed connecting: " + err.Error())
	}

	fmt.Println("Consuming messages...")

	fmt.Println("Subscribing to queue...\n")

	am := stomper.ACKMODE_AUTO
	//am := stomper.ACKMODE_CLIENTINDIVIDUAL

	err = client.Subscribe("/queue/nooq", "mysubrcpt", am)
	if err != nil {
		panic("failed sending: " + err.Error())
	}

	fmt.Println("\nRaw subscribe response:\n", client.Response)
	msg, err := stomper.ParseResponse(client.Response)
	if err != nil {
		fmt.Println("failed parsing subscribe response:", err)
	}
	fmt.Println("Parsed subscribe response:\n", msg)

	// TODO: consider adding error channel.
	recvChan := client.Receive()

	//fin := 0
	for s := range recvChan {
		f, err := stomper.ParseResponse(s)
		if err != nil {
			fmt.Println("response parse err:", err)
			continue
		}

		if am != stomper.ACKMODE_AUTO {
			msgAckId := f.Headers["ack"]
			err = client.Ack(msgAckId, "", "")
			if err != nil {
				fmt.Println("ack err:", err)
				continue
			}
		}

		printer <- fmt.Sprintf("%+v\n", f)
		//fin = fin + 1
		//if fin == 44999 {
		//	break
		//}
	}

	client.Disconnect()
}
