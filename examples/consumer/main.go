package main

import (
	"fmt"
	"log"

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
	conn, err := stomper.NewConnection(queueIp, queuePort)
	if err != nil {
		log.Fatal(err)
	}

	client, err = stomper.Connect(conn)
	if err != nil {
		log.Fatal("failed connecting: " + err.Error())
	}

	fmt.Println("Consuming messages...")

	fmt.Println("Subscribing to queue...\n")

	am := stomper.ACKMODE_AUTO
	//am := stomper.ACKMODE_CLIENTINDIVIDUAL

	err = client.Subscribe("/queue/nooq", "mysubrcpt", am)
	if err != nil {
		log.Fatal("failed sending: " + err.Error())
	}

	fmt.Println("\nRaw subscribe response:\n", client.Response)
	msg, err := stomper.ParseResponse(client.Response)
	if err != nil {
		fmt.Println("failed parsing subscribe response:", err)
	}
	fmt.Println("Parsed subscribe response:\n", msg)

	recvChan, errChan := client.Receive()

	for {
		select {
		case err := <-errChan:
			log.Printf("This is unfortunate, but the show must go on.  (err: %s)", err)
		case s := <-recvChan:
			f, err := stomper.ParseResponse(s)
			if err != nil {
				fmt.Println("response parse err:", err)
				continue
			}

			if am != stomper.ACKMODE_AUTO {
				msgAckId := f.Headers["ack"]
				err = client.Ack(msgAckId, "", "")
				if err != nil {
					fmt.Println("failed sending ack:", err)
					continue
				}
			}

			printer <- fmt.Sprintf("%+v\n", f)
		}
	}

	client.Disconnect()
}
