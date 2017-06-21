package main

import (
	"fmt"
	"log"

	stomper "github.com/russmack/stompingophers"
)

const (
	queueIP   = "127.0.0.1"
	queuePort = 61613
	queueName = "/queue/nooq"
)

var (
	printer chan stomper.ServerFrame
	client  stomper.Client
)

func main() {
	printer = make(chan stomper.ServerFrame)

	go func() {
		for {
			msg := <-printer
			_ = msg
		}
	}()
	c := connect()
	sub := subscribe(c)
	consumer(c, sub)
}

func connect() *stomper.Client {
	var err error
	conn, err := stomper.NewConnection(queueIP, queuePort)
	if err != nil {
		log.Fatal(err)
	}

	options := stomper.Options{
		HeartBeat: &stomper.HeartBeat{
			SendInterval: 4000,
			RecvTimeout:  4000,
		},
	}

	client, resp, err := stomper.Connect(conn, &options)
	if err != nil {
		log.Fatal("failed connecting: " + err.Error())
	}

	f, err := stomper.ParseResponse(resp)
	if err != nil {
		log.Fatal("failed parsing connect response:", err)
	}
	fmt.Printf("Conneced: %+v\n", f)

	return &client
}

func subscribe(client *stomper.Client) stomper.Subscription {
	fmt.Println("Subscribing to queue...\n")

	sub, resp, err := client.Subscribe(queueName, "mysubrcpt", stomper.AckModeAuto)
	if err != nil {
		log.Fatal("failed sending: " + err.Error())
	}

	msg, err := stomper.ParseResponse(resp)
	if err != nil {
		fmt.Println("failed parsing subscribe response:", err)
	}
	fmt.Printf("Parsed subscribe response:\n%s\n", string(msg.Headers["receipt-id"]))

	return sub
}

func consumer(client *stomper.Client, sub stomper.Subscription) {
	defer client.Disconnect()

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

			if sub.AckMode != stomper.AckModeAuto {
				msgAckID := string(f.Headers["ack"])
				err = client.Ack(msgAckID, "", "")
				if err != nil {
					fmt.Println("failed sending ack:", err)
					continue
				}
			}
			printer <- f
		}
	}

}
