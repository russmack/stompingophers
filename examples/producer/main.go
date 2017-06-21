package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"strconv"

	//"github.com/pkg/profile"
	stomper "github.com/russmack/stompingophers"
)

const (
	queueIP   = "127.0.0.1"
	queuePort = 61613
	queueName = "/queue/nooq"
)

func main() {
	//defer profile.Start(profile.MemProfile, profile.ProfilePath(".")).Stop()
	//defer profile.Start(profile.CPUProfile, profile.ProfilePath(".")).Stop()

	producer()
}

func producer() {
	conn, err := stomper.NewConnection(queueIP, queuePort)
	if err != nil {
		log.Fatal(err)
	}

	options := stomper.Options{
		HeartBeat: &stomper.HeartBeat{
			SendInterval: 5000,
			RecvTimeout:  5000,
		},
	}

	client, resp, err := stomper.Connect(conn, &options)
	if err != nil {
		log.Fatal("failed connecting: " + err.Error())
	}

	fmt.Printf("Connection response:\n%s\n", resp)

	fmt.Println("Sending messages...")

	gen := gen1 //gen2

	for j := range gen() {
		_, err = client.Send(queueName, j, "", "")
		if err != nil {
			log.Fatal("failed sending: " + err.Error())
		}
	}

	client.Disconnect()
}

//
// Message content generators functions.
//

// gen1 generates small messages.
func gen1() chan []byte {
	c := make(chan []byte)

	go func() {
		for i := 0; i < 45000; i++ {
			c <- []byte("Well, hello, number " + strconv.Itoa(i) + "!")
		}
		close(c)
	}()

	return c
}

// gen2 generates big json messages, from specified file.
func gen2() chan []byte {
	j := readJSONFile("unconfirmed-transactions.json")

	objects := splitJSONArray(j)

	c := make(chan []byte)

	go func() {
		for i := 0; i < 4500; i++ {
			for _, j := range objects {
				c <- j
			}
		}
		close(c)
	}()

	return c
}

func readJSONFile(fname string) []byte {
	f, err := ioutil.ReadFile(fname)
	if err != nil {
		log.Fatal("failed reading json data file:", err)
	}

	return f
}

func splitJSONArray(b []byte) [][]byte {
	var j interface{}

	err := json.Unmarshal(b, &j)
	if err != nil {
		log.Fatal("failed to parse json data:", err)
	}

	li := [][]byte{}

	for _, v := range j.(map[string]interface{}) {
		for _, vv := range v.([]interface{}) {
			o, err := json.Marshal(vv)
			if err != nil {
				log.Println("failed to marshal json to obj:", err)
			}
			li = append(li, o)
		}
	}

	return li
}
