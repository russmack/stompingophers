package main

import (
	"fmt"
	"log"
	"strconv"

	stomper "github.com/russmack/stompingophers"
)

func main() {

	producer()

}

func producer() {
	queueIp := "127.0.0.1"
	queuePort := 61613

	conn, err := stomper.NewConnection(queueIp, queuePort)
	if err != nil {
		log.Fatal(err)
	}

	client, err := stomper.Connect(conn)
	if err != nil {
		log.Fatal("failed connecting: " + err.Error())
	}

	fmt.Printf("Connection response:\n%s\n", client.Response)

	fmt.Println("Sending messages...")

	for j := range gen1() {
		err = client.Send("/queue/nooq", j, "", "")
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
func gen1() chan string {
	c := make(chan string)

	go func() {
		for i := 0; i < 45000; i++ {
			c <- "Well, hello, number " + strconv.Itoa(i) + "!"
		}
		close(c)
	}()

	return c
}

// gen2 generates big json messages.
func gen2() chan string {
	c := make(chan string)
	orders := []string{
		json_01,
		json_02,
		json_03,
		json_04,
		json_05,
		json_06,
		json_07,
		json_08,
		json_09,
		json_10,
	}

	go func() {
		for i := 0; i < 4500; i++ {
			for _, j := range orders {
				c <- j
			}
		}
		close(c)
	}()

	return c
}

const json_01 = `
{  
	"big_json_message":"abcdefg",
}
`

const json_02 = `
{  
	"big_json_message":"abcdefg",
}
`

const json_03 = `
{  
	"big_json_message":"abcdefg",
}
`
const json_04 = `
{  
	"big_json_message":"abcdefg",
}
`

const json_05 = `
{  
	"big_json_message":"abcdefg",
}
`

const json_06 = `
{  
	"big_json_message":"abcdefg",
}
`

const json_07 = `
{  
	"big_json_message":"abcdefg",
}
`

const json_08 = `
{  
	"big_json_message":"abcdefg",
}
`

const json_09 = `
{  
	"big_json_message":"abcdefg",
}
`

const json_10 = `
{  
	"big_json_message":"abcdefg",
}
`
