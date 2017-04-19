package stompingophers

import (
	"bufio"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"strconv"
)

const SUPPORTEDVERSIONS string = "1.0,1.1,1.2"

func init() {
	logEnabled := true
	if !logEnabled {
		log.SetFlags(0)
		log.SetOutput(ioutil.Discard)
	}
}

type Client struct {
	connection net.Conn
	Response   string
}

// Only the Send, Message, and Error frames may have a body, the others must not.
type frame struct {
	command string
	headers headers
	body    string
}

type headers map[string]string

func (h *headers) add(k, v string) {
	map[string]string(*h)[k] = v
}

func (f *frame) addHeader(k, v string) {
	f.headers.add(k, v)
}

func (f *frame) addHeaderAcceptVersion(s string) {
	f.headers.add("accept-version", s)
}

func (f *frame) addHeaderHost(s string) {
	f.headers.add("host", s)
}

func (f *frame) addHeaderContentLength() {
	f.addHeader("content-length", strconv.Itoa(len(f.body)+1))
}

func (f *frame) addHeaderReceipt(s string) {
	f.addHeader("receipt", s)
}

func (f *frame) addHeaderDestination(s string) {
	f.addHeader("destination", s)
}

func (f *frame) addHeaderContentType(s string) {
	f.addHeader("content-type", s)
}

func (f *frame) addHeaderId(s string) {
	f.addHeader("id", s)
}

func (f *frame) addHeaderAck(s string) {
	if s != "auto" &&
		s != "client" &&
		s != "client-individual" {
		panic("invalid ack value")
	}
	f.addHeader("ack", s)
}

func newCmdConnect(host string) frame {
	f := frame{
		command: "CONNECT",
		headers: headers{},
		body:    "",
	}

	f.addHeaderAcceptVersion(SUPPORTEDVERSIONS)
	f.addHeaderHost(host)

	return f
}

func newCmdSend(queueName, body, rcpt string) frame {
	f := frame{
		command: "SEND",
		headers: headers{},
		body:    body,
	}

	f.addHeaderDestination(queueName)
	f.addHeaderContentType("text/plain")
	f.addHeaderContentLength()
	f.addHeaderReceipt(rcpt)

	return f
}

func newCmdSubscribe(queueName, id, ack string) frame {
	f := frame{
		command: "SUBSCRIBE",
		headers: headers{},
		body:    "",
	}

	f.addHeaderId(id)
	f.addHeaderDestination(queueName)
	f.addHeaderAck(ack)
	f.addHeaderReceipt("test-rcpt")

	return f
}

func formatRequest(f frame) string {
	c := f.command + "\n"
	h := ""
	for k, v := range f.headers {
		h += k + ":" + v + "\n"
	}
	b := "\n" + f.body + "\n"
	t := "\000"

	req := c + h + b + t

	log.Printf("Request [dec]:\n%v\n", []byte(req))
	log.Printf("Request [ascii]:\n%s\n", string(req))

	return req
}

func (c *Client) Disconnect() error {
	return c.connection.Close()
}

func sendRequest(conn net.Conn, fr frame) (string, error) {
	req := formatRequest(fr)
	fmt.Fprintf(conn, req)

	resp, err := bufio.NewReader(conn).ReadString('\000')
	if err != nil {
		return "", err
	}
	return resp, nil
}

func Connect(host string, port int) (Client, error) {
	addr := host + ":" + strconv.Itoa(port)

	log.Println("Connecting to:", addr)

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return Client{}, err
	}

	resp, err := sendRequest(conn, newCmdConnect(host))
	if err != nil {
		log.Println("failed connecting:", err)
		return Client{}, err
	}

	client := Client{
		connection: conn,
		Response:   resp,
	}

	return client, nil
}

func (c *Client) Send(queueName, msg string) error {
	// Server will not send a response unless either:
	// a - receipt header is set.
	// b - the server sends an ERROR response and disconnects.

	resp, err := sendRequest(c.connection, newCmdSend(queueName, msg, "rcpt-123"))
	if err != nil {
		log.Println("failed enqueue:", err)
		return err
	}

	c.Response = resp

	return nil
}

func (c *Client) Subscribe(queueName, ackMode string) error {
	resp, err := sendRequest(c.connection, newCmdSubscribe(queueName, "subid-0", ackMode))
	if err != nil {
		log.Println("failed subscribe:", err)
		return err
	}

	c.Response = resp

	return nil
}

func (c *Client) Receive(fn func(string)) error {
	log.Println("Started receiving...")

	reader := bufio.NewReader(c.connection)

	for i := 0; i < 10; i++ {
		resp, err := reader.ReadString('\000')
		if err != nil {
			return err
		}

		go fn(resp)
	}

	return nil
}
