package stompingophers

import (
	"bufio"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"strconv"
)

const (
	SUPPORTEDVERSIONS string = "1.0,1.1,1.2"

	ACKMODE_AUTO             string = "auto"
	ACKMODE_CLIENT           string = "client"
	ACKMODE_CLIENTINDIVIDUAL string = "client-individual"
)

func init() {
	logEnabled := true
	if !logEnabled {
		log.SetFlags(0)
		log.SetOutput(ioutil.Discard)
	}
}

// AckModer is a Golang enum abomination.  TODO: learn to embed Rust.
type AckModer interface {
	getAckMode() string
}

type AckModeAuto struct{}
type AckModeClient struct{}
type AckModeClientIndividual struct{}

func (a *AckModeAuto) getAckMode() string {
	return ACKMODE_AUTO
}

func (a *AckModeClient) getAckMode() string {
	return ACKMODE_CLIENT
}

func (a *AckModeClientIndividual) getAckMode() string {
	return ACKMODE_CLIENTINDIVIDUAL
}

// End of AckMode enum abomination.

type Client struct {
	connection    net.Conn
	Response      string
	Subscriptions []Subscription
}

type Subscription struct {
	Id      string
	Channel Channel
	AckMode string
}

type Channel struct {
	Name string
	Type string // queue or topic
}

// Only the Send, Message, and Error frames may have a body, the others must not.
type frame struct {
	command        string
	headers        headers
	body           string
	expectResponse bool
}

type ServerFrame struct {
	Command string
	Headers map[string]string
	Body    string
}

type headers map[string]string

type Header struct {
	Key   string
	Value string
}

func (h *headers) add(k, v string) {
	map[string]string(*h)[k] = v
}

func (f *frame) addHeader(k, v string) {
	f.headers.add(k, v)
}

func (f *frame) addHeaderAcceptVersion(s string) {
	f.addHeader("accept-version", s)
}

func (f *frame) addHeaderHost(s string) {
	f.addHeader("host", s)
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

func (f *frame) addHeaderAck(am AckModer) {
	f.addHeader("ack", am.getAckMode())
}

func (f *frame) addHeaderTransaction(s string) {
	f.addHeader("transaction", s)
}

func callIfNotEmptyString(fn func(string), val string) {
	if val != "" {
		fn(val)
	}
}

// Server frames

func newCmdConnected() ServerFrame {

	sf := ServerFrame{
		Command: "CONNECTED",
		Headers: map[string]string{},
	}
	// Must
	sf.Headers["version"] = ""
	// May
	sf.Headers["heart-beat"] = ""
	sf.Headers["session"] = ""
	sf.Headers["server"] = ""

	return sf
}

func newCmdMessage() ServerFrame {
	sf := ServerFrame{
		Command: "MESSAGE",
		Headers: map[string]string{},
	}
	// Must
	sf.Headers["destination"] = ""
	sf.Headers["message-id"] = ""
	sf.Headers["subscription"] = ""
	// Must conditionally, if subscription requires ack
	sf.Headers["ack"] = ""
	// Should
	sf.Headers["content-length"] = ""
	sf.Headers["content-type"] = ""
	// And all custom headers sent in source frame

	return sf
}

func newCmdReceipt() ServerFrame {
	sf := ServerFrame{
		Command: "RECEIPT",
		Headers: map[string]string{},
	}
	sf.Headers["receipt-id"] = ""

	return sf
}

func newCmdError() ServerFrame {
	sf := ServerFrame{
		Command: "ERROR",
		Headers: map[string]string{},
	}
	// Should
	sf.Headers["message"] = ""
	// Should conditionally, if request contained receipt header
	sf.Headers["receipt-id"] = ""
	// Should conditionally, if body included
	sf.Headers["content-length"] = ""
	sf.Headers["content-type"] = ""
	// May contain a body

	return sf
}

// Client frames

func newCmdConnect(host string) frame {
	f := frame{
		command:        "CONNECT",
		headers:        headers{},
		body:           "",
		expectResponse: true, // returns CONNECTED frame
	}

	f.addHeaderAcceptVersion(SUPPORTEDVERSIONS)
	f.addHeaderHost(host)

	return f
}

func newCmdDisconnect(rcpt string) frame {
	f := frame{
		command:        "DISCONNECT",
		headers:        headers{},
		body:           "",
		expectResponse: true, // TODO: confirm
	}

	f.addHeaderReceipt(rcpt)

	return f
}

func newCmdSend(queueName, body, rcpt, txn string, custom ...Header) frame {
	f := frame{
		command:        "SEND",
		headers:        headers{},
		body:           body,
		expectResponse: true, // TODO: confirm
	}

	// Required
	f.addHeaderDestination(queueName)
	// Should
	f.addHeaderContentType("text/plain")
	f.addHeaderContentLength()
	// Allows
	callIfNotEmptyString(f.addHeaderReceipt, rcpt)
	callIfNotEmptyString(f.addHeaderTransaction, txn)
	// Custom
	for _, j := range custom {
		f.addHeader(j.Key, j.Value)
	}

	return f
}

func newCmdAck(msgId, txnId string) frame {
	f := frame{
		command:        "ACK",
		headers:        headers{},
		body:           "",
		expectResponse: false,
	}

	f.addHeaderId(msgId)

	if txnId != "" {
		f.addHeaderTransaction(txnId)
	}

	return f
}

func newCmdNack(msgId, txnId string) frame {
	f := frame{
		command:        "NACK",
		headers:        headers{},
		body:           "",
		expectResponse: false,
	}

	f.addHeaderId(msgId)

	if txnId != "" {
		f.addHeaderTransaction(txnId)
	}

	return f
}

func newCmdSubscribe(queueName, subId string, am AckModer) frame {
	f := frame{
		command:        "SUBSCRIBE",
		headers:        headers{},
		body:           "",
		expectResponse: true,
	}

	f.addHeaderId(subId)
	f.addHeaderDestination(queueName)
	f.addHeaderAck(am)
	// TODO: decide what to do with the receipt.
	f.addHeaderReceipt("optional-subscription-rcpt")

	return f
}

func newCmdUnsubscribe(subId string) frame {
	f := frame{
		command:        "UNSUBSCRIBE",
		headers:        headers{},
		body:           "",
		expectResponse: true, // TODO: confirm
	}

	f.addHeaderId(subId)

	return f
}

func newCmdBegin(txnId string) frame {
	f := frame{
		command:        "BEGIN",
		headers:        headers{},
		body:           "",
		expectResponse: true, // TODO: confirm
	}

	f.addHeaderTransaction(txnId)

	return f
}

func newCmdAbort(txnId string) frame {
	f := frame{
		command:        "ABORT",
		headers:        headers{},
		body:           "",
		expectResponse: true, // TODO: confirm
	}

	f.addHeaderTransaction(txnId)

	return f
}

func newCmdCommit(txnId string) frame {
	f := frame{
		command:        "COMMIT",
		headers:        headers{},
		body:           "",
		expectResponse: true, // TODO: confirm
	}

	f.addHeaderTransaction(txnId)

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

func sendRequest(conn net.Conn, fr frame) (string, error) {
	req := formatRequest(fr)
	fmt.Fprintf(conn, req)

	if !fr.expectResponse {
		return "", nil
	}

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

func (c *Client) Disconnect() error {
	// Graceful shutdown: send disconnect frame, check rcpt received, then close socket.
	// Do not send any more frames after the DISCONNECT frame has been sent.

	// TODO: sort out the receipt
	resp, err := sendRequest(c.connection, newCmdDisconnect("rcpt-disconnect-123"))
	if err != nil {
		log.Println("failed sending disconnect:", err)
		return err
	}

	c.Response = resp

	// TODO: Parse returned RECEIPT frame for receipt-id

	return c.connection.Close()
}

func (c *Client) Send(queueName, msg, rcpt, txn string, custom ...Header) error {
	// Server will not send a response unless either:
	// a - receipt header is set.
	// b - the server sends an ERROR response and disconnects.

	resp, err := sendRequest(c.connection, newCmdSend(queueName, msg, rcpt, txn, custom...))
	if err != nil {
		// If the server returned an error here then it will also have disconnected.
		log.Println("failed enqueue:", err)
		return err
	}
	// No error, implies a successful send, despite no response.

	if rcpt != "" {
		c.Response = resp
	}

	return nil
}

func (c *Client) Ack(msgId, transactionId string) error {
	_, err := sendRequest(c.connection, newCmdAck(msgId, transactionId))
	if err != nil {
		log.Println("failed sending ack:", err)
		return err
	}

	return nil
}

func (c *Client) Nack(msgId, transactionId string) error {
	_, err := sendRequest(c.connection, newCmdNack(msgId, transactionId))
	if err != nil {
		log.Println("failed sending nack:", err)
		return err
	}

	return nil
}

func (c *Client) Subscribe(queueName string, am AckModer) error {
	// Simple approach of setting id to list index.
	subId := strconv.Itoa(len(c.Subscriptions))
	resp, err := sendRequest(c.connection, newCmdSubscribe(queueName, subId, am))
	if err != nil {
		log.Println("failed subscribe:", err)
		return err
	}

	c.Subscriptions = append(c.Subscriptions, Subscription{
		Id: subId,
		Channel: Channel{
			Name: queueName,
			Type: "not implemented",
		},
		AckMode: am.getAckMode(),
	})

	c.Response = resp

	return nil
}

func (c *Client) Unsubscribe(subId string) error {
	resp, err := sendRequest(c.connection, newCmdUnsubscribe(subId))
	if err != nil {
		log.Println("failed unsubscribe:", err)
		return err
	}

	c.Response = resp

	return nil
}

func (c *Client) Receive(fn func(string, chan int), ch chan int) error {
	log.Println("Started receiving...")

	reader := bufio.NewReader(c.connection)

	// TODO: remove this dev limit.
	for i := 0; i < 1; i++ {
		//for i := 0; i < 45000; i++ {
		//for {
		resp, err := reader.ReadString('\000')
		if err != nil {
			return err
		}

		log.Printf("DEBUG RESP:\n%+v\n", resp)

		go fn(resp, ch)
	}

	return nil
}

func (c *Client) Begin(transactionId string) error {
	resp, err := sendRequest(c.connection, newCmdBegin(transactionId))
	if err != nil {
		log.Println("failed begin:", err)
		return err
	}

	c.Response = resp

	return nil
}

func (c *Client) Abort(transactionId string) error {
	resp, err := sendRequest(c.connection, newCmdAbort(transactionId))
	if err != nil {
		log.Println("failed abort:", err)
		return err
	}

	c.Response = resp

	return nil
}

func (c *Client) Commit(transactionId string) error {
	resp, err := sendRequest(c.connection, newCmdCommit(transactionId))
	if err != nil {
		log.Println("failed commit:", err)
		return err
	}

	c.Response = resp

	return nil
}

func ParseResponse(s string) (ServerFrame, error) {
	sf := ServerFrame{
		Command: "",
		Headers: make(map[string]string),
	}
	var cmd string
	var headStart int
	sfLen := len(s)
	for i := 0; i < sfLen; i++ {
		if s[i] == '\n' {
			// TODO: investigate this anomaly -
			// some repsonses being with '\n'
			if i == 0 {
				// Ignore \n if it is the first response character.
				continue
			}
			cmd = s[:i]
			headStart = i + 1
			break
		}
	}

	if cmd == "" {
		log.Printf("failed parsing message, no lines in: %s", s)
		return sf, errors.New("failed parsing invalid message")
	}

	sf.Command = cmd

	// Rest of sf, without command.
	remMsg := s[headStart:]

	kStart := 0
	kDone := false
	vStart := 0
	var k string
	var v string
	for i, j := range remMsg {
		if !kDone && j == ':' {
			k = remMsg[kStart:i]
			vStart = i + 1
			kDone = true
		}

		if kDone && j == '\n' {
			v = remMsg[vStart:i]
			sf.Headers[k] = v

			kStart = i + 1
			kDone = false
		}
	}

	sf.Body = remMsg[kStart:]

	return sf, nil
}
