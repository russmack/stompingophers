package stompingophers

import (
	"bufio"
	"errors"
	"fmt"
	"net"
	"strconv"
)

const (
	SUPPORTEDVERSIONS string = "1.0,1.1,1.2"

	// TODO: replace these with a struct and validating ctor until Go gets proper enums.
	ACKMODE_AUTO             string = "auto"
	ACKMODE_CLIENT           string = "client"
	ACKMODE_CLIENTINDIVIDUAL string = "client-individual"
)

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

func (sf *ServerFrame) String() string {
	return fmt.Sprintf("COMMAND: %s ; HEADERS: %+v ; BODY: %s", sf.Command, sf.Headers, sf.Body)
}

type headers map[string]string

type Header struct {
	Key   string
	Value string
}

func (h headers) add(k, v string) {
	h[k] = v
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

func (f *frame) addHeaderAck(am string) error {
	// Server will use auto by default.
	if am != ACKMODE_AUTO &&
		am != ACKMODE_CLIENT &&
		am != ACKMODE_CLIENTINDIVIDUAL {
		return errors.New("invalid ack mode")
	}
	f.addHeader("ack", am)
	return nil
}

func (f *frame) addHeaderTransaction(s string) {
	f.addHeader("transaction", s)
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

// Any client frame other than CONNECT MAY specify a receipt header

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
		expectResponse: false,
	}

	if rcpt != "" {
		f.addHeaderReceipt(rcpt)
		f.expectResponse = true
	}

	return f
}

func newCmdSend(queueName, body, rcpt, txn string, custom ...Header) frame {
	f := frame{
		command:        "SEND",
		headers:        headers{},
		body:           body,
		expectResponse: false,
		// TODO: consider: expectErrorResponse: true,
	}

	// Must
	f.addHeaderDestination(queueName)
	// Should
	f.addHeaderContentType("text/plain")
	f.addHeaderContentLength()
	// Allows
	if rcpt != "" {
		f.addHeaderReceipt(rcpt)
		f.expectResponse = true
	}
	if txn != "" {
		f.addHeaderTransaction(txn)
	}
	// Custom
	for _, j := range custom {
		f.addHeader(j.Key, j.Value)
	}

	return f
}

func newCmdAck(msgId, rcpt, txn string) frame {
	f := frame{
		command:        "ACK",
		headers:        headers{},
		body:           "",
		expectResponse: false,
	}

	// Must
	f.addHeaderId(msgId)
	// Allows
	if rcpt != "" {
		f.addHeaderReceipt(rcpt)
		f.expectResponse = true
	}
	if txn != "" {
		f.addHeaderTransaction(txn)
	}

	return f
}

func newCmdNack(msgId, txn, rcpt string) frame {
	f := frame{
		command:        "NACK",
		headers:        headers{},
		body:           "",
		expectResponse: false,
	}

	// Must
	f.addHeaderId(msgId)
	// Allows
	if rcpt != "" {
		f.addHeaderReceipt(rcpt)
		f.expectResponse = true
	}
	if txn != "" {
		f.addHeaderTransaction(txn)
	}

	return f
}

func newCmdSubscribe(queueName, subId, rcpt string, am string) (frame, error) {
	f := frame{
		command:        "SUBSCRIBE",
		headers:        headers{},
		body:           "",
		expectResponse: false,
		// TODO: consider: expectErrorResponse: true,
	}

	// Must
	f.addHeaderId(subId)
	f.addHeaderDestination(queueName)
	// Allows
	if am != "" {
		err := f.addHeaderAck(am)
		if err != nil {
			return f, err
		}
	}

	if rcpt != "" {
		f.addHeaderReceipt(rcpt)
		f.expectResponse = true
	}

	return f, nil
}

func newCmdUnsubscribe(subId, rcpt string) frame {
	f := frame{
		command:        "UNSUBSCRIBE",
		headers:        headers{},
		body:           "",
		expectResponse: false,
	}

	// Must
	f.addHeaderId(subId)
	// Allows
	if rcpt != "" {
		f.addHeaderReceipt(rcpt)
		f.expectResponse = true
	}

	return f
}

func newCmdBegin(txn, rcpt string) frame {
	f := frame{
		command:        "BEGIN",
		headers:        headers{},
		body:           "",
		expectResponse: false,
	}

	// Must
	f.addHeaderTransaction(txn)
	// Allows
	if rcpt != "" {
		f.addHeaderReceipt(rcpt)
		f.expectResponse = true
	}

	return f
}

func newCmdAbort(txn, rcpt string) frame {
	f := frame{
		command:        "ABORT",
		headers:        headers{},
		body:           "",
		expectResponse: false,
	}

	// Must
	f.addHeaderTransaction(txn)
	// Allows
	if rcpt != "" {
		f.addHeaderReceipt(rcpt)
		f.expectResponse = true
	}

	return f
}

func newCmdCommit(txn, rcpt string) frame {
	f := frame{
		command:        "COMMIT",
		headers:        headers{},
		body:           "",
		expectResponse: false,
	}

	// Must
	f.addHeaderTransaction(txn)
	// Allows
	if rcpt != "" {
		f.addHeaderReceipt(rcpt)
		f.expectResponse = true
	}

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

	return req
}

func sendRequest(conn net.Conn, fr frame) (string, error) {
	req := formatRequest(fr)
	conn.Write([]byte(req))

	if !fr.expectResponse {
		return "", nil
	}

	resp, err := bufio.NewReader(conn).ReadBytes('\000')
	if err != nil {
		return "", err
	}

	return string(resp), nil
}

func NewConnection(host string, port int) (net.Conn, error) {
	addr := host + ":" + strconv.Itoa(port)
	return net.Dial("tcp", addr)
}

func Connect(conn net.Conn) (Client, error) {
	resp, err := sendRequest(conn, newCmdConnect(conn.RemoteAddr().String()))
	if err != nil {
		return Client{}, fmt.Errorf("failed connecting: %s", err)
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

	rcptId := "rcpt-disconnect-123"
	resp, err := sendRequest(c.connection, newCmdDisconnect(rcptId))
	if err != nil {
		return fmt.Errorf("failed sending disconnect: %s", err)
	}

	c.Response = resp

	sf, err := ParseResponse(resp)
	if err != nil {
		return errors.New("failed disconnecting, unable to parse response: " + err.Error())
	}

	v, ok := sf.Headers["receipt-id"]
	if !ok || sf.Command != "RECEIPT" {
		return errors.New("failed disconnecting, invalid receipt response: " + sf.String())
	}

	if v != rcptId {
		return errors.New(
			"failed disconnecting, invalid receipt id in response - " +
				"expected: " + rcptId + ", got: " + v)
	}

	return c.connection.Close()
}

func (c *Client) Send(queueName, msg, rcpt, txn string, custom ...Header) error {
	// Default ack mode is auto.
	// Server will not send a response unless either:
	// a - receipt header is set.
	// b - the server sends an ERROR response and disconnects.

	resp, err := sendRequest(c.connection, newCmdSend(queueName, msg, rcpt, txn, custom...))
	if err != nil {
		// If the server returned an error here then it will also have disconnected.
		return fmt.Errorf("failed enqueue: %s", err)
	}
	// No error, implies a successful send, despite no response.

	if rcpt != "" {
		c.Response = resp
	}

	return nil
}

func (c *Client) Ack(msgId, rcpt, transactionId string) error {
	_, err := sendRequest(c.connection, newCmdAck(msgId, rcpt, transactionId))
	if err != nil {
		return fmt.Errorf("failed sending ack: %s", err)
	}

	return nil
}

func (c *Client) Nack(msgId, transactionId, rcpt string) error {
	_, err := sendRequest(c.connection, newCmdNack(msgId, transactionId, rcpt))
	if err != nil {
		return fmt.Errorf("failed sending nack: %s", err)
	}

	return nil
}

func (c *Client) Subscribe(queueName, rcpt string, am string) error {
	// Simple approach of setting id to list index.
	subId := strconv.Itoa(len(c.Subscriptions))
	f, err := newCmdSubscribe(queueName, subId, rcpt, am)
	if err != nil {
		return fmt.Errorf("failed creating subscribe command: %s", err)
	}
	resp, err := sendRequest(c.connection, f)
	if err != nil {
		return fmt.Errorf("failed subscribing: %s", err)
	}

	c.Subscriptions = append(c.Subscriptions, Subscription{
		Id: subId,
		Channel: Channel{
			Name: queueName,
			Type: "not implemented",
		},
		AckMode: am,
	})

	c.Response = resp

	return nil
}

func (c *Client) Unsubscribe(subId, rcpt string) error {
	resp, err := sendRequest(c.connection, newCmdUnsubscribe(subId, rcpt))
	if err != nil {
		return fmt.Errorf("failed unsubscribing: %s", err)
	}

	c.Response = resp

	return nil
}

func (c *Client) Receive() (chan string, chan error) {
	reader := bufio.NewReader(c.connection)

	recvChan := make(chan string)
	errChan := make(chan error)

	go func() {
		for {
			resp, err := reader.ReadString('\000')
			if err != nil {
				errChan <- fmt.Errorf("failed reading response: %s", err)
				continue
			}

			recvChan <- resp

			// Remove end of packet newline.
			b, err := reader.Peek(1)
			if err != nil {
				errChan <- fmt.Errorf("failed reading bytes after \\x0: %s", err)
			}
			if b[0] == '\n' {
				_, err := reader.ReadByte()
				if err != nil {
					errChan <- fmt.Errorf("failed reading final newline: %s", err)
				}
			}
		}
	}()

	return recvChan, errChan
}

func (c *Client) Begin(transactionId, rcpt string) error {
	resp, err := sendRequest(c.connection, newCmdBegin(transactionId, rcpt))
	if err != nil {
		return fmt.Errorf("failed transaction begin: %s", err)
	}

	c.Response = resp

	return nil
}

func (c *Client) Abort(transactionId, rcpt string) error {
	resp, err := sendRequest(c.connection, newCmdAbort(transactionId, rcpt))
	if err != nil {
		fmt.Errorf("failed abort: %s", err)
		return err
	}

	c.Response = resp

	return nil
}

func (c *Client) Commit(transactionId, rcpt string) error {
	resp, err := sendRequest(c.connection, newCmdCommit(transactionId, rcpt))
	if err != nil {
		fmt.Errorf("failed commit: %s", err)
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
			cmd = s[:i]
			headStart = i + 1
			break
		}
	}

	if cmd == "" {
		return sf, fmt.Errorf("failed parsing invalid message, no lines in: '%s'", s)
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
