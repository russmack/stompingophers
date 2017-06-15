package stompingophers

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
)

const (
	SupportedVersions = "1.0,1.1,1.2"
	ContentTypeText   = "text/plain"
	CmdConnect        = "CONNECT"
	CmdConnected      = "CONNECTED"
	CmdMessage        = "MESSAGE"
	CmdReceipt        = "RECEIPT"
	CmdError          = "ERROR"
	CmdDisconnect     = "DISCONNECT"
	CmdSend           = "SEND"
	CmdAck            = "ACK"
	CmdNack           = "NACK"
	CmdSubscribe      = "SUBSCRIBE"
	CmdUnsubscribe    = "UNSUBSCRIBE"
	CmdBegin          = "BEGIN"
	CmdAbort          = "ABORT"
	CmdCommit         = "COMMIT"

	HeaderAcceptVersion = "accept-version"
	HeaderHost          = "host"
	HeaderContentLength = "content-length"
	HeaderReceipt       = "receipt"
	HeaderReceiptID     = "receipt-id"
	HeaderDestination   = "destination"
	HeaderContentType   = "content-type"
	HeaderID            = "id"
	HeaderAck           = "ack"
	HeaderTransaction   = "transaction"

	byteNull     = 0x00
	byteLineFeed = 0x0a
	byteColon    = 0x3a

	AckModeAuto             int = 0
	AckModeClient           int = 1
	AckModeClientIndividual int = 2

	ackModeStringAuto             string = "auto"
	ackModeStringClient           string = "client"
	ackModeStringClientIndividual string = "client-individual"
)

func parseAckModeInt(n int) (string, error) {
	switch n {
	case AckModeAuto:
		return ackModeStringAuto, nil
	case AckModeClient:
		return ackModeStringClient, nil
	case AckModeClientIndividual:
		return ackModeStringClientIndividual, nil
	default:
		return "", errors.New("invalid ack mode")
	}
}

type Client struct {
	connection    net.Conn
	Response      []byte
	Subscriptions []Subscription
}

type Subscription struct {
	ID      string
	Channel Channel
	AckMode int
}

type Channel struct {
	Name string
	Type string // queue or topic
}

// Only the Send, Message, and Error frames may have a body, the others must not.
type frame struct {
	command        string
	headers        headers
	body           []byte
	expectResponse bool
}

type ServerFrame struct {
	Command string
	Headers map[string][]byte
	Body    []byte
}

func (sf *ServerFrame) String() string {
	return fmt.Sprintf("COMMAND: %s ; HEADERS: %+v ; BODY: %s", sf.Command, sf.Headers, sf.Body)
}

type headers map[string][]byte

type Header struct {
	Key   string
	Value string
}

func (h headers) add(k string, v []byte) {
	h[k] = v
}

func (f *frame) addHeader(k, v string) {
	f.headers.add(k, []byte(v))
}

func (f *frame) addHeaderAcceptVersion(s string) {
	f.addHeader(HeaderAcceptVersion, s)
}

func (f *frame) addHeaderHost(s string) {
	f.addHeader(HeaderHost, s)
}

func (f *frame) addHeaderContentLength() {
	f.addHeader(HeaderContentLength, strconv.Itoa(len(f.body)+1))
}

func (f *frame) addHeaderReceipt(s string) {
	f.addHeader(HeaderReceipt, s)
}

func (f *frame) addHeaderDestination(s string) {
	f.addHeader(HeaderDestination, s)
}

func (f *frame) addHeaderContentType(s string) {
	f.addHeader(HeaderContentType, s)
}

func (f *frame) addHeaderID(s string) {
	f.addHeader(HeaderID, s)
}

func (f *frame) addHeaderAck(am string) {
	f.addHeader(HeaderAck, am)
}

func (f *frame) addHeaderTransaction(s string) {
	f.addHeader(HeaderTransaction, s)
}

// Server frames

func newServerFrame(c string) ServerFrame {
	return ServerFrame{
		Command: c,
		Headers: map[string][]byte{},
	}
}

func newCmdConnected() ServerFrame {
	sf := newServerFrame(CmdConnected)

	// Must
	sf.Headers["version"] = nil

	// May
	sf.Headers["heart-beat"] = nil
	sf.Headers["session"] = nil
	sf.Headers["server"] = nil

	return sf
}

func newCmdMessage() ServerFrame {
	sf := newServerFrame(CmdMessage)

	// Must
	// Destination should be identical to the one used
	// in the corresponding SEND frame
	sf.Headers["destination"] = nil
	sf.Headers["message-id"] = nil
	sf.Headers["subscription"] = nil

	// Must conditionally, if subscription requires ack
	// Used to relate to a subsequent ACK or NACK frame.
	sf.Headers["ack"] = nil

	// Should, if a body is present
	sf.Headers["content-length"] = nil
	sf.Headers["content-type"] = nil

	// And all user defined headers sent in source frame

	return sf
}

func newCmdReceipt() ServerFrame {
	sf := newServerFrame(CmdReceipt)

	sf.Headers["receipt-id"] = nil

	return sf
}

func newCmdError() ServerFrame {
	sf := newServerFrame(CmdError)

	// Should
	sf.Headers["message"] = nil

	// Should conditionally, if request contained receipt header
	sf.Headers["receipt-id"] = nil

	// Should conditionally, if body included
	sf.Headers["content-length"] = nil
	sf.Headers["content-type"] = nil

	// May contain a body with more detail error info.

	return sf
}

// Client frames

// Any client frame other than CONNECT MAY specify a receipt header

func newCmdConnect(host string) frame {
	f := frame{
		command:        CmdConnect,
		body:           nil,
		expectResponse: true, // returns CONNECTED frame
		headers:        headers{},
	}

	// Must
	f.addHeaderAcceptVersion(SupportedVersions)
	f.addHeaderHost(host)

	// May
	// login
	// passcode
	// heartbeat

	return f
}

func newCmdDisconnect(rcpt string) frame {
	f := frame{
		command:        CmdDisconnect,
		headers:        headers{},
		body:           nil,
		expectResponse: false,
	}

	if rcpt != "" {
		f.addHeaderReceipt(rcpt)
		f.expectResponse = true
	}

	return f
}

func newCmdSend(queueName string, body []byte, rcpt, txn string, custom ...Header) frame {
	f := frame{
		command:        CmdSend,
		headers:        headers{},
		body:           body,
		expectResponse: false,
		// TODO: consider: expectErrorResponse: true,
	}

	// Must
	f.addHeaderDestination(queueName)

	// Should
	f.addHeaderContentType(ContentTypeText)
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

func newCmdAck(msgID, rcpt, txn string) frame {
	f := frame{
		command:        CmdAck,
		headers:        headers{},
		body:           nil,
		expectResponse: false,
	}

	// Must
	f.addHeaderID(msgID)

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

func newCmdNack(msgID, txn, rcpt string) frame {
	f := frame{
		command:        CmdNack,
		headers:        headers{},
		body:           nil,
		expectResponse: false,
	}

	// Must
	f.addHeaderID(msgID)

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

func newCmdSubscribe(queueName, subID, rcpt string, am int) (frame, error) {
	f := frame{
		command:        CmdSubscribe,
		headers:        headers{},
		body:           nil,
		expectResponse: false,
		// TODO: consider: expectErrorResponse: true,
	}

	// Must
	f.addHeaderID(subID)
	f.addHeaderDestination(queueName)

	// Allows
	a, err := parseAckModeInt(am)
	if err != nil {
		return f, err
	}
	f.addHeaderAck(a)

	if rcpt != "" {
		f.addHeaderReceipt(rcpt)
		f.expectResponse = true
	}

	return f, nil
}

func newCmdUnsubscribe(subID, rcpt string) frame {
	f := frame{
		command:        CmdUnsubscribe,
		headers:        headers{},
		body:           nil,
		expectResponse: false,
	}

	// Must
	f.addHeaderID(subID)

	// Allows
	if rcpt != "" {
		f.addHeaderReceipt(rcpt)
		f.expectResponse = true
	}

	return f
}

func newCmdBegin(txn, rcpt string) frame {
	f := frame{
		command:        CmdBegin,
		headers:        headers{},
		body:           nil,
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
		command:        CmdAbort,
		headers:        headers{},
		body:           nil,
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
		command:        CmdCommit,
		headers:        headers{},
		body:           nil,
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

func formatRequest(f frame) []byte {
	var b bytes.Buffer

	b.WriteString(f.command)
	b.WriteByte(byteLineFeed)

	for k, v := range f.headers {
		b.WriteString(k)
		b.WriteByte(byteColon)
		b.Write(v)
		b.WriteByte(byteLineFeed)
	}

	b.WriteByte(byteLineFeed)
	b.Write(f.body)
	b.WriteByte(byteLineFeed)

	b.WriteByte(byteNull)

	return b.Bytes()
}

func sendRequest(c io.ReadWriter, f frame) ([]byte, error) {
	req := formatRequest(f)
	_, err := c.Write(req)
	if err != nil {
		return nil, err
	}

	if !f.expectResponse {
		return nil, nil
	}

	r, err := bufio.NewReader(c).ReadBytes(byteNull)
	if err != nil {
		return nil, err
	}

	return r, nil
}

func NewConnection(host string, port int) (net.Conn, error) {
	var b bytes.Buffer
	b.WriteString(host)
	b.WriteByte(byteColon)
	b.Write(intToByteSlice(port))
	return net.Dial("tcp", b.String())
}

func Connect(conn net.Conn) (Client, []byte, error) {
	resp, err := sendRequest(conn, newCmdConnect(conn.RemoteAddr().String()))
	if err != nil {
		return Client{}, nil, fmt.Errorf("failed connecting: %s", err)
	}

	return Client{
		connection: conn,
	}, resp, nil
}

func (c *Client) Disconnect() error {
	// Graceful shutdown: send disconnect frame, check rcpt received, then close socket.
	// Do not send any more frames after the DISCONNECT frame has been sent.

	rcptID := "rcpt-disconnect-123"
	resp, err := sendRequest(c.connection, newCmdDisconnect(rcptID))
	if err != nil {
		return fmt.Errorf("failed sending disconnect: %s", err)
	}

	c.Response = resp

	sf, err := ParseResponse(resp)
	if err != nil {
		return errors.New("failed disconnecting, unable to parse response: " + err.Error())
	}

	v, ok := sf.Headers[HeaderReceiptID]
	if !ok || sf.Command != CmdReceipt {
	}

	if string(v) != rcptID {
		return errors.New(
			"failed disconnecting, invalid receipt id in response - " +
				"expected: " + rcptID + ", got: " + string(v))
	}

	return c.connection.Close()
}

func (c *Client) Send(queue string, msg []byte, rcpt, txn string, custom ...Header) ([]byte, error) {
	// Default ack mode is auto.
	// Server will not send a response unless either:
	// a - receipt header is set.
	// b - the server sends an ERROR response and disconnects.

	resp, err := sendRequest(
		c.connection, newCmdSend(
			queue,
			msg,
			rcpt,
			txn,
			custom...))
	if err != nil {
		// If the server returned an error here then it will also have disconnected.
		return nil, fmt.Errorf("failed enqueue: %s", err)
	}
	// No error, implies a successful send, despite no response.

	return resp, nil
}

func (c *Client) Ack(msgID, rcpt, transactionID string) error {
	_, err := sendRequest(c.connection, newCmdAck(msgID, rcpt, transactionID))
	if err != nil {
		return fmt.Errorf("failed sending ack: %s", err)
	}

	return nil
}

func (c *Client) Nack(msgID, transactionID, rcpt string) error {
	_, err := sendRequest(c.connection, newCmdNack(msgID, transactionID, rcpt))
	if err != nil {
		return fmt.Errorf("failed sending nack: %s", err)
	}

	return nil
}

func intToByteSlice(n int) []byte {
	return []byte(strconv.Itoa(n))
}

func (c *Client) Subscribe(queueName, rcpt string, am int) (Subscription, []byte, error) {
	// Simple approach of setting subscription id to list index.
	// Might need to be enhanced in future.
	subID := strconv.Itoa(len(c.Subscriptions))

	sub := Subscription{
		ID: subID,
		Channel: Channel{
			Name: queueName,
			Type: "not implemented",
		},
		AckMode: am,
	}

	f, err := newCmdSubscribe(queueName, subID, rcpt, am)
	if err != nil {
		return sub, nil, fmt.Errorf("failed creating subscribe command: %s", err)
	}

	resp, err := sendRequest(c.connection, f)
	if err != nil {
		return sub, nil, fmt.Errorf("failed subscribing: %s", err)
	}

	c.Subscriptions = append(c.Subscriptions, sub)

	return sub, resp, nil
}

func (c *Client) Unsubscribe(subID, rcpt string) ([]byte, error) {
	resp, err := sendRequest(c.connection, newCmdUnsubscribe(subID, rcpt))
	if err != nil {
		return nil, fmt.Errorf("failed unsubscribing: %s", err)
	}

	return resp, nil
}

func (c *Client) Receive() (chan []byte, chan error) {
	c.Response = nil
	reader := bufio.NewReader(c.connection)

	recvChan := make(chan []byte)
	errChan := make(chan error)

	go func() {
		for {
			resp, err := reader.ReadBytes(byteNull)
			if err != nil {
				errChan <- fmt.Errorf("failed reading response: %s", err)
				continue
			}

			recvChan <- resp

			// Remove end of packet newline,
			// otherwise next packet starts with newline.
			b, err := reader.Peek(1)
			if err != nil {
				errChan <- fmt.Errorf("failed reading bytes after \\x0: %s", err)
			}
			if b[0] == byteLineFeed {
				_, err := reader.ReadByte()
				if err != nil {
					errChan <- fmt.Errorf("failed reading final newline: %s", err)
				}
			}
		}
	}()

	return recvChan, errChan
}

func (c *Client) Begin(transactionID, rcpt string) ([]byte, error) {
	resp, err := sendRequest(c.connection, newCmdBegin(transactionID, rcpt))
	if err != nil {
		return nil, fmt.Errorf("failed transaction begin: %s", err)
	}

	return resp, nil
}

func (c *Client) Abort(transactionID, rcpt string) ([]byte, error) {
	resp, err := sendRequest(c.connection, newCmdAbort(transactionID, rcpt))
	if err != nil {
		return nil, fmt.Errorf("failed abort: %s", err)
	}

	return resp, nil
}

func (c *Client) Commit(transactionID, rcpt string) ([]byte, error) {
	resp, err := sendRequest(c.connection, newCmdCommit(transactionID, rcpt))
	if err != nil {
		return nil, fmt.Errorf("failed commit: %s", err)
	}

	return resp, nil
}

func ParseResponse(s []byte) (ServerFrame, error) {
	var cmd string

	sz := len(s)

	headerStart := 0
	for i := 0; i < sz; i++ {
		if s[i] == byteLineFeed {
			cmd = string(s[:i])
			headerStart = i + 1
			break
		}
	}

	if cmd == "" {
		return ServerFrame{}, errors.New("failed parsing invalid message, no lines")
	}

	f := newServerFrame(cmd)

	// Rest of f, without command.
	msg := s[headerStart:]

	tokStart := 0
	var k []byte
	var v []byte
	i := 0

Headers:
	for {
	Key:
		for {
			if msg[i] == byteColon {
				k = msg[tokStart:i]
				tokStart = i + 1
				break Key
			}
			i++
		}
	Val:
		for {
			if msg[i] == byteLineFeed {
				v = msg[tokStart:i]
				f.Headers[string(k)] = v
				i++
				tokStart = i
				if msg[i] == byteLineFeed {
					i++
					break Headers
				}
				break Val
			}
			i++
		}
	}

	f.Body = msg[i:]

	return f, nil
}
