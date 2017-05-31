package stompingophers

import (
	"testing"

	"bufio"
	"net"
)

func Benchmark_formatRequest(b *testing.B) {
	b.ReportAllocs()

	f := frame{command: "command", headers: headers{}, body: "body", expectResponse: false}

	for i := 0; i < b.N; i++ {
		_ = formatRequest(f)
	}
}

func Benchmark_ParseResponse(b *testing.B) {
	b.ReportAllocs()

	s := `MESSAGE
content-length:27
expires:0
destination:/queue/nooq
subscription:0
priority:4
message-id:ID\cRuss-MBP-53014-1496183632740-3\c2\c-1\c1\c16791
content-type:text/plain
timestamp:1496183739815

Well, hello, number 16790!
`

	for i := 0; i < b.N; i++ {
		_, _ = ParseResponse(s)
	}
}

func Benchmark_connect(b *testing.B) {
	b.ReportAllocs()

	queueIp := "127.0.0.1"
	queuePort := 61613

	for i := 0; i < b.N; i++ {
		_, _ = Connect(queueIp, queuePort)
	}
}

func Benchmark_newCmdConnect(b *testing.B) {
	b.ReportAllocs()

	h := "127.0.0.1"

	for i := 0; i < b.N; i++ {
		_ = newCmdConnect(h)
	}
}

func Benchmark_sendRequest(b *testing.B) {
	b.ReportAllocs()

	response := `CONNECTED
server:MockMQ/1.00.0
heart-beat:0,0
session:ID:Russ-MBP-53014-1496183632740-3:9384
version:1.2

` + "\000"

	h := "127.0.0.1"
	f := newCmdConnect(h)

	cliconn, srvconn := net.Pipe()

	go func() {
		for {
			_, _ = bufio.NewReader(srvconn).ReadBytes('\000')
			srvconn.Write([]byte(response))
		}
	}()

	for i := 0; i < b.N; i++ {
		_, err := sendRequest(cliconn, f)
		if err != nil {
			b.Error(err)
		}
	}

	cliconn.Close()
	srvconn.Close()
}

func Benchmark_subscribe(b *testing.B) {
	b.ReportAllocs()

	queueIp := "127.0.0.1"
	queuePort := 61613

	client, err := Connect(queueIp, queuePort)
	if err != nil {
		panic("failed connecting: " + err.Error())
	}

	queue := "/queue/nooq"
	rcpt := "mysubrcpt"
	ackmode := ACKMODE_AUTO

	for i := 0; i < b.N; i++ {
		_ = client.Subscribe(queue, rcpt, ackmode)
	}
}
