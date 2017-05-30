package stompingophers

import (
	"testing"
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

func Benchmark_newCmdConnect(b *testing.B) {
	b.ReportAllocs()

	h := "127.0.0.1"

	for i := 0; i < b.N; i++ {
		_ = newCmdConnect(h)
	}
}
