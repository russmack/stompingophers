package stompingophers

import (
	"testing"

	"bufio"
	"bytes"
	"net"
	"strconv"
	"sync"
)

func Benchmark_formatRequest(b *testing.B) {
	b.ReportAllocs()

	f := frame{command: "command",
		headers:        headers{},
		body:           []byte("body"),
		expectResponse: false}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = formatRequest(f)
	}
}

func Test_ParseResponse(t *testing.T) {
	s := []byte(`MESSAGE
content-length:27
expires:0
destination:/queue/nooq
subscription:0
priority:4
message-id:ID\cRuss-MBP-53014-1496183632740-3\c2\c-1\c1\c16791
content-type:text/plain
timestamp:1496183739815

Well, hello,: number 16790!
`)

	sf, err := ParseResponse(s)
	if err != nil {
		t.Error("failed parsing response:", err)
	}

	expected := []byte("Well, hello,: number 16790!\n")
	if !bytes.Equal(sf.Body, expected) {
		t.Error("Expected:", string(expected), "\nGot:", string(sf.Body))
	}
}

func Benchmark_ParseResponse(b *testing.B) {
	b.ReportAllocs()

	s := []byte(`MESSAGE
content-length:27
expires:0
destination:/queue/nooq
subscription:0
priority:4
message-id:ID\cRuss-MBP-53014-1496183632740-3\c2\c-1\c1\c16791
content-type:text/plain
timestamp:1496183739815

Well, hello, number 16790!
`)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = ParseResponse(s)
	}
}

func Benchmark_NewConnection(b *testing.B) {
	b.ReportAllocs()

	h := "127.0.0.1"
	p := 61613

	port := ":" + strconv.Itoa(p)

	ln, err := net.Listen("tcp", port)
	if err != nil {
		b.Fatal(err)
	}

	done := make(chan bool)

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			conn, err := NewConnection(h, p)
			if err != nil {
				b.Fatal(err)
			}
			conn.Close()

			if i >= b.N-1 {
				done <- true
				ln.Close()
			} else {
				done <- false
			}
		}
		wg.Done()
	}()

	wg.Add(1)
	go func() {
		for {
			_, err := ln.Accept()
			if err != nil {
				b.Fatal(err)
			}
			//c.Close()

			exit := <-done
			if exit {
				break
			}
		}
		wg.Done()
	}()

	wg.Wait()
}

func Benchmark_Connect(b *testing.B) {
	b.ReportAllocs()

	response := `CONNECTED
server:MockMQ/1.00.0
heart-beat:0,0
session:ID:Russ-MBP-53014-1496183632740-3:9384
version:1.2

` + "\000"

	cliconn, srvconn := net.Pipe()

	go func() {
		for {
			_, _ = bufio.NewReader(srvconn).ReadBytes('\000')
			srvconn.Write([]byte(response))
		}
	}()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, _ = Connect(cliconn)
	}

	cliconn.Close()
	srvconn.Close()
}

func Benchmark_newCmdConnect(b *testing.B) {
	b.ReportAllocs()

	h := "127.0.0.1"

	b.ResetTimer()
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

	b.ResetTimer()
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

	response := `CONNECTED
server:MockMQ/1.00.0
heart-beat:0,0
session:ID:Russ-MBP-53014-1496183632740-3:9384
version:1.2

` + "\000"

	subscribeResponse := `RECEIPT
receipt-id:mysubrcpt

` + "\000"

	cliconn, srvconn := net.Pipe()

	go func() {
		for {
			_, _ = bufio.NewReader(srvconn).ReadBytes('\000')
			srvconn.Write([]byte(response))

			_, _ = bufio.NewReader(srvconn).ReadBytes('\000')
			srvconn.Write([]byte(subscribeResponse))

		}
	}()

	client, _, err := Connect(cliconn)
	if err != nil {
		b.Error(err)
	}

	queue := "/queue/nooq"
	rcpt := "mysubrcpt"
	ackmode := AckModeAuto

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, _ = client.Subscribe(queue, rcpt, ackmode)
	}
}
