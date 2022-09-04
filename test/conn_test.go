package test

import (
	"bufio"
	"fmt"
	"go-tbs/pkg/bio"
	"net"
	"sync"
	"testing"
	"time"
)

func TestConn(t *testing.T) {
	wg := sync.WaitGroup{}
	wg.Add(2)
	go func() {
		listener, err := net.Listen("tcp", "127.0.0.1:9000")
		if err != nil {
			fmt.Println(err)
		}
		_, err = listener.Accept()
		if err != nil {
			fmt.Println(err)
		} else {
			fmt.Println("accept")
		}

		//r := bufio.NewReader(conn)
		//wb := bio.NewWriteBuffer(bufio.NewWriter(conn))

		//err = wb.WriteString("123")
		//if err != nil {
		//	fmt.Println(err)
		//}
		//err = wb.Flush()
		//if err != nil {
		//	fmt.Println(err)
		//}

		//for {
		//	time.Sleep(time.Second)
		//	_, err := bio.ReadString(r)
		//	if err != nil {
		//		fmt.Println(err)
		//	}
		//}
	}()

	time.Sleep(time.Second)

	go func() {
		conn, err := net.Dial("tcp", "127.0.0.1:9000")
		if err != nil {
			panic(err)
		} else {
			fmt.Println("dial")
		}
		conn.Close()
		fmt.Println("close")
		conn.Close()
		r := bufio.NewReader(conn)

		s, err := bio.ReadString(r)
		if err != nil {
			fmt.Println(err)
		}
		fmt.Println(s)

		//err = conn.Close()
		//if err != nil {
		//	fmt.Println(err)
		//}
		//
		//s, err = bio.ReadString(r)
		//if err != nil {
		//	fmt.Println(err)
		//}
		//fmt.Println(s)
	}()
	wg.Wait()
}
