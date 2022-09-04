package test

import (
	"fmt"
	"testing"
)

func TestChan(t *testing.T) {
	c1 := make(chan int)
	close(c1)
	i, ok := <-c1
	fmt.Println(i, ok)
}
