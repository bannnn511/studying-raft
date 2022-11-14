package main

import (
	"fmt"
	"time"
)

func main() {
	testChan := make(chan bool)

	go func() {
		ticker := time.NewTicker(1 * time.Second)
		for {
			<-ticker.C
			testChan <- true
		}
	}()

	fmt.Print("fsafdsfs")
	for range testChan {
		fmt.Println("here her")
	}
}
