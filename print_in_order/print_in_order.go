package main

import "fmt"
//https://leetcode.com/problems/print-zero-even-odd/
//goroutine 若不刻意控制，将无法保证执行的先后顺序，因此本题就是要考核对 goroutine 顺序控制的能力。
func first(streamSync [3]chan interface{})  {
	fmt.Println("~~first~~")
	streamSync[0] <- nil
	return
}

func second(streamSync [3]chan interface{})  {
	<-streamSync[0]
	fmt.Println("~~second~~")
	streamSync[1]<-nil
	return
}

func third(streamSync [3]chan interface{})  {
	<- streamSync[1]
	fmt.Println("~~third~~")
	streamSync[2] <- nil
	return
}

func printInOrder(callOrder [3]int)  {
	input := callOrder
	var streamSync [3]chan interface{}
	for i := range streamSync{
		streamSync[i] = make(chan interface{})
	}
	var funcTable = map[int]func([3]chan interface{}){
		1:first,
		2:second,
		3:third,
	}
	for _,v := range input {
		go funcTable[v](streamSync)
	}
	<-streamSync[2]
}

func main() {
	var testCase = [][3]int{
		{1,	2, 3},
		{1, 3, 2},
		{2, 1, 3},
		{2, 3, 1},
		{3, 1, 2},
		{3, 2, 1},
	}
	for _,v := range testCase {
		printInOrder(v)
		fmt.Println()
		fmt.Println()
	}
}
