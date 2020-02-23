package main

import "fmt"
//两个不同的线程将会共用一个 FooBar 实例。其中一个线程将会调用 foo() 方法，另一个线程将会调用 bar() 方法。
//
//请设计修改程序，以确保 "foobar" 被输出 n 次。
//
//来源：力扣（LeetCode）
//链接：https://leetcode-cn.com/problems/print-foobar-alternately
//著作权归领扣网络所有。商业转载请联系官方授权，非商业转载请注明出处。
type FooBar struct {
	n int
	bar2foo chan struct{}
	foo2bar chan struct{}
	bar2end chan struct{}

}

func (this *FooBar)Foo(printFoo func())  {
	for i := 0;i<2*this.n;i++ {
		<-this.bar2foo
		printFoo()
		i++
		this.foo2bar <- struct{}{}
	}
	<-this.bar2foo
}

func (this *FooBar)Bar(printBar func())  {
	for i:=0;i<2*this.n ;i++  {
		<-this.foo2bar
		printBar()
		i++
		this.bar2foo <- struct{}{}
	}
	this.bar2end <- struct{}{} //zuihou shuchu de yiding shi bar
}
func main() {
	foobar := &FooBar{
		n:       10,
		bar2foo: make(chan struct{}),
		foo2bar: make(chan struct{}),
		bar2end: make(chan struct{}),
	}
	go foobar.Foo(func() {
		fmt.Print("foo")
	})
	go foobar.Bar(func() {
		fmt.Print("bar")
	})
	foobar.bar2foo <- struct{}{}//ziqidong
	<-foobar.bar2end
}
