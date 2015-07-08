package main

import (
		"./kproxy"
		"fmt"
		"os"
		"runtime"
		"flag"
	   )

func main () {
    confFile := flag.String("f", "./conf/kproxy.yaml", "kproxy config file")
	flag.Parse()
	kproxy, err := kproxy.NewKProxy(*confFile)
	if err != nil {
		fmt.Println("fail to new kproxy: ", err)
		os.Exit(-1)
	}
	runtime.GOMAXPROCS(runtime.NumCPU())
	kproxy.Run()
	os.Exit(-1)
}
