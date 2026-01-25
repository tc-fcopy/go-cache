package main

import (
	"flag"
	"fmt"
	"log"
	lcache "github.com/tc-fcopy/go-cache"
)

func main() {

	port := flag.Int("port", 8001, "节点端口")
	nodeID := flag.String("node", "A", "节点标识符")
	flag.Parse()

	addr := fmt.Sprintf(":%d", *port)
	log.Printf("[节点%s] 已启动, 地址： %s", *nodeID, addr)

	// 创建节点
	node, err := lcache.
	select {}
}
