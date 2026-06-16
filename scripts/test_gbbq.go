package main

import (
	"fmt"
	"time"

	"github.com/injoyai/tdx"
)

func main() {
	// 定义测试服务器池
	servers := []struct {
		Name string
		Addr string
	}{
		{"华为云代理节点 (原代码使用)", "124.71.187.122:7709"},
		{"深圳电信大世界 (高带宽推荐)", "119.147.212.81:7709"},
		{"北京联通主干节点 (备用推荐)", "119.119.115.132:7709"},
	}

	fmt.Println("==================================================")
	fmt.Println("TDX GBBQ DOWNLOAD DIAGNOSTIC TEST")
	fmt.Println("==================================================")

	for _, s := range servers {
		fmt.Printf("\n[Test] Target: %s (%s)\n", s.Name, s.Addr)
		
		// 1. 测试拨号连接性能
		dialStart := time.Now()
		cli, err := tdx.Dial(s.Addr)
		if err != nil {
			fmt.Printf("  [-] Dial failed in %v: %v\n", time.Since(dialStart), err)
			continue
		}
		fmt.Printf("  [+] Dial successfully connected in %v\n", time.Since(dialStart))
		
		// 2. 挂载 20 秒强超时，确保测试任务绝不永久挂起
		cli.SetTimeout(time.Second * 20)
		
		// 3. 测试 GBBQ 数据库下载性能与大小
		fmt.Println("  [*] Launching GetGbbqAll() request...")
		fetchStart := time.Now()
		gbbq, err := cli.GetGbbqAll()
		fetchDuration := time.Since(fetchStart)
		
		if err != nil {
			fmt.Printf("  [-] GetGbbqAll failed after %v: %v\n", fetchDuration, err)
		} else {
			fmt.Printf("  [+] GetGbbqAll SUCCESS!\n")
			fmt.Printf("      - Total Download Time: %v\n", fetchDuration)
			fmt.Printf("      - Total Unique Stocks parsed: %d\n", len(gbbq))
		}
		
		cli.Close()
	}
	
	fmt.Println("\n==================================================")
	fmt.Println("GBBQ Diagnostic completed.")
	fmt.Println("==================================================")
}
