package main

import (
	"fmt"
	"time"

	"github.com/injoyai/tdx"
)

func main() {
	// 🚀 调换顺序：把高带宽官方主干网节点排在前面测试
	servers := []struct {
		Name string
		Addr string
	}{
		{"深圳电信大世界 (主干网推荐)", "119.147.212.81:7709"},
		{"北京联通主干节点 (备用推荐)", "119.119.115.132:7709"},
		{"华为云代理节点 (原代码使用)", "124.71.187.122:7709"},
	}

	fmt.Println("==================================================")
	fmt.Println("TDX GBBQ DOWNLOAD DIAGNOSTIC (v2 - Go Channel Safe)")
	fmt.Println("==================================================")

	for _, s := range servers {
		fmt.Printf("\n[Test] Target: %s (%s)\n", s.Name, s.Addr)
		
		dialStart := time.Now()
		cli, err := tdx.Dial(s.Addr)
		if err != nil {
			fmt.Printf("  [-] Dial failed in %v: %v\n", time.Since(dialStart), err)
			continue
		}
		fmt.Printf("  [+] Dial successfully connected in %v\n", time.Since(dialStart))
		
		cli.SetTimeout(time.Second * 10) // 10秒连接级超时
		
		fmt.Println("  [*] Launching GetGbbqAll() in a protected goroutine...")
		
		type result struct {
			Count int
			Err   error
		}
		resChan := make(chan result, 1)
		fetchStart := time.Now()
		
		// 🚀 核心升级：将下载任务丢入协程，防止其通道死锁卡死主线程
		go func() {
			gbbq, err := cli.GetGbbqAll()
			if err != nil {
				resChan <- result{0, err}
			} else {
				resChan <- result{len(gbbq), nil}
			}
		}()
		
		// 🚀 核心升级：Go 级 Select 监听，一旦 15 秒不响应，强行砸断
		select {
		case res := <-resChan:
			fetchDuration := time.Since(fetchStart)
			if res.Err != nil {
				fmt.Printf("  [-] GetGbbqAll failed after %v: %v\n", fetchDuration, res.Err)
			} else {
				fmt.Printf("  [+] GetGbbqAll SUCCESS!\n")
				fmt.Printf("      - Total Download Time: %v\n", fetchDuration)
				fmt.Printf("      - Total Unique Stocks parsed: %d\n", res.Count)
			}
		case <-time.After(15 * time.Second):
			// 强行砸断死锁
			fmt.Printf("  [-] GetGbbqAll HARD TIMEOUT (15s) triggered! Go Channel was blocked.\n")
		}
		
		cli.Close() // 显式关闭，释放连接并砸断后台协程
	}
	
	fmt.Println("\n==================================================")
	fmt.Println("GBBQ Diagnostic completed.")
	fmt.Println("==================================================")
}
