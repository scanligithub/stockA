package main

import (
	"fmt"
	"time"

	"github.com/injoyai/tdx/protocol/gbbq"
)

func main() {
	fmt.Println("==================================================")
	fmt.Println("TDX GBBQ HTTP DOWNLOAD DIAGNOSTIC TEST")
	fmt.Println("==================================================")

	start := time.Now()

	// 🚀 核心：直接调用内置的 HTTP 下载及 DBF 解码接口，绕过所有 TCP 行情协议限制
	fmt.Println("[HTTP] Initiating tdx.com.cn official ZIP download...")
	data, err := gbbq.DownloadAndDecode()
	
	duration := time.Since(start)

	if err != nil {
		fmt.Printf("[-] HTTP Download or Decode failed after %v: %v\n", duration, err)
		fmt.Println("    (Please check if the import path or method signature needs adjustment)")
		return
	}

	fmt.Printf("[+] SUCCESS! Official GBBQ downloaded and decoded.\n")
	fmt.Printf("    - Total Time Cost: %v\n", duration)
	fmt.Printf("    - Total Unique Stocks parsed: %d\n", len(data))
	fmt.Println("==================================================")
}
