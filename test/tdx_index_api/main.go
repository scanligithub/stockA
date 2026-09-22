package main

import (
	"fmt"
	"os"
	"runtime/debug"
	"time"

	"github.com/injoyai/tdx"
)

type TestCase struct {
	Code string
	Name string
}

var testCases = []TestCase{
	{"000001", "上证指数"},
	{"000300", "沪深300"},
	{"000905", "中证500"},
	{"000852", "中证1000"},
	{"000688", "科创50"},
	{"399001", "深证成指"},
	{"399006", "创业板指"},
}

func main() {
	fmt.Println("==============================================================")
	fmt.Println("stockA TDX Index API Comparison Test")
	fmt.Println("==============================================================")
	fmt.Printf("Go version: %s\n", runtimeVersion())
	fmt.Printf("Time: %s\n", time.Now().Format(time.RFC3339))
	fmt.Println()

	// 使用默认 TDX Client。
	client := tdx.NewClient()

	fmt.Println("Testing APIs:")
	fmt.Println("  1. GetKlineDayAll(code)")
	fmt.Println("  2. GetIndexDayAll(code)")
	fmt.Println()

	for _, tc := range testCases {
		testOne(client, tc)
		fmt.Println()
	}

	fmt.Println("==============================================================")
	fmt.Println("Test completed")
	fmt.Println("==============================================================")
}

func testOne(client *tdx.Client, tc TestCase) {
	fmt.Println("--------------------------------------------------------------")
	fmt.Printf("Index: %s (%s)\n", tc.Name, tc.Code)
	fmt.Println("--------------------------------------------------------------")

	// ----------------------------------------------------------
	// API 1: 普通股票 K 线 API
	// ----------------------------------------------------------
	fmt.Println("[1] GetKlineDayAll")

	resp1, err1 := client.GetKlineDayAll(tc.Code)

	if err1 != nil {
		fmt.Printf("  ERROR: %v\n", err1)
	} else {
		printResult(resp1)
	}

	// ----------------------------------------------------------
	// API 2: 指数 K 线 API
	// ----------------------------------------------------------
	fmt.Println("[2] GetIndexDayAll")

	resp2, err2 := client.GetIndexDayAll(tc.Code)

	if err2 != nil {
		fmt.Printf("  ERROR: %v\n", err2)
	} else {
		printResult(resp2)
	}
}

func printResult(resp interface{}) {
	if resp == nil {
		fmt.Println("  response = nil")
		return
	}

	// 这里不依赖具体 KlineResp 的字段结构，
	// 第一阶段只确认请求是否成功以及返回对象是否为空。
	fmt.Printf("  response type: %T\n", resp)
	fmt.Printf("  response: %+v\n", resp)
}

func runtimeVersion() string {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return "unknown"
	}

	return fmt.Sprintf(
		"%s\n  main module: %s %s",
		info.GoVersion,
		info.Main.Path,
		info.Main.Version,
	)
}

func init() {
	// 确保某些 CI 环境下 stdout 不被缓冲影响观察。
	_ = os.Stdout
}
