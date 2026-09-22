package main

import (
	"fmt"
	"runtime"
	"strings"
	"time"

	"github.com/injoyai/tdx"
	"github.com/injoyai/tdx/protocol"
)

type TestCase struct {
	Code string
	Name string
}

func main() {
	fmt.Println("==============================================================")
	fmt.Println("TDX 中证2000 / 000851 行情测试")
	fmt.Println("==============================================================")
	fmt.Printf("Go version: %s\n", runtime.Version())
	fmt.Printf("Time: %s\n", time.Now().Format(time.RFC3339))
	fmt.Println()

	// ----------------------------------------------------------
	// 连接 TDX
	// ----------------------------------------------------------
	fmt.Println("Connecting to TDX...")

	client, err := tdx.DialDefault()
	if err != nil {
		fmt.Printf("ERROR: TDX connection failed: %v\n", err)
		return
	}
	defer client.Close()

	fmt.Println("TDX connection OK")
	fmt.Println()

	// ----------------------------------------------------------
	// 1. 查看全部 spblock
	// ----------------------------------------------------------
	fmt.Println("==============================================================")
	fmt.Println("[1] GetSpBlock()")
	fmt.Println("==============================================================")

	blocks, err := client.GetSpBlock()
	if err != nil {
		fmt.Printf("GetSpBlock ERROR: %v\n", err)
		return
	}

	fmt.Printf("GetSpBlock OK, blocks = %d\n", len(blocks))
	fmt.Println()

	for _, block := range blocks {
		fmt.Printf("Name=%-20s Codes=%d\n",
			block.Name,
			len(block.Codes))
	}

	fmt.Println()

	// ----------------------------------------------------------
	// 2. 查找中证2000
	// ----------------------------------------------------------
	fmt.Println("==============================================================")
	fmt.Println("[2] 中证2000 spblock")
	fmt.Println("==============================================================")

	found := false

	for _, block := range blocks {
		if strings.Contains(block.Name, "中证2000") {
			found = true

			fmt.Printf("Name: %s\n", block.Name)
			fmt.Printf("Codes: %d\n", len(block.Codes))
			fmt.Println("First 30 codes:")

			for i, code := range block.Codes {
				if i >= 30 {
					break
				}
				fmt.Printf("  %s\n", code)
			}
		}
	}

	if !found {
		fmt.Println("未找到中证2000")
	}

	fmt.Println()

	// ----------------------------------------------------------
	// 3. 测试 stockA 当前使用的 000851
	// ----------------------------------------------------------
	fmt.Println("==============================================================")
	fmt.Println("[3] 测试 stockA 当前代码 000851")
	fmt.Println("==============================================================")

	testCases := []TestCase{
		{"000851", "000851"},
		{"sh000851", "sh000851"},
		{"sz000851", "sz000851"},
	}

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
	fmt.Printf("Code: %s\n", tc.Code)
	fmt.Println("--------------------------------------------------------------")

	// ----------------------------------------------------------
	// GetKlineDayAll
	// ----------------------------------------------------------
	fmt.Println("[1] GetKlineDayAll")

	start := time.Now()

	resp1, err1 := client.GetKlineDayAll(tc.Code)

	elapsed := time.Since(start)

	if err1 != nil {
		fmt.Printf("  ERROR: %v\n", err1)
	} else {
		printKlineResult(resp1, elapsed)
	}

	// ----------------------------------------------------------
	// GetIndexDayAll
	// ----------------------------------------------------------
	fmt.Println("[2] GetIndexDayAll")

	start = time.Now()

	resp2, err2 := client.GetIndexDayAll(tc.Code)

	elapsed = time.Since(start)

	if err2 != nil {
		fmt.Printf("  ERROR: %v\n", err2)
	} else {
		printKlineResult(resp2, elapsed)
	}
}

func printKlineResult(resp *protocol.KlineResp, elapsed time.Duration) {
	if resp == nil {
		fmt.Println("  response = nil")
		return
	}

	fmt.Printf("  count: %d\n", resp.Count)
	fmt.Printf("  list length: %d\n", len(resp.List))
	fmt.Printf("  elapsed: %s\n", elapsed)

	if len(resp.List) == 0 {
		fmt.Println("  first_date: -")
		fmt.Println("  last_date:  -")
		return
	}

	first := resp.List[0]
	last := resp.List[len(resp.List)-1]

	fmt.Printf("  first_date: %s\n",
		first.Time.Format("2006-01-02"))

	fmt.Printf("  last_date:  %s\n",
		last.Time.Format("2006-01-02"))

	fmt.Printf("  first_close: %v\n", first.Close)
	fmt.Printf("  last_close:  %v\n", last.Close)
}
