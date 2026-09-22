package main

import (
	"fmt"
	"runtime"
	"time"

	"github.com/injoyai/tdx"
	"github.com/injoyai/tdx/protocol"
)

type TestCase struct {
	Code string
	Name string
}

var testCases = []TestCase{
    {"932000", "中证2000"},
    {"sh932000", "中证2000(sh)"},
    {"sz932000", "中证2000(sz)"},
}

func main() {
	fmt.Println("==============================================================")
	fmt.Println("stockA TDX Index API Comparison Test")
	fmt.Println("==============================================================")
	fmt.Printf("Go version: %s\n", runtime.Version())
	fmt.Printf("Time: %s\n", time.Now().Format(time.RFC3339))
	fmt.Println()

	fmt.Println("Connecting to TDX...")
	client, err := tdx.DialDefault()
	if err != nil {
		fmt.Printf("ERROR: TDX connection failed: %v\n", err)
		return
	}
	defer client.Close()

	fmt.Println("TDX connection OK")
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
	// API 1: 普通股票 K 线
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
	// API 2: 指数 K 线
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

	fmt.Printf("  first_date: %s\n", first.Time.Format("2006-01-02"))
	fmt.Printf("  last_date:  %s\n", last.Time.Format("2006-01-02"))

	fmt.Printf(
		"  first_close: %.4f\n",
		first.Close,
	)

	fmt.Printf(
		"  last_close:  %.4f\n",
		last.Close,
	)
}
