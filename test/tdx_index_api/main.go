package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/injoyai/tdx"
	"github.com/injoyai/tdx/protocol"
)

var indexList = []string{
	"sh.000001",
	"sz.399001",
	"sz.399006",
	"sh.000688",
	"bj.899050",
	"sh.000016",
	"sh.000300",
	"sh.000905",
	"sh.000852",
	"sh.000851",
	"sz.399303",
	"sz.399330",
	"sh.000090",
	"sz.399324",
	"sh.000015",
	"sh.000827",
	"sz.399317",
	"sz.399807",
	"sz.399812",
	"sz.399354",
	"sz.399673",
	"sz.399285",
	"sz.399008",
	"sz.399993",
	"sz.399975",
	"sz.399986",
	"sz.399932",
	"sz.399933",
	"sz.399967",
	"sz.399989",
	"sz.399971",
	"sz.399997",
	"sh.000934",
	"sh.000935",
	"sz.399990",
	"sz.399998",
	"sz.399974",
}

func main() {
	outPath := flag.String(
		"out",
		"temp_index_kline.csv",
		"output CSV",
	)
	flag.Parse()

	fmt.Printf("==============================================================\n")
	fmt.Printf("stockA index fetch integration test\n")
	fmt.Printf("==============================================================\n")
	fmt.Printf("Indices: %d\n", len(indexList))
	fmt.Printf("Output:  %s\n\n", *outPath)

	// 1. 原有37个指数完整历史K线测试
	fetch(*outPath)

	// 2. CSI 2000 候选代码直接探测
	probeCSI2000()

	// 3. 从 TDX 服务器证券代码表反查“中证2000”
	probeCSI2000FromCodeList()

	fmt.Println()
	fmt.Println("==============================================================")
	fmt.Println("All tests completed")
	fmt.Println("==============================================================")
}

// ============================================================================
// 1. 原有37个指数完整K线测试
// ============================================================================

func fetch(outPath string) {
	file, err := os.Create(outPath)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	err = writer.Write([]string{
		"code",
		"date",
		"open",
		"high",
		"low",
		"close",
		"volume",
		"amount",
	})
	if err != nil {
		panic(err)
	}

	jobs := make(chan string)
	var wg sync.WaitGroup
	var mu sync.Mutex

	success := make(map[string]int)
	failed := make([]string, 0)

	// 8个并发连接
	for i := 0; i < 8; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			client, err := tdx.DialDefault()
			if err != nil {
				fmt.Printf("CONNECT ERROR: %v\n", err)
				return
			}
			defer client.Close()

			for code := range jobs {
				tdxCode := strings.ReplaceAll(code, ".", "")

				resp, err := client.GetIndexDayAll(tdxCode)

				if err != nil || resp == nil || len(resp.List) == 0 {
					fmt.Printf(
						"❌ %s: no data (%v)\n",
						code,
						err,
					)

					mu.Lock()
					failed = append(failed, code)
					mu.Unlock()

					continue
				}

				rows := make([][]string, 0, len(resp.List))

				for _, bar := range resp.List {
					rows = append(rows, []string{
						code,
						bar.Time.Format("2006-01-02"),
						fmt.Sprintf("%.3f", float64(bar.Open)/1000),
						fmt.Sprintf("%.3f", float64(bar.High)/1000),
						fmt.Sprintf("%.3f", float64(bar.Low)/1000),
						fmt.Sprintf("%.3f", float64(bar.Close)/1000),
						strconv.FormatInt(int64(bar.Volume), 10),
						fmt.Sprintf("%.3f", float64(bar.Amount)/1000),
					})
				}

				// 确保按日期排序
				sort.Slice(rows, func(i, j int) bool {
					return rows[i][1] < rows[j][1]
				})

				mu.Lock()

				for _, row := range rows {
					_ = writer.Write(row)
				}

				success[code] = len(rows)

				fmt.Printf(
					"✅ %s: %d rows, %s -> %s\n",
					code,
					len(rows),
					rows[0][1],
					rows[len(rows)-1][1],
				)

				mu.Unlock()
			}
		}()
	}

	for _, code := range indexList {
		jobs <- code
	}

	close(jobs)
	wg.Wait()

	fmt.Println()
	fmt.Println("==============================================================")
	fmt.Println("Go fetch summary")
	fmt.Println("==============================================================")
	fmt.Printf("Success: %d / %d\n", len(success), len(indexList))
	fmt.Printf("Failed:  %d\n", len(failed))

	if len(failed) > 0 {
		fmt.Println("Failed codes:")
		for _, code := range failed {
			fmt.Printf("  %s\n", code)
		}
	}

	fmt.Printf("\nCSV written: %s\n", outPath)
}

// ============================================================================
// 2. 直接测试中证2000官方代码以及几个候选 TDX 代码
// ============================================================================

func probeCSI2000() {
	fmt.Println()
	fmt.Println("==============================================================")
	fmt.Println("CSI 2000 TDX code probe")
	fmt.Println("==============================================================")

	// 注意：
	// 932000 是官方中证2000代码，但 TDX K线接口要求6位代码，
	// 因此这里仍然保留直接测试，以观察服务器行为。
	//
	// sh000851 / sh000852 用于验证此前怀疑的代码。
	//
	// 不再测试 sz000852，因为 000852 是上海指数，
	// 查询 sz000852 会触发 TDX v0.0.83 decoder 异常。
	candidates := []string{
		"932000",
		"sh932000",
		"sz932000",
		"sh000851",
		"sh000852",
	}

	client, err := tdx.DialDefault()
	if err != nil {
		fmt.Printf("CONNECT ERROR: %v\n", err)
		return
	}
	defer client.Close()

	for _, code := range candidates {
		fmt.Println()
		fmt.Printf("--------------------------------------------------------------\n")
		fmt.Printf("Code: %s\n", code)

		resp, err := client.GetIndexDayAll(code)

		if err != nil {
			fmt.Printf("ERROR: %v\n", err)
			continue
		}

		if resp == nil {
			fmt.Println("NO RESPONSE")
			continue
		}

		if len(resp.List) == 0 {
			fmt.Println("ROWS: 0")
			continue
		}

		fmt.Printf("ROWS: %d\n", len(resp.List))

		first := resp.List[0]
		last := resp.List[len(resp.List)-1]

		fmt.Printf(
			"FIRST: %s O=%.3f H=%.3f L=%.3f C=%.3f\n",
			first.Time.Format("2006-01-02"),
			float64(first.Open)/1000,
			float64(first.High)/1000,
			float64(first.Low)/1000,
			float64(first.Close)/1000,
		)

		fmt.Printf(
			"LAST:  %s O=%.3f H=%.3f L=%.3f C=%.3f\n",
			last.Time.Format("2006-01-02"),
			float64(last.Open)/1000,
			float64(last.High)/1000,
			float64(last.Low)/1000,
			float64(last.Close)/1000,
		)

		fmt.Printf(
			"LAST:  Volume=%d Amount=%.3f\n",
			int64(last.Volume),
			float64(last.Amount)/1000,
		)

		fmt.Println("Last 5 rows:")

		start := len(resp.List) - 5
		if start < 0 {
			start = 0
		}

		for i := start; i < len(resp.List); i++ {
			bar := resp.List[i]

			fmt.Printf(
				"  %s O=%.3f H=%.3f L=%.3f C=%.3f V=%d A=%.3f\n",
				bar.Time.Format("2006-01-02"),
				float64(bar.Open)/1000,
				float64(bar.High)/1000,
				float64(bar.Low)/1000,
				float64(bar.Close)/1000,
				int64(bar.Volume),
				float64(bar.Amount)/1000,
			)
		}
	}
}

// ============================================================================
// 3. 从 TDX 服务器证券代码表反查“中证2000”
// ============================================================================
//
// 核心目的：
// 不再猜 sh000851 / sh000852 等代码，
// 直接从 TDX 服务器返回的证券代码表中寻找：
//
//     名称 = 中证2000
//
// 或名称中包含：
//
//     中证2000
//     2000
//
// 如果这里找到：
//
//     sh.xxxxxx -> 中证2000
//
// 再用 GetIndexDayAll(xxxxxx) 验证其K线。
//
// 如果完全找不到，则说明当前 TDX 服务器代码表没有提供
// “中证2000”这个指数行情代码。

func probeCSI2000FromCodeList() {
	fmt.Println()
	fmt.Println("==============================================================")
	fmt.Println("TDX server code-list reverse lookup: CSI 2000")
	fmt.Println("==============================================================")

	client, err := tdx.DialDefault()
	if err != nil {
		fmt.Printf("CONNECT ERROR: %v\n", err)
		return
	}
	defer client.Close()

	for _, market := range []string{"SH", "SZ"} {
		var exchange protocol.Exchange

		switch market {
		case "SH":
			exchange = protocol.ExchangeSH
		case "SZ":
			exchange = protocol.ExchangeSZ
		default:
			continue
		}

		fmt.Println()
		fmt.Printf("--------------------------------------------------------------\n")
		fmt.Printf("Market: %s\n", market)

		resp, err := client.GetCodeAll(exchange)
		if err != nil {
			fmt.Printf("ERROR: %v\n", err)
			continue
		}

		if resp == nil {
			fmt.Println("NO RESPONSE")
			continue
		}

		fmt.Printf("Total codes: %d\n", len(resp.List))

		found := 0

		for _, item := range resp.List {
			name := strings.TrimSpace(item.Name)
			code := strings.TrimSpace(item.Code)

			// 目标1：精确或直接包含“中证2000”
			//
			// 目标2：包含“2000”，方便发现：
			//   中证2000
			//   国证2000
			//   其他2000指数
			if strings.Contains(name, "中证2000") ||
				strings.Contains(name, "2000") {

				found++

				fmt.Printf(
					"FOUND: %s.%s  name=%s",
					strings.ToLower(market),
					code,
					name,
				)

				// 尽量输出行情字段。
				// 不同版本的 CodeItem 字段可能有所不同，
				// 因此这里只使用当前 v0.0.83 已存在的基础字段。
				fmt.Printf(
					"  LastPrice=%v",
					item.LastPrice,
				)

				fmt.Println()
			}
		}

		fmt.Printf("Matched: %d\n", found)

		if found == 0 {
			fmt.Println("No index name containing \"中证2000\" or \"2000\" was found.")
		}
	}
}
