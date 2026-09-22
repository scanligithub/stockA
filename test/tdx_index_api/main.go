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

	// ------------------------------------------------------------
	// 1. 原有 37 个指数完整测试
	// ------------------------------------------------------------
	fetch(*outPath)

	// ------------------------------------------------------------
	// 2. 独立测试官方中证2000代码 932000
	// ------------------------------------------------------------
	probeCSI2000()

	fmt.Println()
	fmt.Println("==============================================================")
	fmt.Println("All tests completed")
	fmt.Println("==============================================================")
}

func fetch(outPath string) {
	file, err := os.Create(outPath)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	writer.Write([]string{
		"code",
		"date",
		"open",
		"high",
		"low",
		"close",
		"volume",
		"amount",
	})

	jobs := make(chan string)
	var wg sync.WaitGroup
	var mu sync.Mutex

	success := make(map[string]int)
	failed := make([]string, 0)

	// 与 stockA tdx_fetcher.go 一致：8 个 worker
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

				// 关键：
				// 指数必须使用 GetIndexDayAll()
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
						fmt.Sprintf(
							"%.3f",
							float64(bar.Open)/1000,
						),
						fmt.Sprintf(
							"%.3f",
							float64(bar.High)/1000,
						),
						fmt.Sprintf(
							"%.3f",
							float64(bar.Low)/1000,
						),
						fmt.Sprintf(
							"%.3f",
							float64(bar.Close)/1000,
						),
						strconv.FormatInt(
							int64(bar.Volume),
							10,
						),
						fmt.Sprintf(
							"%.3f",
							float64(bar.Amount)/1000,
						),
					})
				}

				// 排序，保证 CSV 稳定
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
	fmt.Printf(
		"Success: %d / %d\n",
		len(success),
		len(indexList),
	)
	fmt.Printf("Failed:  %d\n", len(failed))

	if len(failed) > 0 {
		fmt.Println("Failed codes:")
		for _, code := range failed {
			fmt.Printf("  %s\n", code)
		}
	}

	fmt.Printf("\nCSV written: %s\n", outPath)
}

// ================================================================
// 中证2000 / 932000 专项探针
// ================================================================
//
// 官方代码：932000
//
// 这里故意测试多个可能的 TDX 表示：
//
//   932000
//   sh932000
//   sz932000
//
// 同时保留当前 stockA 中的：
//
//   sh000851
//
// 目的不是假定哪一个正确，而是直接观察 TDX 返回结果，
// 再与中证指数官网的 932000 数据进行比较。
// ================================================================

func probeCSI2000() {
	fmt.Println()
	fmt.Println("==============================================================")
	fmt.Println("CSI 2000 / 932000 TDX probe")
	fmt.Println("==============================================================")

	fmt.Println()
	fmt.Println("Official CSI index code:")
	fmt.Println("  932000  中证2000")

	fmt.Println()
	fmt.Println("TDX candidate codes:")
	fmt.Println("  932000")
	fmt.Println("  sh932000")
	fmt.Println("  sz932000")
	fmt.Println("  sh000851")

	client, err := tdx.DialDefault()
	if err != nil {
		fmt.Printf("❌ TDX connect error: %v\n", err)
		return
	}
	defer client.Close()

	candidates := []string{
		"932000",
		"sh932000",
		"sz932000",
		"sh000851",
	}

	for _, code := range candidates {
		probeOneIndex(client, code)
	}

	fmt.Println()
	fmt.Println("==============================================================")
	fmt.Println("932000 probe completed")
	fmt.Println("==============================================================")
}

func probeOneIndex(
	client *tdx.Tdx,
	code string,
) {
	fmt.Println()
	fmt.Println("--------------------------------------------------------------")
	fmt.Printf("Testing TDX code: %s\n", code)
	fmt.Println("--------------------------------------------------------------")

	resp, err := client.GetIndexDayAll(code)

	if err != nil {
		fmt.Printf("GetIndexDayAll: ERROR: %v\n", err)
		return
	}

	if resp == nil {
		fmt.Println("GetIndexDayAll: response = nil")
		return
	}

	if len(resp.List) == 0 {
		fmt.Println("GetIndexDayAll: 0 rows")
		return
	}

	// 按日期排序，避免服务端返回顺序影响显示。
	rows := make([]struct {
		date   string
		open   float64
		high   float64
		low    float64
		close  float64
		volume int64
		amount float64
	}, 0, len(resp.List))

	for _, bar := range resp.List {
		rows = append(rows, struct {
			date   string
			open   float64
			high   float64
			low    float64
			close  float64
			volume int64
			amount float64
		}{
			date:   bar.Time.Format("2006-01-02"),
			open:   float64(bar.Open) / 1000,
			high:   float64(bar.High) / 1000,
			low:    float64(bar.Low) / 1000,
			close:  float64(bar.Close) / 1000,
			volume: int64(bar.Volume),
			amount: float64(bar.Amount) / 1000,
		})
	}

	sort.Slice(rows, func(i, j int) bool {
		return rows[i].date < rows[j].date
	})

	first := rows[0]
	last := rows[len(rows)-1]

	fmt.Printf("GetIndexDayAll: %d rows\n", len(rows))
	fmt.Printf("First: %s\n", first.date)
	fmt.Printf(
		"  O=%.3f H=%.3f L=%.3f C=%.3f\n",
		first.open,
		first.high,
		first.low,
		first.close,
	)

	fmt.Printf("Last:  %s\n", last.date)
	fmt.Printf(
		"  O=%.3f H=%.3f L=%.3f C=%.3f\n",
		last.open,
		last.high,
		last.low,
		last.close,
	)

	fmt.Printf(
		"  Volume=%d Amount=%.3f\n",
		last.volume,
		last.amount,
	)

	// 输出最后 5 个交易日，便于直接与中证官网比较。
	fmt.Println()
	fmt.Println("Last 5 rows:")

	start := len(rows) - 5
	if start < 0 {
		start = 0
	}

	for i := start; i < len(rows); i++ {
		r := rows[i]

		fmt.Printf(
			"  %s  O=%.3f H=%.3f L=%.3f C=%.3f Vol=%d Amt=%.3f\n",
			r.date,
			r.open,
			r.high,
			r.low,
			r.close,
			r.volume,
			r.amount,
		)
	}
}
