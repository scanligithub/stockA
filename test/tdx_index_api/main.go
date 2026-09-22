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
	outPath := flag.String("out", "temp_index_kline.csv", "output CSV")
	flag.Parse()

	fmt.Printf("==============================================================\n")
	fmt.Printf("stockA index fetch integration test\n")
	fmt.Printf("==============================================================\n")
	fmt.Printf("Indices: %d\n", len(indexList))
	fmt.Printf("Output:  %s\n\n", *outPath)

	fetch(*outPath)
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
						fmt.Sprintf("%.3f", float64(bar.Open)/1000),
						fmt.Sprintf("%.3f", float64(bar.High)/1000),
						fmt.Sprintf("%.3f", float64(bar.Low)/1000),
						fmt.Sprintf("%.3f", float64(bar.Close)/1000),
						strconv.FormatInt(int64(bar.Volume), 10),
						fmt.Sprintf("%.3f", float64(bar.Amount)/1000),
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
