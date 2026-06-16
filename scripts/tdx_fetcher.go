package main

import (
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/injoyai/tdx"
)

func main() {
	codesFlag := flag.String("codes", "", "Comma separated stock codes (e.g., sh.600000,sz.000001)")
	outFlag := flag.String("out", "temp_kline.csv", "Output CSV file path")
	flag.Parse()

	if *codesFlag == "" {
		fmt.Println("No codes provided.")
		return
	}

	rawCodes := strings.Split(*codesFlag, ",")
	var tdxCodes []string
	codeMap := make(map[string]string) // TDX Code -> BS Code

	for _, c := range rawCodes {
		c = strings.TrimSpace(c)
		if c == "" {
			continue
		}
		// sh.600000 -> sh600000
		tdxCode := strings.ReplaceAll(c, ".", "")
		tdxCodes = append(tdxCodes, tdxCode)
		codeMap[tdxCode] = c
	}

	cli, err := tdx.DialDefault()
	if err != nil {
		panic(fmt.Sprintf("TDX Dial Failed: %v", err))
	}
	defer cli.Close()

	fmt.Println("[Go Engine] Fetching Global GBBQ Database...")
	gbbqRaw, err := cli.GetGbbqAll()
	if err != nil {
		panic(fmt.Sprintf("Failed to fetch GBBQ: %v", err))
	}

	// 采用 JSON 序列化绕过底层库非导出结构体字段的限制
	gbbqBytes, _ := json.Marshal(gbbqRaw)
	var gbbqDyn map[string][]map[string]interface{}
	json.Unmarshal(gbbqBytes, &gbbqDyn)

	fmt.Printf("[Go Engine] Successfully loaded GBBQ. Processing %d stocks...\n", len(tdxCodes))

	outFile, err := os.Create(*outFlag)
	if err != nil {
		panic(err)
	}
	defer outFile.Close()

	csvWriter := csv.NewWriter(outFile)
	csvWriter.Write([]string{"date", "code", "open", "high", "low", "close", "volume", "amount", "adjustFactor"})
	var mu sync.Mutex

	var wg sync.WaitGroup
	jobChan := make(chan string, len(tdxCodes))
	for _, c := range tdxCodes {
		jobChan <- c
	}
	close(jobChan)

	concurrency := 8
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			workerCli, err := tdx.DialDefault()
			if err != nil {
				return
			}
			defer workerCli.Close()

			for tcode := range jobChan {
				resp, err := workerCli.GetKlineDayAll(tcode)
				if err != nil || resp == nil || len(resp.List) == 0 {
					continue
				}

				events := gbbqDyn[tcode]
				eventMap := make(map[int]map[string]interface{})
				for _, ev := range events {
					dateVal := getFloat(ev, "Date", "date", "Time", "time", "WeightDate")
					if dateVal > 0 {
						eventMap[int(dateVal)] = ev
					}
				}

				var records [][]string
				adjustFactor := 1.0
				var prevClose float64 = -1.0

				// 按时间正序遍历K线计算后复权
				for _, bar := range resp.List {
					// Date parse: "2026-06-16 15:00:00" -> 20260616
					dateStr := strings.Split(bar.Time, " ")[0]
					dateInt, _ := strconv.Atoi(strings.ReplaceAll(dateStr, "-", ""))

					// 检查是否有除权事件
					if ev, ok := eventMap[dateInt]; ok && prevClose > 0 {
						fh := getFloat(ev, "FenHong") / 10.0
						sg := getFloat(ev, "SongGu") / 10.0
						zz := getFloat(ev, "ZhuanZeng") / 10.0
						pg := getFloat(ev, "PeiGu") / 10.0
						pj := getFloat(ev, "PeiGuJia", "PeiJia")

						bonus := sg + zz
						// 除权除息公式
						pEx := (prevClose - fh + pg*pj) / (1.0 + bonus + pg)
						if pEx > 0 {
							adjustFactor *= (prevClose / pEx)
						}
					}

					records = append(records, []string{
						dateStr,
						codeMap[tcode],
						fmt.Sprintf("%.3f", bar.Open),
						fmt.Sprintf("%.3f", bar.High),
						fmt.Sprintf("%.3f", bar.Low),
						fmt.Sprintf("%.3f", bar.Close),
						fmt.Sprintf("%.0f", bar.Volume),
						fmt.Sprintf("%.3f", bar.Amount),
						fmt.Sprintf("%.6f", adjustFactor),
					})
					prevClose = float64(bar.Close)
				}

				mu.Lock()
				csvWriter.WriteAll(records)
				mu.Unlock()
			}
		}()
	}

	wg.Wait()
	csvWriter.Flush()
	fmt.Println("[Go Engine] All downloads and adjustments completed.")
}

// 辅助函数：安全提取 JSON Map 中的浮点数
func getFloat(m map[string]interface{}, keys ...string) float64 {
	for _, k := range keys {
		if val, ok := m[k]; ok {
			switch v := val.(type) {
			case float64:
				return v
			case float32:
				return float64(v)
			case int:
				return float64(v)
			}
		}
	}
	return 0.0
}
