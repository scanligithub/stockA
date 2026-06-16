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

	"github.com/injoyai/tdx"
	"github.com/injoyai/tdx/protocol"
)

type StockMaster struct {
	Code     string `json:"code"`
	CodeName string `json:"code_name"`
}

func main() {
	modeFlag := flag.String("mode", "fetch", "Mode: 'list' (fetch list & gbbq) or 'fetch' (download klines)")
	codesFlag := flag.String("codes", "", "Comma separated stock codes")
	gbbqFlag := flag.String("gbbq", "gbbq_all.json", "Local GBBQ JSON path")
	outFlag := flag.String("out", "temp_kline.csv", "Output CSV path")
	flag.Parse()

	if *modeFlag == "list" {
		runFetchListAndGbbq(*gbbqFlag)
		return
	}

	if *modeFlag == "fetch" {
		runFetchKlinesWithLocalGbbq(*codesFlag, *gbbqFlag, *outFlag)
		return
	}

	fmt.Println("Unknown mode.")
}

// ---------------------------------------------------------
// 1. Prepare 阶段：单点下载全量列表 + 全量 GBBQ 数据并落盘
// ---------------------------------------------------------
func runFetchListAndGbbq(gbbqPath string) {
	fmt.Println("[Go Engine] Mode: LIST - Connecting to TDX...")
	cli, err := tdx.DialDefault()
	if err != nil {
		panic(err)
	}
	defer cli.Close()

	// A. 下载股票代码
	var masterList []StockMaster
	exchanges := []protocol.Exchange{protocol.ExchangeSH, protocol.ExchangeSZ, protocol.ExchangeBJ}

	for _, ex := range exchanges {
		resp, err := cli.GetCodeAll(ex)
		if err != nil || resp == nil {
			continue
		}
		for _, item := range resp.List {
			if strings.HasPrefix(item.Code, "60") || strings.HasPrefix(item.Code, "68") ||
				strings.HasPrefix(item.Code, "00") || strings.HasPrefix(item.Code, "30") ||
				strings.HasPrefix(item.Code, "4") || strings.HasPrefix(item.Code, "8") {

				prefix := "sh"
				if ex == protocol.ExchangeSZ {
					prefix = "sz"
				} else if ex == protocol.ExchangeBJ {
					prefix = "bj"
				}

				masterList = append(masterList, StockMaster{
					Code:     fmt.Sprintf("%s.%s", prefix, item.Code),
					CodeName: item.Name,
				})
			}
		}
	}
	fmt.Printf("[Go Engine] Master stock list resolved: %d stocks.\n", len(masterList))

	file, _ := os.Create("stock_list_master.json")
	json.NewEncoder(file).Encode(masterList)
	file.Close()

	// B. 单点拉取全量 GBBQ 数据库
	fmt.Println("[Go Engine] Downloading Global GBBQ Database...")
	gbbqRaw, err := cli.GetGbbqAll()
	if err != nil {
		panic(err)
	}

	gbbqFile, _ := os.Create(gbbqPath)
	defer gbbqFile.Close()
	json.NewEncoder(gbbqFile).Encode(gbbqRaw)
	fmt.Println("[Go Engine] Master list and GBBQ local cache created successfully.")
}

// ---------------------------------------------------------
// 2. Fetch 阶段：19 并发，完全基于本地缓存 GBBQ，零网络开销计算复权
// ---------------------------------------------------------
func runFetchKlinesWithLocalGbbq(codesStr, gbbqPath, outPath string) {
	if codesStr == "" {
		return
	}

	rawCodes := strings.Split(codesStr, ",")
	var tdxCodes []string
	codeMap := make(map[string]string)

	for _, c := range rawCodes {
		c = strings.TrimSpace(c)
		if c == "" {
			continue
		}
		tdxCode := strings.ReplaceAll(c, ".", "")
		tdxCodes = append(tdxCodes, tdxCode)
		codeMap[tdxCode] = c
	}

	// 从本地磁盘加载 GBBQ (耗时只需 0.1s)
	fmt.Printf("[Go Engine] Loading GBBQ from local cache: %s...\n", gbbqPath)
	gbbqBytes, err := os.ReadFile(gbbqPath)
	if err != nil {
		panic(fmt.Sprintf("Failed to read local GBBQ: %v", err))
	}
	var gbbqDyn map[string][]map[string]interface{}
	json.Unmarshal(gbbqBytes, &gbbqDyn)

	outFile, err := os.Create(outPath)
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

				for _, bar := range resp.List {
					dateStr := bar.Time.Format("2006-01-02")
					dateIntStr := bar.Time.Format("20060102")
					dateInt, _ := strconv.Atoi(dateIntStr)

					if ev, ok := eventMap[dateInt]; ok && prevClose > 0 {
						fh := getFloat(ev, "FenHong") / 10.0
						sg := getFloat(ev, "SongGu") / 10.0
						zz := getFloat(ev, "ZhuanZeng") / 10.0
						pg := getFloat(ev, "PeiGu") / 10.0
						pj := getFloat(ev, "PeiGuJia", "PeiJia")

						bonus := sg + zz
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
	fmt.Println("[Go Engine] Download completed.")
}

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
