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

// 导出给 Python 使用的结构体
type StockMaster struct {
	Code     string `json:"code"`
	CodeName string `json:"code_name"`
}

func main() {
	modeFlag := flag.String("mode", "fetch", "Mode: 'list' (fetch stock list) or 'fetch' (download klines)")
	codesFlag := flag.String("codes", "", "Comma separated stock codes (e.g., sh.600000,sz.000001)")
	outFlag := flag.String("out", "temp_kline.csv", "Output CSV file path")
	flag.Parse()

	if *modeFlag == "list" {
		runFetchList()
		return
	}

	if *modeFlag == "fetch" {
		runFetchKlines(*codesFlag, *outFlag)
		return
	}

	fmt.Println("Unknown mode. Use --mode=list or --mode=fetch")
}

// ---------------------------------------------------------
// 模式 1: 获取全量股票列表
// ---------------------------------------------------------
func runFetchList() {
	fmt.Println("[Go Engine] Mode: LIST - Fetching master stock list from TDX...")
	cli, err := tdx.DialDefault()
	if err != nil {
		panic(fmt.Sprintf("TDX Dial Failed: %v", err))
	}
	defer cli.Close()

	var masterList []StockMaster

	// 遍历沪、深、北交易所 (1=SH, 0=SZ, 2=BJ 等，TDX内部自动处理)
	// injoyai/tdx 提供的 GetStockCodeAll 返回的是如 "sh600000" 的字符串切片
	// 但我们需要股票的中文名称，所以调用更底层的 GetCodeAll
	exchanges := []protocol.Exchange{protocol.ExchangeSH, protocol.ExchangeSZ, protocol.ExchangeBJ}

	for _, ex := range exchanges {
		resp, err := cli.GetCodeAll(ex)
		if err != nil || resp == nil {
			continue
		}
		for _, item := range resp.List {
			// 过滤 A 股: 沪市(60,68开头)，深市(00,30开头)，北交所(4,8开头)
			if strings.HasPrefix(item.Code, "60") || strings.HasPrefix(item.Code, "68") ||
				strings.HasPrefix(item.Code, "00") || strings.HasPrefix(item.Code, "30") ||
				strings.HasPrefix(item.Code, "4") || strings.HasPrefix(item.Code, "8") {

				// 构造 baostock 格式的代码 (sh.600000)
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

	fmt.Printf("[Go Engine] Successfully fetched %d A-shares.\n", len(masterList))

	// 写入供 Python 使用的 JSON 文件
	file, _ := os.Create("stock_list_master.json")
	defer file.Close()
	encoder := json.NewEncoder(file)
	encoder.SetEscapeHTML(false)
	encoder.Encode(masterList)
}

// ---------------------------------------------------------
// 模式 2: 并发下载 K 线与复权
// ---------------------------------------------------------
func runFetchKlines(codesStr, outPath string) {
	if codesStr == "" {
		fmt.Println("No codes provided.")
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

	cli, err := tdx.DialDefault()
	if err != nil {
		panic(fmt.Sprintf("TDX Dial Failed: %v", err))
	}
	defer cli.Close()

	fmt.Println("[Go Engine] Mode: FETCH - Fetching Global GBBQ Database...")
	gbbqRaw, err := cli.GetGbbqAll()
	if err != nil {
		panic(fmt.Sprintf("Failed to fetch GBBQ: %v", err))
	}

	gbbqBytes, _ := json.Marshal(gbbqRaw)
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
					dateStr := strings.Split(bar.Time, " ")[0]
					dateInt, _ := strconv.Atoi(strings.ReplaceAll(dateStr, "-", ""))

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
	fmt.Println("[Go Engine] All downloads and adjustments completed.")
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
