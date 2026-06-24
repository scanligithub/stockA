package main

import (
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"sort"
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

type GbbqEvent struct {
	FenHong float64
	PeiJia  float64
	SongGu  float64
	PeiGu   float64
}

type EquityEventOrdered struct {
	Date        int
	FloatShares float64
	TotalShares float64
}

func main() {
	modeFlag := flag.String("mode", "fetch", "Mode: 'list' (fetch master list) or 'fetch' (download klines)")
	codesFlag := flag.String("codes", "", "Comma separated stock codes")
	gbbqPath := flag.String("gbbq", "gbbq_clean.csv", "Local clean GBBQ CSV file path")
	outFlag := flag.String("out", "temp_kline.csv", "Output CSV path")
	flag.Parse()

	if *modeFlag == "list" {
		runFetchList()
		return
	}

	if *modeFlag == "fetch" {
		runFetchKlinesWithLocalCSV(*codesFlag, *gbbqPath, *outFlag)
		return
	}

	fmt.Println("Unknown mode.")
}

// isIndex 🛡️ 高性能无冲突前缀匹配。满足上海000/930/931/932，深圳399，北京899特征的一律判定为指数
func isIndex(code string) bool {
	return strings.HasPrefix(code, "sh000") || 
		strings.HasPrefix(code, "sh9") || 
		strings.HasPrefix(code, "sz399") || 
		strings.HasPrefix(code, "bj899")
}

// LoadGbbqCSV 🛡️ 严格按 PyTDX 标准 CSV 列索引和“万股”单位对齐读取 GBBQ
func LoadGbbqCSV(filePath string) (map[string]map[int]GbbqEvent, map[string][]EquityEventOrdered, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, nil, err
	}
	defer file.Close()

	gbbqMap := make(map[string]map[int]GbbqEvent)
	equityMap := make(map[string][]EquityEventOrdered)

	reader := csv.NewReader(file)
	_, err = reader.Read() // Skip Header
	if err != nil {
		return nil, nil, err
	}

	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, nil, err
		}

		codeStr := record[1]
		date, _ := strconv.Atoi(record[2])
		category, _ := strconv.Atoi(record[3])
		fenHong, _ := strconv.ParseFloat(record[4], 64)
		peiJia, _ := strconv.ParseFloat(record[5], 64)
		songGu, _ := strconv.ParseFloat(record[6], 64)
		peiGu, _ := strconv.ParseFloat(record[7], 64)

		prefix := "sz"
		if strings.HasPrefix(codeStr, "60") || strings.HasPrefix(codeStr, "68") {
			prefix = "sh"
		} else if strings.HasPrefix(codeStr, "43") || strings.HasPrefix(codeStr, "83") || strings.HasPrefix(codeStr, "87") || strings.HasPrefix(codeStr, "88") || strings.HasPrefix(codeStr, "92") {
			prefix = "bj"
		}
		tdxCode := prefix + codeStr

		if category == 1 {
			if _, ok := gbbqMap[tdxCode]; !ok {
				gbbqMap[tdxCode] = make(map[int]GbbqEvent)
			}
			gbbqMap[tdxCode][date] = GbbqEvent{
				FenHong: fenHong,
				PeiJia:  peiJia,
				SongGu:  songGu,
				PeiGu:   peiGu,
			}
		} else if category == 2 || category == 3 || category == 5 || category == 7 || category == 8 || category == 9 || category == 10 {
			equityMap[tdxCode] = append(equityMap[tdxCode], EquityEventOrdered{
				Date:        date,
				FloatShares: songGu * 10000.0,
				TotalShares: peiGu * 10000.0,
			})
		}
	}

	for code := range equityMap {
		sort.Slice(equityMap[code], func(i, j int) bool {
			return equityMap[code][i].Date < equityMap[code][j].Date
		})
	}

	return gbbqMap, equityMap, nil
}

func runFetchList() {
	fmt.Println("[Go Engine] Mode: LIST - Fetching A-shares list...")
	cli, err := tdx.DialDefault()
	if err != nil {
		panic(err)
	}
	defer cli.Close()

	var masterList []StockMaster
	exchanges := []protocol.Exchange{protocol.ExchangeSH, protocol.ExchangeSZ, protocol.ExchangeBJ}

	for _, ex := range exchanges {
		resp, err := cli.GetCodeAll(ex)
		if err != nil || resp == nil {
			continue
		}
		for _, item := range resp.List {
			if ex == protocol.ExchangeSH {
				if strings.HasPrefix(item.Code, "60") || strings.HasPrefix(item.Code, "68") {
					masterList = append(masterList, StockMaster{
						Code:     fmt.Sprintf("sh.%s", item.Code),
						CodeName: item.Name,
					})
				}
			} else if ex == protocol.ExchangeSZ {
				if strings.HasPrefix(item.Code, "00") || strings.HasPrefix(item.Code, "30") {
					masterList = append(masterList, StockMaster{
						Code:     fmt.Sprintf("sz.%s", item.Code),
						CodeName: item.Name,
					})
				}
			} else if ex == protocol.ExchangeBJ {
				if strings.HasPrefix(item.Code, "43") || strings.HasPrefix(item.Code, "83") ||
					strings.HasPrefix(item.Code, "87") || strings.HasPrefix(item.Code, "88") ||
					strings.HasPrefix(item.Code, "92") {
					masterList = append(masterList, StockMaster{
						Code:     fmt.Sprintf("bj.%s", item.Code),
						CodeName: item.Name,
					})
				}
			}
		}
	}

	file, _ := os.Create("stock_list_master.json")
	json.NewEncoder(file).Encode(masterList)
	file.Close()
	fmt.Printf("[Go Engine] Master stock list resolved: %d stocks.\n", len(masterList))
}

func runFetchKlinesWithLocalCSV(codesStr, gbbqPath, outPath string) {
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

	gbbqMap, equityMap, err := LoadGbbqCSV(gbbqPath)
	if err != nil {
		panic(fmt.Sprintf("Failed to load clean GBBQ CSV: %v", err))
	}

	outFile, err := os.Create(outPath)
	if err != nil {
		panic(err)
	}
	defer outFile.Close()

	csvWriter := csv.NewWriter(outFile)
	csvWriter.Write([]string{
		"code", "date", "open", "high", "low", "close", "volume", "amount", 
		"adjustFactor", "total_shares", "float_shares", "total_mv", "float_mv", "turn",
	})
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

				events := gbbqMap[tcode]
				equities := equityMap[tcode]
				
				var records [][]string
				adjustFactor := 1.0
				var prevClose float64 = -1.0

				// 股本初始化
				var lastFloatShares float64 = 0.0
				var lastTotalShares float64 = 0.0
				if len(equities) > 0 {
					lastFloatShares = equities[0].FloatShares
					lastTotalShares = equities[0].TotalShares
				}

				isIdx := isIndex(tcode)

				for _, bar := range resp.List {
					dateStr := bar.Time.Format("2006-01-02")
					dateIntStr := bar.Time.Format("20060102")
					dateInt, _ := strconv.Atoi(dateIntStr)

					pOpen := float64(bar.Open) / 1000.0
					pHigh := float64(bar.High) / 1000.0
					pLow := float64(bar.Low) / 1000.0
					pClose := float64(bar.Close) / 1000.0
					pVolume := float64(bar.Volume)
					pAmount := float64(bar.Amount) / 1000.0

					var totalMV, floatMV, turn float64 = 0.0, 0.0, 0.0

					if !isIdx {
						// 🚀 A. [个股逻辑] 计算复权因子 adjustFactor
						if ev, ok := events[dateInt]; ok && prevClose > 0 {
							fh := ev.FenHong / 10.0
							sg := ev.SongGu / 10.0
							pg := ev.PeiGu / 10.0
							pj := ev.PeiJia

							pEx := (prevClose - fh + pg*pj) / (1.0 + sg + pg)
							if pEx > 0 {
								adjustFactor *= (prevClose / pEx)
							}
						}

						// 🚀 B. [个股逻辑] ASOF 时序非等值关联：对齐当日最新的股本快照
						for _, eq := range equities {
							if eq.Date <= dateInt {
								lastFloatShares = eq.FloatShares
								lastTotalShares = eq.TotalShares
							} else {
								break
							}
						}

						totalMV = pClose * lastTotalShares
						floatMV = pClose * lastFloatShares
						if lastFloatShares > 0 {
							turn = (pVolume * 10000.0 / lastFloatShares)
						}
					} else {
						// 📈 [指数逻辑] 免复权，指标安全置零
						adjustFactor = 1.0
						lastTotalShares = 0.0
						lastFloatShares = 0.0
					}

					records = append(records, []string{
						codeMap[tcode],
						dateStr,
						fmt.Sprintf("%.3f", pOpen),
						fmt.Sprintf("%.3f", pHigh),
						fmt.Sprintf("%.3f", pLow),
						fmt.Sprintf("%.3f", pClose),
						fmt.Sprintf("%.0f", pVolume),
						fmt.Sprintf("%.3f", pAmount),
						fmt.Sprintf("%.6f", adjustFactor),
						fmt.Sprintf("%.0f", lastTotalShares),
						fmt.Sprintf("%.0f", lastFloatShares),
						fmt.Sprintf("%.3f", totalMV),
						fmt.Sprintf("%.3f", floatMV),
						fmt.Sprintf("%.4f", turn),
					})
					prevClose = pClose
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
