package main

import (
	"encoding/binary"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math"
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

type GbbqEvent struct {
	FenHong float64
	PeiJia  float64
	SongGu  float64
	PeiGu   float64
}

func main() {
	modeFlag := flag.String("mode", "fetch", "Mode: 'list' (fetch master list) or 'fetch' (download klines)")
	codesFlag := flag.String("codes", "", "Comma separated stock codes")
	gbbqPath := flag.String("gbbq", "gbbq.dat", "Local binary gbbq.dat file path")
	outFlag := flag.String("out", "temp_kline.csv", "Output CSV path")
	flag.Parse()

	if *modeFlag == "list" {
		runFetchList()
		return
	}

	if *modeFlag == "fetch" {
		runFetchKlinesWithLocalDat(*codesFlag, *gbbqPath, *outFlag)
		return
	}

	fmt.Println("Unknown mode.")
}

// ---------------------------------------------------------
// 🛡️ 极速二进制解析器：直接解析本地 29 字节标准的 gbbq.dat 文件
// ---------------------------------------------------------
func LoadGbbqDat(filePath string) (map[string]map[int]GbbqEvent, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	gbbqMap := make(map[string]map[int]GbbqEvent)
	buf := make([]byte, 29)

	for {
		_, err := io.ReadFull(file, buf)
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			break
		}
		if err != nil {
			return nil, err
		}

		codeStr := strings.TrimSpace(string(buf[0:6]))
		market := buf[6]
		date := int(binary.LittleEndian.Uint32(buf[7:11]))
		category := buf[11]

		if category != 1 {
			continue // 仅筛选 Category = 1 (除权除息)
		}

		fenHong := float64(math.Float32frombits(binary.LittleEndian.Uint32(buf[12:16])))
		peiJia := float64(math.Float32frombits(binary.LittleEndian.Uint32(buf[16:20])))
		songGu := float64(math.Float32frombits(binary.LittleEndian.Uint32(buf[20:24])))
		peiGu := float64(math.Float32frombits(binary.LittleEndian.Uint32(buf[24:28])))

		prefix := "sz"
		if market == 1 {
			prefix = "sh"
		} else if market == 2 {
			prefix = "bj"
		}
		tdxCode := prefix + codeStr

		if _, ok := gbbqMap[tdxCode]; !ok {
			gbbqMap[tdxCode] = make(map[int]GbbqEvent)
		}

		gbbqMap[tdxCode][date] = GbbqEvent{
			FenHong: fenHong,
			PeiJia:  peiJia,
			SongGu:  songGu,
			PeiGu:   peiGu,
		}
	}

	return gbbqMap, nil
}

// ---------------------------------------------------------
// 1. Prepare 阶段：仅下载全市场股票主列表
// ---------------------------------------------------------
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

	file, _ := os.Create("stock_list_master.json")
	json.NewEncoder(file).Encode(masterList)
	file.Close()
	fmt.Printf("[Go Engine] Master stock list resolved: %d stocks.\n", len(masterList))
}

// ---------------------------------------------------------
// 2. Fetch 阶段：多协程并行，无网环境下极速解析 gbbq.dat 算复权
// ---------------------------------------------------------
func runFetchKlinesWithLocalDat(codesStr, gbbqPath, outPath string) {
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

	// 0.05 秒极速加载本地二进制权息库
	fmt.Printf("[Go Engine] Parsing local binary GBBQ: %s...\n", gbbqPath)
	gbbqMap, err := LoadGbbqDat(gbbqPath)
	if err != nil {
		panic(fmt.Sprintf("Failed to parse local gbbq.dat: %v", err))
	}

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

				events := gbbqMap[tcode]
				var records [][]string
				adjustFactor := 1.0
				var prevClose float64 = -1.0

				for _, bar := range resp.List {
					dateStr := bar.Time.Format("2006-01-02")
					dateIntStr := bar.Time.Format("20060102")
					dateInt, _ := strconv.Atoi(dateIntStr)

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
