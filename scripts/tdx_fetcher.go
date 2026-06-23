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

// GbbqEvent 对应 Category 1 的除权除息数据
type GbbqEvent struct {
	FenHong float64
	PeiJia  float64
	SongGu  float64
	PeiGu   float64
}

// EquityEventOrdered 对应 Category 2-10 的股本变动快照，按时间升序对齐
type EquityEventOrdered struct {
	Date        int
	FloatShares float64 // 盘后流通股本 (单位：股)
	TotalShares float64 // 盘后总股本 (单位：股)
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

// LoadGbbqDat 🛡️ 严格按通达信标准的 270 字节头部跳过 + 29 字节磁盘物理对齐解析 gbbq.dat 文件
func LoadGbbqDat(filePath string) (map[string]map[int]GbbqEvent, map[string][]EquityEventOrdered, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, nil, err
	}
	defer file.Close()

	// 🎯 核心修正 A：通达信 gbbq.dat 文件头部有 270 字节的系统信息，必须跳过才能对齐后续的 29 字节记录体
	_, err = file.Seek(270, io.SeekStart)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to seek past 270-byte header: %v", err)
	}

	gbbqMap := make(map[string]map[int]GbbqEvent)
	equityMap := make(map[string][]EquityEventOrdered)
	
	// 读取 29 字节（保持文件指针移动对齐），解析前 28 字节，丢弃第 29 字节。
	buf := make([]byte, 29)

	for {
		_, err := io.ReadFull(file, buf)
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			break
		}
		if err != nil {
			return nil, nil, err
		}

		// 🎯 严格对应前 28 字节的物理存储布局偏移量
		date := int(binary.LittleEndian.Uint32(buf[0:4])) // 0-3 字节：日期 (YYYYMMDD)
		market := buf[4]                                 // 4 字节：市场标识 (0: sz, 1: sh, 2: bj)
		
		// 5-10 字节为 6 位股票代码，安全剔除可能存在的 \x00 (Null 字符) 和空白字符
		codeRaw := string(buf[5:11])
		codeClean := strings.ReplaceAll(codeRaw, "\x00", "")
		codeStr := strings.TrimSpace(codeClean)
		
		category := buf[11]                              // 11 字节：变更类别

		// 防御性设计：兼容二进制整数与 ASCII 字符两套底层编码方案
		prefix := "sz"
		if market == 1 || market == '1' {
			prefix = "sh"
		} else if market == 2 || market == '2' {
			prefix = "bj"
		}
		tdxCode := prefix + codeStr

		fenHong := float64(math.Float32frombits(binary.LittleEndian.Uint32(buf[12:16])))
		peiJia := float64(math.Float32frombits(binary.LittleEndian.Uint32(buf[16:20])))
		songGu := float64(math.Float32frombits(binary.LittleEndian.Uint32(buf[20:24])))
		peiGu := float64(math.Float32frombits(binary.LittleEndian.Uint32(buf[24:28])))

		if category == 1 {
			// A. 处理除权除息
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
			// B. 处理股本变动快照
			equityMap[tdxCode] = append(equityMap[tdxCode], EquityEventOrdered{
				Date:        date,
				FloatShares: songGu,
				TotalShares: peiGu,
			})
		}
	}

	// 按照时间轴严格升序排序
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

	fmt.Printf("[Go Engine] Parsing local binary GBBQ: %s...\n", gbbqPath)
	gbbqMap, equityMap, err := LoadGbbqDat(gbbqPath)
	if err != nil {
		panic(fmt.Sprintf("Failed to parse local gbbq.dat: %v", err))
	}

	gbbqCount := 0
	for _, m := range gbbqMap {
		gbbqCount += len(m)
	}
	equityCount := 0
	for _, arr := range equityMap {
		equityCount += len(arr)
	}
	fmt.Printf("[Go Engine] GBBQ resolved: %d stocks with %d ex-div events, %d stocks with %d capital events.\n", len(gbbqMap), gbbqCount, len(equityMap), equityCount)

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

				for _, bar := range resp.List {
					dateStr := bar.Time.Format("2006-01-02")
					dateIntStr := bar.Time.Format("20060102")
					dateInt, _ := strconv.Atoi(dateIntStr)

					pOpen := float64(bar.Open) / 1000.0
					pHigh := float64(bar.High) / 1000.0
					pLow := float64(bar.Low) / 1000.0
					pClose := float64(bar.Close) / 1000.0
					pVolume := float64(bar.Volume)
					
					// 🎯 核心修正 B：显式将自定义整型 protocol.Price 强转为 float64，并除以 1000.0 换算为标准元
					pAmount := float64(bar.Amount) / 1000.0

					// 🚀 A. 计算复权因子 adjustFactor
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

					// 🚀 B. ASOF 时序非等值关联：对齐当日最新的股本快照
					for _, eq := range equities {
						if eq.Date <= dateInt {
							lastFloatShares = eq.FloatShares
							lastTotalShares = eq.TotalShares
						} else {
							break
						}
					}

					// 🚀 C. 衍生指标动态计算
					totalMV := pClose * lastTotalShares
					floatMV := pClose * lastFloatShares

					// 换手率
					turn := 0.0
					if lastFloatShares > 0 {
						turn = (pVolume * 10000.0 / lastFloatShares)
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
