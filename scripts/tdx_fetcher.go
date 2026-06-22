package main

import (
	"encoding/binary"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"
	"sync"

	"github.com/injoyai/tdx"
)

type StockInfo struct {
	Code     string `json:"code"`
	CodeName string `json:"code_name"`
}

type GbbqEvent struct {
	DateInt int
	Cat     uint8
	F1      float64
	F2      float64
	F3      float64
	F4      float64
}

func parseDate(d int) string {
	y := d / 10000
	m := (d % 10000) / 100
	day := d % 100
	return fmt.Sprintf("%04d-%02d-%02d", y, m, day)
}

func loadGbbq() map[string][]GbbqEvent {
	m := make(map[string][]GbbqEvent)
	b, err := os.ReadFile("gbbq.dat")
	if err != nil {
		fmt.Println("Warning: gbbq.dat not found!")
		return m
	}
	
	for i := 0; i+29 <= len(b); i += 29 {
		rec := b[i : i+29]
		market := rec[0]
		codeStr := strings.TrimSpace(string(rec[1:7]))
		dateInt := int(binary.LittleEndian.Uint32(rec[7:11]))
		cat := rec[11]

		f1 := float64(math.Float32frombits(binary.LittleEndian.Uint32(rec[12:16])))
		f2 := float64(math.Float32frombits(binary.LittleEndian.Uint32(rec[16:20])))
		f3 := float64(math.Float32frombits(binary.LittleEndian.Uint32(rec[20:24])))
		f4 := float64(math.Float32frombits(binary.LittleEndian.Uint32(rec[24:28])))

		prefix := "sz."
		if market == 1 || strings.HasPrefix(codeStr, "6") {
			prefix = "sh."
		} else if strings.HasPrefix(codeStr, "4") || strings.HasPrefix(codeStr, "8") || strings.HasPrefix(codeStr, "9") || strings.HasPrefix(codeStr, "2") {
			prefix = "bj."
		}
		
		fullCode := prefix + codeStr
		m[fullCode] = append(m[fullCode], GbbqEvent{DateInt: dateInt, Cat: cat, F1: f1, F2: f2, F3: f3, F4: f4})
	}
	return m
}

func main() {
	mode := flag.String("mode", "fetch", "list or fetch")
	codesFlag := flag.String("codes", "", "comma separated codes")
	outFlag := flag.String("out", "out.csv", "output csv")
	flag.Parse()

	c, err := tdx.DialDefault()
	if err != nil {
		panic(err)
	}
	defer c.Close()

	// 🌟 修复：使用 GetStockAll 获取含有 Code 与 Name 的结构体
	if *mode == "list" {
		fmt.Println("📡 Go Engine: 正在通过通达信极速同步全量 A 股列表及中文简称...")
		stocks, err := c.GetStockAll()
		if err != nil {
			panic(err)
		}

		var masterList []StockInfo
		for _, s := range stocks {
			codeStr := strings.TrimSpace(s.Code)
			if len(codeStr) != 6 {
				continue
			}

			prefix := "sz."
			if strings.HasPrefix(codeStr, "6") {
				prefix = "sh."
			} else if strings.HasPrefix(codeStr, "4") || strings.HasPrefix(codeStr, "8") || strings.HasPrefix(codeStr, "9") || strings.HasPrefix(codeStr, "2") {
				prefix = "bj."
			}
			
			masterList = append(masterList, StockInfo{
				Code:     prefix + codeStr,
				CodeName: s.Name,
			})
		}

		jsonBytes, err := json.MarshalIndent(masterList, "", "  ")
		if err != nil {
			panic(err)
		}

		err = os.WriteFile("stock_list_master.json", jsonBytes, 0644)
		if err != nil {
			panic(err)
		}
		fmt.Printf("✅ Go Engine: 股票列表同步成功！共保存 %d 只 A 股到 stock_list_master.json\n", len(masterList))
		return
	}

	gbbqMap := loadGbbq()
	codes := strings.Split(*codesFlag, ",")

	f, _ := os.Create(*outFlag)
	defer f.Close()
	w := csv.NewWriter(f)
	w.Write([]string{"date", "code", "open", "high", "low", "close", "volume", "amount", "adjustFactor", "totalShares", "floatShares"})

	var wg sync.WaitGroup
	sem := make(chan struct{}, 10)
	var mu sync.Mutex

	for _, code := range codes {
		wg.Add(1)
		sem <- struct{}{}
		go func(code string) {
			defer wg.Done()
			defer func() { <-sem }()

			parts := strings.Split(code, ".")
			if len(parts) != 2 {
				return
			}
			
			tdxCode := parts[0] + parts[1]

			// 获取包装结构体 KlineResp
			resp, err := c.GetKlineDayAll(tdxCode)
			if err != nil || resp == nil || len(resp.List) == 0 {
				return
			}

			// 🌟 修复：提取真实的 K 线切片 List 进行后续处理
			klines := resp.List
			events := gbbqMap[code]
			
			var records [][]string
			adjFactor := 1.0
			totalShares := 0.0
			floatShares := 0.0

			for _, k := range klines {
				dateStr := fmt.Sprintf("%04d-%02d-%02d", k.Time.Year(), k.Time.Month(), k.Time.Day())
				dateInt := k.Time.Year()*10000 + int(k.Time.Month())*100 + k.Time.Day()

				for _, e := range events {
					if e.DateInt == dateInt {
						if e.Cat == 1 {
							pPrev := k.Close
							pEx := (pPrev - e.F4 + e.F2*e.F3) / (1.0 + e.F1 + e.F2)
							if pEx > 0 {
								adjFactor *= (pPrev / pEx)
							}
						} else {
							floatShares = e.F1
							totalShares = e.F3
						}
					}
				}

				records = append(records, []string{
					dateStr,
					code,
					strconv.FormatFloat(k.Open, 'f', 4, 64),
					strconv.FormatFloat(k.High, 'f', 4, 64),
					strconv.FormatFloat(k.Low, 'f', 4, 64),
					strconv.FormatFloat(k.Close, 'f', 4, 64),
					strconv.FormatFloat(k.Volume, 'f', 0, 64),
					strconv.FormatFloat(k.Amount, 'f', 0, 64),
					strconv.FormatFloat(adjFactor, 'f', 4, 64),
					strconv.FormatFloat(totalShares, 'f', 4, 64),
					strconv.FormatFloat(floatShares, 'f', 4, 64),
				})
			}

			mu.Lock()
			w.WriteAll(records)
			mu.Unlock()
		}(code)
	}
	wg.Wait()
	w.Flush()
}
