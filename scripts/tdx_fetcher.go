package main

import (
	"encoding/binary"
	"encoding/csv"
	"flag"
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"
	"sync"

	"github.com/injoyai/tdx"
)

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

	// 🌟 修复：直接使用 tdx 根包导出的 DialDefault 建立最优连接
	c, err := tdx.DialDefault()
	if err != nil {
		panic(err)
	}
	defer c.Close()

	if *mode == "list" {
		os.WriteFile("stock_list_master.json", []byte("[]"), 0644)
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
			
			// 转换代码格式 sh.600519 -> sh600519，契合通达信标准传参
			tdxCode := parts[0] + parts[1]

			// 🌟 修复：直接调用根包客户端的 GetKlineDayAll
			klines, err := c.GetKlineDayAll(tdxCode)
			if err != nil || len(klines) == 0 {
				return
			}

			events := gbbqMap[code]
			
			var records [][]string
			adjFactor := 1.0
			totalShares := 0.0
			floatShares := 0.0

			for _, k := range klines {
				dateStr := fmt.Sprintf("%04d-%02d-%02d", k.Time.Year(), k.Time.Month(), k.Time.Day())
				dateInt := k.Time.Year()*10000 + int(k.Time.Month())*100 + k.Time.Day()

				// 股本时间轴正序对齐状态机
				for _, e := range events {
					if e.DateInt == dateInt {
						if e.Cat == 1 {
							// 除权除息
							pPrev := k.Close
							pEx := (pPrev - e.F4 + e.F2*e.F3) / (1.0 + e.F1 + e.F2)
							if pEx > 0 {
								adjFactor *= (pPrev / pEx)
							}
						} else {
							// 股本快照
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
