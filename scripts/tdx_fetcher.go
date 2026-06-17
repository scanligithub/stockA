package main

import (
	"encoding/binary"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"strings"

	"github.com/injoyai/tdx"
	"github.com/injoyai/tdx/protocol"
)

// GbbqEvent 记录通达信的权息和股本变动快照
type GbbqEvent struct {
	Date     uint32
	Category uint8
	F1, F2, F3, F4 float32
}

// parseGbbqDat: 极速读取 gbbq.dat 二进制文件，无惧内存对齐问题
func parseGbbqDat(path string) map[string][]GbbqEvent {
	f, err := os.Open(path)
	if err != nil {
		log.Fatalf("无法打开 gbbq.dat: %v", err)
	}
	defer f.Close()

	gbbqMap := make(map[string][]GbbqEvent)
	buf := make([]byte, 29)

	for {
		_, err := io.ReadFull(f, buf)
		if err != nil {
			break
		}

		marketByte := buf[0]
		codeStr := strings.Trim(string(buf[1:8]), "\x00")
		date := binary.LittleEndian.Uint32(buf[8:12])
		category := buf[12]
		f1 := math.Float32frombits(binary.LittleEndian.Uint32(buf[13:17]))
		f2 := math.Float32frombits(binary.LittleEndian.Uint32(buf[17:21]))
		f3 := math.Float32frombits(binary.LittleEndian.Uint32(buf[21:25]))
		f4 := math.Float32frombits(binary.LittleEndian.Uint32(buf[25:29]))

		prefix := "sz"
		if marketByte == 1 {
			prefix = "sh"
		} else if marketByte == 2 {
			prefix = "bj"
		}
		fullCode := prefix + "." + codeStr

		gbbqMap[fullCode] = append(gbbqMap[fullCode], GbbqEvent{
			Date:     date,
			Category: category,
			F1: f1, F2: f2, F3: f3, F4: f4,
		})
	}
	return gbbqMap
}

func fetchKlines(client *tdx.Client, market uint8, code string) []protocol.KLine {
	var all []protocol.KLine
	start := uint16(0)
	for {
		kl, err := client.GetKLine(market, code, protocol.KLineDay, start, 800)
		if err != nil || len(kl) == 0 {
			break
		}
		all = append(all, kl...)
		if len(kl) < 800 {
			break
		}
		start += 800
	}
	// 将数据翻转为时间正序 (历史 -> 至今)
	for i, j := 0, len(all)-1; i < j; i, j = i+1, j-1 {
		all[i], all[j] = all[j], all[i]
	}
	return all
}

func getStockList(client *tdx.Client) {
	var list []map[string]interface{}
	for m := uint8(0); m <= 1; m++ {
		start := uint16(0)
		for {
			stocks, _ := client.GetSecurityList(m, start)
			if len(stocks) == 0 {
				break
			}
			for _, s := range stocks {
				code := s.Code
				if (m == 1 && strings.HasPrefix(code, "6")) || (m == 0 && (strings.HasPrefix(code, "0") || strings.HasPrefix(code, "3"))) {
					prefix := "sz"
					if m == 1 {
						prefix = "sh"
					}
					list = append(list, map[string]interface{}{
						"code":      prefix + "." + code,
						"code_name": s.Name,
					})
				}
			}
			start += 1000
		}
	}
	b, _ := json.Marshal(list)
	os.WriteFile("stock_list_master.json", b, 0644)
	fmt.Println("✅ 股票列表已生成.")
}

func main() {
	mode := flag.String("mode", "fetch", "list or fetch")
	codesParam := flag.String("codes", "", "comma separated codes")
	outParam := flag.String("out", "out.csv", "output csv")
	flag.Parse()

	client, err := tdx.DialDefault()
	if err != nil {
		log.Fatalf("TDX 拨号失败: %v", err)
	}
	defer client.Close()

	if *mode == "list" {
		getStockList(client)
		return
	}

	codes := strings.Split(*codesParam, ",")
	gbbqMap := parseGbbqDat("gbbq.dat")

	f, _ := os.Create(*outParam)
	defer f.Close()
	writer := csv.NewWriter(f)
	// 写入新的 14 列表头
	writer.Write([]string{"date", "code", "open", "high", "low", "close", "volume", "amount", "adjustFactor", "turn", "total_shares", "float_shares", "total_mv", "float_mv"})

	for _, fullCode := range codes {
		parts := strings.Split(fullCode, ".")
		if len(parts) != 2 {
			continue
		}
		market := uint8(0)
		if parts[0] == "sh" {
			market = 1
		}
		pureCode := parts[1]

		klines := fetchKlines(client, market, pureCode)
		events := gbbqMap[fullCode]

		eventIdx := 0
		var totalShares float64 = 0.0
		var floatShares float64 = 0.0
		var adjFactor float64 = 1.0
		var prevClose float64 = 0.0

		for _, kl := range klines {
			y, m, d := kl.Date.Date()
			dateInt := uint32(y*10000 + int(m)*100 + d)
			dateStr := kl.Date.Format("2006-01-02")

			// 使用 ASOF (As of) 逻辑步进匹配最新的股本与除权快照
			for eventIdx < len(events) && events[eventIdx].Date <= dateInt {
				ev := events[eventIdx]
				if ev.Category == 1 {
					// 1=除权除息: 计算后复权因子
					pEx := (prevClose - float64(ev.F4)/10.0 + float64(ev.F2/10.0*ev.F3)) / (1.0 + float64(ev.F1)/10.0 + float64(ev.F2)/10.0)
					if pEx > 0 && prevClose > 0 {
						adjFactor *= (prevClose / pEx)
					}
				} else if ev.Category >= 2 && ev.Category <= 10 {
					// 2~10=股本变动快照 (F3:流通盘, F4:总股本, 单位:万股)
					if ev.F3 > 0 {
						floatShares = float64(ev.F3) * 10000.0 // 转为实际股数
					}
					if ev.F4 > 0 {
						totalShares = float64(ev.F4) * 10000.0 // 转为实际股数
					}
				}
				eventIdx++
			}

			// 衍生指标离线测算
			turn := 0.0
			if floatShares > 0 {
				turn = (kl.Vol / floatShares) * 100.0
			}
			totalMV := kl.Close * totalShares
			floatMV := kl.Close * floatShares

			writer.Write([]string{
				dateStr, fullCode,
				fmt.Sprintf("%.2f", kl.Open), fmt.Sprintf("%.2f", kl.High),
				fmt.Sprintf("%.2f", kl.Low), fmt.Sprintf("%.2f", kl.Close),
				fmt.Sprintf("%.0f", kl.Vol), fmt.Sprintf("%.0f", kl.Amount),
				fmt.Sprintf("%.6f", adjFactor),
				fmt.Sprintf("%.4f", turn),
				fmt.Sprintf("%.0f", totalShares), fmt.Sprintf("%.0f", floatShares),
				fmt.Sprintf("%.2f", totalMV), fmt.Sprintf("%.2f", floatMV),
			})
			prevClose = kl.Close
		}
	}
	writer.Flush()
}
