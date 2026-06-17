package main

import (
	"archive/zip"
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/injoyai/tdx"
)

// GbbqEvent 对应 Category=1 的除权除息事件
type GbbqEvent struct {
	Code     string
	Date     uint32
	Category uint8
	Fh       float64 // 分红 (C1)
	PeiJia   float64 // 配股价 (C2)
	Sg       float64 // 送转股 (C3)
	Pei      float64 // 配股比例 (C4)
}

// CapitalEvent 对应 Category >= 2 的股本变动事件
type CapitalEvent struct {
	Code        string
	Date        uint32
	TotalShares float64 // C4 - 后总股本
	FloatShares float64 // C3 - 盘后流通股本
}

// FinancialRecord 对应从 gpcw 财务包解析的季度快照
type FinancialRecord struct {
	ReportDate   uint32  // 报告期 (如 20231231)
	AnnounceDate uint32  // 实际披露公告日
	NetAssets    float64 // 归属母公司股东权益合计 (Field 37)
	NetProfit    float64 // 归属母公司股东净利润 (Field 64)
}

// CSVRow 定义输出的 CSV 结构，严格对齐数据格式
type CSVRow struct {
	Date         string
	Code         string
	Open         float64
	High         float64
	Low          float64
	Close        float64
	Volume       float64
	Amount       float64
	AdjustFactor float64
	Turn         float64
	PeTTM        float64
	PbMRQ        float64
	TotalShares  float64
	FloatShares  float64
	TotalMV      float64
	FloatMV      float64
}

// parseGbbqDBF 实现 dBASE III (DBF) 格式的本地极速解析
func parseGbbqDBF(filePath string) ([]GbbqEvent, []CapitalEvent, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, nil, err
	}
	defer file.Close()

	header := make([]byte, 32)
	if _, err := io.ReadFull(file, header); err != nil {
		return nil, nil, err
	}

	headerLen := binary.LittleEndian.Uint16(header[8:10])
	recordLen := binary.LittleEndian.Uint16(header[10:12])

	if _, err := file.Seek(int64(headerLen), io.SeekStart); err != nil {
		return nil, nil, err
	}

	var gbbqEvents []GbbqEvent
	var capEvents []CapitalEvent

	buf := make([]byte, recordLen)
	for {
		_, err := io.ReadFull(file, buf)
		if err == io.EOF || (len(buf) > 0 && buf[0] == 0x1A) {
			break
		}
		if err != nil {
			break
		}

		if buf[0] == '*' { // 被删除的记录
			continue
		}

		// 静态偏移量读取 DBF ASCII 数据列
		code := strings.TrimSpace(string(buf[1:7]))
		dateStr := strings.TrimSpace(string(buf[7:15]))
		catStr := strings.TrimSpace(string(buf[15:17]))

		dateVal, _ := strconv.Atoi(dateStr)
		catVal, _ := strconv.Atoi(catStr)

		if dateVal == 0 || code == "" {
			continue
		}

		c1Val, _ := strconv.ParseFloat(strings.TrimSpace(string(buf[17:31])), 64)
		c2Val, _ := strconv.ParseFloat(strings.TrimSpace(string(buf[31:45])), 64)
		c3Val, _ := strconv.ParseFloat(strings.TrimSpace(string(buf[45:59])), 64)
		c4Val, _ := strconv.ParseFloat(strings.TrimSpace(string(buf[59:73])), 64)

		if catVal == 1 {
			gbbqEvents = append(gbbqEvents, GbbqEvent{
				Code:     code,
				Date:     uint32(dateVal),
				Category: uint8(catVal),
				Fh:       c1Val,
				PeiJia:   c2Val,
				Sg:       c3Val,
				Pei:      c4Val,
			})
		} else if catVal >= 2 && catVal <= 10 {
			capEvents = append(capEvents, CapitalEvent{
				Code:        code,
				Date:        uint32(dateVal),
				TotalShares: c4Val,
				FloatShares: c3Val,
			})
		}
	}

	return gbbqEvents, capEvents, nil
}

// parseGpcwFiles 并行解析 gpcw_zips 目录下的全量季度财务 ZIP 文件
func parseGpcwFiles(dirPath string) (map[string][]FinancialRecord, map[string]map[uint32]FinancialRecord, error) {
	files, err := os.ReadDir(dirPath)
	if err != nil {
		return nil, nil, err
	}

	announceMap := make(map[string][]FinancialRecord)
	reportMap := make(map[string]map[uint32]FinancialRecord)

	reNum := regexp.MustCompile(`\d+`)

	for _, f := range files {
		if !strings.HasSuffix(f.Name(), ".zip") || !strings.HasPrefix(f.Name(), "gpcw") {
			continue
		}

		nums := reNum.FindString(f.Name())
		if len(nums) < 4 {
			continue
		}

		var reportDateVal int
		// 适配简写与标准格式
		if len(nums) == 4 {
			reportDateVal, _ = strconv.Atoi("20" + nums + "00") // 简写 fallback
		} else if len(nums) >= 8 {
			reportDateVal, _ = strconv.Atoi(nums[:8])
		} else {
			continue
		}

		zipPath := filepath.Join(dirPath, f.Name())
		zipReader, err := zip.OpenReader(zipPath)
		if err != nil {
			continue
		}

		for _, zf := range zipReader.File {
			// 🌟 修复点 1：使用 zf.Name 成员字段而非 zf.Name() 函数调用
			if !strings.HasSuffix(zf.Name, ".dat") {
				continue
			}
			rc, err := zf.Open()
			if err != nil {
				continue
			}

			// 跳过 26 字节文件头
			header := make([]byte, 26)
			if _, err := io.ReadFull(rc, header); err != nil {
				rc.Close()
				continue
			}

			buf := make([]byte, 308)
			for {
				_, err := io.ReadFull(rc, buf)
				if err == io.EOF {
					break
				}
				if err != nil {
					break
				}

				stockCode := strings.TrimSpace(string(buf[0:6]))
				if len(stockCode) != 6 {
					continue
				}

				announceDate := binary.LittleEndian.Uint32(buf[8:12])
				if announceDate == 0 || announceDate > 20991231 || announceDate < 19900101 {
					// 容错性防御：公告日异常时，强制以报告期 + 30天 占位推进时间轴
					announceDate = uint32(reportDateVal) + 30
				}

				// 归属母公司所有者权益 (Field 37 -> Offset 160)
				netAssetsBits := binary.LittleEndian.Uint32(buf[160:164])
				netAssets := float64(math.Float32frombits(netAssetsBits))

				// 归属母公司净利润 (Field 64 -> Offset 268)
				netProfitBits := binary.LittleEndian.Uint32(buf[268:272])
				netProfit := float64(math.Float32frombits(netProfitBits))

				record := FinancialRecord{
					ReportDate:   uint32(reportDateVal),
					AnnounceDate: announceDate,
					NetAssets:    netAssets,
					NetProfit:    netProfit,
				}

				announceMap[stockCode] = append(announceMap[stockCode], record)

				if reportMap[stockCode] == nil {
					reportMap[stockCode] = make(map[uint32]FinancialRecord)
				}
				reportMap[stockCode][uint32(reportDateVal)] = record
			}
			rc.Close()
		}
		zipReader.Close()
	}

	// 保证匹配时间轴严格升序
	for code := range announceMap {
		sort.Slice(announceMap[code], func(i, j int) bool {
			return announceMap[code][i].AnnounceDate < announceMap[code][j].AnnounceDate
		})
	}

	return announceMap, reportMap, nil
}

// getTTMProfit 执行跨季累计逻辑，算出 12 个月滚动净利润
func getTTMProfit(code string, rec FinancialRecord, reportMap map[string]map[uint32]FinancialRecord) float64 {
	reportDate := rec.ReportDate
	year := reportDate / 10000
	month := (reportDate % 10000) / 100

	stocksMap, exists := reportMap[code]
	if !exists {
		return rec.NetProfit
	}

	switch month {
	case 12: // 年报利润本身即为整年 TTM
		return rec.NetProfit
	case 3: // 一季报 TTM = Q1_Y + Q4_Y1 - Q1_Y1
		q4LastYear := (year-1)*10000 + 1231
		q1LastYear := (year-1)*10000 + 331
		r4, ex4 := stocksMap[q4LastYear]
		r1, ex1 := stocksMap[q1LastYear]
		if ex4 && ex1 {
			return rec.NetProfit + r4.NetProfit - r1.NetProfit
		}
	case 6: // 半年报 TTM = Q2_Y + Q4_Y1 - Q2_Y1
		q4LastYear := (year-1)*10000 + 1231
		q2LastYear := (year-1)*10000 + 630
		r4, ex4 := stocksMap[q4LastYear]
		r2, ex2 := stocksMap[q2LastYear]
		if ex4 && ex2 {
			return rec.NetProfit + r4.NetProfit - r2.NetProfit
		}
	case 9: // 三季报 TTM = Q3_Y + Q4_Y1 - Q3_Y1
		q4LastYear := (year-1)*10000 + 1231
		q3LastYear := (year-1)*10000 + 930
		r4, ex4 := stocksMap[q4LastYear]
		r3, ex3 := stocksMap[q3LastYear]
		if ex4 && ex3 {
			return rec.NetProfit + r4.NetProfit - r3.NetProfit
		}
	}
	return rec.NetProfit
}

func main() {
	modeFlag := flag.String("mode", "fetch", "list or fetch")
	codesFlag := flag.String("codes", "", "comma separated stock codes")
	outFlag := flag.String("out", "kline_out.csv", "output CSV file path")
	flag.Parse()

	if *modeFlag == "list" {
		cli, err := tdx.DialDefault()
		if err != nil {
			fmt.Printf("Dial Default Error: %v\n", err)
			os.Exit(1)
		}
		defer cli.Close()

		// 🌟 修复点 2：使用实例方法 cli.GetStockCodeAll() 代替包级别函数
		stocks, err := cli.GetStockCodeAll()
		if err != nil {
			fmt.Printf("Get Stock List Error: %v\n", err)
			os.Exit(1)
		}

		type MasterStock struct {
			Code     string `json:"code"`
			CodeName string `json:"code_name"`
		}

		var list []MasterStock
		for _, s := range stocks {
			code := s.Code
			if len(code) == 6 {
				prefix := ""
				if strings.HasPrefix(code, "6") {
					prefix = "sh."
				} else if strings.HasPrefix(code, "0") || strings.HasPrefix(code, "3") {
					prefix = "sz."
				} else if strings.HasPrefix(code, "4") || strings.HasPrefix(code, "8") {
					prefix = "bj."
				}
				if prefix != "" {
					list = append(list, MasterStock{
						Code:     prefix + code,
						CodeName: s.Name,
					})
				}
			}
		}

		fileData, _ := json.MarshalIndent(list, "", "  ")
		_ = os.WriteFile("stock_list_master.json", fileData, 0644)
		fmt.Printf("[+] 成功获取 %d 个 A 股股票信息并导出。\n", len(list))
		return
	}

	// Fetch 流程开始
	if *codesFlag == "" {
		fmt.Println("❌ 错误: --codes 参数不可为空。")
		os.Exit(1)
	}

	codesList := strings.Split(*codesFlag, ",")

	// 1. 加载 GBBQ 权息库与股本变更
	gEvents, cEvents, err := parseGbbqDBF("gbbq.dat")
	if err != nil {
		fmt.Printf("⚠️ 读取权息文件 gbbq.dat 失败: %v\n", err)
		os.Exit(1)
	}

	gbbqMap := make(map[string][]GbbqEvent)
	for _, ev := range gEvents {
		gbbqMap[ev.Code] = append(gbbqMap[ev.Code], ev)
	}
	for code := range gbbqMap {
		sort.Slice(gbbqMap[code], func(i, j int) bool {
			return gbbqMap[code][i].Date < gbbqMap[code][j].Date
		})
	}

	capMap := make(map[string][]CapitalEvent)
	for _, ev := range cEvents {
		capMap[ev.Code] = append(capMap[ev.Code], ev)
	}
	for code := range capMap {
		sort.Slice(capMap[code], func(i, j int) bool {
			return capMap[code][i].Date < capMap[code][j].Date
		})
	}

	// 2. 加载 GPCW 财务报表序列
	announceMap, reportMap, err := parseGpcwFiles("gpcw_zips")
	if err != nil {
		fmt.Printf("[!] 提示: 载入财务数据失败(正常现象): %v\n", err)
	}

	// 3. 多线程极速抓取 K 线，限制最大 8 连接，防止节点瞬时大流量被限流
	var wg sync.WaitGroup
	var mu sync.Mutex
	var allRows []CSVRow

	sem := make(chan struct{}, 8)

	for _, rawCode := range codesList {
		sem <- struct{}{}
		wg.Add(1)
		go func(code string) {
			defer func() {
				<-sem
				wg.Done()
			}()

			parts := strings.Split(code, ".")
			if len(parts) < 2 {
				return
			}
			pureCode := parts[1]

			cli, err := tdx.DialDefault()
			if err != nil {
				return
			}
			defer cli.Close()

			tdxPrefix := parts[0]
			tdxCode := tdxPrefix + pureCode

			// 🌟 修复点 3：使用实例方法 cli.GetKlineDayAll(tdxCode) 代替包级别函数
			resp, err := cli.GetKlineDayAll(tdxCode)
			if err != nil {
				return
			}

			events := gbbqMap[pureCode]
			caps := capMap[pureCode]
			finances := announceMap[pureCode]

			var localRows []CSVRow
			adjustFactor := 1.0

			for _, bar := range resp.List {
				dateVal, _ := strconv.Atoi(bar.Date)
				dateInt := uint32(dateVal)

				// A. 复权因子变动检测
				for _, ev := range events {
					if ev.Date == dateInt {
						pEx := (bar.Close - ev.Fh + ev.Pei*ev.PeiJia) / (1.0 + ev.Sg + ev.Pei)
						if pEx > 0 && bar.Close > 0 {
							adjustFactor *= (bar.Close / pEx)
						}
					}
				}

				// B. 时间轴 Asof 检索最新流通与总股本
				var totalShares float64 = 0.0
				var floatShares float64 = 0.0
				for _, cp := range caps {
					if cp.Date <= dateInt {
						totalShares = cp.TotalShares
						floatShares = cp.FloatShares
					} else {
						break
					}
				}

				// C. 时间轴 Asof 检索最新披露的财务指标（无未来函数）
				var netAssets float64 = 0.0
				var ttmProfit float64 = 0.0
				for _, fin := range finances {
					if fin.AnnounceDate <= dateInt {
						netAssets = fin.NetAssets
						ttmProfit = getTTMProfit(pureCode, fin, reportMap)
					} else {
						break
					}
				}

				// D. 派生指标复合计算
				totalMV := bar.Close * totalShares
				floatMV := bar.Close * floatShares

				turn := 0.0
				if floatShares > 0 {
					turn = (bar.Volume / floatShares) * 100.0
				}

				peTTM := 0.0
				if ttmProfit > 0 && totalMV > 0 {
					peTTM = totalMV / ttmProfit
				}

				pbMRQ := 0.0
				if netAssets > 0 && totalMV > 0 {
					pbMRQ = totalMV / netAssets
				}

				localRows = append(localRows, CSVRow{
					Date:         bar.Date,
					Code:         code,
					Open:         bar.Open,
					High:         bar.High,
					Low:          bar.Low,
					Close:        bar.Close,
					Volume:       bar.Volume,
					Amount:       bar.Amount,
					AdjustFactor: adjustFactor,
					Turn:         turn,
					PeTTM:        peTTM,
					PbMRQ:        pbMRQ,
					TotalShares:  totalShares,
					FloatShares:  floatShares,
					TotalMV:      totalMV,
					FloatMV:      floatMV,
				})
			}

			mu.Lock()
			allRows = append(allRows, localRows...)
			mu.Unlock()

		}(rawCode)
	}

	wg.Wait()

	// 4. 数据高精度落盘
	csvFile, err := os.Create(*outFlag)
	if err != nil {
		fmt.Printf("❌ 无法创建输出 CSV 文件: %v\n", err)
		os.Exit(1)
	}
	defer csvFile.Close()

	// 写入严格表头
	_, _ = csvFile.WriteString("date,code,open,high,low,close,volume,amount,adjustFactor,turn,peTTM,pbMRQ,total_shares,float_shares,total_mv,float_mv\n")

	for _, r := range allRows {
		line := fmt.Sprintf("%s,%s,%.4f,%.4f,%.4f,%.4f,%.4f,%.4f,%.6f,%.4f,%.4f,%.4f,%.4f,%.4f,%.4f,%.4f\n",
			r.Date, r.Code, r.Open, r.High, r.Low, r.Close, r.Volume, r.Amount, r.AdjustFactor,
			r.Turn, r.PeTTM, r.PbMRQ, r.TotalShares, r.FloatShares, r.TotalMV, r.FloatMV)
		_, _ = csvFile.WriteString(line)
	}

	fmt.Printf("[✓] 全量数据抓取及复权/估值指标计算完成，成功写入: %s\n", *outFlag)
}
