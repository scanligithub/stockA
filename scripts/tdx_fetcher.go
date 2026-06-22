package main

import (
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/injoyai/tdx"
)

// ==========================================
// 1. GBBQ 权息与股本快照数据结构
// ==========================================

// GbbqRaw 定义通达信 gbbq.dat 的 29 字节二进制结构
type GbbqRaw struct {
	Market   byte    // 0 = SZ, 1 = SH
	Code     [6]byte // 股票代码
	Date     uint32  // 日期 (如 20240331)
	Category byte    // 1=除权除息, 2/3/5/7/8/9/10=股本快照
	Value1   float32 // Category1: 分红 | Category2-10: 盘前流通股本
	Value2   float32 // Category1: 配股 | Category2-10: 盘前总股本
	Value3   float32 // Category1: 配股价 | Category2-10: 盘后流通股本
	Value4   float32 // Category1: 送转股 | Category2-10: 盘后总股本
}

// XrxdEvent 除权除息事件
type XrxdEvent struct {
	Date     uint32
	FenHong  float64 // 每股分红 (已除以10.0)
	PeiShare float64 // 每股配股 (已除以10.0)
	PeiPrice float64 // 配股价
	SongShare float64 // 每股送转股 (已除以10.0)
}

// EquityEvent 股本快照事件
type EquityEvent struct {
	Date        uint32
	FloatShares float64 // 盘后流通股本 (单位：股)
	TotalShares float64 // 盘后总股本 (单位：股)
}

// GlobalGbbqStore 内存缓存
var (
	GlobalXrxdMap   = make(map[string][]XrxdEvent)
	GlobalEquityMap = make(map[string][]EquityEvent)
)

// ==========================================
// 2. 二进制 GBBQ 本地文件解析引擎
// ==========================================
func loadGbbqDat(filePath string) error {
	file, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	var count int
	buf := make([]byte, 29) // 严格 29 字节物理对齐，防止 Go 编译器结构体内存对齐优化导致的错位

	for {
		_, err := io.ReadFull(file, buf)
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			break
		}
		if err != nil {
			return err
		}

		// 极速二进制解包
		code := string(buf[1:7])
		date := binary.LittleEndian.Uint32(buf[7:11])
		category := buf[11]
		val1 := math.Float32frombits(binary.LittleEndian.Uint32(buf[12:16]))
		val2 := math.Float32frombits(binary.LittleEndian.Uint32(buf[16:20]))
		val3 := math.Float32frombits(binary.LittleEndian.Uint32(buf[20:24]))
		val4 := math.Float32frombits(binary.LittleEndian.Uint32(buf[24:28]))

		if category == 1 {
			// Category 1: 除权除息事件 (原数值为每 10 股，除以 10 换算为单股)
			event := XrxdEvent{
				Date:      date,
				FenHong:   float64(val1) / 10.0,
				PeiShare:  float64(val2) / 10.0,
				PeiPrice:  float64(val3),
				SongShare: float64(val4) / 10.0,
			}
			GlobalXrxdMap[code] = append(GlobalXrxdMap[code], event)
		} else if category == 2 || category == 3 || category == 5 || category == 7 || category == 8 || category == 9 || category == 10 {
			// Category 2-10: 股本快照事件 (val3和val4单位通常为股，保持原样)
			event := EquityEvent{
				Date:        date,
				FloatShares: float64(val3),
				TotalShares: float64(val4),
			}
			GlobalEquityMap[code] = append(GlobalEquityMap[code], event)
		}
		count++
	}

	// 按时间升序对每只股票的所有事件进行严格排序，以便后续双指针 AsOf 对齐
	for code := range GlobalXrxdMap {
		sort.Slice(GlobalXrxdMap[code], func(i, j int) bool {
			return GlobalXrxdMap[code][i].Date < GlobalXrxdMap[code][j].Date
		})
	}
	for code := range GlobalEquityMap {
		sort.Slice(GlobalEquityMap[code], func(i, j int) bool {
			return GlobalEquityMap[code][i].Date < GlobalEquityMap[code][j].Date
		})
	}

	fmt.Printf("[+] GBBQ 解析引擎运行完毕。共处理二进制记录: %d 行 | 缓存除权股: %d | 缓存股本快照股: %d\n", 
		count, len(GlobalXrxdMap), len(GlobalEquityMap))
	return nil
}

// ==========================================
// 3. 股票列表拉取元数据生成器
// ==========================================
type StockMeta struct {
	Code     string `json:"code"`
	CodeName string `json:"code_name"`
}

func fetchAndSaveStockList() {
	fmt.Println("[*] 正在拨号通达信获取全市场最新股票列表...")
	cli, err := tdx.DialDefault()
	if err != nil {
		fmt.Printf("❌ 拨号失败: %v\n", err)
		os.exit(1)
	}
	defer cli.Close()

	stocks, err := cli.GetStockCodeAll()
	if err != nil {
		fmt.Printf("❌ 获取股票列表失败: %v\n", err)
		os.exit(1)
	}

	var metaList []StockMeta
	for _, s := range stocks {
		// 统一转换为标准的 lower_case 带后缀格式 (如 sz.000001)
		market := "sz"
		if s.Market == 1 {
			market = "sh"
		} else if s.Market == 2 {
			market = "bj"
		}
		formattedCode := fmt.Sprintf("%s.%s", market, s.Code)
		metaList = append(metaList, StockMeta{
			Code:     formattedCode,
			CodeName: s.Name,
		})
	}

	outBytes, err := json.MarshalIndent(metaList, "", "  ")
	if err != nil {
		fmt.Printf("❌ JSON 序列化失败: %v\n", err)
		os.exit(1)
	}

	err = os.WriteFile("stock_list_master.json", outBytes, 0644)
	if err != nil {
		fmt.Printf("❌ 写入 stock_list_master.json 失败: %v\n", err)
		os.exit(1)
	}
	fmt.Printf("✅ 成功拉取全市场 %d 只个股元数据并保存至 stock_list_master.json\n", len(metaList))
}

// ==========================================
// 4. 并发 K 线拉取与股本/市值对齐主逻辑
// ==========================================
func parseDateString(dateStr string) uint32 {
	// "2024-03-31" -> 20240331
	var y, m, d int
	_, err := fmt.Sscanf(dateStr, "%04d-%02d-%02d", &y, &m, &d)
	if err != nil {
		return 0
	}
	return uint32(y*10000 + m*100 + d)
}

func main() {
	modeFlag := flag.String("mode", "fetch", "运行模式: list 或 fetch")
	codesFlag := flag.String("codes", "", "待下载的股票代码列表 (逗号分隔，如 sz.000001,sh.600000)")
	outFlag := flag.String("out", "temp_kline.csv", "输出的临时 CSV 文件路径")
	flag.Parse()

	// 1. 如果是 list 模式，秒级生成 Master List 并退出
	if *modeFlag == "list" {
		fetchAndSaveStockList()
		return
	}

	// 2. 载入本地 GBBQ 除权除息与股本快照数据库
	gbbqPath := "gbbq.dat"
	if _, err := os.Stat(gbbqPath); os.IsNotExist(err) {
		fmt.Printf("❌ 错误: 未在根目录下找到 %s 权息文件！请检查 Actions 下载步骤。\n", gbbqPath)
		os.exit(1)
	}
	err := loadGbbqDat(gbbqPath)
	if err != nil {
		fmt.Printf("❌ 错误: 解析 GBBQ 文件失败: %v\n", err)
		os.exit(1)
	}

	// 3. 解析输入股票
	if *codesFlag == "" {
		fmt.Println("❌ 错误: fetch 模式下 -codes 参数不能为空！")
		os.exit(1)
	}
	rawCodes := strings.Split(*codesFlag, ",")
	fmt.Printf("[*] 接收本批待下载任务标的: %d 只\n", len(rawCodes))

	// 4. 创建 CSV 文件并写入 14 列标准量化 Schema
	outFile, err := os.Create(*outFlag)
	if err != nil {
		fmt.Printf("❌ 无法创建输出文件 %s: %v\n", *outFlag, err)
		os.exit(1)
	}
	defer outFile.Close()

	// 写入标准的 14 列头部
	_, _ = outFile.WriteString("code,date,open,high,low,close,volume,amount,adjustFactor,total_shares,float_shares,total_mv,float_mv,turn\n")

	var (
		mu         sync.Mutex
		wg         sync.WaitGroup
		semaphore  = make(chan struct{}, 8) // 严格限制 8 并发，保护通达信免费服务器
		errorCount int
	)

	// 5. 并发执行 K 线拉取与 AsOf 闭环算法
	for _, fullCode := range rawCodes {
		wg.Add(1)
		semaphore <- struct{}{}

		go func(code string) {
			defer wg.Done()
			defer func() { <-semaphore }()

			// 转换为通达信原生代码：sh.600000 -> 600000
			parts := strings.Split(code, ".")
			if len(parts) != 2 {
				return
			}
			pureCode := parts[1]

			// 独立拨号，协程间连接隔离
			cli, err := tdx.DialDefault()
			if err != nil {
				mu.Lock()
				errorCount++
				mu.Unlock()
				return
			}
			defer cli.Close()

			// 顺向抓取全量日线
			klines, err := cli.GetKlineDayAll(pureCode)
			if err != nil {
				mu.Lock()
				errorCount++
				mu.Unlock()
				return
			}

			if len(klines) == 0 {
				return
			}

			// 获取该股对应的 XRXD（复权）和 Equity（股本）事件流
			xrxdList := GlobalXrxdMap[pureCode]
			equityList := GlobalEquityMap[pureCode]

			// 核心算法初始化
			adjustFactor := 1.0
			var (
				lastFloatShares float64 = 0.0
				lastTotalShares float64 = 0.0
			)

			// 如果该股有股本记录，且首个记录在上市之后，我们尝试用首条记录作为上市日默认底牌
			if len(equityList) > 0 {
				lastFloatShares = equityList[0].FloatShares
				lastTotalShares = equityList[0].TotalShares
			}

			var csvRows []string

			// 正序时间轴迭代
			for i, bar := range klines {
				barDateInt := parseDateString(bar.Date)

				// 🚀 A. 动态计算复权因子 adjustFactor (Category = 1)
				// 寻找当日发生的除权除息事件并计算理论除权价
				for _, x := range xrxdList {
					if x.Date == barDateInt && i > 0 {
						prevClose := klines[i-1].Close
						// 理论除权价格计算公式 (通达信官方)
						pEx := (prevClose - x.FenHong + x.PeiShare*x.PeiPrice) / (1.0 + x.SongShare + x.PeiShare)
						if pEx > 0 {
							adjustFactor *= (prevClose / pEx)
						}
						break
					}
				}

				// 🚀 B. 动态匹配最新股本快照 (Category = 2/3/5/7/8/9/10)
				// 使用双指针 ASOF 寻找在当日或之前发生的最新一次股本变动
				for _, eq := range equityList {
					if eq.Date <= barDateInt {
						lastFloatShares = eq.FloatShares
						lastTotalShares = eq.TotalShares
					} else {
						break
					}
				}

				// 🚀 C. 衍生指标计算
				totalMV := float64(bar.Close) * lastTotalShares
				floatMV := float64(bar.Close) * lastFloatShares

				// 换手率 = (成交量 / 流通股本) * 100
				// 注意：通达信 K 线的 Volume 单位为手 (1手=100股)，而 GBBQ 股本单位为股，因此分子要乘以 100
				turn := 0.0
				if lastFloatShares > 0 {
					turn = (float64(bar.Volume) * 100.0 / lastFloatShares) * 100.0
				}

				// 构造极度紧凑的 CSV 格式行
				row := fmt.Sprintf("%s,%s,%.3f,%.3f,%.3f,%.3f,%.0f,%.0f,%.6f,%.0f,%.0f,%.0f,%.0f,%.4f\n",
					code,
					bar.Date,
					bar.Open,
					bar.High,
					bar.Low,
					bar.Close,
					bar.Volume,
					bar.Amount,
					adjustFactor,
					lastTotalShares,
					lastFloatShares,
					totalMV,
					floatMV,
					turn,
				)
				csvRows = append(csvRows, row)
			}

			// 协程安全写入
			mu.Lock()
			for _, r := range csvRows {
				_, _ = outFile.WriteString(r)
			}
			mu.Unlock()

		}(fullCode)
	}

	wg.Wait()
	
	// 计算完成后的磁盘大小
	fInfo, _ := outFile.Stat()
	fmt.Printf("✅ 批次下载完成。生成临时物理文件: %s (大小: %.2f MB) | 失败协程数: %d\n", 
		*outFlag, float64(fInfo.Size())/(1024*1024), errorCount)
}
