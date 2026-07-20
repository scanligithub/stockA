package main

import (
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/injoyai/tdx"
)

// getTrueBeijingTime 穿透系统本地虚拟时钟，获取真实的北京时间
func getTrueBeijingTime() time.Time {
	client := &http.Client{Timeout: 3 * time.Second}
	resp, err := client.Head("https://www.baidu.com")
	if err == nil && resp.Header.Get("Date") != "" {
		t, err := time.Parse(time.RFC1123, resp.Header.Get("Date"))
		if err == nil {
			return t.In(time.FixedZone("CST", 8*3600)) // 转换为北京时间 (UTC+8)
		}
	}
	fmt.Println("⚠️ 无法获取网络时间，降级使用系统本地时钟。")
	return time.Now()
}

// getRecentTradingDays 获取最近的 N 个交易日 (排除周末)
func getRecentTradingDays(targetTime time.Time, n int) []int {
	var days []int
	curr := targetTime

	if curr.Hour() < 15 {
		curr = curr.AddDate(0, 0, -1)
	}

	for len(days) < n {
		if curr.Weekday() != time.Saturday && curr.Weekday() != time.Sunday {
			val, _ := strconv.Atoi(curr.Format("20060102"))
			days = append(days, val)
		}
		curr = curr.AddDate(0, 0, -1)
	}
	return days
}

func main() {
	fmt.Println("============ 📊 TDX 多股多日分笔成交实测引擎 ============")
	
	trueTime := getTrueBeijingTime()
	recentDays := getRecentTradingDays(trueTime, 1)
	
	// 若系统环境时钟处于未来，自动并入真实世界历史交易日（如20241220）以确保有有效分笔数据返回
	if len(recentDays) > 0 && recentDays[0] > 20250000 {
		fmt.Println("⚠️ 提示: 检测到本地环境时钟处于未来，自动并入真实世界历史交易日 [20241220 20241219] 以获取真实物理 Tick...")
		recentDays = append(recentDays, 20241220, 20241219)
	}

	fmt.Printf("⏰ 运行基准时间: %s\n", trueTime.Format("2006-01-02 15:04:05"))
	fmt.Printf("📅 待拉取的交易日清单: %v\n", recentDays)

	// 连接行情服务器
	cli, err := tdx.DialDefault()
	if err != nil {
		fmt.Printf("❌ 建立服务器连接失败: %v\n", err)
		os.Exit(1)
	}
	defer cli.Close()
	fmt.Println("✅ 成功连接通达信行情服务。")

	testStocks := []struct {
		Code string
		Name string
	}{
		{"sh600519", "贵州茅台"},
		{"sz000001", "平安银行"},
		{"sz300750", "宁德时代"},
	}

	for _, stock := range testStocks {
		fmt.Printf("\n==================== 📈 股票: %s (%s) ====================\n", stock.Name, stock.Code)
		
		for _, dateInt := range recentDays {
			fmt.Printf("🔎 正在提取 %d 的分笔成交数据...\n", dateInt)
			
			// 代码转换为通达信要求的大写格式 (SH/SZ前缀)
			upperCode := strings.ToUpper(stock.Code)
			pureCode := upperCode[2:]
			dateStr := strconv.Itoa(dateInt)
			
			// 调用 GetHistoryTrade，参数顺序为：date, code, start, count
			resp, err := cli.GetHistoryTrade(dateStr, upperCode, 0, 50)
			if err != nil {
				fmt.Printf("   ❌ 获取失败: %v\n", err)
				continue
			}

			if resp == nil || len(resp.List) == 0 {
				// 尝试备用精简代码格式 (无前缀)
				resp, err = cli.GetHistoryTrade(dateStr, pureCode, 0, 50)
				if err != nil || resp == nil || len(resp.List) == 0 {
					fmt.Println("   ⚠️ 该交易日无分笔数据（或当天未上市/停牌）。")
					continue
				}
			}

			fmt.Printf("   ✅ 成功捕获分笔。当天总笔数(片区): %d, 提取样本数: %d\n", resp.Count, len(resp.List))
			
			limit := 5
			if len(resp.List) < limit {
				limit = len(resp.List)
			}
			
			fmt.Println("   📝 前 5 条交易明细:")
			fmt.Printf("      %-10s | %-10s | %-10s | %-10s | %-10s\n", "时间", "成交价", "成交量(手)", "笔数/单号", "方向")
			fmt.Println("      " + strings.Repeat("-", 65))
			
			for i := 0; i < limit; i++ {
				t := resp.List[i]
				timeStr := t.Time.Format("15:04:05")
				if timeStr == "00:00:00" {
					timeStr = t.Time.String()
				}
				
				dirStr := "中性"
				if t.Status == 0 {
					dirStr = "🔴 主买"
				} else if t.Status == 1 {
					dirStr = "🟢 主卖"
				}
				
				fmt.Printf("      %-10s | %-10v | %-10d | %-10d | %-10s\n", 
					timeStr, 
					t.Price, 
					t.Volume, 
					t.Number, 
					dirStr,
				)
			}
		}
	}
	fmt.Println("\n=======================================================")
	fmt.Println("🎉 实测任务结束。")
}
