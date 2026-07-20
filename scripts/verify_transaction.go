// FILE: scripts/test_fetch_trades.go
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
	// 通过公共服务器的 Date 响应头获取真实世界的时间
	resp, err := client.Head("https://www.baidu.com")
	if err == nil && resp.Header.Get("Date") != "" {
		t, err := time.Parse(time.RFC1123, resp.Header.Get("Date"))
		if err == nil {
			return t.In(time.FixedZone("CST", 8*3600)) // 强制转换为北京时间 (UTC+8)
		}
	}
	fmt.Println("⚠️ 无法获取网络时间，降级使用系统本地时钟。")
	return time.Now()
}

// getRecentTradingDays 获取最近的 N 个真实交易日 (排除周末)
func getRecentTradingDays(targetTime time.Time, n int) []int {
	var days []int
	curr := targetTime

	// 如果当前时间还未收盘(15:00前)，不包含今天，从昨天开始算
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
	
	// 1. 获取真实世界的交易日期线
	trueTime := getTrueBeijingTime()
	recentDays := getRecentTradingDays(trueTime, 3)
	
	fmt.Printf("⏰ 真实北京时间: %s\n", trueTime.Format("2006-01-02 15:04:05"))
	fmt.Printf("📅 探测提取的最近 3 个真实交易日: %v\n", recentDays)

	// 2. 连接通达信行情服务器
	cli, err := tdx.DialDefault()
	if err != nil {
		fmt.Printf("❌ 建立服务器连接失败: %v\n", err)
		os.Exit(1)
	}
	defer cli.Close()
	fmt.Println("✅ 成功连接通达信行情服务。")

	// 3. 定义测试股票列表
	testStocks := []struct {
		Code string
		Name string
	}{
		{"sh600519", "贵州茅台"},
		{"sz000001", "平安银行"},
		{"sz300750", "宁德时代"},
	}

	// 4. 执行数据抓取
	for _, stock := range testStocks {
		fmt.Printf("\n==================== 📈 股票: %s (%s) ====================\n", stock.Name, stock.Code)
		
		for _, dateInt := range recentDays {
			fmt.Printf("🔎 正在提取 %d 的分笔成交数据...\n", dateInt)
			
			// 转换代码格式：sz000001 -> 000001
			pureCode := stock.Code[2:]
			
			// 调用 GetHistoryTrade 提取单日历史 Tick (参数：代码, 日期, 起始索引, 数量)
			// 注意：部分历史节点需要指定对应的具体市场代码前缀，或通过接口自适应
			resp, err := cli.GetHistoryTrade(stock.Code, uint32(dateInt), 0, 50)
			if err != nil {
				fmt.Printf("   ❌ 获取失败: %v\n", err)
				continue
			}

			if resp == nil || len(resp.List) == 0 {
				// 尝试备用接口或不带前缀格式
				resp, err = cli.GetHistoryTrade(pureCode, uint32(dateInt), 0, 50)
				if err != nil || resp == nil || len(resp.List) == 0 {
					fmt.Println("   ⚠️ 该交易日无分笔数据（可能服务器该节点未缓存此历史分笔，或当天停牌）。")
					continue
				}
			}

			fmt.Printf("   ✅ 成功捕获分笔。当天总笔数(片区): %d, 提取样本数: %d\n", resp.Count, len(resp.List))
			
			// 打印前 5 条分笔成交明细
			limit := 5
			if len(resp.List) < limit {
				limit = len(resp.List)
			}
			
			fmt.Println("   📝 前 5 条交易明细:")
			fmt.Printf("      %-10s | %-10s | %-10s | %-10s | %-10s\n", "时间", "成交价", "成交量(手)", "笔数/单号", "方向(0:买/1:卖/2:中)")
			fmt.Println("      " + strings.Repeat("-", 65))
			
			for i := 0; i < limit; i++ {
				t := resp.List[i]
				
				// 格式化时间
				timeStr := t.Time.Format("15:04:05")
				if timeStr == "00:00:00" {
					timeStr = t.Time.String()
				}
				
				// 转换成交方向符号
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
