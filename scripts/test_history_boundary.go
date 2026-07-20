package main

import (
	"fmt"
	"os"

	"github.com/injoyai/tdx"
)

type Checkpoint struct {
	Label string
	Date  string
}

func main() {
	fmt.Println("============ 🧭 TDX 历史分笔深度探测引擎 ============")
	
	cli, err := tdx.DialDefault()
	if err != nil {
		fmt.Printf("❌ 连接服务器失败: %v\n", err)
		os.Exit(1)
	}
	defer cli.Close()
	fmt.Println("✅ 成功连接行情服务。")

	// 定义测试回溯检查点
	checkpoints := []Checkpoint{
		{"1 年前  (2024-01-15 星期一)", "20240115"},
		{"2 年前  (2023-01-16 星期一)", "20230116"},
		{"3 年前  (2022-01-14 星期五)", "20220114"},
		{"4 年前  (2021-01-15 星期五)", "20210115"},
		{"5 年前  (2020-01-15 星期三)", "20200115"},
		{"6 年前  (2019-01-15 星期二)", "20190115"},
		{"8 年前  (2017-01-16 星期一)", "20170116"},
		{"10 年前 (2015-01-15 星期四)", "20150115"},
		{"15 年前 (2010-01-15 星期五)", "20100115"},
		{"20 年前 (2005-01-14 星期五)", "20050114"},
	}

	testStock := "SZ000001" // 平安银行 (1991年上市)
	fmt.Printf("🎯 探测标的: %s\n\n", testStock)

	for _, cp := range checkpoints {
		fmt.Printf("🔎 正在尝试探测 [%s] 的成交分笔...\n", cp.Label)
		
		resp, err := cli.GetHistoryTrade(cp.Date, testStock, 0, 5)
		if err != nil {
			fmt.Printf("   ❌ 失败 (接口报错): %v\n", err)
			continue
		}

		if resp == nil || len(resp.List) == 0 {
			fmt.Println("   ⚠️ 空数据 (服务器无此历史缓存)")
			continue
		}

		fmt.Printf("   🎉 成功！当天分笔总数(片区): %d | 获取样本首笔价格: %v 元 | 时间: %v\n", 
			resp.Count, 
			resp.List[0].Price, 
			resp.List[0].Time.Format("15:04:05"),
		)
	}
	fmt.Println("\n=======================================================")
	fmt.Println("🎉 深度探测任务完成。")
}
