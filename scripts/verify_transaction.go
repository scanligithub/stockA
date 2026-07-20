// FILE: scripts/verify_transaction.go
package main

import (
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/injoyai/tdx"
)

// getRecentTradingDay 计算最近一个可能已收盘的交易日 (格式 YYYYMMDD)
func getRecentTradingDay() int {
	now := time.Now()
	// 如果是周六，退到周五
	if now.Weekday() == time.Saturday {
		now = now.AddDate(0, 0, -1)
	} else if now.Weekday() == time.Sunday {
		// 如果是周日，退到周五
		now = now.AddDate(0, 0, -2)
	} else if now.Hour() < 15 {
		// 如果是周一至周五但未收盘，使用前一天
		now = now.AddDate(0, 0, -1)
		// 如果退回后变成了周末，继续退到周五
		if now.Weekday() == time.Saturday {
			now = now.AddDate(0, 0, -1)
		} else if now.Weekday() == time.Sunday {
			now = now.AddDate(0, 0, -2)
		}
	}
	val, _ := strconv.Atoi(now.Format("20060102"))
	return val
}

func main() {
	fmt.Println("============ 📡 TDX 分笔成交 (Trade) 接口探测引擎 ============")
	fmt.Println("正在连接通达信行情服务器...")

	cli, err := tdx.DialDefault()
	if err != nil {
		fmt.Printf("❌ 连接失败: %v\n", err)
		os.Exit(1)
	}
	defer cli.Close()
	fmt.Println("✅ 成功建立连接。")

	val := reflect.ValueOf(cli)
	typ := val.Type()

	// 1. 过滤获取包含 "Trade" 且不含外部市场/底层通讯相关的候选方法
	var targetMethods []string
	for i := 0; i < typ.NumMethod(); i++ {
		m := typ.Method(i)
		nameLower := strings.ToLower(m.Name)
		// 排除 Ex 协议头（通常用于外盘/扩展行情）以及非数据请求函数
		if strings.Contains(nameLower, "trade") && !strings.HasPrefix(m.Name, "Ex") && m.Name != "GetTrade" && m.Name != "GetTradeAll" {
			// 将 GetTrade 与 GetTradeAll 放在最前面优先执行
			targetMethods = append(targetMethods, m.Name)
		}
	}
	// 优先加入核心 Tick 接口
	targetMethods = append([]string{"GetTrade", "GetTradeAll"}, targetMethods...)

	tradingDay := getRecentTradingDay()
	fmt.Printf("📅 自动推导测试交易日: %d\n", tradingDay)

	// 2. 依次测试候选方法
	for _, methodName := range targetMethods {
		mVal := val.MethodByName(methodName)
		if !mVal.IsValid() {
			continue
		}

		fmt.Printf("\n⚡ 正在探测方法 [%s]...\n", methodName)
		mType := mVal.Type()
		numIn := mType.NumIn()

		// 动态构建参数
		args := make([]reflect.Value, numIn)
		for i := 0; i < numIn; i++ {
			argType := mType.In(i)
			switch argType.Kind() {
			case reflect.String:
				args[i] = reflect.ValueOf("sz000001") // 以平安银行为测试标的
			case reflect.Uint32:
				args[i] = reflect.ValueOf(uint32(tradingDay)) // 历史交易日
			case reflect.Int32:
				args[i] = reflect.ValueOf(int32(tradingDay))
			case reflect.Int:
				args[i] = reflect.ValueOf(tradingDay)
			case reflect.Uint16:
				if i == 1 {
					args[i] = reflect.ValueOf(uint16(0)) // start
				} else {
					args[i] = reflect.ValueOf(uint16(30)) // count (提取30条样本)
				}
			default:
				args[i] = reflect.Zero(argType)
			}
		}

		// 安全执行调用，防止底层解析畸变字段导致 panic
		err := callAndDump(methodName, mVal, args)
		if err != nil {
			fmt.Printf("   ⚠️ 调用执行未成功: %v\n", err)
		}
	}
}

// callAndDump 执行反射调用并输出 JSON
func callAndDump(methodName string, mVal reflect.Value, args []reflect.Value) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("触发 Panic 保护: %v", r)
		}
	}()

	results := mVal.Call(args)
	if len(results) == 0 {
		return fmt.Errorf("无返回值")
	}

	for idx, res := range results {
		// 检查是否为 error 返回值
		if res.Type().Implements(reflect.TypeOf((*error)(nil)).Elem()) {
			if !res.IsNil() {
				return fmt.Errorf("API 级返回错误: %v", res.Interface())
			}
			continue
		}

		// 穿透指针
		if res.Kind() == reflect.Ptr {
			if res.IsNil() {
				fmt.Printf("   📝 返回空指针: %s\n", res.Type())
				continue
			}
			res = res.Elem()
		}

		if res.IsValid() {
			// 若为 struct 或 slice 且非空，则进行 JSON 序列化并存储
			if (res.Kind() == reflect.Struct || res.Kind() == reflect.Slice) && res.Len() > 0 {
				jsonData, jsonErr := json.MarshalIndent(res.Interface(), "", "  ")
				if jsonErr == nil {
					fmt.Printf("   📥 [%s] 成功获取物理行 %d 行数据结构:\n", methodName, res.Len())
					
					// 仅控制台打印前 50 行，避免超出终端缓冲区
					lines := strings.Split(string(jsonData), "\n")
					limit := 50
					if len(lines) < limit {
						limit = len(lines)
					}
					fmt.Println(strings.Join(lines[:limit], "\n"))
					if len(lines) > limit {
						fmt.Println("   ... (后文已省略)")
					}

					// 物理落盘完整数据供 GitHub Artifacts 收集
					filename := fmt.Sprintf("structure_%s.json", methodName)
					_ = os.WriteFile(filename, jsonData, 0644)
					fmt.Printf("   💾 完整数据结构已输出至: %s\n", filename)
					return nil
				}
			} else {
				fmt.Printf("   📝 返回非空基本值/空切片 (索引 %d) 类型: %s | 值: %v\n", idx, res.Type(), res.Interface())
			}
		}
	}
	return nil
}
