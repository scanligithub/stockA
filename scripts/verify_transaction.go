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

// getRecentTradingDay 计算最近一个交易日 (格式 YYYYMMDD)
func getRecentTradingDay() int {
	now := time.Now()
	if now.Weekday() == time.Saturday {
		now = now.AddDate(0, 0, -1)
	} else if now.Weekday() == time.Sunday {
		now = now.AddDate(0, 0, -2)
	} else if now.Hour() < 15 {
		now = now.AddDate(0, 0, -1)
		if now.Weekday() == time.Saturday {
			now = now.AddDate(0, 0, -1)
		} else if now.Weekday() == time.Sunday {
			now = now.AddDate(0, 0, -2)
		}
	}
	val, _ := strconv.Atoi(now.Format("20060102"))
	return val
}

// dumpTypeFields 递归自省并打印结构体或切片的字段定义
func dumpTypeFields(t reflect.Type, indent string) {
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	if t.Kind() == reflect.Slice || t.Kind() == reflect.Array {
		fmt.Printf("%s[ ]集合成员类型:\n", indent)
		dumpTypeFields(t.Elem(), indent+"  ")
		return
	}
	if t.Kind() != reflect.Struct {
		fmt.Printf("%s%s\n", indent, t.String())
		return
	}
	
	fmt.Printf("%s结构体定义: %s {\n", indent, t.String())
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		// 略过私有字段
		if f.PkgPath != "" {
			continue
		}
		jsonTag := f.Tag.Get("json")
		if jsonTag == "" {
			jsonTag = "-"
		}
		fmt.Printf("%s  %-15s %-12s (json: %q)\n", indent, f.Name, f.Type.String(), jsonTag)
		
		// 对嵌套结构体（非原生基础类型）进行二级展开
		ft := f.Type
		if ft.Kind() == reflect.Ptr {
			ft = ft.Elem()
		}
		if ft.Kind() == reflect.Struct && !strings.HasPrefix(ft.String(), "time.Time") {
			dumpTypeFields(ft, indent+"    ")
		}
	}
	fmt.Printf("%s}\n", indent)
}

func main() {
	fmt.Println("============ 📡 TDX 分笔成交 (Trade) 接口类型自省引擎 ============")
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

	// 优先探查核心接口
	targetMethods := []string{"GetTrade", "GetTradeAll"}
	
	// 扫描其他包含 "Trade" 的方法
	for i := 0; i < typ.NumMethod(); i++ {
		m := typ.Method(i)
		nameLower := strings.ToLower(m.Name)
		if strings.Contains(nameLower, "trade") && !strings.HasPrefix(m.Name, "Ex") && m.Name != "GetTrade" && m.Name != "GetTradeAll" {
			targetMethods = append(targetMethods, m.Name)
		}
	}

	tradingDay := getRecentTradingDay()
	fmt.Printf("📅 自动推导测试交易日: %d\n", tradingDay)

	for _, methodName := range targetMethods {
		mVal := val.MethodByName(methodName)
		if !mVal.IsValid() {
			continue
		}

		fmt.Printf("\n⚡ 正在探测方法 [%s]...\n", methodName)
		mType := mVal.Type()
		numIn := mType.NumIn()

		// 动态匹配参数
		args := make([]reflect.Value, numIn)
		for i := 0; i < numIn; i++ {
			argType := mType.In(i)
			switch argType.Kind() {
			case reflect.String:
				args[i] = reflect.ValueOf("sz000001") // 测试个股
			case reflect.Uint32:
				args[i] = reflect.ValueOf(uint32(tradingDay))
			case reflect.Int32:
				args[i] = reflect.ValueOf(int32(tradingDay))
			case reflect.Int:
				args[i] = reflect.ValueOf(tradingDay)
			case reflect.Uint16:
				if i == 1 {
					args[i] = reflect.ValueOf(uint16(0))
				} else {
					args[i] = reflect.ValueOf(uint16(30))
				}
			default:
				args[i] = reflect.Zero(argType)
			}
		}

		err := callAndDump(methodName, mVal, args)
		if err != nil {
			fmt.Printf("   ⚠️ 调用执行未成功: %v\n", err)
		}
	}
}

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

	for _, res := range results {
		// 忽略 error 返回值
		if res.Type().Implements(reflect.TypeOf((*error)(nil)).Elem()) {
			if !res.IsNil() {
				return fmt.Errorf("API 级返回错误: %v", res.Interface())
			}
			continue
		}

		// 指针解引用
		if res.Kind() == reflect.Ptr {
			if res.IsNil() {
				fmt.Printf("   📝 返回空指针类型: %s\n", res.Type())
				continue
			}
			res = res.Elem()
		}

		if res.IsValid() {
			resType := res.Type()
			fmt.Printf("   🧬 检测到返回类型: %s\n", resType.String())

			// 🌟 1. 无论数据是否为空，使用 reflect.Type 打印静态结构体/切片定义
			fmt.Println("   📋 [类型自省] 物理字段结构如下:")
			dumpTypeFields(resType, "      ")

			// 🌟 2. 尝试提取具体的值
			isSlice := res.Kind() == reflect.Slice
			isStruct := res.Kind() == reflect.Struct

			if isSlice || isStruct {
				// 若为 Slice 且长度为 0，不进行 JSON 序列化，直接返回
				if isSlice && res.Len() == 0 {
					fmt.Println("   📝 [当前值] 空数据集 (当前非交易时段或无成交数据)。")
					return nil
				}

				jsonData, jsonErr := json.MarshalIndent(res.Interface(), "", "  ")
				if jsonErr == nil {
					fmt.Println("   📥 [当前值] 成功捕获实体数据:")
					lines := strings.Split(string(jsonData), "\n")
					limit := 40
					if len(lines) < limit {
						limit = len(lines)
					}
					fmt.Println(strings.Join(lines[:limit], "\n"))
					if len(lines) > limit {
						fmt.Println("      ... (后文已省略)")
					}

					// 写入本地 json 文件
					filename := fmt.Sprintf("structure_%s.json", methodName)
					_ = os.WriteFile(filename, jsonData, 0644)
					fmt.Printf("   💾 实体数据已保存到: %s\n", filename)
					return nil
				}
			}
		}
	}
	return nil
}
