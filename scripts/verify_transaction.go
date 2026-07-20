// FILE: scripts/verify_transaction.go
package main

import (
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"strings"

	"github.com/injoyai/tdx"
)

func main() {
	fmt.Println("============ 📡 TDX 单笔成交接口探测引擎 ============")
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

	// 1. 扫描所有与成交数据相关的候选方法
	var targetMethods []string
	fmt.Println("\n🔍 正在检索 tdx.Client 中相关联的方法...")
	for i := 0; i < typ.NumMethod(); i++ {
		m := typ.Method(i)
		nameLower := strings.ToLower(m.Name)
		if strings.Contains(nameLower, "transaction") || strings.Contains(nameLower, "tick") {
			targetMethods = append(targetMethods, m.Name)
			fmt.Printf("  • 发现候选方法: %s | 签名: %s\n", m.Name, m.Type)
		}
	}

	if len(targetMethods) == 0 {
		fmt.Println("⚠️ 未检测到包含 'Transaction' 或 'Tick' 的方法，将打印全量 API 列表以供排查。")
		for i := 0; i < typ.NumMethod(); i++ {
			fmt.Printf("  • %s\n", typ.Method(i).Name)
		}
		return
	}

	// 2. 循环尝试调用探测到的方法
	for _, methodName := range targetMethods {
		fmt.Printf("\n⚡ 正在对方法 [%s] 进行动态参数构建...\n", methodName)
		mVal := val.MethodByName(methodName)
		mType := mVal.Type()
		numIn := mType.NumIn()

		args := make([]reflect.Value, numIn)
		for i := 0; i < numIn; i++ {
			argType := mType.In(i)
			fmt.Printf("    - 参数 %d: 类型=%s\n", i, argType)
			
			// 根据类型动态填充安全值
			switch argType.Kind() {
			case reflect.String:
				args[i] = reflect.ValueOf("sz000001") // 以平安银行为测试对象
			case reflect.Uint16:
				if i == 1 {
					args[i] = reflect.ValueOf(uint16(0))   // 起始位置
				} else {
					args[i] = reflect.ValueOf(uint16(50))  // 获取条数
				}
			case reflect.Int:
				if i == 1 {
					args[i] = reflect.ValueOf(0)
				} else {
					args[i] = reflect.ValueOf(50)
				}
			case reflect.Uint32:
				if i == 1 {
					args[i] = reflect.ValueOf(uint32(0))
				} else {
					args[i] = reflect.ValueOf(uint32(50))
				}
			default:
				args[i] = reflect.Zero(argType)
			}
		}

		fmt.Printf("⚙️ 正在执行 [%s]...\n", methodName)
		results := mVal.Call(args)

		// 3. 解析并打印执行结果
		if len(results) > 0 {
			for idx, res := range results {
				// 检查是否返回错误
				if res.Type().Implements(reflect.TypeOf((*error)(nil)).Elem()) {
					if !res.IsNil() {
						fmt.Printf("   ❌ 执行返回错误: %v\n", res.Interface())
					}
					continue
				}

				// 检查并输出非空有效数据
				if res.Kind() == reflect.Ptr && !res.IsNil() {
					res = res.Elem()
				}

				if res.IsValid() && (res.Kind() == reflect.Struct || res.Kind() == reflect.Slice) {
					jsonData, jsonErr := json.MarshalIndent(res.Interface(), "", "  ")
					if jsonErr == nil {
						fmt.Println("   📥 成功捕获数据结构样本:")
						fmt.Println(string(jsonData))
						
						// 保存一份到本地，方便工作流归档
						_ = os.WriteFile("transaction_structure.json", jsonData, 0644)
					}
				} else {
					fmt.Printf("   📝 返回值 %d 类型: %s | 原始值: %v\n", idx, res.Type(), res.Interface())
				}
			}
		}
	}
}
