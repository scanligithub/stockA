export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);
    const targetFunc = url.searchParams.get("target_func");
    
    // === 核心配置升级 ===
    // 1. 去除 "17.", "4." 这种硬编码的脆弱子节点，使用根网关 push2 实现自动负载均衡
    // 2. 全部升级为 https:// ，彻底规避运营商拦截和 302 重定向耗时
    const API_LIST = "https://push2.eastmoney.com/api/qt/clist/get";
    const API_KLINE = "https://push2his.eastmoney.com/api/qt/stock/kline/get";
    const API_FLOW = "https://push2his.eastmoney.com/api/qt/stock/fflow/daykline/get";
    const API_CONST = "https://push2.eastmoney.com/api/qt/clist/get";
    
    let targetApi = "";
    if (targetFunc === "list") {
      targetApi = API_LIST;
    } else if (targetFunc === "kline") {
      targetApi = API_KLINE;
    } else if (targetFunc === "flow") {
      targetApi = API_FLOW;
    } else if (targetFunc === "constituents") {
      targetApi = API_CONST;
    } else {
      return new Response("Error: Invalid 'target_func'.", { status: 400 });
    }

    let newUrl = new URL(targetApi);
    url.searchParams.forEach((value, key) => {
      if (key !== "target_func") {
        newUrl.searchParams.append(key, value);
      }
    });

    // 伪装请求头
    const newRequest = new Request(newUrl, {
      method: "GET",
      headers: {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Referer": "https://quote.eastmoney.com/",
        "Connection": "keep-alive",
        "Accept": "*/*"
      }
    });

    try {
      // 在 Worker 层面限制等待时间，防止被后端无响应挂死
      const response = await fetch(newRequest);
      return response;
    } catch (e) {
      return new Response(`Proxy Error: ${e.message}`, { status: 502 });
    }
  },
};
