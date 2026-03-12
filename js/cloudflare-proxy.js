export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);
    const targetFunc = url.searchParams.get("target_func");
    
    // === 核心配置 ===
    const API_LIST = "http://17.push2.eastmoney.com/api/qt/clist/get";
    const API_KLINE = "http://push2his.eastmoney.com/api/qt/stock/kline/get";
    // 保留 flow 和 constituents 以兼容 Python 端的调用逻辑，尽管 flow 我们可能暂时不用
    const API_FLOW = "http://push2his.eastmoney.com/api/qt/stock/fflow/daykline/get";
    const API_CONST = "http://4.push2.eastmoney.com/api/qt/clist/get";
    
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

    const newRequest = new Request(newUrl, {
      method: "GET",
      headers: {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0 Safari/537.36",
        "Referer": "http://quote.eastmoney.com/",
        "Connection": "keep-alive"
      }
    });

    try {
      const response = await fetch(newRequest);
      return response;
    } catch (e) {
      return new Response(`Proxy Error: ${e.message}`, { status: 500 });
    }
  },
};

//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
//20260312
export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);
    const targetFunc = url.searchParams.get("target_func");
    
    // === 核心配置：全部升级为 https，提高穿透成功率 ===
    const API_LIST = "https://push2.eastmoney.com/api/qt/clist/get";
    const API_KLINE = "https://push2his.eastmoney.com/api/qt/stock/kline/get";
    const API_FLOW = "https://push2his.eastmoney.com/api/qt/stock/fflow/daykline/get";
    const API_CONST = "https://push2.eastmoney.com/api/qt/clist/get";
    const API_SINA_FLOW = "https://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/MoneyFlow.ssl_qsfx_lscjfb";
    
    let targetApi = "";
    let referer = "https://quote.eastmoney.com/";

    // === 路由映射 ===
    if (targetFunc === "list") {
      targetApi = API_LIST;
    } else if (targetFunc === "kline") {
      targetApi = API_KLINE;
    } else if (targetFunc === "flow") {
      targetApi = API_FLOW;
    } else if (targetFunc === "constituents") {
      targetApi = API_CONST;
    } else if (targetFunc === "sina_flow") {
      targetApi = API_SINA_FLOW;
      referer = "https://finance.sina.com.cn/";
    } else {
      return new Response("Error: Invalid 'target_func'.", { status: 400 });
    }

    // === 组装参数 ===
    const targetUrl = new URL(targetApi);
    url.searchParams.forEach((value, key) => {
      if (key !== "target_func") {
        targetUrl.searchParams.append(key, value);
      }
    });

    // === 构造增强型请求头 ===
    const newHeaders = new Headers();
    newHeaders.set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36");
    newHeaders.set("Referer", referer);
    newHeaders.set("Accept", "*/*");
    newHeaders.set("Accept-Language", "zh-CN,zh;q=0.9");
    // 关键：确保 Host 头部与目标域名一致
    newHeaders.set("Host", targetUrl.hostname);

    try {
      // 这里的 fetch 选项优化连接策略
      const response = await fetch(targetUrl.toString(), {
        method: "GET",
        headers: newHeaders,
        cf: {
          cacheEverything: true,
          cacheTtl: 60, // 缓存 60 秒，减少对后端服务器的直接冲击
        }
      });

      // 处理非 200 响应
      if (!response.ok) {
        return new Response(`Backend Error: ${response.status} ${response.statusText}`, { 
          status: response.status,
          headers: { "Access-Control-Allow-Origin": "*" }
        });
      }

      const newResponse = new Response(response.body, response);
      newResponse.headers.set("Access-Control-Allow-Origin", "*");
      return newResponse;

    } catch (e) {
      // 捕获超时的详细信息
      return new Response(`Worker Fetch Error: ${e.message}`, { 
        status: 502,
        headers: { "Access-Control-Allow-Origin": "*" }
      });
    }
  },
};
