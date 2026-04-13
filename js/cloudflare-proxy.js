function getRandomChinaIP() {
  const prefixes = [
    [113, 114, 115, 116, 117, 118, 119], 
    [218, 219, 220, 221, 222],           
    [112, 120, 122],                     
    [58, 59, 60, 61, 27]                 
  ];
  const group = prefixes[Math.floor(Math.random() * prefixes.length)];
  const p1 = group[Math.floor(Math.random() * group.length)];
  const p2 = Math.floor(Math.random() * 255);
  const p3 = Math.floor(Math.random() * 255);
  const p4 = Math.floor(Math.random() * 254) + 1;
  return `${p1}.${p2}.${p3}.${p4}`;
}

export default {
  async fetch(request, env, ctx) {
    if (request.method === "OPTIONS") {
      return new Response(null, {
        headers: {
          "Access-Control-Allow-Origin": "*",
          "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
          "Access-Control-Allow-Headers": "*",
        },
      });
    }

    const url = new URL(request.url);
    const targetFunc = url.searchParams.get("target_func");
    
    // 💥 核心：随机生成 1~99 的前缀，将流量散射到东财的 99 个底层节点
    // 彻底稀释单 IP/单域名的请求频率，击穿 WAF 的速率限制
    const nodeIds = Array.from({length: 99}, (_, i) => i + 1);
    const randNode = nodeIds[Math.floor(Math.random() * nodeIds.length)];
    
    const API_MAP = {
      "list": `https://${randNode}.push2.eastmoney.com/api/qt/clist/get`,
      "kline": `https://${randNode}.push2his.eastmoney.com/api/qt/stock/kline/get`,
      "flow": `https://${randNode}.push2his.eastmoney.com/api/qt/stock/fflow/daykline/get`,
      "constituents": `https://${randNode}.push2.eastmoney.com/api/qt/clist/get`
    };

    const targetApi = API_MAP[targetFunc];
    if (!targetApi) {
      return new Response(JSON.stringify({ rc: -1, msg: "Invalid target_func" }), { status: 400 });
    }

    let newUrl = new URL(targetApi);
    url.searchParams.forEach((value, key) => {
      if (key !== "target_func") newUrl.searchParams.append(key, value);
    });

    const fakeIp = getRandomChinaIP();
    const headers = new Headers({
      "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
      "Referer": "https://quote.eastmoney.com/",
      "Accept": "*/*",
      "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
      "Accept-Encoding": "gzip, deflate, br",
      "Connection": "keep-alive",
      "X-Forwarded-For": fakeIp,
      "X-Real-IP": fakeIp,
      "Client-IP": fakeIp
    });

    const newRequest = new Request(newUrl, { method: "GET", headers: headers });

    try {
      const controller = new AbortController();
      const timeoutId = setTimeout(() => controller.abort(), 12000); // 缩短至12秒快速失败
      const response = await fetch(newRequest, { signal: controller.signal });
      clearTimeout(timeoutId);

      const newResponse = new Response(response.body, response);
      newResponse.headers.set("Access-Control-Allow-Origin", "*");
      return newResponse;

    } catch (e) {
      return new Response(
        JSON.stringify({ rc: -2, msg: e.name === "AbortError" ? "Timeout" : e.message }), 
        { status: 502, headers: { "Content-Type": "application/json", "Access-Control-Allow-Origin": "*" } }
      );
    }
  },
};
