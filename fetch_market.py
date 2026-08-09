#!/usr/bin/env python3
"""
抓取自选股行情 + 新闻,产出 data/market.json。

依赖:  pip3 install yfinance akshare requests pandas
运行:  python3 fetch_market.py

可选环境变量:
  ANTHROPIC_API_KEY  —— 设了才有 AI 解读和英文版;不设会退回规则版(仅中文)
"""

import json, os, time, datetime, traceback
from pathlib import Path

OUT = Path(__file__).parent / "data" / "market.json"

# ═════════════════════════════════════════════════════════════
# 自选股 —— 按 AI 算力价值链分层
#   market:  us = 美股 | hk = 港股 | kr = 韩股 | cn = A股(填6位代码)
#   加减标的直接改这里,前端会自动跟着变
# ═════════════════════════════════════════════════════════════
WATCHLIST = [
    {"zh": "上游 · 存储", "en": "Upstream · Memory", "sub": "HBM / DRAM", "items": [
        {"zh": "美光科技",   "en": "Micron",          "tk": "MU",        "market": "us"},
        {"zh": "西部数据",   "en": "Western Digital", "tk": "WDC",       "market": "us"},
        {"zh": "SK 海力士",  "en": "SK Hynix",        "tk": "000660.KS", "market": "kr"},
        {"zh": "三星电子",   "en": "Samsung Elec.",   "tk": "005930.KS", "market": "kr"},
        {"zh": "兆易创新",   "en": "GigaDevice",      "tk": "603986",    "market": "cn"},
        {"zh": "澜起科技",   "en": "Montage Tech",    "tk": "688008",    "market": "cn"},
    ]},
    {"zh": "上游 · 代工设备", "en": "Upstream · Foundry & Equip", "sub": "Foundry / WFE", "items": [
        {"zh": "台积电",     "en": "TSMC",            "tk": "TSM",       "market": "us"},
        {"zh": "阿斯麦",     "en": "ASML",            "tk": "ASML",      "market": "us"},
        {"zh": "应用材料",   "en": "Applied Mat.",    "tk": "AMAT",      "market": "us"},
        {"zh": "中芯国际",   "en": "SMIC",            "tk": "0981.HK",   "market": "hk"},
        {"zh": "华虹半导体", "en": "Hua Hong",        "tk": "1347.HK",   "market": "hk"},
        {"zh": "北方华创",   "en": "Naura",           "tk": "002371",    "market": "cn"},
    ]},
    {"zh": "中游 · 芯片", "en": "Midstream · Silicon", "sub": "ASIC / GPU / Networking", "items": [
        {"zh": "博通",       "en": "Broadcom",        "tk": "AVGO",      "market": "us"},
        {"zh": "迈威尔科技", "en": "Marvell",         "tk": "MRVL",      "market": "us"},
        {"zh": "英伟达",     "en": "NVIDIA",          "tk": "NVDA",      "market": "us"},
        {"zh": "超威半导体", "en": "AMD",             "tk": "AMD",       "market": "us"},
        {"zh": "Arista",     "en": "Arista Networks", "tk": "ANET",      "market": "us"},
        {"zh": "Astera Labs", "en": "Astera Labs",    "tk": "ALAB",      "market": "us"},
    ]},
    {"zh": "下游 · 光模块", "en": "Downstream · Optics", "sub": "Optical Modules", "items": [
        {"zh": "中际旭创",   "en": "Innolight",       "tk": "300308",    "market": "cn"},
        {"zh": "新易盛",     "en": "Eoptolink",       "tk": "300502",    "market": "cn"},
        {"zh": "天孚通信",   "en": "T&S Comm.",       "tk": "300394",    "market": "cn"},
        {"zh": "光迅科技",   "en": "Accelink",        "tk": "002281",    "market": "cn"},
        {"zh": "Coherent",   "en": "Coherent",        "tk": "COHR",      "market": "us"},
        {"zh": "Fabrinet",   "en": "Fabrinet",        "tk": "FN",        "market": "us"},
    ]},
    {"zh": "终端 · 云厂商", "en": "Demand · Hyperscalers", "sub": "Capex Buyers", "items": [
        {"zh": "微软",       "en": "Microsoft",       "tk": "MSFT",      "market": "us"},
        {"zh": "谷歌",       "en": "Alphabet",        "tk": "GOOGL",     "market": "us"},
        {"zh": "亚马逊",     "en": "Amazon",          "tk": "AMZN",      "market": "us"},
        {"zh": "Meta",       "en": "Meta",            "tk": "META",      "market": "us"},
        {"zh": "甲骨文",     "en": "Oracle",          "tk": "ORCL",      "market": "us"},
        {"zh": "阿里巴巴",   "en": "Alibaba",         "tk": "BABA",      "market": "us"},
    ]},
]

CCY = {"us": "$", "hk": "HK$", "kr": "₩", "cn": "¥"}
SPARK_DAYS = 30
NEWS_LIMIT = 15
AK_SLEEP = 0.7          # A股每次请求之间的停顿,防限速
AK_RETRY = 3


# ═════════ 收益率计算 ═════════════════════════════════════════
def returns_from(closes, dates):
    """从收盘价序列算 5 个周期的涨跌幅(%)。closes/dates 升序。"""
    if len(closes) < 2:
        return {}
    last = closes[-1]

    def back(n):
        return closes[-(n + 1)] if len(closes) > n else closes[0]

    def pct(base):
        return round((last - base) / base * 100, 2) if base else None

    r = {"1d": pct(back(1)), "1w": pct(back(5)),
         "1m": pct(back(21)), "1y": pct(back(250))}

    # 年初至今:去年最后一个交易日的收盘价为基准
    year = dates[-1].year
    prior = [c for c, d in zip(closes, dates) if d.year < year]
    r["ytd"] = pct(prior[-1]) if prior else pct(closes[0])
    return r


# ═════════ 海外标的:一次批量拉,把 24 个请求压成 1 个 ═══════════
def fetch_yf_batch(tickers):
    import yfinance as yf
    print(f"  批量拉取 {len(tickers)} 个海外标的…")
    raw = yf.download(tickers, period="2y", interval="1d",
                      group_by="ticker", auto_adjust=True,
                      progress=False, threads=True)
    out = {}
    for tk in tickers:
        try:
            df = raw[tk] if len(tickers) > 1 else raw
            s = df["Close"].dropna()
            if len(s) < 2:
                continue
            closes = [round(float(v), 3) for v in s]
            dates = [d.date() if hasattr(d, "date") else d for d in s.index]
            out[tk] = {"px": closes[-1],
                       "ret": returns_from(closes, dates),
                       "spark": closes[-SPARK_DAYS:]}
        except Exception:
            print(f"    ! {tk} 解析失败")
    return out


# ═════════ A股:逐个拉,带停顿和退避重试 ═══════════════════════
def fetch_ak(code):
    import akshare as ak
    end = datetime.date.today().strftime("%Y%m%d")
    start = (datetime.date.today() - datetime.timedelta(days=760)).strftime("%Y%m%d")
    for attempt in range(AK_RETRY):
        try:
            df = ak.stock_zh_a_hist(symbol=code, period="daily",
                                    start_date=start, end_date=end, adjust="qfq")
            if df is None or df.empty or len(df) < 2:
                return None
            closes = [round(float(v), 2) for v in df["收盘"]]
            dates = list(df["日期"])
            return {"px": closes[-1],
                    "ret": returns_from(closes, dates),
                    "spark": closes[-SPARK_DAYS:]}
        except Exception:
            if attempt == AK_RETRY - 1:
                print(f"    ! {code} 三次都失败")
                traceback.print_exc(limit=1)
            else:
                time.sleep(1.5 * (attempt + 1))
    return None


def build_tiers():
    overseas = [i["tk"] for g in WATCHLIST for i in g["items"] if i["market"] != "cn"]
    yf_data = {}
    try:
        yf_data = fetch_yf_batch(overseas)
    except Exception:
        print("  ! 批量拉取整体失败")
        traceback.print_exc(limit=2)

    tiers, ok, fail = [], 0, 0
    for g in WATCHLIST:
        items = []
        for it in g["items"]:
            row = {"zh": it["zh"], "en": it["en"], "tk": it["tk"],
                   "ccy": CCY.get(it["market"], "")}
            if it["market"] == "cn":
                q = fetch_ak(it["tk"])
                time.sleep(AK_SLEEP)
            else:
                q = yf_data.get(it["tk"])
            if q:
                row.update(q); ok += 1
                print(f"  · {it['zh']:<12} {q['ret'].get('1d')}")
            else:
                fail += 1
                print(f"  · {it['zh']:<12} —  (失败)")
            items.append(row)
        tiers.append({"zh": g["zh"], "en": g["en"], "sub": g["sub"], "items": items})

    print(f"  → 成功 {ok} / 失败 {fail}")
    return tiers


# ═════════ 新闻 ═══════════════════════════════════════════════
def build_news():
    import akshare as ak
    out = []
    try:
        df = ak.stock_info_global_cls(symbol="重点")
        # akshare 返回「旧 → 新」升序,取尾部再倒序才是最新的
        for _, r in df.tail(NEWS_LIMIT).iloc[::-1].iterrows():
            t = r.get("标题")
            if not isinstance(t, str) or not t.strip():
                t = str(r.get("内容", ""))
            out.append({"src": "财联社", "time": str(r.get("发布时间", ""))[:5],
                        "zh": t.strip()[:80], "en": "",
                        "url": "https://www.cls.cn/telegraph"})
    except Exception:
        print("  ! 新闻抓取失败")
        traceback.print_exc(limit=1)
    return out


# ═════════ AI 解读(双语)═══════════════════════════════════════
def call_claude(prompt, max_tokens=3000):
    import requests
    key = os.environ.get("ANTHROPIC_API_KEY")
    if not key:
        return None
    r = requests.post("https://api.anthropic.com/v1/messages",
        headers={"x-api-key": key, "anthropic-version": "2023-06-01",
                 "content-type": "application/json"},
        json={"model": "claude-sonnet-4-6", "max_tokens": max_tokens,
              "messages": [{"role": "user", "content": prompt}]},
        timeout=120)
    r.raise_for_status()
    text = "".join(b.get("text", "") for b in r.json()["content"]).strip()
    text = text.removeprefix("```json").removeprefix("```").removesuffix("```").strip()
    return json.loads(text)


def build_commentary(tiers, news):
    """顶部要点 + 每个板块一句解读,中英双语。"""
    blocks = []
    for t in tiers:
        rows = []
        for i in t["items"]:
            r = i.get("ret", {})
            if r.get("1d") is None:
                continue
            rows.append(f"  {i['zh']} 日{r['1d']:+.2f}% "
                        f"周{(r.get('1w') or 0):+.1f}% 月{(r.get('1m') or 0):+.1f}% "
                        f"YTD{(r.get('ytd') or 0):+.1f}%")
        blocks.append(f"【{t['zh']}】\n" + "\n".join(rows))
    headlines = "\n".join("- " + n["zh"] for n in news[:12])

    prompt = f"""你是一位覆盖 AI 算力产业链的卖方分析师,正在写今日盘后简评。

各层级表现(价值链顺序:存储 → 代工设备 → 芯片 → 光模块 → 云厂商):

{chr(10).join(blocks)}

今日新闻标题:
{headlines}

输出一个 JSON 对象,不要任何其他内容、不要 markdown 代码块:

{{
  "brief_zh": ["4条要点,每条1-2句"],
  "brief_en": ["same 4 points in English"],
  "notes_zh": {{"板块中文名": "该板块一句话解读"}},
  "notes_en": {{"板块中文名": "one-line read in English"}}
}}

写作要求:
- 要点聚焦**价值链传导**:某层普涨是不是同一催化剂驱动?上下游有没有背离?
  背离往往比普涨更有信息量(例:光模块涨而云厂商跌 = 市场在抢预期而非确认订单)。
- 结合周/月/YTD 判断今天是趋势延续还是反转,不要只看单日。
- 新闻只在能解释价格时才引用。解释不了就直说数据不支持明确归因,不要硬编故事。
- 每条要点把关键主语用 <b></b> 包起来。
- notes 的 key 必须用上面【】里的中文板块名,一字不差(中英文两个字典都用中文 key)。
- 中文简洁专业;英文用地道的卖方研究口吻,不是直译。"""

    try:
        d = call_claude(prompt)
        if d:
            print("  → AI 解读生成成功")
            return d
    except Exception:
        print("  ! AI 解读失败,改用规则版")
        traceback.print_exc(limit=1)
    return rule_commentary(tiers)


def rule_commentary(tiers):
    """没有 API key 时的兜底:纯规则,只有中文。"""
    brief, notes, avgs = [], {}, {}
    for t in tiers:
        v = [i["ret"]["1d"] for i in t["items"] if i.get("ret", {}).get("1d") is not None]
        if not v:
            continue
        a = sum(v) / len(v)
        avgs[t["zh"]] = a
        top = max((i for i in t["items"] if i.get("ret", {}).get("1d") is not None),
                  key=lambda i: abs(i["ret"]["1d"]), default=None)
        note = f"板块均值 {a:+.2f}%"
        if top:
            note += f",{top['zh']} {top['ret']['1d']:+.2f}% 幅度最大"
        notes[t["zh"]] = note
        if abs(a) >= 1.5:
            brief.append(f"<b>{t['zh']}</b> 整体{'走强' if a > 0 else '走弱'},均值 {a:+.2f}%。")

    up, dn = avgs.get("下游 · 光模块"), avgs.get("终端 · 云厂商")
    if up is not None and dn is not None and abs(up - dn) >= 2:
        brief.append(f"<b>上下游背离</b>:光模块 {up:+.2f}% vs 云厂商 {dn:+.2f}%,"
                     f"价差 {up - dn:+.2f}pct。")

    return {"brief_zh": brief or ["今日各层级波动均在 1.5% 以内,无明显异动。"],
            "brief_en": ["Set ANTHROPIC_API_KEY to enable English commentary."],
            "notes_zh": notes, "notes_en": {}}


# ═════════ 主流程 ═════════════════════════════════════════════
def main():
    print("抓行情…")
    tiers = build_tiers()
    print("抓新闻…")
    news = build_news()
    print("生成解读…")
    c = build_commentary(tiers, news)

    for t in tiers:
        t["note"] = {"zh": c.get("notes_zh", {}).get(t["zh"], ""),
                     "en": c.get("notes_en", {}).get(t["zh"], "")}

    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps({
        "updated": datetime.datetime.now().strftime("%m-%d %H:%M"),
        "brief": {"zh": c.get("brief_zh", []), "en": c.get("brief_en", [])},
        "tiers": tiers,
        "news": news,
    }, ensure_ascii=False, indent=1), encoding="utf-8")
    print(f"完成 → {OUT}")


if __name__ == "__main__":
    main()
