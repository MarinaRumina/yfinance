import asyncio
import logging
import os
from typing import Optional, Any, List, Dict
from datetime import datetime
from fastapi import FastAPI, HTTPException, Query, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
import yfinance as yf
import pandas as pd
import numpy as np

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="YFinance Ultimate API", version="1.5.0")

app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

# --- СЛОВАРЬ БИРЖ ---
# Расширенный словарь для расшифровки кодов бирж
EXCHANGE_MAP = {
    # США и Канада
    "NMS": "NASDAQ",
    "NYQ": "NYSE",
    "NCM": "NASDAQ Capital Market",
    "NGM": "NASDAQ Global Market",
    "PCX": "NYSE Arca",
    "TOR": "Toronto Stock Exchange",
    # Израиль
    "TAE": "Tel Aviv Stock Exchange",
    # Европа
    "LSE": "London Stock Exchange",
    "FRA": "Frankfurt Stock Exchange",
    "GER": "XETRA (Germany)",
    "PAR": "Euronext Paris",
    "AMS": "Euronext Amsterdam",
    "MIL": "Borsa Italiana",
    "EBS": "SIX Swiss Exchange",
    "MCW": "BME Spanish Exchanges",
    # СНГ
    "MCX": "Moscow Exchange"
}

# --- ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ---
def normalize_value(v: Any) -> Any:
    if isinstance(v, (pd.Timestamp, datetime)): return v.isoformat()
    if isinstance(v, (np.integer, int)): return int(v)
    if isinstance(v, (np.floating, float)):
        return None if np.isnan(v) or np.isinf(v) else float(v)
    if isinstance(v, pd.DataFrame): return v.reset_index().to_dict(orient="records")
    if isinstance(v, pd.Series): return v.to_dict()
    if isinstance(v, (dict, list)):
        if isinstance(v, dict): return {str(k): normalize_value(val) for k, val in v.items()}
        return [normalize_value(i) for i in v]
    return v

def get_exchange_name(ticker_obj, fast_info):
    raw = fast_info.get('exchange')
    try:
        # Пробуем получить официальное имя, если нет - из словаря
        return ticker_obj.info.get('exchDisp') or EXCHANGE_MAP.get(raw, raw)
    except:
        return EXCHANGE_MAP.get(raw, raw)

def get_usd_rate(currency: str):
    """Улучшенное получение курса валют (с поддержкой ILS/ILA)"""
    if not currency or currency == "USD": return 1.0
    # Исправление для израильских агор/шекелей
    curr_map = {"ILA": "ILS", "ILR": "ILS"}
    target_curr = curr_map.get(currency.upper(), currency.upper())
    
    try:
        pair = f"{target_curr}USD=X"
        rate = yf.Ticker(pair).fast_info.get('last_price')
        if rate is None or np.isnan(rate):
            inv = yf.Ticker(f"USD{target_curr}=X").fast_info.get('last_price')
            if inv and inv > 0: rate = 1 / inv
        return rate or 1.0
    except:
        return 1.0

# --- УТИЛИТЫ ---

@app.get("/search", tags=["Utility"])
def search_ticker(query: str = Query(..., description="Название или тикер")):
    """Поиск с приоритетом тикеров, начинающихся на запрос"""
    try:
        q = query.strip().upper()
        s = yf.Search(q, max_results=15)
        quotes = s.quotes
        if not quotes: return {"results": []}
        # Сортировка: сначала совпадения по началу тикера, потом по весу (score)
        sorted_quotes = sorted(
            quotes, 
            key=lambda x: (not x.get('symbol', '').startswith(q), -x.get('score', 0))
        )
        return {"results": normalize_value(sorted_quotes)}
    except Exception as e:
        logger.error(f"Search error: {e}")
        raise HTTPException(status_code=500, detail="Search failed")


# --- РЫНОЧНЫЕ ДАННЫЕ ---

@app.get("/tickers/quote", tags=["Market Data"])
def get_multiple_quotes(symbols: str = Query(...)):
    try:
        ticker_list = [s.strip().upper() for s in symbols.split(",")]
        result = {}
        for symbol in ticker_list:
            t = yf.Ticker(symbol)
            f = t.fast_info
            curr = f.get('last_price')
            prev = f.get('previous_close')
            
            # Если данные пустые (как для ESLT.TA на скрине), берем из истории
            if curr is None or np.isnan(curr) or f.get('day_high') is None:
                h = t.history(period="2d")
                if not h.empty:
                    curr = h['Close'].iloc[-1]
                    prev = h['Close'].iloc[-2] if len(h) > 1 else prev
                    hi, lo, vo = h['High'].iloc[-1], h['Low'].iloc[-1], h['Volume'].iloc[-1]
                    op = h['Open'].iloc[-1]
                else:
                    hi, lo, vo, op = [None]*4
            else:
                hi, lo, vo, op = f.get('day_high'), f.get('day_low'), f.get('last_volume'), f.get('open')

            rate = get_usd_rate(f.get('currency'))
            price_usd = curr * rate if curr else None
            change_pct = ((curr - prev) / prev * 100) if curr and prev else 0

            result[symbol] = {
                "current_price": normalize_value(curr),
                "current_price_usd": normalize_value(round(price_usd, 2)) if price_usd else None,
                "change_percent": normalize_value(round(change_pct, 2)),
                "open": normalize_value(op),
                "high": normalize_value(hi),
                "low": normalize_value(lo),
                "volume": normalize_value(vo),
                "currency": f.get('currency'),
                "exchange": get_exchange_name(t, f)
            }
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.websocket("/ws/price/{ticker}")
async def websocket_price(websocket: WebSocket, ticker: str):
    await websocket.accept()
    ticker_sym = ticker.upper()
    t = yf.Ticker(ticker_sym)
    try:
        # Получаем статические данные один раз при подключении
        f_init = t.fast_info
        currency = f_init.get('currency')
        rate = get_usd_rate(currency)
        ex_name = get_exchange_name(t, f_init)
        
        while True:
            f = t.fast_info
            curr = f.get('last_price')
            prev = f.get('previous_close')
            
            # Fallback для пустых полей
            hi, lo, vo = f.get('day_high'), f.get('day_low'), f.get('last_volume')
            if hi is None or np.isnan(hi):
                h_data = t.history(period="1d")
                if not h_data.empty:
                    curr = h_data['Close'].iloc[-1]
                    hi, lo, vo = h_data['High'].iloc[-1], h_data['Low'].iloc[-1], h_data['Volume'].iloc[-1]

            change_pct = ((curr - prev) / prev * 100) if curr and prev else 0
            
            await websocket.send_json({
                "symbol": ticker_sym,
                "price": normalize_value(curr),
                "price_usd": normalize_value(round(curr * rate, 2)) if curr else None,
                "change_percent": normalize_value(round(change_pct, 2)),
                "high": normalize_value(hi),
                "low": normalize_value(lo),
                "volume": normalize_value(vo),
                "exchange": ex_name,
                "time": datetime.now().isoformat()
            })
            await asyncio.sleep(2)
    except Exception as e:
        logger.error(f"WS error: {e}")
        await websocket.close()



# --- ФИНАНСЫ И ОТЧЕТНОСТЬ ---
@app.get("/financials/{ticker}", tags=["Financials"])
def get_financials(ticker: str):
    """Баланс, Прибыли и Кэшфлоу."""
    try:
        t = yf.Ticker(ticker.upper())
        return normalize_value({
            "income_statement": t.income_stmt,
            "balance_sheet": t.balance_sheet,
            "cashflow": t.cashflow
        })
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))



@app.get("/info/{ticker}", tags=["Information"])
def get_info(ticker: str):
    """Полная информация о компании."""
    t = yf.Ticker(ticker.upper())
    return normalize_value(t.info)

@app.get("/history/{ticker}", tags=["Market Data"])
def get_history(ticker: str, period: str = "1mo", interval: str = "1d"):
    """Исторические данные OHLC + Volume + Dividends + Splits."""
    t = yf.Ticker(ticker.upper())
    hist = t.history(period=period, interval=interval)
    return normalize_value(hist)

# --- КОРПОРАТИВНЫЕ СОБЫТИЯ ---
@app.get("/dividends/{ticker}", tags=["Corporate Actions"])
def get_dividends(ticker: str):
    """История дивидендов."""
    t = yf.Ticker(ticker.upper())
    divs = t.dividends
    if divs.empty:
        return {"symbol": ticker.upper(), "message": "No dividends found", "data": []}
    return normalize_value(divs)

@app.get("/splits/{ticker}", tags=["Corporate Actions"])
def get_splits(ticker: str):
    """История сплитов."""
    t = yf.Ticker(ticker.upper())
    splits = t.splits
    if splits.empty:
        return {"symbol": ticker.upper(), "message": "No splits found", "data": []}
    return normalize_value(splits)

@app.get("/actions/{ticker}", tags=["Corporate Actions"])
def get_actions(ticker: str):
    """Все действия (дивиденды + сплиты)."""
    t = yf.Ticker(ticker.upper())
    if t.actions.empty:
        return {"symbol": ticker.upper(), "message": "No actions found", "data": []}
    return normalize_value(t.actions)

@app.get("/holders/{ticker}", tags=["Information"])
def get_holders(ticker: str):
    """Крупнейшие держатели акций."""
    t = yf.Ticker(ticker.upper())
    return normalize_value({
        "major": t.major_holders,
        "institutional": t.institutional_holders
    })

# --- ДОПОЛНИТЕЛЬНЫЕ ЭНДПОИНТЫ ---

@app.get("/news/{ticker}", tags=["Information"])
def get_news(ticker: str):
    """Последние новости по тикеру."""
    t = yf.Ticker(ticker.upper())
    return normalize_value(t.news)

@app.get("/recommendations/{ticker}", tags=["Information"])
def get_recommendations(ticker: str):
    """Рекомендации аналитиков."""
    t = yf.Ticker(ticker.upper())
    return normalize_value(t.recommendations)

@app.get("/calendar/{ticker}", tags=["Information"])
def get_calendar(ticker: str):
    """Календарь событий (отчеты, дивиденды)."""
    t = yf.Ticker(ticker.upper())
    return normalize_value(t.calendar)

@app.get("/health", tags=["Utility"])
def health():
    return {"status": "online", "timestamp": datetime.now().isoformat()}

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
