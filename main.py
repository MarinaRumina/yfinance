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

app = FastAPI(title="YFinance Ultimate API", version="1.7.0")

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
        return ticker_obj.info.get('exchDisp') or EXCHANGE_MAP.get(raw, raw)
    except:
        return EXCHANGE_MAP.get(raw, raw)


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
    """Получение котировок для списка тикеров."""
    try:
        ticker_list = [s.strip().upper() for s in symbols.split(",")]
        result = {}
        for symbol in ticker_list:
            t = yf.Ticker(symbol)
            f = t.fast_info
            curr = f.get('last_price')
            prev = f.get('previous_close')
            
            # Если данных нет, подтягиваем из истории (решает проблему 0% изменения)
            if curr is None or np.isnan(curr) or prev is None or np.isnan(prev):
                h = t.history(period="5d") # Берем с запасом на выходные
                if not h.empty:
                    curr = h['Close'].iloc[-1]
                    prev = h['Close'].iloc[-2] if len(h) > 1 else curr
                    hi, lo, vo, op = h['High'].iloc[-1], h['Low'].iloc[-1], h['Volume'].iloc[-1], h['Open'].iloc[-1]
                else:
                    hi, lo, vo, op = [None]*4
            else:
                hi, lo, vo, op = f.get('day_high'), f.get('day_low'), f.get('last_volume'), f.get('open')

            change_pct = ((curr - prev) / prev * 100) if curr and prev else 0

            result[symbol] = {
                "current_price": normalize_value(curr),
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

@app.websocket("/ws/price/{tickers}")
async def websocket_price(websocket: WebSocket, tickers: str):
    """
    Асинхронный WebSocket для получения цен в реальном времени. 
    Принимает один или несколько тикеров через запятую (напр. AAPL,ESLT.TA,MSFT,TSLA).
    """
    await websocket.accept()
    ticker_list = [s.strip().upper() for s in tickers.split(",")]
    ticker_objects = {sym: yf.Ticker(sym) for sym in ticker_list}
    
    # Кэш для цен закрытия, чтобы не дергать историю каждые 2 секунды
    prev_closes = {}
for sym, t in ticker_objects.items():
        pc = t.fast_info.get('previous_close')
        if pc is None or np.isnan(pc):
            h = t.history(period="5d")
            pc = h['Close'].iloc[-2] if len(h) > 1 else (h['Close'].iloc[-1] if not h.empty else None)
        prev_closes[sym] = pc

    try:
        while True:
            updates = {}
            for sym, t in ticker_objects.items():
                f = t.fast_info
                curr = f.get('last_price')
                hi = f.get('day_high')
                lo = f.get('day_low')
                vo = f.get('last_volume')
                
                # Если high/low/volume равны null, берем из истории за текущий день
                if hi is None or np.isnan(hi) or vo is None or np.isnan(vo):
                    h_today = t.history(period="1d")
                    if not h_today.empty:
                        curr = curr or h_today['Close'].iloc[-1]
                        hi = h_today['High'].iloc[-1]
                        lo = h_today['Low'].iloc[-1]
                        vo = h_today['Volume'].iloc[-1]

                prev = prev_closes.get(sym)
                change_pct = ((curr - prev) / prev * 100) if curr and prev else 0
                
                updates[sym] = {
                    "price": normalize_value(curr),
                    "change_percent": normalize_value(round(change_pct, 2)),
                    "high": normalize_value(hi),
                    "low": normalize_value(lo),
                    "volume": normalize_value(vo),
                    "time": datetime.now().isoformat()
                }
            
            await websocket.send_json(updates)
            await asyncio.sleep(2) 
            
    except WebSocketDisconnect:
        logger.info(f"Client disconnected: {tickers}")
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
