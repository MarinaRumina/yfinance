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
    """Получает курс валюты к USD"""
    if not currency or currency == "USD": return 1.0
    try:
        # Для Израиля курс в yfinance обычно ILSUSD=X
        pair = f"{currency}USD=X"
        rate_ticker = yf.Ticker(pair)
        rate = rate_ticker.fast_info.get('last_price')
        # Если yfinance не нашел прямую пару, пробуем обратную
        if rate is None:
            rate_ticker = yf.Ticker(f"USD{currency}=X")
            inv_rate = rate_ticker.fast_info.get('last_price')
            if inv_rate: rate = 1 / inv_rate
        return rate or 1.0
    except:
        return 1.0

# --- УТИЛИТЫ ---

@app.get("/search", tags=["Utility"])
def search_ticker(query: str = Query(..., description="Название или тикер")):
    """Поиск тикера по названию или тикеру (напр. Apple или GOOGL)."""
    try:
        s = yf.Search(query, max_results=10)
        return {"results": normalize_value(s.quotes)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# --- РЫНОЧНЫЕ ДАННЫЕ ---

@app.get("/tickers/quote", tags=["Market Data"])
def get_multiple_quotes(symbols: str = Query(..., description="AAPL,TEVA.TA,LSEG.L")):
    try:
        ticker_list = [s.strip().upper() for s in symbols.split(",")]
        # Используем yf.Tickers только для инициализации, но опрашиваем в цикле для стабильности
        tickers = yf.Tickers(" ".join(ticker_list))
        result = {}

        for symbol in ticker_list:
            t = tickers.tickers[symbol]
            fast = t.fast_info
            
            curr = fast.get('last_price')
            prev = fast.get('previous_close')
            
            # ЗАЩИТА ОТ ПУСТЫХ ДАННЫХ (ПРАЗДНИКИ ДО 30 ДНЕЙ)
            if curr is None or np.isnan(curr):
                hist = t.history(period="1mo")
                if not hist.empty:
                    curr = hist['Close'].iloc[-1]
                    prev = hist['Close'].iloc[-2] if len(hist) > 1 else curr
                    o, h, l, v = hist['Open'].iloc[-1], hist['High'].iloc[-1], hist['Low'].iloc[-1], hist['Volume'].iloc[-1]
                else:
                    curr, prev, o, h, l, v = [None]*6
            else:
                o, h, l, v = fast.get('open'), fast.get('day_high'), fast.get('day_low'), fast.get('last_volume')

            # КОНВЕРТАЦИЯ В USD
            currency = fast.get('currency')
            price_usd = curr
            if currency and currency != "USD":
                rate = get_usd_rate(currency)
                price_usd = (curr * rate) if curr is not None else None

            change_pct = ((curr - prev) / prev * 100) if curr and prev else 0

            result[symbol] = {
                "current_price": normalize_value(curr),
                "current_price_usd": normalize_value(round(price_usd, 2)) if price_usd else None,
                "change_percent": normalize_value(round(change_pct, 2)),
                "previous_close": normalize_value(prev),
                "open": normalize_value(o),
                "high": normalize_value(h),
                "low": normalize_value(l),
                "volume": normalize_value(v),
                "exchange": get_exchange_name(t, fast),
                "currency": currency
            }
        return result
    except Exception as e:
        logger.error(f"Quote error: {e}")
        raise HTTPException(status_code=500, detail="Error fetching quotes")

@app.websocket("/ws/price/{ticker}")
async def websocket_price(websocket: WebSocket, ticker: str):
    await websocket.accept()
    ticker_sym = ticker.upper()
    t = yf.Ticker(ticker_sym)
    try:
        prev_close = t.fast_info.get('previous_close')
        exchange_name = get_exchange_name(t, t.fast_info)
        currency = t.fast_info.get('currency')
        rate = get_usd_rate(currency) if currency != "USD" else 1.0

        while True:
            fast = t.fast_info
            curr = fast.get('last_price')
            
            # ЗАЩИТА ДЛЯ СОКЕТА
            if curr is None or np.isnan(curr):
                hist = t.history(period="1d")
                curr = hist['Close'].iloc[-1] if not hist.empty else None

            change_pct = ((curr - prev_close) / prev_close * 100) if curr and prev_close else 0
            
            await websocket.send_json({
                "symbol": ticker_sym,
                "price": normalize_value(curr),
                "price_usd": normalize_value(round(curr * rate, 2)) if curr else None,
                "change_percent": normalize_value(round(change_pct, 2)),
                "high": normalize_value(fast.get('day_high')),
                "low": normalize_value(fast.get('day_low')),
                "volume": normalize_value(fast.get('last_volume')),
                "exchange": exchange_name,
                "time": datetime.now().isoformat()
            })
            await asyncio.sleep(2)
            
    except WebSocketDisconnect:
        logger.info(f"Client disconnected from {ticker_sym}")
        
    except Exception as e: # ИСПРАВЛЕНО: добавлено 'as e'
        logger.error(f"WebSocket error for {ticker_sym}: {e}")
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
