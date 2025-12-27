import asyncio
import logging
from typing import Optional, Any, List, Dict
from datetime import datetime
from fastapi import FastAPI, HTTPException, Query, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
import yfinance as yf
import pandas as pd
import numpy as np

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="YFinance Pro API",
    description="Полный API с поддержкой длительных праздников и WebSockets.",
    version="1.3.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

def normalize_value(v: Any) -> Any:
    """Преобразование типов данных для JSON-ответа."""
    if isinstance(v, (pd.Timestamp, datetime)):
        return v.isoformat()
    if isinstance(v, (np.integer, int)):
        return int(v)
    if isinstance(v, (np.floating, float)):
        return None if np.isnan(v) or np.isinf(v) else float(v)
    if isinstance(v, pd.DataFrame):
        return v.reset_index().to_dict(orient="records")
    if isinstance(v, pd.Series):
        return v.to_dict()
    return v

# --- УТИЛИТЫ ---

@app.get("/search", tags=["Utility"])
def search_ticker(query: str = Query(..., description="Название или тикер")):
    """Поиск тикера (напр. Apple или Газпром)."""
    try:
        s = yf.Search(query, max_results=10)
        return {"results": normalize_value(s.quotes)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# --- РЫНОЧНЫЕ ДАННЫЕ ---

@app.get("/tickers/quote", tags=["Market Data"])
def get_multiple_quotes(symbols: str = Query(..., description="Тикеры через запятую: AAPL,MSFT")):
    """
    Расширенная котировка. Поддерживает поиск цены, если торгов не было до 7 дней.
    """
    try:
        ticker_list = [s.strip().upper() for s in symbols.split(",")]
        tickers = yf.Tickers(" ".join(ticker_list))
        result = {}

        for symbol in ticker_list:
            t = tickers.tickers[symbol]
            fast = t.fast_info
            
            # Пытаемся взять живую цену
            current_price = fast.get('last_price')
            prev_close = fast.get('previous_close')
            
            # Если пусто (праздники/выходные), берем историю за последние 7 дней
            if current_price is None or prev_close is None or np.isnan(current_price):
                hist = t.history(period="7d") 
                if not hist.empty:
                    current_price = hist['Close'].iloc[-1]
                    prev_close = hist['Close'].iloc[-2] if len(hist) > 1 else current_price
                    open_p, high_p, low_p, vol = hist['Open'].iloc[-1], hist['High'].iloc[-1], hist['Low'].iloc[-1], hist['Volume'].iloc[-1]
                else:
                    current_price, prev_close, open_p, high_p, low_p, vol = [None]*6
            else:
                open_p, high_p, low_p, vol = fast.get('open'), fast.get('day_high'), fast.get('day_low'), fast.get('last_volume')

            change_pct = ((current_price - prev_close) / prev_close * 100) if current_price and prev_close else 0

            result[symbol] = {
                "current_price": normalize_value(current_price),
                "change_percent": normalize_value(round(change_pct, 2)),
                "previous_close": normalize_value(prev_close),
                "open": normalize_value(open_p),
                "high": normalize_value(high_p),
                "low": normalize_value(low_p),
                "volume": normalize_value(vol),
                "exchange": t.info.get('exchDisp', fast.get('exchange')), # Понятное имя
                "exchange_code": fast.get('exchange'),
                "currency": fast.get('currency')
            }
        return result
    except Exception as e:
        logger.error(f"Quote error: {e}")
        raise HTTPException(status_code=500, detail="Error fetching quotes")

@app.websocket("/ws/price/{ticker}")
async def websocket_price(websocket: WebSocket, ticker: str):
    """WebSocket котировки с обновлением раз в 2 секунды."""
    await websocket.accept()
    t = yf.Ticker(ticker.upper())
    try:
        # Получаем данные один раз для базы
        prev_close = t.fast_info.get('previous_close')
        exchange_name = t.info.get('exchDisp', t.fast_info.get('exchange'))

        while True:
            fast = t.fast_info
            curr = fast.get('last_price')
            
            # Защита от пустоты при праздниках
            if curr is None or np.isnan(curr):
                hist = t.history(period="1d")
                curr = hist['Close'].iloc[-1] if not hist.empty else None

            change_pct = ((curr - prev_close) / prev_close * 100) if curr and prev_close else 0
            
            await websocket.send_json({
                "symbol": ticker.upper(),
                "price": normalize_value(curr),
                "change_percent": normalize_value(round(change_pct, 2)),
                "high": normalize_value(fast.get('day_high')),
                "low": normalize_value(fast.get('day_low')),
                "exchange": exchange_name,
                "time": datetime.now().isoformat()
            })
            await asyncio.sleep(2)
    except Exception:
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

@app.get("/history/{ticker}", tags=["Information"])
def get_history(ticker: str, period: str = "1mo", interval: str = "1d"):
    """История свечей."""
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

# ---------------------------
@app.get("/news/{ticker}", tags=["Information"])
def get_news(ticker: str):
    """Последние новости по компании."""
    t = yf.Ticker(ticker.upper())
    news = t.news
    return normalize_value(news) if news else {"message": "No news found"}

@app.get("/recommendations/{ticker}", tags=["Information"])
def get_recommendations(ticker: str):
    """Рекомендации аналитиков (Buy/Hold/Sell)."""
    try:
        t = yf.Ticker(ticker.upper())
        rec = t.recommendations
        if rec is None or rec.empty:
            return {"symbol": ticker.upper(), "message": "No recommendations found"}
        return normalize_value(rec)
    except Exception:
        return {"symbol": ticker.upper(), "message": "Recommendations unavailable"}

@app.get("/calendar/{ticker}", tags=["Information"])
def get_calendar(ticker: str):
    """Календарь предстоящих событий (отчеты, дивиденды)."""
    try:
        t = yf.Ticker(ticker.upper())
        cal = t.calendar
        if not cal:
            return {"symbol": ticker.upper(), "message": "No calendar data found"}
        return normalize_value(cal)
    except Exception:
        return {"symbol": ticker.upper(), "message": "Calendar unavailable"}

@app.get("/health", tags=["Utility"])
def health():
    """Проверка статуса сервера."""
    return {"status": "online", "timestamp": datetime.now().isoformat()}

if __name__ == "__main__":
    import uvicorn
    # Порт лучше брать из переменной окружения для Render
    import os
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
