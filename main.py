import asyncio
import logging
from typing import Optional, Any, List, Dict
from fastapi import FastAPI, HTTPException, Query, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
import yfinance as yf
import pandas as pd
import numpy as np
from datetime import datetime

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="yfinance API",
    description="Full-featured API for Yahoo Finance with WebSockets and Search",
    version="1.1.0"
)

# Настройка CORS для доступа из браузера
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- Вспомогательная функция для очистки данных перед отправкой в JSON ---
def normalize_value(v: Any) -> Any:
    """Преобразует типы данных Pandas/NumPy в стандартные типы Python"""
    if isinstance(v, (pd.Timestamp, datetime)):
        return v.isoformat()
    if isinstance(v, (np.integer, int)):
        return int(v)
    if isinstance(v, (np.floating, float)):
        if np.isnan(v) or np.isinf(v):
            return None
        return float(v)
    if isinstance(v, pd.DataFrame):
        return v.reset_index().to_dict(orient="records")
    if isinstance(v, pd.Series):
        return v.to_dict()
    if isinstance(v, dict):
        return {str(k): normalize_value(val) for k, val in v.items()}
    if isinstance(v, list):
        return [normalize_value(i) for i in v]
    return v

# --- НОВЫЙ: Эндпоинт поиска ---
@app.get("/search", tags=["Utility"])
def search_ticker(query: str = Query(..., description="Название компании или символ")):
    """Поиск тикеров по названию (напр. Apple или Газпром)"""
    try:
        s = yf.Search(query, max_results=10)
        return {"results": normalize_value(s.quotes)}
    except Exception as e:
        logger.error(f"Search error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# --- НОВЫЙ: WebSocket для живых котировок ---
@app.websocket("/ws/price/{ticker}")
async def websocket_price(websocket: WebSocket, ticker: str):
    await websocket.accept()
    ticker_sym = ticker.upper()
    try:
        # Пытаемся использовать официальный WebSocket yfinance
        try:
            from yfinance import yf_websocket
            async with yf_websocket.YfWebsocket() as yfw:
                yfw.subscribe([ticker_sym])
                async for quote in yfw.messages():
                    await websocket.send_json({
                        "symbol": ticker_sym,
                        "price": normalize_value(getattr(quote, 'price', None)),
                        "time": datetime.now().isoformat(),
                        "source": "yfinance_ws"
                    })
        except (ImportError, Exception):
            # Резервный вариант: Polling (опрос раз в 2 секунды)
            logger.info(f"Falling back to polling for {ticker_sym}")
            t = yf.Ticker(ticker_sym)
            while True:
                price = t.fast_info.get('last_price')
                await websocket.send_json({
                    "symbol": ticker_sym, 
                    "price": normalize_value(price),
                    "time": datetime.now().isoformat(),
                    "source": "polling"
                })
                await asyncio.sleep(2)
    except WebSocketDisconnect:
        logger.info(f"Client disconnected from {ticker_sym}")
    except Exception as e:
        logger.error(f"WebSocket error: {e}")
        await websocket.close()

# --- ВАШИ ОРИГИНАЛЬНЫЕ ЭНДПОИНТЫ (УЛУЧШЕННЫЕ) ---

@app.get("/info/{ticker}")
def get_info(ticker: str):
    t = yf.Ticker(ticker)
    info = t.info
    if not info:
        raise HTTPException(status_code=404, detail="Ticker not found")
    return normalize_value(info)

@app.get("/history/{ticker}")
def get_history(ticker: str, period: str = "1mo", interval: str = "1d"):
    t = yf.Ticker(ticker)
    hist = t.history(period=period, interval=interval)
    if hist.empty:
        raise HTTPException(status_code=404, detail="No history found")
    return normalize_value(hist)

@app.get("/financials/{ticker}")
def get_financials(ticker: str):
    t = yf.Ticker(ticker)
    data = {
        "income_statement": t.income_stmt,
        "balance_sheet": t.balance_sheet,
        "cashflow": t.cashflow
    }
    return normalize_value(data)

@app.get("/holders/{ticker}")
def get_holders(ticker: str):
    t = yf.Ticker(ticker)
    return {
        "major_holders": normalize_value(t.major_holders),
        "institutional_holders": normalize_value(t.institutional_holders)
    }

@app.get("/news/{ticker}")
def get_news(ticker: str):
    t = yf.Ticker(ticker)
    return normalize_value(t.news)

@app.get("/recommendations/{ticker}")
def get_recommendations(ticker: str):
    t = yf.Ticker(ticker)
    return normalize_value(t.recommendations)

@app.get("/calendar/{ticker}")
def get_calendar(ticker: str):
    t = yf.Ticker(ticker)
    return normalize_value(t.calendar)

@app.get("/health")
def health():
    return {"status": "online", "timestamp": datetime.now().isoformat()}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
