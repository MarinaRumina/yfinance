from fastapi import FastAPI, Query
import yfinance as yf
import pandas as pd

app = FastAPI()

def safe_to_dict(obj):
    if isinstance(obj, (pd.DataFrame, pd.Series)):
        if not obj.empty:
            # Если DataFrame, приводим к списку dict'ов
            # Для Series — просто к dict'у
            if isinstance(obj, pd.DataFrame):
                return obj.reset_index().to_dict(orient="records")
            else:
                return obj.to_dict()
        else:
            return []
    elif isinstance(obj, dict):
        return obj
    elif obj is None:
        return {}
    else:
        return str(obj)

# Single data endpoints
@app.get("/quote")
def quote(ticker: str = Query(...)):
    try:
        t = yf.Ticker(ticker)
        data = t.history(period="1d")
        return safe_to_dict(data)
    except Exception as e:
        return {"error": str(e)}

@app.get("/info")
def info(ticker: str = Query(...)):
    try:
        t = yf.Ticker(ticker)
        return safe_to_dict(t.info)
    except Exception as e:
        return {"error": str(e)}

@app.get("/dividends")
def dividends(ticker: str = Query(...)):
    try:
        t = yf.Ticker(ticker)
        return safe_to_dict(t.dividends)
    except Exception as e:
        return {"error": str(e)}

@app.get("/actions")
def actions(ticker: str = Query(...)):
    try:
        t = yf.Ticker(ticker)
        return safe_to_dict(t.actions)
    except Exception as e:
        return {"error": str(e)}

@app.get("/splits")
def splits(ticker: str = Query(...)):
    try:
        t = yf.Ticker(ticker)
        return safe_to_dict(t.splits)
    except Exception as e:
        return {"error": str(e)}

@app.get("/financials")
def financials(ticker: str = Query(...)):
    try:
        t = yf.Ticker(ticker)
        return safe_to_dict(t.financials)
    except Exception as e:
        return {"error": str(e)}

@app.get("/balance_sheet")
def balance_sheet(ticker: str = Query(...)):
    try:
        t = yf.Ticker(ticker)
        return safe_to_dict(t.balance_sheet)
    except Exception as e:
        return {"error": str(e)}

@app.get("/cashflow")
def cashflow(ticker: str = Query(...)):
    try:
        t = yf.Ticker(ticker)
        return safe_to_dict(t.cashflow)
    except Exception as e:
        return {"error": str(e)}

@app.get("/calendar")
def calendar(ticker: str = Query(...)):
    try:
        t = yf.Ticker(ticker)
        return safe_to_dict(t.calendar)
    except Exception as e:
        return {"error": str(e)}

@app.get("/history")
def history(
    ticker: str = Query(...),
    period: str = Query("1mo"),
    interval: str = Query("1d")
):
    try:
        data = yf.download(ticker, period=period, interval=interval)
        return safe_to_dict(data)
    except Exception as e:
        return {"error": str(e)}

# Multi-ticker and advanced endpoints
@app.get("/tickers")
def tickers(symbols: str = Query(...)):
    try:
        tickers_obj = yf.Tickers(symbols)
        # Возвращаем список тикеров и их info
        result = {sym: safe_to_dict(t.info) for sym, t in tickers_obj.tickers.items()}
        return result
    except Exception as e:
        return {"error": str(e)}

@app.get("/ticker")
def ticker(ticker: str = Query(...)):
    try:
        t = yf.Ticker(ticker)
        return {
            "info": safe_to_dict(t.info),
            "history": safe_to_dict(t.history(period="1y")),
            "actions": safe_to_dict(t.actions),
            "dividends": safe_to_dict(t.dividends),
            "splits": safe_to_dict(t.splits),
            "financials": safe_to_dict(t.financials),
            "balance_sheet": safe_to_dict(t.balance_sheet),
            "cashflow": safe_to_dict(t.cashflow),
            "calendar": safe_to_dict(t.calendar),
            "earnings": safe_to_dict(getattr(t, "earnings", {})),
            "sustainability": safe_to_dict(getattr(t, "sustainability", {})),
            "isin": getattr(t, "isin", None),
            "major_holders": safe_to_dict(getattr(t, "major_holders", {})),
            "institutional_holders": safe_to_dict(getattr(t, "institutional_holders", {}))
        }
    except Exception as e:
        return {"error": str(e)}

@app.get("/download")
def download(
    tickers: str = Query(...),
    period: str = Query("1mo"),
    interval: str = Query("1d")
):
    try:
        data = yf.download(tickers, period=period, interval=interval, group_by=None)
        return safe_to_dict(data)
    except Exception as e:
        return {"error": str(e)}

@app.get("/market")
def market(region: str = Query("US")):
    try:
        market = getattr(yf, "Market", None)
        if market:
            return market(region=region)
    except Exception as e:
        return {"error": str(e)}
    return {"error": "Market API not available"}

@app.get("/search")
def search(query: str = Query(...)):
    try:
        search = getattr(yf, "Search", None)
        if search:
            res = search(query)
            return res
    except Exception as e:
        return {"error": str(e)}
    return {"error": "Search API not found"}

@app.get("/sector")
def sector():
    try:
        sector = getattr(yf, "Sector", None)
        if sector:
            return sector()
    except Exception as e:
        return {"error": str(e)}
    return {"error": "Sector API not found"}

@app.get("/industry")
def industry():
    try:
        industry = getattr(yf, "Industry", None)
        if industry:
            return industry()
    except Exception as e:
        return {"error": str(e)}
    return {"error": "Industry API not found"}

@app.get("/equityquery")
def equityquery(query: str = Query(...)):
    try:
        eq = getattr(yf, "EquityQuery", None)
        if eq:
            return eq(query)
    except Exception as e:
        return {"error": str(e)}
    return {"error": "EquityQuery API not found"}

@app.get("/screener")
def screener(query: str = Query(...)):
    try:
        screener = getattr(yf, "Screener", None)
        if screener:
            return screener(query)
    except Exception as e:
        return {"error": str(e)}
    return {"error": "Screener API not found"}

@app.get("/news")
def news(ticker: str = Query(...)):
    try:
        t = yf.Ticker(ticker)
        return safe_to_dict(getattr(t, "news", {}))
    except Exception as e:
        return {"error": str(e)}
