from fastapi import FastAPI, Query
import yfinance as yf

app = FastAPI()

# Single data endpoints
@app.get("/quote")
def quote(ticker: str = Query(...)):
    try:
        t = yf.Ticker(ticker)
        data = t.history(period="1d")
        return data.reset_index().to_dict(orient="records")
    except Exception as e:
        return {"error": str(e)}

@app.get("/info")
def info(ticker: str = Query(...)):
    try:
        t = yf.Ticker(ticker)
        return t.info
    except Exception as e:
        return {"error": str(e)}

@app.get("/dividends")
def dividends(ticker: str = Query(...)):
    try:
        t = yf.Ticker(ticker)
        return t.dividends.to_dict()
    except Exception as e:
        return {"error": str(e)}

@app.get("/actions")
def actions(ticker: str = Query(...)):
    try:
        t = yf.Ticker(ticker)
        return t.actions.to_dict()
    except Exception as e:
        return {"error": str(e)}

@app.get("/splits")
def splits(ticker: str = Query(...)):
    try:
        t = yf.Ticker(ticker)
        return t.splits.to_dict()
    except Exception as e:
        return {"error": str(e)}

@app.get("/financials")
def financials(ticker: str = Query(...)):
    try:
        t = yf.Ticker(ticker)
        return t.financials.to_dict()
    except Exception as e:
        return {"error": str(e)}

@app.get("/balance_sheet")
def balance_sheet(ticker: str = Query(...)):
    try:
        t = yf.Ticker(ticker)
        return t.balance_sheet.to_dict()
    except Exception as e:
        return {"error": str(e)}

@app.get("/cashflow")
def cashflow(ticker: str = Query(...)):
    try:
        t = yf.Ticker(ticker)
        return t.cashflow.to_dict()
    except Exception as e:
        return {"error": str(e)}

@app.get("/calendar")
def calendar(ticker: str = Query(...)):
    try:
        t = yf.Ticker(ticker)
        return t.calendar.to_dict()
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
        return data.reset_index().to_dict(orient="records")
    except Exception as e:
        return {"error": str(e)}

# Multi-ticker and advanced endpoints
@app.get("/tickers")
def tickers(symbols: str = Query(...)):
    try:
        return yf.Tickers(symbols).tickers
    except Exception as e:
        return {"error": str(e)}

@app.get("/ticker")
def ticker(ticker: str = Query(...)):
    try:
        t = yf.Ticker(ticker)
        return {
            "info": t.info,
            "history": t.history(period="1y").reset_index().to_dict(orient="records"),
            "actions": t.actions.to_dict(),
            "dividends": t.dividends.to_dict(),
            "splits": t.splits.to_dict(),
            "financials": t.financials.to_dict(),
            "balance_sheet": t.balance_sheet.to_dict(),
            "cashflow": t.cashflow.to_dict(),
            "calendar": t.calendar.to_dict(),
            "earnings": t.earnings.to_dict(),
            "sustainability": getattr(t, "sustainability", None),
            "isin": getattr(t, "isin", None),
            "major_holders": t.major_holders.to_dict(),
            "institutional_holders": t.institutional_holders.to_dict()
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
        data = yf.download(tickers, period=period, interval=interval)
        return data.reset_index().to_dict(orient="records")
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
        return t.news
    except Exception as e:
        return {"error": str(e)}
    return {"error": "News API not found"}
