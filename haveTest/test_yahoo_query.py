import yfinance as yf


def get_dollar_index_yahoo():
    ticker = 'DX-Y.NYB'
    data = yf.download(ticker, period='1d')
    return data


def get_yahoo():
    df = yf.download('TSLA', interval='1d', start='2022-04-17', end='2022-04-24', threads=True, proxy='127.0.0.1:9981')
    print(type(df))
    print(df)


# def get_yahoo_data():
#     import yfinance as yf
#
#     msft = yf.Ticker("MSFT")
#
#     # get all stock info
#     msft.info
#
#     # get historical market data
#     hist = msft.history(period="1mo")
#
#     # show meta information about the history (requires history() to be called first)
#     msft.history_metadata
#
#     # show actions (dividends, splits, capital gains)
#     msft.actions
#     msft.dividends
#     msft.splits
#     msft.capital_gains  # only for mutual funds & etfs
#
#     # show share count
#     msft.get_shares_full(start="2022-01-01", end=None)
#
#     # show financials:
#     # - income statement
#     msft.income_stmt
#     msft.quarterly_income_stmt
#     # - balance sheet
#     msft.balance_sheet
#     msft.quarterly_balance_sheet
#     # - cash flow statement
#     msft.cashflow
#     msft.quarterly_cashflow
#     # see `Ticker.get_income_stmt()` for more options
#
#     # show holders
#     msft.major_holders
#     msft.institutional_holders
#     msft.mutualfund_holders
#     msft.insider_transactions
#     msft.insider_purchases
#     msft.insider_roster_holders
#
#     # show recommendations
#     msft.recommendations
#     msft.recommendations_summary
#     msft.upgrades_downgrades
#
#     # Show future and historic earnings dates, returns at most next 4 quarters and last 8 quarters by default.
#     # Note: If more are needed use msft.get_earnings_dates(limit=XX) with increased limit argument.
#     msft.earnings_dates
#
#     # show ISIN code - *experimental*
#     # ISIN = International Securities Identification Number
#     msft.isin
#
#     # show options expirations
#     msft.options
#
#     # show news
#     msft.news
#
#     # get option chain for specific expiration
#     opt = msft.option_chain('YYYY-MM-DD')
#     # data available via: opt.calls, opt.puts


if __name__ == "__main__":
    # get_dollar_index_yahoo()
    # get_yahoo()
    # get_yahoo_data()

    from yahoo_fin.stock_info import *

    df = get_data("TSLA")

