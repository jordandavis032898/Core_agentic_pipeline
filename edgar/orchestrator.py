from typing import Dict, Any


class AggregatedFinancialScraper:
    def __init__(self, ticker: str, max_workers: int = 3):
        self.ticker = ticker
        self.max_workers = max_workers

    def run(self) -> Dict[str, Any]:
        return {
            "ticker": self.ticker,
            "balance_sheet_data": [],
            "income_statement_data": [],
            "cash_flow_data": [],
            "summary": {
                "ticker": self.ticker,
                "total_filings": 0,
                "balance_sheets": 0,
                "income_statements": 0,
                "cash_flows": 0,
            },
        }
