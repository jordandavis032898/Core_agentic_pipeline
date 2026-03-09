# ============================================================================
# AGGREGATED MULTI-YEAR FINANCIAL STATEMENT SCRAPER (ORCHESTRATOR)
# ============================================================================

import json
import requests
from bs4 import BeautifulSoup
import time
import re
from typing import List, Dict, Optional
try:
    from openpyxl import Workbook
    from openpyxl.styles import Font, PatternFill
except ImportError:
    # openpyxl is optional - only needed for Excel export functionality
    Workbook = None
    Font = None
    PatternFill = None
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
import traceback
from collections import defaultdict

# Import the scraper and merger helpers
from .scraper_final import FinancialStatementScraper
from .merger_final import build_unified_catalog_all_statements


# ============================================================================
# PART 1: LINK SCRAPER
# ============================================================================

def get_cik_from_ticker(ticker: str, headers: dict) -> Optional[str]:
    """Get CIK number from ticker using SEC's company_tickers.json"""
    try:
        url = "https://www.sec.gov/files/company_tickers.json"
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        companies = response.json()
        
        for company_id, company_data in companies.items():
            if company_data['ticker'].upper() == ticker:
                cik = str(company_data['cik_str']).zfill(10)
                return cik
        return None
    except Exception as e:
        print(f"Error fetching CIK: {str(e)}")
        return None


def get_10k_filings(ticker: str) -> List[Dict[str, str]]:
    """Scrape SEC 10-K filings for a given ticker (2020 onwards only)"""
    headers = {'User-Agent': 'harshagr838@gmail.com'}
    
    try:
        cik = get_cik_from_ticker(ticker.upper(), headers)
        if not cik:
            print(f"Ticker '{ticker}' not found")
            return []
        
        print(f"Found CIK: {cik} for ticker: {ticker}")
        time.sleep(0.5)
        
        filings_url = f"https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK={cik}&type=10-K&count=100"
        response = requests.get(filings_url, headers=headers)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.content, 'html.parser')
        filings_table = soup.find('table', class_='tableFile2')
        
        if not filings_table:
            print("No filings table found")
            return []
        
        rows = filings_table.find_all('tr')[1:]
        filings_data = []
        
        for row in rows:
            if len(filings_data) >= 10:
                break
            
            cols = row.find_all('td')
            if len(cols) >= 4:
                filing_type = cols[0].text.strip()
                if filing_type == '10-K':
                    filing_date = cols[3].text.strip()
                    
                    # Filter for 2020 onwards only
                    filing_year = int(filing_date.split('-')[0])
                    if filing_year < 2020:
                        continue
                    
                    description = cols[2].text.strip()
                    acc_match = re.search(r'Acc-no:\s*(\d{10}-\d{2}-\d{6})', description)
                    
                    if acc_match:
                        accession_number = acc_match.group(1)
                        filings_data.append({
                            'accession_number': accession_number,
                            'filing_date': filing_date
                        })
        
        print(f"Found {len(filings_data)} 10-K filings (2020 onwards)")
        
        results = []
        for filing in filings_data:
            time.sleep(0.5)
            
            accession_no_hyphens = filing['accession_number'].replace('-', '')
            accession_with_hyphens = filing['accession_number']
            
            index_url = f"https://www.sec.gov/Archives/edgar/data/{cik}/{accession_no_hyphens}/{accession_with_hyphens}-index.htm"
            
            try:
                index_response = requests.get(index_url, headers=headers)
                index_response.raise_for_status()
                index_soup = BeautifulSoup(index_response.content, 'html.parser')
                
                doc_table = index_soup.find('table', class_='tableFile')
                if not doc_table:
                    tables = index_soup.find_all('table')
                    for table in tables:
                        header_row = table.find('tr')
                        if header_row and 'document' in header_row.text.lower():
                            doc_table = table
                            break
                
                if doc_table:
                    primary_htm = None
                    doc_rows = doc_table.find_all('tr')[1:]
                    
                    for doc_row in doc_rows:
                        doc_cols = doc_row.find_all('td')
                        if len(doc_cols) >= 4:
                            doc_link = doc_cols[2].find('a')
                            doc_type = doc_cols[3].text.strip()
                            
                            if doc_link:
                                doc_name = doc_link.text.strip()
                                is_htm = doc_name.lower().endswith(('.htm', '.html'))
                                is_10k = (doc_type.upper() == '10-K' or '10-K' in doc_cols[1].text.upper())
                                is_not_exhibit = not doc_name.lower().startswith('ex')
                                is_not_graphic = 'graphic' not in doc_name.lower()
                                is_not_xml = not doc_name.lower().endswith('.xml')
                                
                                if (is_htm and is_10k and is_not_exhibit and is_not_graphic and is_not_xml):
                                    primary_htm = doc_name
                                    break
                    
                    if not primary_htm:
                        for doc_row in doc_rows:
                            doc_cols = doc_row.find_all('td')
                            if len(doc_cols) >= 3:
                                doc_link = doc_cols[2].find('a')
                                if doc_link:
                                    doc_name = doc_link.text.strip()
                                    if (doc_name.lower().endswith(('.htm', '.html')) and
                                        not doc_name.lower().startswith('ex') and
                                        'graphic' not in doc_name.lower() and
                                        not doc_name.lower().endswith('.xml')):
                                        primary_htm = doc_name
                                        break
                    
                    if primary_htm:
                        ix_url = f"https://www.sec.gov/ix?doc=/Archives/edgar/data/{cik}/{accession_no_hyphens}/{primary_htm}"
                        report_year = filing['filing_date'].split('-')[0]
                        
                        results.append({
                            'filing_date': filing['filing_date'],
                            'report_year': report_year,
                            'accession_number': accession_with_hyphens,
                            'ix_viewer_url': ix_url
                        })
                        print(f"  ✓ {report_year}: {accession_with_hyphens}")
                    else:
                        print(f"  ✗ {filing['filing_date']}: Could not find primary document")
            
            except Exception as e:
                print(f"Error processing filing {filing['accession_number']}: {str(e)}")
                continue
        
        return results
    
    except Exception as e:
        print(f"Error: {str(e)}")
        return []


# ============================================================================
# PART 2: NUMBER CONVERSION UTILITY
# ============================================================================

def parse_financial_value(value_str):
    """
    Convert financial string values to proper numbers for Excel.
    Handles: parentheses (negative), commas, dashes, em-dashes, etc.
    
    Returns tuple: (converted_value, is_numeric)
    """
    if value_str is None or value_str == "":
        return ("", False)
    
    if isinstance(value_str, (int, float)):
        return (value_str, True)
    
    value_str = str(value_str).strip()
    
    if not value_str or value_str == "":
        return ("", False)
    
    if value_str in ['-', '—', '–', 'N/A', 'n/a', 'NA', '***', '*']:
        return ("", False)
    
    original = value_str
    
    try:
        is_negative = False
        if value_str.startswith('(') and value_str.endswith(')'):
            is_negative = True
            value_str = value_str[1:-1].strip()
        
        value_str = re.sub(r'[$€£¥₹]', '', value_str)
        value_str = value_str.replace(',', '')
        value_str = value_str.replace(' ', '')
        
        is_percentage = value_str.endswith('%')
        if is_percentage:
            value_str = value_str[:-1]
        
        if value_str:
            num_value = float(value_str)
            
            if is_negative:
                num_value = -num_value
            
            if is_percentage:
                num_value = num_value / 100
            
            return (num_value, True)
        else:
            return (original, False)
    
    except (ValueError, AttributeError):
        return (original, False)


# ============================================================================
# PART 3: AGGREGATED MULTI-YEAR SCRAPER
# ============================================================================

class AggregatedFinancialScraper:
    """
    Scrapes financial statements from multiple years in parallel
    and aggregates them into single JSON files and Excel workbook
    """
    
    def __init__(self, ticker: str, max_workers: int = 3):
        self.ticker = ticker
        self.max_workers = max_workers
        self.lock = Lock()
        
        # Storage for aggregated data
        self.balance_sheet_data = []
        self.income_statement_data = []
        self.cash_flow_data = []
    
    def scrape_single_filing(self, filing_info: Dict) -> Dict:
        """Scrape a single filing and return extracted statements"""
        year = filing_info['report_year']
        url = filing_info['ix_viewer_url']
        
        print(f"\n{'='*80}")
        print(f"🔍 Processing {year} - {self.ticker}")
        print(f"{'='*80}\n")
        
        try:
            scraper = FinancialStatementScraper(url)
            
            results = {
                'year': year,
                'filing_date': filing_info['filing_date'],
                'url': url,
                'statements': {}
            }
            
            # Extract each statement type
            for stmt_type, stmt_name in [
                ('balance_sheet', 'Balance Sheet'),
                ('income_statement', 'Income Statement'),
                ('cash_flow', 'Cash Flow')
            ]:
                result = scraper.extract_statement(None, stmt_name, stmt_type, f"{stmt_type}.xlsx", False)
                if result['status'] == 'success':
                    results['statements'][stmt_type] = result['json']
                    print(f"✅ Successfully extracted {stmt_name} for {year}")
                else:
                    print(f"❌ Failed to extract {stmt_name} for {year}: {result.get('error', 'Unknown error')}")
                    results['statements'][stmt_type] = None
            
            return results
            
        except Exception as e:
            print(f"❌ Error processing {year}: {str(e)}")
            traceback.print_exc()
            return {
                'year': year,
                'filing_date': filing_info['filing_date'],
                'url': url,
                'error': str(e),
                'statements': {}
            }
    
    def aggregate_statements(self, all_results: List[Dict]):
        """Aggregate statements from all years"""
        print("\n" + "="*80)
        print("📊 Aggregating statements across all years...")
        print("="*80 + "\n")
        
        # Sort by year ascending (oldest to newest)
        all_results.sort(key=lambda x: x['year'], reverse=False)
        
        for result in all_results:
            year = result['year']
            statements = result.get('statements', {})
            
            # Add metadata to each statement
            for stmt_type, stmt_data in statements.items():
                if stmt_data:
                    stmt_data['filing_year'] = year
                    stmt_data['filing_date'] = result['filing_date']
                    stmt_data['filing_url'] = result['url']
                    
                    # Append to appropriate list
                    if stmt_type == 'balance_sheet':
                        self.balance_sheet_data.append(stmt_data)
                    elif stmt_type == 'income_statement':
                        self.income_statement_data.append(stmt_data)
                    elif stmt_type == 'cash_flow':
                        self.cash_flow_data.append(stmt_data)
    
    def get_aggregated_data(self):
        """Return aggregated data in memory (no file saving)"""
        return {
            'balance_sheet': self.balance_sheet_data,
            'income_statement': self.income_statement_data,
            'cash_flow': self.cash_flow_data
        }
    
    def run(self):
        """Main execution method with parallelization - returns in-memory data only"""
        print(f"\n{'='*80}")
        print(f"🚀 Starting aggregated scraper for {self.ticker}")
        print(f"{'='*80}\n")
        
        # Step 1: Get all filing links
        print("Step 1: Fetching 10-K filing links...")
        filings = get_10k_filings(self.ticker)
        
        if not filings:
            print(f"❌ No filings found for {self.ticker}")
            return None
        
        print(f"\n✅ Found {len(filings)} filings to process\n")
        
        # Step 2: Scrape filings in parallel
        print(f"Step 2: Scraping {len(filings)} filings in parallel (max {self.max_workers} workers)...")
        all_results = []
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_filing = {
                executor.submit(self.scrape_single_filing, filing): filing 
                for filing in filings
            }
            
            for future in as_completed(future_to_filing):
                filing = future_to_filing[future]
                try:
                    result = future.result()
                    all_results.append(result)
                except Exception as e:
                    print(f"❌ Exception for {filing['report_year']}: {str(e)}")
        
        # Step 3: Aggregate data
        self.aggregate_statements(all_results)
        
        print("\n" + "="*80)
        print("✅ SCRAPING COMPLETE!")
        print("="*80)
        print(f"\n📊 Data aggregated in memory (no files saved)")
        print(f"  • Balance sheets: {len(self.balance_sheet_data)} years")
        print(f"  • Income statements: {len(self.income_statement_data)} years")
        print(f"  • Cash flows: {len(self.cash_flow_data)} years\n")
        
        return {
            'ticker': self.ticker,
            'balance_sheet_data': self.balance_sheet_data,
            'income_statement_data': self.income_statement_data,
            'cash_flow_data': self.cash_flow_data,
            'summary': {
                'ticker': self.ticker,
                'total_filings': len(filings),
                'balance_sheets': len(self.balance_sheet_data),
                'income_statements': len(self.income_statement_data),
                'cash_flows': len(self.cash_flow_data)
            }
        }