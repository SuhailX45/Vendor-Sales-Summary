import pandas as pd 
import logging
import sqlite3
from script import ingest_db
logging.basicConfig(
    filename="logs/get_Vendor_Summary.log",
    level=logging.DEBUG,
    format="%(asctime)s-%(levelname)s-%(message)s",
    filemode="a"
)
    
def create_vendor_summary(con):
        
    Vendor_sales_summary = pd.read_sql_query("""
    WITH 
    FreightSummary AS (
        SELECT VendorNumber,
        SUM(freight) AS Freight_cost 
        FROM vendor_invoice 
        GROUP BY VendorNumber
    ),
    
    PurchaseSummary AS (
        SELECT
            p.Brand,
            p.VendorNumber,
            p.VendorName,
            p.PurchasePrice,
            p.Description,
            SUM(p.Quantity) AS totalPurchaseQuantity,
            SUM(p.Dollars) AS totalPurchaseDollars,
            pp.volume,
            pp.Price AS actual_price
        FROM purchases p 
        JOIN purchase_prices pp
          ON p.Brand = pp.Brand
        WHERE p.PurchasePrice > 0
        GROUP BY p.Brand, p.VendorName, p.VendorNumber, p.Description, p.PurchasePrice, pp.Price, pp.volume
    ),
    
    SalesSummary AS (
        SELECT 
            VendorNo,
            Brand,
            SUM(SalesDollars) AS totalsalesdollars,
            SUM(SalesQuantity) AS totalsalesquantity,
            SUM(ExciseTax) AS totalexciseTax,
            SUM(SalesPrice) AS totalsalesprice
        FROM sales
        GROUP BY VendorNo, Brand
    )
    
    SELECT 
        ps.VendorNumber,
        ps.VendorName,
        ps.Brand,
        ps.Description,
        ps.PurchasePrice,
        ps.actual_price,
        ps.volume,
        ps.totalPurchaseQuantity,
        ps.totalPurchaseDollars,
        ss.totalsalesquantity,
        ss.totalsalesdollars,
        ss.totalsalesprice,
        ss.totalexciseTax,
        fs.Freight_cost
    FROM PurchaseSummary ps
    LEFT JOIN SalesSummary ss
           ON ps.VendorNumber = ss.VendorNo
          AND ps.Brand = ss.Brand
    LEFT JOIN FreightSummary fs
           ON ps.VendorNumber = fs.VendorNumber
    ORDER BY ps.totalPurchaseDollars DESC
    """, con)
    return Vendor_sales_summary

def clean_data(df):
    Vendor_sales_summary['volume']=Vendor_sales_summary['volume'].astype('float64')
    Vendor_sales_summary.fillna(0,inplace=True)
    Vendor_sales_summary['VendorName']=Vendor_sales_summary['VendorName'].str.strip()
    
    Vendor_sales_summary['GrossProfit']=Vendor_sales_summary['totalsalesdollars']-Vendor_sales_summary['totalPurchaseDollars']
    Vendor_sales_summary['ProfitMargin']=Vendor_sales_summary['GrossProfit'] / Vendor_sales_summary['totalsalesdollars']*100
    Vendor_sales_summary['StockTurnover']=Vendor_sales_summary['totalsalesquantity']-Vendor_sales_summary['totalPurchaseQuantity']
    Vendor_sales_summary['SalesToPurchaseRatio']=Vendor_sales_summary['totalsalesdollars'] / Vendor_sales_summary['totalPurchaseDollars']
    return df
if __name__='__main__':
    # for database connectivity
    con=sqlite3.connect('inventory.db')
    logging.info('creating vendor_summary _table...')
    summary_df=create_vendor_summary(con)
    logging.info(summary_df.head())

    # for cleaning data
    logging.info('cleaning data....')
    clean_df=clean_data(summary_df)
    logging.info(clean_df.head())

    logging.info('ingesting data...')
    ingest_db(clean_df,'vendor_sales_summary',con)
    logging.info('complete')

    
        
            