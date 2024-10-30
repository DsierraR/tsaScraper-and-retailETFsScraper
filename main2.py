import matplotlib.pyplot as plt
import pandas as pd
from io import BytesIO
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.image import MIMEImage
from email.mime.application import MIMEApplication
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from openpyxl import load_workbook, Workbook
from datetime import datetime
import logging
import os
import numpy as np
import time
import requests
from bs4 import BeautifulSoup

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Email Configuration
sender_email = "dsierraramirez115@gmail.com"
receiver_email = ["diegosierra01@yahoo.com","arnav.ashruchi@gmail.com"]
email_password = 'qmdm rqgv bork eukg'

# ETF Tickers
ETF_TICKERS_FIRST = ['USO', 'BNO', 'UGA']
ETF_TICKERS_SECOND = ['UCO', 'DBO', 'SCO']
BASE_URL_FIRST = "https://www.uscfinvestments.com/"
INVESTING_URLS = {
    'UCO': "https://www.investing.com/etfs/proshares-ultra-dj-ubs-crude-oil",
    'DBO': "https://www.investing.com/etfs/powershares-db-oil-fund?cid=980444",
    'SCO': "https://www.investing.com/etfs/proshares-ultrashort-dj-ubs-crude-o"
}

# Excel file path
excel_path = 'shares_outstanding_data.xlsx'

# Set up Selenium with simulated headful Chrome
chrome_options = Options()
chrome_options.add_argument("--headless")
chrome_options.add_argument("--disable-gpu")
chrome_options.add_argument("--window-size=1920,1080")
chrome_options.add_argument("--start-maximized")
chrome_options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36")
chrome_options.add_argument("--disable-blink-features=AutomationControlled")
chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
chrome_options.add_experimental_option("useAutomationExtension", False)
service = ChromeService()

def fetch_shares_outstanding_first(etf_ticker):
    """Fetches shares outstanding for ETFs from the first website."""
    url = f"{BASE_URL_FIRST}{etf_ticker.lower()}"
    logging.info(f"Fetching data for {etf_ticker}. URL: {url}")

    with webdriver.Chrome(service=service, options=chrome_options) as driver:
        driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        driver.get(url)
        time.sleep(3)  # Give the page time to load

        try:
            shares_outstanding = WebDriverWait(driver, 15).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "td[data-key='so']"))
            ).text.strip().replace(",", "")
            logging.info(f"{etf_ticker}: Shares Outstanding = {shares_outstanding}")
            return shares_outstanding
        except Exception as e:
            logging.error(f"Failed to retrieve shares outstanding for {etf_ticker}: {e}")
            return "N/A"

def fetch_shares_outstanding_static(etf_ticker):
    """Fetches shares outstanding for ETFs from Investing.com using requests for static content."""
    url = INVESTING_URLS[etf_ticker]
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36"
    }
    response = requests.get(url, headers=headers, verify=False)  # Disable SSL verification
    soup = BeautifulSoup(response.content, 'html.parser')
    try:
        shares_outstanding = soup.select_one("dd[data-test='sharesOutstanding'] .key-info_dd-numeric__ZQFIs span:nth-child(2)").text.strip().replace(",", "")
        logging.info(f"{etf_ticker}: Shares Outstanding = {shares_outstanding}")
        return shares_outstanding
    except Exception as e:
        logging.error(f"Failed to retrieve shares outstanding for {etf_ticker}: {e}")
        return "N/A"


def update_excel(etf_data):
    """Updates the Excel file with new ETF data or creates a new file if not found."""
    today_date = datetime.now().strftime('%Y-%m-%d')
    all_tickers = ETF_TICKERS_FIRST + ETF_TICKERS_SECOND
    new_row = [today_date] + [etf_data.get(ticker, 'N/A') for ticker in all_tickers]
    
    if os.path.exists(excel_path):
        workbook = load_workbook(excel_path)
        sheet = workbook.active
        # Ensure all columns for all tickers are present
        current_columns = [cell.value for cell in sheet[1]]
        missing_tickers = [ticker for ticker in all_tickers if ticker not in current_columns]
        for ticker in missing_tickers:
            sheet.cell(row=1, column=len(current_columns) + 1, value=ticker)
            current_columns.append(ticker)
        # Fetch previous row data as a dictionary
        previous_row = {ticker: sheet.cell(row=sheet.max_row, column=i + 2).value for i, ticker in enumerate(all_tickers)}
    else:
        workbook = Workbook()
        sheet = workbook.active
        sheet.title = 'Shares Outstanding'
        sheet.append(['Date'] + all_tickers)  # Add headers
        previous_row = {ticker: 'N/A' for ticker in all_tickers}  # Initialize if no previous data

    sheet.append(new_row)
    workbook.save(excel_path)
    return new_row, previous_row

def create_visualization():
    """Creates a visualization of shares outstanding with stacked bars and a single trend line for the total shares outstanding."""
    # Load the data from the Excel file
    df = pd.read_excel(excel_path)
    
    # Convert the date column to datetime
    df['Date'] = pd.to_datetime(df['Date'])
    
    # Ensure all ETF columns are present in the DataFrame
    for ticker in ETF_TICKERS_FIRST + ETF_TICKERS_SECOND:
        if ticker not in df.columns:
            df[ticker] = 0  # Fill missing columns with 0

    # Calculate the total shares outstanding across all ETFs for each date
    df['Total Shares'] = df[ETF_TICKERS_FIRST + ETF_TICKERS_SECOND].sum(axis=1)

    # Set up the figure and axis
    fig, ax = plt.subplots(figsize=(10, 6))

    # Plot stacked bars for each ETF
    bottom_values = np.zeros(len(df))
    colors = ['#FF9999', '#66B2FF', '#fa7832', '#FFC107', '#8BC34A', '#C09ADB']  # Colors for each ETF
    for i, ticker in enumerate(ETF_TICKERS_FIRST + ETF_TICKERS_SECOND):
        ax.bar(df['Date'], df[ticker], bottom=bottom_values, color=colors[i], label=ticker, width=0.5)
        bottom_values += df[ticker]

    # Plot a single trend line for the total shares outstanding on top of the bars
    ax.plot(df['Date'], df['Total Shares'], label='Total Shares Trend', color='black', linewidth=2)

    # Formatting and grids
    ax.set_title('Shares Outstanding Over Time')
    ax.set_xlabel('Date')
    ax.set_ylabel('Shares Outstanding')
    fig.autofmt_xdate(rotation=45)

    # Add grid lines
    ax.grid(True, which='both', linestyle='--', linewidth=0.5, color='gray', alpha=0.7)

    # Configure a smaller legend
    ax.legend(loc='lower left', fontsize='small', frameon=True)

    # Save the figure to a BytesIO object
    img_buffer = BytesIO()
    plt.savefig(img_buffer, format='png', bbox_inches='tight')
    img_buffer.seek(0)
    plt.close(fig)
    return img_buffer

def send_email_with_visualization(new_row, previous_row):
    """Sends an email with the updated shares outstanding data, embedded visualization, and Excel attachment."""
    date, *values = new_row
    message = MIMEMultipart("related")
    message["From"] = sender_email
    message["To"] = ", ".join(receiver_email)
    message["Subject"] = "Retail Oil ETFs - Shares Outstanding Update"

    # Create the email body, only including changes
    body = f"<p>Date: {date}</p>"
    changes = False
    for i, ticker in enumerate(ETF_TICKERS_FIRST + ETF_TICKERS_SECOND):
        if previous_row[ticker] != values[i]:  # Only add if there's a change
            body += f"<p>{ticker}: {values[i]} (previous: {previous_row[ticker]})</p>"
            changes = True

    if not changes:
        body += "<p>No changes in shares outstanding.</p>"

    body += "<h2>Shares Outstanding Over Time</h2><br><img src='cid:visualization'><br>"

    message.attach(MIMEText(body, "html"))

    # Attach the visualization as an inline image
    img_buffer = create_visualization()
    img = MIMEImage(img_buffer.getvalue())
    img.add_header("Content-ID", "<visualization>")
    message.attach(img)

    # Attach the Excel file
    with open(excel_path, 'rb') as file:
        excel_attachment = MIMEApplication(file.read(), _subtype="xlsx")
        excel_attachment.add_header("Content-Disposition", "attachment", filename="shares_outstanding_data.xlsx")
        message.attach(excel_attachment)

    # Send the email
    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(sender_email, email_password)
            server.send_message(message)
        logging.info("Email with visualization and Excel attachment sent successfully.")
    except Exception as e:
        logging.error(f"An error occurred while sending the email: {e}")

def main():
    # Fetch shares outstanding for both sets of ETFs
    etf_data = {ticker: fetch_shares_outstanding_first(ticker) for ticker in ETF_TICKERS_FIRST}
    etf_data.update({ticker: fetch_shares_outstanding_static(ticker) for ticker in ETF_TICKERS_SECOND})
    
    # Update Excel and send email with visualization
    new_row, previous_row = update_excel(etf_data)
    send_email_with_visualization(new_row, previous_row)

if __name__ == "__main__":
    main()
