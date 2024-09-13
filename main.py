import requests
from bs4 import BeautifulSoup
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime, timedelta
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.image import MIMEImage
from email.mime.application import MIMEApplication
import io
import os
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import boto3

# AWS S3 configuration
s3_client = boto3.client(
    's3',
    aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'],
    aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'],
    region_name=os.environ['AWS_REGION']
)
bucket_name = os.environ['ctabucketdata']
file_key = 'tsa_data.xlsx'

# Function to scrape TSA data
def scrape_tsa_data():
    url = "https://www.tsa.gov/travel/passenger-volumes"
    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'html.parser')
    
    dates = soup.find_all('td', class_="views-field views-field-field-travel-number-date views-align-center")
    numbers = soup.find_all('td', class_="views-field views-field-field-travel-number views-align-center")
    
    data = []
    for date, number in zip(dates, numbers):
        data.append({
            'Date': datetime.strptime(date.text.strip(), '%m/%d/%Y'),
            'Travel Number': int(number.text.strip().replace(',', ''))
        })
    
    return pd.DataFrame(data)

# Function to update Excel file in S3
def update_excel(new_data):
    try:
        # Download existing file from S3
        response = s3_client.get_object(Bucket=bucket_name, Key=file_key)
        existing_data = pd.read_excel(io.BytesIO(response['Body'].read()))
        
        # Combine and deduplicate data
        combined_data = pd.concat([existing_data, new_data]).drop_duplicates(subset='Date', keep='last')
        combined_data.sort_values('Date', ascending=False, inplace=True)
        
        # Save updated data back to S3
        buffer = io.BytesIO()
        with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
            combined_data.to_excel(writer, index=False)
        buffer.seek(0)
        s3_client.put_object(Bucket=bucket_name, Key=file_key, Body=buffer.getvalue())
        
        return combined_data
    except s3_client.exceptions.NoSuchKey:
        # If file doesn't exist, create a new one
        buffer = io.BytesIO()
        with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
            new_data.to_excel(writer, index=False)
        buffer.seek(0)
        s3_client.put_object(Bucket=bucket_name, Key=file_key, Body=buffer.getvalue())
        return new_data

# Function to create visualizations
def create_visualizations(data):
    # Prepare data for seasonality plot
    data['Year'] = data['Date'].dt.year
    data['DayOfYear'] = data['Date'].dt.dayofyear
    
    # Get current year and previous year
    current_year = datetime.now().year
    previous_year = current_year - 1
    
    # Filter last 5 years for seasonality plot
    data_5years = data[data['Year'] >= current_year - 5]
    
    # Create static matplotlib plot for seasonality
    plt.figure(figsize=(15, 8))
    
    colors = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd', '#8c564b']
    years = sorted(data_5years['Year'].unique())
    
    for i, year in enumerate(years):
        year_data = data_5years[data_5years['Year'] == year]
        alpha = 0.3 if year != current_year else 1.0
        plt.plot(year_data['DayOfYear'], year_data['Travel Number'], 
                 label=str(year), color=colors[i % len(colors)], linewidth=2, alpha=alpha)
    
    plt.title('TSA Travel Numbers - Seasonality (Last 5 Years)', fontsize=16)
    plt.xlabel('Month', fontsize=12)
    plt.ylabel('Number of Travelers', fontsize=12)
    plt.legend(title='Year', title_fontsize='12', fontsize='10')
    
    plt.xticks([1, 32, 60, 91, 121, 152, 182, 213, 244, 274, 305, 335],
               ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'])
    
    plt.grid(True, linestyle='--', alpha=0.7)
    plt.gca().yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: format(int(x), ',')))
    
    # Save static seasonality plot to buffer
    seasonality_buf = io.BytesIO()
    plt.savefig(seasonality_buf, format='png', dpi=300, bbox_inches='tight')
    seasonality_buf.seek(0)
    plt.close()
    
    # Create YoY comparison chart
    plt.figure(figsize=(15, 8))
    
    # Calculate 2-week moving average for current and previous year
    data_prev_year = data[data['Year'] == previous_year].sort_values('Date')
    data_current_year = data[data['Year'] == current_year].sort_values('Date')
    
    data_prev_year['MA_14'] = data_prev_year['Travel Number'].rolling(window=14).mean()
    data_current_year['MA_14'] = data_current_year['Travel Number'].rolling(window=14).mean()
    
    plt.plot(data_prev_year['DayOfYear'], data_prev_year['MA_14'], label=str(previous_year), color='#ff7f0e', linewidth=2)
    plt.plot(data_current_year['DayOfYear'], data_current_year['MA_14'], label=str(current_year), color='#1f77b4', linewidth=2)
    
    plt.title(f'TSA Travel Numbers - YoY Comparison (2-Week Moving Average)', fontsize=16)
    plt.xlabel('Month', fontsize=12)
    plt.ylabel('Number of Travelers (2-Week Moving Average)', fontsize=12)
    plt.legend(title='Year', title_fontsize='12', fontsize='10')
    
    plt.xticks([1, 32, 60, 91, 121, 152, 182, 213, 244, 274, 305, 335],
               ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'])
    
    plt.grid(True, linestyle='--', alpha=0.7)
    plt.gca().yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: format(int(x), ',')))
    
    # Save YoY comparison plot to buffer
    yoy_buf = io.BytesIO()
    plt.savefig(yoy_buf, format='png', dpi=300, bbox_inches='tight')
    yoy_buf.seek(0)
    plt.close()
    
    # Create interactive Plotly plot for seasonality
    fig = make_subplots(specs=[[{"secondary_y": False}]])
    
    for i, year in enumerate(years):
        year_data = data_5years[data_5years['Year'] == year]
        fig.add_trace(
            go.Scatter(x=year_data['DayOfYear'], y=year_data['Travel Number'], 
                       mode='lines', name=str(year), line=dict(color=colors[i % len(colors)]))
        )
    
    fig.update_layout(
        title='TSA Travel Numbers - Seasonality (Last 5 Years)',
        xaxis_title='Month',
        yaxis_title='Number of Travelers',
        legend_title='Year',
        hovermode="x unified"
    )
    
    fig.update_xaxes(
        tickvals=[1, 32, 60, 91, 121, 152, 182, 213, 244, 274, 305, 335],
        ticktext=['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
    )
    
    # Save interactive plot to HTML file
    interactive_buf = io.StringIO()
    fig.write_html(interactive_buf)
    interactive_buf.seek(0)
    
    return seasonality_buf, yoy_buf, interactive_buf

# Function to send email
def send_email(new_data, seasonality_buf, yoy_buf, interactive_buf, excel_buffer):
    logging.info("Starting email send process...")
    sender_email = "dsierraramirez115@gmail.com"
    receiver_email = "diegosierra01@yahoo.com"
    password = os.environ['EMAIL_PASSWORD']
    
    logging.info(f"Sender: {sender_email}, Receiver: {receiver_email}")
    
    message = MIMEMultipart()
    message["From"] = sender_email
    message["To"] = receiver_email
    message["Subject"] = f"TSA Travel Update - {new_data['Date'].iloc[0].strftime('%Y-%m-%d')}"
    
    body = f"""
    <html>
    <body>
    <h2>TSA Travel Update</h2>
    <p>Date: {new_data['Date'].iloc[0].strftime('%Y-%m-%d')}</p>
    <p>Travel Number: {new_data['Travel Number'].iloc[0]:,}</p>
    <p>Please find attached:</p>
    <ul>
        <li>A static image of the seasonality plot</li>
        <li>A static image of the YoY comparison plot</li>
        <li>An interactive HTML file with the seasonality plot</li>
        <li>The updated Excel file with all data</li>
    </ul>
    <h3>Seasonality Plot</h3>
    <img src="cid:seasonality_plot">
    <h3>Year-over-Year Comparison</h3>
    <img src="cid:yoy_plot">
    </body>
    </html>
    """
    
    message.attach(MIMEText(body, "html"))
    
    # Attach seasonality visualization
    seasonality_image = MIMEImage(seasonality_buf.getvalue())
    seasonality_image.add_header('Content-ID', '<seasonality_plot>')
    message.attach(seasonality_image)
    
    # Attach YoY comparison visualization
    yoy_image = MIMEImage(yoy_buf.getvalue())
    yoy_image.add_header('Content-ID', '<yoy_plot>')
    message.attach(yoy_image)
    
    # Attach interactive HTML plot
    html_attachment = MIMEText(interactive_buf.getvalue(), 'html')
    html_attachment.add_header('Content-Disposition', 'attachment', filename='interactive_plot.html')
    message.attach(html_attachment)
    
    # Attach Excel file
    excel_attachment = MIMEApplication(excel_buffer.getvalue())
    excel_attachment.add_header('Content-Disposition', 'attachment', filename='tsa_data.xlsx')
    message.attach(excel_attachment)
    
    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            logging.info("Connecting to SMTP server...")
            server.login(sender_email, password)
            logging.info("Logged in successfully")
            server.send_message(message)
            logging.info("Email sent successfully")
    except Exception as e:
        logging.error(f"Failed to send email: {str(e)}")
        raise

# Main function
def main():
    new_data = scrape_tsa_data()
    all_data = update_excel(new_data)
    seasonality_buf, yoy_buf, interactive_buf = create_visualizations(all_data)
    
    # Create a BytesIO object for the Excel file
    excel_buffer = io.BytesIO()
    with pd.ExcelWriter(excel_buffer, engine='openpyxl') as writer:
        all_data.to_excel(writer, index=False)
    excel_buffer.seek(0)
    
    send_email(new_data, seasonality_buf, yoy_buf, interactive_buf, excel_buffer)

if __name__ == "__main__":
    main()