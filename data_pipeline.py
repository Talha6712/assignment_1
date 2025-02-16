import praw
import yfinance as yf
import pandas as pd
import requests
import json
import os
import csv
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Reddit API 
reddit = praw.Reddit(
    client_id=os.getenv('REDDIT_CLIENT_ID'),
    client_secret=os.getenv('REDDIT_CLIENT_SECRET'),
    user_agent=os.getenv('REDDIT_USER_AGENT'),
    username=os.getenv('REDDIT_USER_NAME'),
    password=os.getenv('REDDIT_USER_PASSWORD')
)

def fetch_reddit_data(subreddits, keyword, limit=200):
    data = []
    for subreddit in subreddits:
        for submission in reddit.subreddit(subreddit).search(keyword, limit=limit):
            data.append({
                'title': submission.title,
                'text': submission.selftext,
                'author': submission.author.name if submission.author else None,
                'date': submission.created_utc,
                'upvotes': submission.score,
                'subreddit': subreddit
            })
    return data

#  Yahoo Finance Data
def fetch_finance_data(tickers, start_date, end_date):
    combined_data = pd.DataFrame()
    for ticker in tickers:
        stock = yf.Ticker(ticker)
        df = stock.history(start=start_date, end=end_date)[['Close']]
        df.reset_index(inplace=True)
        df['Ticker'] = ticker
        combined_data = pd.concat([combined_data, df])
    return combined_data

#  Public Data 
def fetch_public_data(url):
    response = requests.get(url)
    if response.status_code == 200:
        return pd.DataFrame(json.loads(response.text))
    else:
        print(f"Error fetching data from {url}")
        return pd.DataFrame()

# parameters
subreddits = ['CryptoCurrency', 'Bitcoin']
keyword = 'crypto'
tickers = ['BTC-USD', 'ETH-USD', 'AAPL', 'TSLA', 'GOOGL', 'MSFT']  
start_date = '2023-01-01'
end_date = '2025-01-01'
public_data_url = 'https://raw.githubusercontent.com/ugurcan-sevinc/Curr-JSON-Dataset/refs/heads/main/CurrencyJSONDataSet.json'  


reddit_data = fetch_reddit_data(subreddits, keyword)
finance_data = fetch_finance_data(tickers, start_date, end_date)
public_df = fetch_public_data(public_data_url)

# Data Cleaning
def clean_data(data):
    return [entry for entry in data if None not in entry.values()]

reddit_data = clean_data(reddit_data)
public_df.drop_duplicates(inplace=True)
public_df.dropna(inplace=True)
finance_data.drop_duplicates(inplace=True)
finance_data.dropna(inplace=True)


# Save  data to CSV using csv library
reddit_file_path = os.path.join('datasets', 'reddit_data.csv')
with open(reddit_file_path, mode='w', newline='', encoding='utf-8') as file:
    writer = csv.DictWriter(file, fieldnames=['title', 'text', 'author', 'date', 'upvotes', 'subreddit'])
    writer.writeheader()
    writer.writerows(reddit_data)

public_df.to_csv(os.path.join('datasets', 'public_data.csv'), index=False)
finance_data.to_csv(os.path.join('datasets', 'finance_data.csv'), index=False)

print("Data collection and processing complete! Files saved in 'datasets' folder.")
