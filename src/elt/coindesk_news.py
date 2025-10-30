import requests
import json

def fetch_coindesk_news():
    """Simple function to fetch CoinDesk news and save as JSON"""
    url = "https://data-api.coindesk.com/news/v1/article/list"
    params = {
        'lang': 'EN',
        'limit': 10
    }
    
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        
        raw_data = response.json()
        
        # Create the formatted structure
        formatted_data = {
            "Data": raw_data.get('Data', []),
            "Err": {}
        }
        
        # Save to file
        with open('coindesk_news_latest.json', 'w', encoding='utf-8') as f:
            json.dump(formatted_data, f, indent=2, ensure_ascii=False)
        
        print(f"Successfully fetched {len(formatted_data['Data'])} articles")
        return formatted_data
        
    except Exception as e:
        error_data = {
            "Data": [],
            "Err": {"message": str(e)}
        }
        return error_data

# Run the simple version
if __name__ == "__main__":
    fetch_coindesk_news()