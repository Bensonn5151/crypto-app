import requests
import json
import os

def fetch_coindesk_news():
    """Simple function to fetch CoinDesk news and save as JSON"""
    url = "https://data-api.coindesk.com/news/v1/article/list"
    params = {
        'lang': 'EN',
        'limit': 10
    }

    # Define destination folder and file path
    destination_folder = "/Users/apple/Desktop/DEV/PORTFOLIO/crypto-app"
    os.makedirs(destination_folder, exist_ok=True)  # ensure folder exists
    output_path = os.path.join(destination_folder, "coindesk_news_latest.json")
    
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        
        raw_data = response.json()
        
        # Create the formatted structure
        formatted_data = {
            "Data": raw_data.get('Data', []),
            "Err": {}
        }
        
        # Save to file in the specified folder
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(formatted_data, f, indent=2, ensure_ascii=False)
        
        print(f"✅ Successfully fetched {len(formatted_data['Data'])} articles")
        print(f"📁 Saved to: {output_path}")
        return formatted_data
        
    except Exception as e:
        error_data = {
            "Data": [],
            "Err": {"message": str(e)}
        }
        print(f"❌ Error: {e}")
        return error_data


# Run the function
if __name__ == "__main__":
    fetch_coindesk_news()
