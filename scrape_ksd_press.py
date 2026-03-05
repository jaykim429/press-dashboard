import urllib.request
import json
import time

def scrape_ksd_press_releases(max_pages=3):
    base_url = "https://ksd.or.kr/ko/api/content"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
        "Accept": "application/json"
    }
    
    all_news = []
    
    for page in range(1, max_pages + 1):
        print(f"Scraping Page {page}...")
        
        # Parameters for pagination
        # menuId=KR_ABT_070200 is the specific menu ID for press releases
        # pagingYn=Y tells the API we want paginated results
        # currentPage changes the page
        url = f"{base_url}?menuId=KR_ABT_070200&pagingYn=Y&currentPage={page}&recordCountPerPage=10"
        
        try:
            req = urllib.request.Request(url, headers=headers)
            response = urllib.request.urlopen(req)
            data = json.loads(response.read().decode('utf-8'))
            
            if 'body' in data and 'list' in data['body']:
                items = data['body']['list']
                
                if not items:
                    print(f"No more items found on page {page}. Stopping.")
                    break
                    
                for item in items:
                    title = item.get('bbsSj', 'No Title')
                    date_raw = str(item.get('frstRegistPnttm', ''))
                    # Format date roughly YYYY-MM-DD
                    date_formatted = f"{date_raw[:4]}-{date_raw[4:6]}-{date_raw[6:8]}" if len(date_raw) >= 8 else date_raw
                    
                    ntt_id = item.get('nttId', '')
                    # The viewing URL on the frontend uses this ID
                    link = f"https://ksd.or.kr/ko/about-ksd/ksd-news/press-release/{ntt_id}"
                    
                    all_news.append({
                        'title': title,
                        'date': date_formatted,
                        'link': link
                    })
            else:
                print(f"Unexpected JSON structure on page {page}.")
                break
                
        except Exception as e:
            print(f"Error fetching page {page}: {e}")
            break
            
        # Be polite to the server
        time.sleep(1)
        
    return all_news

if __name__ == "__main__":
    print("Starting KSD Press Release Scraper...")
    # Let's scrape the first 3 pages as a test
    results = scrape_ksd_press_releases(max_pages=3)
    
    print(f"\n--- Total Collected: {len(results)} ---")
    for i, news in enumerate(results, 1):
        print(f"{i}. [{news['date']}] {news['title']}")
        print(f"   Link: {news['link']}\n")
