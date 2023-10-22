from wiki_crawler import WikiCrawler
import time
import requests
# Create a crawler object

starting_url = "https://en.wikipedia.org/wiki/Wikipedia"
pickle_dir = "wiki_pickle"
num_pages_to_crawl = 1000

session = requests.Session()
wc = WikiCrawler(
    starting_url=starting_url,
    pickle_dir=pickle_dir, 
    num_workers=61, 
    request_session=session
)

# Crawl the page
t = time.time()
wc.crawl(num_pages_to_crawl)
print(f"Time to crawl {num_pages_to_crawl} pages: {time.time() - t:.2f}s")

session.close()