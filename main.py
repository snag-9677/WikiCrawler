from wiki_crawler import (
    WikiCrawler, 
    ArchivedWikiCrawler
)
import time
import requests
import sys
# Create a crawler object

starting_url = "https://en.wikipedia.org/wiki/Wikipedia"
pickle_dir = "wiki_pickle/arhive_crawler"
archive = None

# If arg passed, use that as the number of pages to crawl. Default to 1000
if len(sys.argv) > 1:
    num_pages_to_crawl = int(sys.argv[1])
else:
    num_pages_to_crawl = 2000

# archive = ArchivedWikiCrawler(
#     pickle_file=f"{pickle_dir}/1.pkl"
# )

session = requests.Session()
wc = WikiCrawler(
    starting_url=starting_url,
    pickle_dir=pickle_dir,
    request_session=session,
    archived_crawler=archive,
)

# Crawl the page
t = time.time()
wc.crawl(num_pages_to_crawl)
print(f"Time to crawl {num_pages_to_crawl} pages: {time.time() - t:.2f}s")

session.close()