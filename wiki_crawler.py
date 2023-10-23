import concurrent.futures as cf
import os
import pickle
import warnings
from collections import abc
import time

from web_scrape import ScrapePageRegex as ScrapePage

class WikiCrawler:
    
    def __init__(
        self, 
        starting_url:str="https://en.wikipedia.org/wiki/Wikipedia", 
        pickle_dir:str="wiki_pickle", 
        request_session = None,
        queue:list=[],
        log_results_after_n_nodes_scanned:int=100,
        num_workers:int=1
    ) -> None:
        
        if not os.path.exists(pickle_dir):
            os.mkdir(pickle_dir)
        
        # Pickle Settings 
        self.pickle_dir = pickle_dir
        self.pickle_limit = 1_000
        self.pickle_counter = 0
        
        # Web Scraping Object
        self.s = ScrapePage(request_session=request_session)
        
        # Storage
        self.id_counter = 0
        self.url_to_id = {}
        self.visited_urls = set()
        
        self.graph = {} # {id0: [id1, id2, id3]}
        self.nodes = {} # {id0: {"url": url, "data": data}}
        
        self.errors = {}
        
        # RAM Storage
        if not isinstance(queue, abc.MutableSequence):
            raise TypeError(f"queue must be a list. Got {type(queue)}")    
        self.queue = queue
        if not self.queue:
            self.queue.append(starting_url)
        else:
            # Check if queue is valid
            for url in self.queue.copy():
                if not isinstance(url, str):
                    raise TypeError(f"queue must be a list of strings. Got {type(url)}")
                if not url.startswith("https://en.wikipedia.org/wiki/"):
                    # Remove invalid url and warn
                    warnings.warn(f"Invalid URL in queue: {url}. Removing from queue.")
                    self.queue.remove(url)
        
        # Params
        self.starting_url = starting_url
        self.log_results_after_n_nodes_scanned = log_results_after_n_nodes_scanned
        self.num_workers = num_workers

        # To keep track of time
        self.t = time.time()
        

    def _crawl_page(self, url):
        """
        Crawl a single page and add to graph and nodes.

        Args:
            url (str): URL to crawl

        Returns:
            _id (int): ID of crawled page
        """
        if url in self.visited_urls:
            return

        txt = self.s.load_page(url)
        links = self.s.find_links(txt)

        if not links:
            return

        _id = self.id_counter
        self.id_counter += 1

        self.url_to_id[url] = _id
        self.visited_urls.add(url)

        self.nodes[_id] = {"url": url, "data": self.s.find_data_block(txt)}

        for link in links:
            if link not in self.url_to_id:
                self.url_to_id[link] = self.id_counter
                self.id_counter += 1

            self.graph[_id] = self.graph.get(_id, []) + [self.url_to_id[link]]

        self.queue.extend(links)

        return _id
    
    
    def pickle_progress(self):
        """
        Pickle graph and nodes.
        """
        # Pickle graph and nodes
        pickle.dump(self.graph, open(f"{self.pickle_dir}/graph_{self.pickle_counter}.pkl", "wb"))
        pickle.dump(self.nodes, open(f"{self.pickle_dir}/nodes_{self.pickle_counter}.pkl", "wb"))
        pickle.dump(self.errors, open(f"{self.pickle_dir}/errors_{self.pickle_counter}.pkl", "wb"))
        pickle.dump(self.queue, open(f"{self.pickle_dir}/queue_{self.pickle_counter}.pkl", "wb"))
        self.pickle_counter += 1
         
    
    def log_results(self):
        """
        Log results to console.
        
        Prints progress every 100 pages.
        
        Pickles progress every self.pickle_limit pages.
        """
        if len(self.nodes) % self.log_results_after_n_nodes_scanned == 0:
            print(f"Progress:\nPages in Queue (past and current) {self.id_counter}\nPages Scanned: {len(self.nodes)}\n===================================\nTime: {time.time() - self.t:.2f}s\n")
            self.t = time.time()
        if len(self.nodes) % self.pickle_limit == 0:
            self.pickle_progress()
        
            
            
    def crawl(self, max_pages=1000):
        """
        Crawl pages until max_pages is reached or queue has been emptied.
        
        Multiprocessing needs to be tested. Current equipment does not have a GPU.
        
        Args:
            max_pages (int): Maximum number of pages to crawl
            
        Returns:
            None
        """
        # Multiprocessing
        with cf.ProcessPoolExecutor(max_workers=self.num_workers) as executor:
            while len(self.nodes) < max_pages:
                if not self.queue:
                    print("Waiting for processes to finish...")
                    cf.wait(executor, timeout=None, return_when=cf.ALL_COMPLETED)
                    if len(self.queue) == 0:
                        break

                # Using multiprocessing within the thread
                with cf.ThreadPoolExecutor() as process_executor:
                    url = self.queue.pop(0)
                    future = process_executor.submit(self._crawl_page, url)
                    future.add_done_callback(lambda _: self.log_results())

        # Pickle final results
        self.pickle_progress()

        
    