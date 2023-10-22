import concurrent.futures as cf
import os
import pickle

from web_scrape import ScrapePageRegex as ScrapePage

class WikiCrawler:
    
    def __init__(
        self, 
        starting_url:str="https://en.wikipedia.org/wiki/Wikipedia", 
        pickle_dir:str="wiki_pickle", 
        request_session = None,
        log_results_after_n_nodes_scanned:int=100,
        num_workers:int=1
    ) -> None:
        
        if not os.path.exists(pickle_dir):
            os.mkdir(pickle_dir)
        
        # Pickle Settings 
        self.pickle_dir = pickle_dir
        self.pickle_limit = 100_000
        self.pickle_counter = 0
        
        # Web Scraping Object
        self.s = ScrapePage(request_session=request_session)
        
        # Storage
        self.id_counter = 0
        self.url_to_id = {}
        
        self.graph = {} # {id0: [id1, id2, id3]}
        self.nodes = {} # {id0: {"url": url, "data": data}}
        
        self.errors = {}
        
        # RAM Storage
        self.queue = []
        self.queue.append(starting_url)
        
        # Params
        self.starting_url = starting_url
        self.log_results_after_n_nodes_scanned = log_results_after_n_nodes_scanned
        self.num_workers = num_workers

        
    def _crawl_page(self, url):
        """
        Crawl a single page and add to graph and nodes.
        
        Args:
            url (str): URL to crawl
            
        Returns:
            _id (int): ID of crawled page
        """
        if url in self.url_to_id.keys() and self.url_to_id[url] in self.graph.keys():
            return
        
        txt = self.s.load_page(url)
        links = self.s.find_links(txt)
        
        if len(links) == 0:
            return
        
        _id = self.id_counter
        self.id_counter += 1
        
        self.url_to_id[url] = _id
        

        self.nodes[_id] = {"url": url, "data": self.s.find_data_block(txt)}
        
        for link in links:
            if not link in self.url_to_id:
            
                self.url_to_id[link] = self.id_counter
                self.id_counter += 1
            
            self.graph[_id] = self.graph.get(_id, []) + [self.url_to_id[link]]
                
            self.queue.append(link)
            
        return _id
    
    
    def pickle_progress(self):
        """
        Pickle graph and nodes.
        """
        # Pickle graph and nodes
        pickle.dump(self.graph, open(f"{self.pickle_dir}/graph_{self.pickle_counter}.pkl", "wb"))
        pickle.dump(self.nodes, open(f"{self.pickle_dir}/nodes_{self.pickle_counter}.pkl", "wb"))
        pickle.dump(self.errors, open(f"{self.pickle_dir}/errors_{self.pickle_counter}.pkl", "wb"))
        self.pickle_counter += 1
         
    
    def log_results(self):
        """
        Log results to console.
        
        Prints progress every 100 pages.
        
        Pickles progress every self.pickle_limit pages.
        """
        if len(self.nodes) % self.log_results_after_n_nodes_scanned == 0:
            print(f"Progress:\nPages in Queue (past and current) {self.id_counter}\nPages Scanned: {len(self.nodes)}\n===================================")
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
        with cf.ThreadPoolExecutor(max_workers=self.num_workers) as executor:
            while len(self.nodes) < max_pages:
                # If queue is empty, wait for all processes to finish and check again
                if len(self.queue) == 0:
                    print("Waiting for processes to finish...")
                    cf.wait(executor, timeout=None, return_when=cf.ALL_COMPLETED)
                    # If queue is still empty, break
                    if len(self.queue) == 0:
                        break
                    
                # If queue is not empty, pop first url and crawl
                url = self.queue.pop(0)
                try:
                    self._crawl_page(url)
                except Exception as e:
                    self.errors["url"] = e
                    continue
                self.log_results()
                
        # Pickle final results
        self.pickle_progress()

        
    