import concurrent.futures as cf
import os
import pickle
import warnings
from collections import abc
import time

from web_scrape import ScrapePageRegex as ScrapePage


class ArhivedWikiCrawler:
    
    def __init__(
        self,
        pickle_dir:str="wiki_pickle",
        error_pkl_name:str="errors_0.pkl",
        graph_pkl_name:str="graph_0.pkl",
        nodes_pkl_name:str="nodes_0.pkl",
        queue_pkl_name:str="queue_0.pkl",
    ):
        
        if not os.path.exists(pickle_dir):
            raise FileNotFoundError(f"{pickle_dir} does not exist.")
        
        self.errors = self.open_and_load_pkl(f"{pickle_dir}/{error_pkl_name}")
        self.check_valid_errors(self.errors)
        
        self.graph = self.open_and_load_pkl(f"{pickle_dir}/{graph_pkl_name}")
        self.check_valid_graph(self.graph)
        
        self.nodes = self.open_and_load_pkl(f"{pickle_dir}/{nodes_pkl_name}")
        self.check_valid_nodes(self.nodes)
        
        self.queue = self.open_and_load_pkl(f"{pickle_dir}/{queue_pkl_name}")
        self.check_valid_queue(self.queue)
        
        
        
    def open_and_load_pkl(self, path):
        """
        Open pickle directory. Raise error if path does not exist.
        """
        if not os.path.exists(f"{path}"):
            raise FileNotFoundError(f"{path} does not exist.")
        
        with open(f"{path}", "rb") as f:
            try:
                file = pickle.load(f)
            except Exception as E:
                raise Exception(f"Error loading {path}. {E}")
        
        return file
    
    
    def check_valid_graph(self):
        # Check graph is valid: Dict[str,List]
        if not isinstance(self.graph, dict):
            raise TypeError(f"graph must be a dict. Got {type(self.graph)}")
        for k,v in self.graph.items():
            if not isinstance(k, str):
                raise TypeError(f"graph keys must be strings. Got {type(k)}")
            if not isinstance(v, list):
                raise TypeError(f"graph values must be lists. Got {type(v)}")
            
    
    def check_valid_nodes(self):
        # Check nodes is valid: Dict[str,str] (url or json)
        if not isinstance(self.nodes, dict):
            raise TypeError(f"nodes must be a dict. Got {type(self.nodes)}")
        for k,v in self.nodes.items():
            if not isinstance(k, str):
                raise TypeError(f"nodes keys must be strings. Got {type(k)}")
            if not isinstance(v, str):
                raise TypeError(f"nodes values must be sre (url or json). Got {type(v)}")
            
            
    def check_valid_errors(self):
        # Check errors is valid: Dict[str,str] (url or json)
        if not isinstance(self.errors, dict):
            raise TypeError(f"errors must be a dict. Got {type(self.errors)}")
        for k,v in self.errors.items():
            if not isinstance(k, str):
                raise TypeError(f"errors keys must be strings. Got {type(k)}")
            if not isinstance(v, str):
                raise TypeError(f"errors values must be strings. Got {type(v)}")
            
            
    def check_valid_queue(self):
        if not isinstance(self.queue, abc.MutableSequence):
            raise TypeError(f"queue must be a list. Got {type(self.queue)}")
        for url in self.queue.copy():
            if not isinstance(url, str):
                raise TypeError(f"queue must be a list of strings. Got {type(url)}")
            if not url.startswith("https://en.wikipedia.org/wiki/"):
                # Remove invalid url and warn
                warnings.warn(f"Invalid URL in queue: {url}. Removing from queue.")
                self.queue.remove(url)
    
        

class WikiCrawler:
    
    def __init__(
        self, 
        starting_url:str="https://en.wikipedia.org/wiki/Wikipedia", 
        pickle_dir:str="wiki_pickle", 
        request_session = None,
        queue:list=[],
        log_results_after_n_nodes_scanned:int=100,
        num_threads:int=None,
        num_processes:int=None,
        archived_crawler:ArhivedWikiCrawler=None,
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
        
        if archived_crawler is None:
            self.graph = {} # {id0: [id1, id2, id3]}
            self.nodes = {} # {id0: {"url": url, "data": data}}
            self.errors = {}
        else:
            self.graph = archived_crawler.graph
            self.nodes = archived_crawler.nodes
            self.errors = archived_crawler.errors
            queue = archived_crawler.queue
        
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
        
        if not (num_threads is None or isinstance(num_threads, int)):
            raise TypeError(f"num_threads must be an int or None. Got {type(num_threads)}")
        
        if not (num_processes is None or isinstance(num_processes, int)):
            raise TypeError(f"num_processes must be an int or None. Got {type(num_processes)}")
        
        self.num_threads = num_threads
        self.num_processes = num_processes
        
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
        with cf.ProcessPoolExecutor(max_workers=self.num_processes) as executor:
            while len(self.nodes) < max_pages:
                if not self.queue:
                    print("Waiting for processes to finish...")
                    cf.wait(executor, timeout=None, return_when=cf.ALL_COMPLETED)
                    if len(self.queue) == 0:
                        break

                # Using multiprocessing within the thread
                with cf.ThreadPoolExecutor(max_workers=self.num_threads) as process_executor:
                    url = self.queue.pop(0)
                    future = process_executor.submit(self._crawl_page, url)
                    future.add_done_callback(lambda _: self.log_results())

        # Pickle final results
        self.pickle_progress()