import requests

import re

class ScrapePageRegex:
    
    def __init__(self, request_session=None):
        
        self.self_started_session = False
        self.session = request_session
        
        if self.session is None:
            self.session = requests.Session()
            self.self_started_session = True
    
    def load_page(self, page_link:str):
        """Load the page
        
        Parameters
        ----------
        page_link: str
            The page link
            
        Returns
        -------
        str
            The page text
        """
        # return requests.get(page_link).text
        txt = self.session.get(page_link).text
        if self.self_started_session:
            self.session.close()
        return txt
        
    
    
    def find_in_html(self, regex_pattern:str, page:str):
        """
        Find the regex in the page
        
        Parameters
        ----------
        regex: str
            The regex
        page: str
            The page
        """
        regex = re.compile(regex_pattern, re.DOTALL)
        return regex.findall(page)
        
    
    
    def find_data_block(self, page:str):
        """Find the content
        
        Parameters
        ----------
        page: str
            The page
            
        Returns
        -------
        data block: str
            The content
        """
        regex = r"<script type=\"application/ld\+json\">(.*?)</script>"
        return self.find_in_html(regex, page)[0]
    
    
    def find_links(self, page:str):
        """Find the links
        
        Parameters
        ----------
        page: str
            The page
            
        Returns
        -------
        links: list
            The links
        """
        # Find all links which start with wiki/
        regex = r'href="/wiki/([^:/"]+)"'
        l = self.find_in_html(regex, page)
        constructed_links = [f"https://en.wikipedia.org/wiki/{link}" for link in set(l)]
        return constructed_links
    
    
# SAMPLE
    
# _t = time.time()
# s = ScrapePageRegex()

# t = time.time()
# txt = s.load_page(starting_url)
# print(f"Time to load page: {time.time() - t:.2f}s")

# t = time.time()
# data_block = s.find_data_block(txt)
# print(f"Time to find data block: {time.time() - t:.2f}s")

# t = time.time()
# links = s.find_links(txt)
# print(f"Time to find links: {time.time() - t:.2f}s")

# print(f"Total time: {time.time() - _t:.2f}s")