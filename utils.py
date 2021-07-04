import requests
from bs4 import BeautifulSoup
from joblib import Parallel, delayed
import yaml
import multiprocessing
from tqdm import tqdm
from datetime import datetime
import urllib.parse

def load_mappings(file = "mapping.yaml"):
    with open(file, 'r') as stream:
        try:
            mappings = yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            print(exc)
        return mappings
    
def get_category(url):
    parsed_url = urllib.parse.urlparse(url)
    try:
        if parsed_url.netloc == "www.nejm.org":
            response = requests.get(url)
            soup = BeautifulSoup(response.content, 'html.parser')
            categories = soup.find_all("p", {"class": "m-article-header__type"})
            if len(categories)>0:
                return ",".join(categories[0].a["href"].split("/"));
            elif "pdf" in url:
                return "pdf"
            else:
                return "none"
            
        elif parsed_url.netloc == "jamanetwork.com":
            response = requests.get(url)
            soup = BeautifulSoup(response.content, 'html.parser')
            categories = soup.find_all("div", {"class": "meta-article-type thm-col"})
            if len(categories)>0:
                return ",".join(categories[0].a["href"].split("/"));
            elif "pdf" in url:
                return "pdf"
            else:
                return "none"
            
            
        elif parsed_url.netloc == "www.nature.com":
            response = requests.get(url)
            soup = BeautifulSoup(response.content, 'html.parser')
            categories = soup.find_all("span", {"class": "c-article-identifiers__type"})
            if len(categories)==0:
                categories = soup.find_all("li", {"class": "c-article-identifiers__item"})
            if len(categories)>0:
                return categories[0].text
            else:
                return "none"
            
            
        elif "sciencemag" in parsed_url.netloc:
            response = requests.get(url)
            soup = BeautifulSoup(response.content, 'html.parser')
            categories = soup.find_all("span", {"class": "overline__section"})
            if len(categories)>0:
                return categories[0].text
            else:
                return "none"

        elif "www.pfizer.com" in parsed_url.netloc:
            tag = url.split("/")[3]
            return tag
        
        
    except:
        if "pdf" in url:
            return "pdf"
    

def get_category_parallel(url_list):
    num_cores = multiprocessing.cpu_count()
    output = Parallel(n_jobs=num_cores)(delayed(get_category)(i) for i in tqdm(url_list))
    return output











