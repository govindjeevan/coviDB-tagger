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
        response = requests.get(url)
        soup = BeautifulSoup(response.content, 'html.parser')
        
        if parsed_url.netloc == "www.nejm.org":
            categories = soup.find_all("p", {"class": "m-article-header__type"})
            if len(categories)>0:
                return ",".join(categories[0].a["href"].split("/"));
            return url_classifier(url)
            
        elif parsed_url.netloc == "jamanetwork.com":
            categories = soup.find_all("div", {"class": "meta-article-type thm-col"})
            if len(categories)>0:
                return ",".join(categories[0].a["href"].split("/"));
            return url_classifier(url)
            
            
        elif parsed_url.netloc == "www.nature.com":
            categories = soup.find_all("span", {"class": "c-article-identifiers__type"})
            if len(categories)==0:
                categories = soup.find_all("li", {"class": "c-article-identifiers__item"})
            if len(categories)>0:
                return categories[0].text
            return url_classifier(url)

            
            
        elif "sciencemag" in parsed_url.netloc:
            categories = soup.find_all("span", {"class": "overline__section"})
            if len(categories)>0:
                return categories[0].text
            return url_classifier(url)


        elif "www.pfizer.com" in parsed_url.netloc:
            tag = url.split("/")[3]
            return tag
        
        elif "modernatx.com" in parsed_url.netloc:
            
            if "investors" in url:
                category = url.split("/")[3]
            elif "trial" in parsed_url.netloc:
                category = "trial"
            elif "blog" in url:
                category = "blog"
            else:
                category = url_classifier(url)
                
            return category
        
        
    except:
        return url_classifier(url)

    

def get_category_parallel(url_list):
    num_cores = multiprocessing.cpu_count()
    output = Parallel(n_jobs=num_cores)(delayed(get_category)(i) for i in tqdm(url_list))
    return output


def url_classifier(url):
    if "pdf" in url:
        return "pdf"
    if "event" in url:
        return "event"
    header = requests.head(url).headers
    if "pdf" in header.get('content-type'):
        return "pdf"
    else:
        return "none"

def get_l2_tag(text):
    return ""
    





