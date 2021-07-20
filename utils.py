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
    
def get_host(url):
    parsed_url = urllib.parse.urlparse(url)
    return parsed_url.netloc.replace("www.", "")
def get_category(url):
    parsed_url = urllib.parse.urlparse(url)
    try:
        
        response = requests.get(url)
        soup = BeautifulSoup(response.content, 'html.parser')
        
        if parsed_url.netloc == "www.nejm.org":
            categories = soup.find_all("p", {"class": "m-article-header__type"})
            if len(categories)>0:
                category = ",".join(categories[0].a["href"].split("/"));
                if category[0]==',':
                    category=category[1:]
                return format_category(category)
            return url_classifier(url)
            
        elif parsed_url.netloc == "jamanetwork.com":
            categories = soup.find_all("div", {"class": "meta-article-type thm-col"})
            if len(categories)>0:
                return format_category(",".join(categories[0].a["href"].split("/")))
            return url_classifier(url)
            
            
        elif parsed_url.netloc == "www.nature.com":
            categories = soup.find_all("span", {"class": "c-article-identifiers__type"})
            if len(categories)==0:
                categories = soup.find_all("li", {"class": "c-article-identifiers__item"})
            if len(categories)>0:
                return format_category(categories[0].text)
            return url_classifier(url)

            
            
        elif "sciencemag" in parsed_url.netloc:
            categories = soup.find_all("span", {"class": "overline__section"})
            if len(categories)>0:
                return format_category(categories[0].text)
            return url_classifier(url)


        elif "www.pfizer.com" in parsed_url.netloc:
            tag = url.split("/")[3]
            return format_category(tag)
        
        elif "modernatx.com" in parsed_url.netloc:
            if "investors" in url:
                category = url.split("/")[3]
            elif "trial" in parsed_url.netloc:
                category = "trial"
            elif "blog" in url:
                category = "blog"
            else:
                category = url_classifier(url)
            return format_category(category)
        
        elif "jnj.com" in url:
            categories = soup.find_all("div", {"class": "PressReleasePage-slug"})
            if len(categories) > 0:
                category = categories[0].text
            else:
                category = url.split("/")[3]
            return format_category(category)

        elif "bmj.com" in url:
            categories = soup.find_all("span", {"class": "highwire-cite-article-type"})
            if len(categories) > 0:
                category = categories[0].text
            else:
                category = url_classifier(url)
            return format_category(category)
    
        elif "technologyreview.com" in url:
            categories = soup.find_all("div", {"class": "eyebrow__eyebrow--3MrG8 eyebrow__fullStoryEyebrow--AxoJe"})
            if len(categories) == 0:
                categories = soup.find_all("a", {"class": "eyebrow__eyebrow--3MrG8 eyebrow__fullStoryEyebrow--AxoJe"})
            if len(categories) > 0:
                category = categories[0].text
            elif response.url:
                category = url_classifier(response.url)
            else:
                category = url_classifier(url)
            return format_category(category)
        
        elif "globalhealth.stanford.edu" in url:
            category = url_classifier(url)
            if category=="default" and "wp" not in url:
                category = url.split("/")[3]
            return format_category(category)
        
        elif "covidadvisories.iisc.ac.in" in url:
            category = url_classifier(url)
            return format_category(category)

        elif "scienceexchange.caltech.edu" in url:
            categories = soup.find_all("div", {"class": "sic-header__page-info__breadcrumbs"})
            if len(categories) > 0 and len(list(categories[0].children))>7:
                category = list(categories[0].children)[7].text
            elif len(categories) > 0 and len(list(categories[0].children))>6:
                category = list(categories[0].children)[-1].string.replace("\n ", "").replace("\n", "").replace("  ", "").replace("\xa0/\xa0", "")
            else:
                category = url_classifier(url)
            return format_category(category)
        
        elif "together.caltech.edu" in url:
            category = url.split("/")[3]
            if len(category) ==0:
                category = url_classifier(url)
            return format_category(category)

        elif "hopkinsmedicine.org" in url:
            if "conditions-and-diseases" in url:
                category = "conditions-and-diseases"
            elif "news-releases" in url:
                category =  "news-releases"
            else:
                category = url_classifier(url)
            return format_category(category)
        
        
    except Exception as e:
        print("Error Fetching Category for URL: "+ url)
        print(e)
        return e
        #return url_classifier(url)

    

def get_category_parallel(url_list):
    num_cores = multiprocessing.cpu_count()
    output = Parallel(n_jobs=num_cores)(delayed(get_category)(i) for i in tqdm(url_list))
    return output


def format_category(x):
    return "-".join(x.lower().split(" "))


def get_keywords_parallel(url_list):
    num_cores = multiprocessing.cpu_count()
    output = Parallel(n_jobs=num_cores)(delayed(get_keywords)(i) for i in tqdm(url_list))
    return output



def url_classifier(url):
    if "pdf" in url:
        return "pdf"
    if "jpeg" in url or "png" in url:
        return "image"
    c_type = get_content_type(url)
    if c_type:
        return c_type
    if "event" in url:
        return "event"
    else:
        return "default"

def get_content_type(url):
    header = requests.head(url).headers
    if "pdf" in header.get('content-type'):
        return "pdf"
    elif "image" in header.get('content-type'):
        return "image"
    else:
        return False

def find_tag(mappings, source, category):
    try:
        if category is None:
            return ""
        src = ""
        for key in mappings.keys():
            if source in key:
                src = key
        if len(src)==0:
            return ""
        if type(mappings[src])==type(''):
            return mappings[src]
        categories = category.split(",")
        if len(categories)==1:
            if category in mappings[src]:
                return mappings[src][category]
            elif "default" in mappings[src]:
                mappings[src]["default"]
            else:
                return ""
        else:
            return mappings[src][categories[0]][categories[1]]
    except:
        print(f'Error Processing {category} for {source}')
        return ""

def update_coverage_readme(coverage):
    with open("README.md", "r+") as f:
        d = f.readlines()
        f.seek(0)
        for i in d:
            if "Articles Tagged" in i:
                f.write(f'![Articles Tagged](https://img.shields.io/badge/coverage-{coverage}%25-yellowgreen)')
            else:
                f.write(i)
        f.truncate()