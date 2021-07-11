import requests
from bs4 import BeautifulSoup
from joblib import Parallel, delayed
import yaml
import multiprocessing
from tqdm import tqdm
from datetime import datetime
import urllib.parse
from newspaper import Article
import nltk
nltk.download('punkt')
import difflib
import utils

def read_label_list(filename='labels.csv'):
    import csv
    labels = []
    with open(filename, 'r') as fd:
        reader = csv.reader(fd)
        for row in reader:
            labels.append(row[0])
    return labels

def get_keywords(url):
    try:
        if utils.get_content_type(url):
            return None
        article = Article(url)
        article.download()
        article.parse()
        article.nlp()
        return article.keywords
    except:
        return None


def get_text(url):
    try:
        if utils.get_content_type(url):
            return None
        article = Article(url)
        article.download()
        article.parse()
        article.nlp()
        return article.text
    except:
        return None



def get_text_parallel(url_list):
    labels = read_label_list()
    num_cores = multiprocessing.cpu_count()
    output = Parallel(n_jobs=num_cores)(delayed(get_text)(i) for i in tqdm(url_list))
    return output

def closest_label(keywords, labels, prob=0.8):
    if keywords is None or len(keywords) ==0:
        return None
    return difflib.get_close_matches(keywords, labels, 15, prob)

def closest_label_parallel(keywords_list):
    labels = read_label_list()
    num_cores = multiprocessing.cpu_count()
    output = Parallel(n_jobs=num_cores)(delayed(closest_label)(i,labels) for i in tqdm(keywords_list))
    return output
