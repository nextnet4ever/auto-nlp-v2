import os
from PyPDF2 import PdfReader
import PyPDF2
import requests
from bs4 import BeautifulSoup
from scholarly import scholarly
from scholarly import ProxyGenerator
import pandas as pd
import chunk
import boto3
import nltk
from collections import defaultdict
import numpy
from numpy import real
from REL_HELPERS import *
from io import StringIO # python3; python2: BytesIO 
import boto3
import json
import gzip
import multiprocessing
import nltk
from nltk.corpus import stopwords
import random
import string
import spacy
from celery import Celery
from celery.utils.log import get_task_logger
import traceback


def txt2sentences(txtfile_string):
    return None

def approve(i):
    words = i.split()
    for word in words:
        if word in ["Vol.","Journal","2019","2018","2020"]:
            return False
        if "www" in word:
            return False
        if ".com" in word:
            return False
        if ".edu" in word:
            return False
        if "License" in word:
            return False
        if "doi" in word or "ncbi" in word or "Vol." in word:
            return False
    if len(i) < 25:
        return False
    if i.count(",") > 6:
        return False
    if i.count(".") > 2:
        return False
    return True



# This method takes text and then returns it as a token of sentences.
# From the docs:
# 
# nltk.tokenize.sent_tokenize(text, language='english')
#
# Return a sentence-tokenized copy of text, using NLTK’s recommended 
# sentence tokenizer (currently PunktSentenceTokenizer for the 
# specified language).
#
#
# content = a string containing the text to be parsed
# from_path = a data field specific to pubmed publications that has a pointer to where the plaintext 
#  of that article is
def retrieve_sentences(content, dois, from_path = None):
    print("Retrieve_metadata called")

    # Get the sentences
    sent_text = nltk.sent_tokenize(content)

    buffer = ""
    text = defaultdict(list)
    
    for count, i in enumerate(sent_text):
        if approve(i):
            if ":" in i:
                sample = i.partition(":")[0]
                if len(sample) < 20:
                    buffer = sample
                    i = i.replace(buffer + ":", "")

            temp_context = []
            if count > 0:
                temp_context.append(sent_text[count-1])
            temp_context.append(i)
            if count < len(sent_text)-1:
                temp_context.append(sent_text[count+1])
            
            text["context:String"].append(" ".join(temp_context))
            text["sentence:String"].append(i)
            text["tag:String"].append(buffer)
            text["path:String"].append(from_path)  
    
    df = pd.DataFrame().from_dict(text)
    return df
