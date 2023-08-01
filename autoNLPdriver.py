#
# This is a script designed to take a series of search terms, given as command line arguments, which then are used to create nodes and edges related to the scientific concepts found in the papers.
#
# 
# Derek Park 7/31/2023

# Imports
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
#from lit_data.REL_HELPERS import *
#from lit_data.sentenceMaker import *

import argparse


# Step 1, identify the search terms.
parser = argparse.ArgumentParser(
                    prog='Auto NLP Driver',
                    description='What the program does',
                    epilog='Text at the bottom of help')
parser.add_argument('search_terms', nargs='+')

terms = parser.parse_args().search_terms


# Step 2, look up those terms to find the papers we need to get.


# Step 2A:
# First, check the PMC bucket
 

# Second, check use PyPaperBot.  