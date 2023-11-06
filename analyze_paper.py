"""
This is the overall master script to run that will take a file name, location, and
then download the file and operate on it. 


Derek Park 11/3/2023

"""


# Imports

import tqdm
from google.cloud import storage
import sys
import os
import argparse
from nltk.tokenize import sent_tokenize
import publicationIO

# Environment variables that should be passed to the container via Kubernetes
BUCKET_NAME = os.environ.get("BUCKET_NAME")
TXT_FILE_PATH = os.environ.get("TXT_FILE_PATH")
PROJECT_NAME = os.environ.get("PROJECT_NAME")


# Step 1: Fetch file based on args
article_content = publicationIO.get_file_text(PROJECT_NAME, BUCKET_NAME, TXT_FILE_PATH)

type(article_content)


# # Use nltk to split the text into sentences
# sentences = sent_tokenize(content)

# # Now `sentences` is a list where each element is a sentence from the file
# for s in sentences:
#     print(s)
#     print("\n")

# print(len(sentences))
# Step 2: Parse it into individual sentences.



#authenticate_implicit_with_adc(project_id="psyched-garage-393419")
#print(args.fname)