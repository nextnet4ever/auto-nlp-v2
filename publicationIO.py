"""
This file is for handling data i/o from the GCP bucket of importance for finding literature, etc.


Derek Park 11/3/2023

"""


# Imports

import tqdm
from google.cloud import storage
import sys
import os
import subprocess
import argparse
from nltk.tokenize import sent_tokenize

from typing import List


def authenticate_implicit_with_adc(project_id="your-google-cloud-project-id"):
    """
    When interacting with Google Cloud Client libraries, the library can auto-detect the
    credentials to use.

    // TODO(Developer):
    //  1. Before running this sample,
    //  set up ADC as described in https://cloud.google.com/docs/authentication/external/set-up-adc
    //  2. Replace the project variable.
    //  3. Make sure that the user account or service account that you are using
    //  has the required permissions. For this sample, you must have "storage.buckets.list".
    Args:
        project_id: The project id of your Google Cloud project.
    """

    # This snippet demonstrates how to list buckets.
    # *NOTE*: Replace the client created below with the client required for your application.
    # Note that the credentials are not specified when constructing the client.
    # Hence, the client library will look for credentials using ADC.
    storage_client = storage.Client(project=project_id)
    buckets = storage_client.list_buckets()
    print("Buckets:")
    for bucket in buckets:
        print(bucket.name)
    print("Listed all storage buckets.")

#authenticate_implicit_with_adc(project_id=os.environ.get("PROJECT_NAME"))

def get_file_text(projname: str, bname: str, floc:str):
    """
        Download the article and return its raw text.


    """
    print("Data values are: ")
    print("PROJECT_NAME ", projname)
    print("BUCKET_NAME ", bname)
    print("FILE_NAME ", floc)
    # Initialize a client
    storage_client =  storage.Client(project=projname)

    # The name of the bucket
    bucket_name = bname

    # The name of the file you want to read
    blob_name = floc

    # Create a bucket object
    bucket = storage_client.get_bucket(bucket_name)

    # Create a blob object
    blob = bucket.blob(blob_name)

    # Download the content of the file
    content = blob.download_as_text()

    return content


def clean_sentence(sentence: str):
    """
    Clean and preprocess the sentence.

    """
    clean_string = sentence.strip()
    clean_string = clean_string.replace("\n", " ")
    # Right now, just a placeholder

    return clean_string

def save_to_file(sentences: List[str], temp_fname: str):

    """
    Save the sentences to a text file with 1 sentence per line.

    """

    file_str = ""

    for s in sentences:

        clean_s = clean_sentence(s)

        file_str += clean_s

        file_str += "\n"

    with open(temp_fname, 'w') as f:

        f.write(file_str)

def extract_info(temp_fname: str):

    """
    Use OpenIE6 to extract information from each triplet. 
    
    Arguments:

    sentences -- A List of sentences that, in total, comprise an article. 
    """


    

    def call_other_environment(script_path, *args):
        other_env_python = "/opt/conda/envs/openie6/bin/python"
        result = subprocess.run([other_env_python, script_path, *args], capture_output=True, text=True)
        return result.stdout


    os.chdir('/root/nlp/openie6')
    #os.system(f'conda activate openie6')
    # os.system(f'conda deactivate')
    # os.system(f'conda activate openie6')
    #call_other_environment(run.py, )
    pyloc = os.environ.get("OPENIE6_PYTHON_LOC")
    os.system(f'{pyloc} run.py --save models/oie_model --mode splitpredict --model_str bert-base-cased --task oie --gpus 0 --inp /root/nlp/auto-nlp-v2/{temp_fname} --out /root/nlp/auto-nlp-v2/predictions.txt')
    os.chdir("..")
    # os.system(f'conda deactivate')
    # os.system(f'conda activate nlp')
    