"""
This file is for handling data i/o from the GCP bucket of importance for finding literature, etc.


Derek Park 11/3/2023

"""


# Imports

import tqdm
from google.cloud import storage
import sys
import os
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


def extract_info(sentences: List[str]):

    """
    Use OpenIE6 to extract information from each triplet. 
    
    Arguments:

    sentences -- A List of sentences that, in total, comprise an article. 
    """