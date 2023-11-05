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


# Set up argParser for parsing names
argParser = argparse.ArgumentParser()
argParser.add_argument("-n", "--fname", help="Full path name for the text file of interest.")

args = argParser.parse_args()

# Step 1: Fetch file based on args
# Initialize a client
storage_client =  storage.Client(project="your-google-cloud-project-id")

# The name of the bucket
bucket_name = 'pubmed-ftp-clone'

# The name of the file you want to read
blob_name = 'PMC1043859.txt'

# Create a bucket object
bucket = storage_client.get_bucket(bucket_name)

# Create a blob object
blob = bucket.blob(blob_name)

# Download the content of the file
content = blob.download_as_text()

# Use nltk to split the text into sentences
sentences = sent_tokenize(content)

# Now `sentences` is a list where each element is a sentence from the file
for s in sentences:
    print(s)
    print("\n")

print(len(sentences))
# Step 2: Parse it into individual sentences.

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

#authenticate_implicit_with_adc(project_id="psyched-garage-393419")
#print(args.fname)