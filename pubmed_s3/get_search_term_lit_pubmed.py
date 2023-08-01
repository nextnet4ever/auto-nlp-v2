import pandas as pd
from Bio import Entrez
import boto3
import os
from tqdm import tqdm
import multiprocessing as mp
Entrez.email = "your_email@example.com"
 # Read in the aws credentials

with open('../aws_credentials.txt', 'r') as f:
    for line in f:
        if "aws_access_key_id" in line:
            aws_acc_key_id = line.split("=")[1]
        elif "aws_secret_access_key" in line:
            aws_sec_acc_key = line.split("=")[1]

s3 = boto3.resource('s3', 
        aws_access_key_id= aws_acc_key_id,
        aws_secret_access_key= aws_sec_acc_key
        )
s3client = boto3.client('s3', 
        aws_access_key_id= aws_acc_key_id,
        aws_secret_access_key= aws_sec_acc_key
        )
### search pubmed for pubmed ids
def search_pubmed(keywords):
    # Set the email for Entrez
    
    query = ' AND '.join(keywords)
    keywords.extend([query])#keywords.extend([query])
    keywords = [x for x in keywords if x is not None]
    # Initialize an empty list to store the IDs
    id_list = []
    
    # Loop through each keyword and retrieve a list of IDs
    for keyword in keywords:
        # Use the esearch function to retrieve the PubMed IDs
        handle = Entrez.esearch(db="pmc", term=keyword, retmax=800)# db can be pubmed or pmc or adjust retmax for more articles
        record = Entrez.read(handle)
        id_list.extend(record["IdList"])
        
    return id_list

# Define a function to retrieve the abstracts for a list of PMIDs
def get_abstract(pmids):
    try:
        # Join the PMIDs into a comma-separated string
        pmid_str = pmids#",".join(pmids)
        
        # Use the PubMed API to retrieve the abstracts for the PMIDs
        handle = Entrez.efetch(db="pmc", id=pmid_str, rettype="abstract", retmode="text")
        
        # Parse the abstracts from the API response
        abstract = handle.read().strip()#.split("\n\n")[3]
        
        # Create the output directory if it doesn't already exist
        if not os.path.exists("text"):
            os.makedirs("text")
        
        # Write the abstract to a file in the output directory
        with open(os.path.join("text", f"PMC{pmids}.txt"), "w") as f:
            f.write(abstract)
        
        return 1
    except Exception as e:
        #print(e)
        return 0
      
# define a function to download a single file from the S3 bucket
def download_file(file_name):
    try:
        # download the file from the S3 bucket
        s3client.download_file(bucket_name, prefix + file_name, os.path.join('text', file_name))
        #s3.get_object(Bucket=bucket_name, Key=prefix+file_name)
        print(f"File {file_name} downloaded from S3 bucket")
        return 1
    except Exception as e:
        # if the file is not available in the S3 bucket, print the error message
        #print(f"File {file_name} not available in S3 bucket. Error message: {e}")
        return 0

# set up if/check statement to get type of input so I dont have to do all this commenting
query = ['HIV', 'AGT103-T', 'CCR5', 'CXCR4', 'CD4', 't-cell']
#query = pd.read_csv('keywords_from_derek.txt').astype(str).values.tolist()
pubmed_ids = search_pubmed(query)
str_pubmed_ids = [str(pubmed_ids[i]) for i in range(len(pubmed_ids))]

print(str_pubmed_ids)


### code using data is for taking a list of ids and downloading, rather than using a list of keywords
#data = pd.read_csv('dereks_list.csv').astype(str).values.tolist()
# Convert list of lists to list of strings with commas as separators
#data = [','.join(row) for row in data]

# Convert list of strings to single string with newlines as separators
#pubmed_ids = data#','.join(data)

#str_pubmed_ids = [str(pubmed_ids[i]) for i in range(len(pubmed_ids))]
# define the directory name
dir_name = "text"

# create the directory if it doesn't exist
if not os.path.exists(dir_name):
    os.mkdir(dir_name)


bucket_name = 'pmc-oa-opendata'
prefix = 'oa_noncomm/txt/all/'

# define the list of file names to download
file_list = ['PMC'+pubmed_ids[i]+'.txt' for i in range(len(pubmed_ids))]

# initialize the count of available files to 0
available_count = 0

# create a multiprocessing Pool with 4 worker processes
with mp.Pool(processes=4) as pool:
    # map the download_file function to the file list and get a list of results
    
    results = list(tqdm(pool.imap(download_file, file_list), total=len(file_list)))
    # Close the multiprocessing pool
    pool.close()
    pool.join()

# count the number of available files
available_count = sum(results)
print(f"{available_count} files available for download")

if available_count < 100:
    available_count = 0
    with mp.Pool(processes=8) as pool:
        # map the download_file function to the file list and get a list of results
        results = list(tqdm(pool.imap(get_abstract, str_pubmed_ids[:25]), total=len(str_pubmed_ids[:25])))
        # Close the multiprocessing pool
        pool.close()
        pool.join()
        # count the number of available files
        available_count = sum(results)
        print(f"{available_count} abstracts available for download")
