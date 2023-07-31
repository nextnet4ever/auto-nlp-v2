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
from sentenceMaker import *



client = boto3.client('s3',
        aws_access_key_id='AKIARLSKRVQP433MXUZG',
        aws_secret_access_key='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'
        )

s3 = boto3.resource('s3', 
        aws_access_key_id='AKIARLSKRVQP433MXUZG',
        aws_secret_access_key='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'
        )
bucket = s3.Bucket('semantic-scholar-open-research-corpus-unzipped') #CHANGE THIS TOO

nlp = spacy.load("en_core_sci_lg")
nlp2 = spacy.load("en_core_web_sm")




def find_key_words(dictionary):
    doc = nlp(dictionary["sentence:String"])
    entities = doc.ents
    real_arg1 = dictionary["arg1:String"].lower()
    real_arg2 = dictionary["arg2:String"].lower()
    FLAG_first = False
    FLAG_second = False
    for ent in entities:
        if ent.text in dictionary["arg1:String"]: # overwrites because last search words are better
            real_arg1 = ent.text
            FLAG_first = True
        if ent.text in dictionary["arg2:String"]:
            real_arg2 = ent.text
            FLAG_second = True
    doc = nlp2(dictionary["sentence:String"])
    for chunk in doc.noun_chunks:
        if FLAG_first == False:
            if chunk.root.text in dictionary["arg1:String"]:
                real_arg1 = nlp2(chunk.root.text)[0].lemma_
        
        if FLAG_second == False:
            if chunk.root.text in dictionary["arg2:String"]:
                real_arg2 = nlp2(chunk.root.text)[0].lemma_
    try:
        if real_arg1[0] == dictionary["sentence:String"][0] and str.islower(real_arg1[1]):
            real_arg1 = real_arg1.lower()
    except:
        pass
    if real_arg1 == real_arg2 \
        or real_arg1.lower() in stopwords.words('english') \
        or real_arg2.lower() in stopwords.words('english'):
        return False, None, None
    else:
        return True, real_arg1, real_arg2

def return_random_string(N=20):
    return ''.join(random.SystemRandom().choice(string.ascii_uppercase + string.digits) for _ in range(N))


# This formats data into data frames necessary for functioning
def URANUS_redone(results):
    df_nodes = {"~id":[],"name:String":[]}
    df_edges = pd.DataFrame()

    for i,row in results.iterrows():
        FLAG, real_arg1, real_arg2 = find_key_words(row)
        if FLAG:

            df_nodes["~id"].append("LIT=" + str(real_arg1))
            df_nodes["name:String"].append(str(real_arg1))

            df_nodes["~id"].append("LIT=" + str(real_arg2))
            df_nodes["name:String"].append(str(real_arg2))

            temp_row = row
            temp_row["~from"] = "LIT=" + str(real_arg1)
            temp_row["~to"] = "LIT=" + str(real_arg2)
            temp_row["~id"] = return_random_string() + "==" + "LIT=" + str(real_arg1) + "_TO_" + "LIT=" + str(real_arg2)

            df_edges = df_edges.append(temp_row, ignore_index=True)
    
    nodes_all = pd.DataFrame().from_dict(df_nodes)
    return nodes_all, df_edges


# os code to activate the processing of the literature-- will need boto for upload and such
# run worker celery code here to process the nodes and edges, then boto upload to s3, then neptune


##### celery worker code #####


input_dir = '/home/derek/auto-nlp/auto-nlp-drive/text'
output_dir = '/home/derek/auto-nlp/auto-nlp-drive/processed'

if not os.path.exists(output_dir):
        os.makedirs(output_dir)

df_list = []
#directory_path='/home/ubuntu/lit_data'

directory_path = "/home/derek/auto-nlp/auto-nlp-drive"
# Loop through each sub-directory
for subdir in os.listdir(directory_path):
    # Check if the sub-directory is a directory
    if os.path.isdir(os.path.join(directory_path, subdir)):
        # Check if a "result.csv" file exists in the sub-directory
        result_path = os.path.join(directory_path, subdir, 'result.csv')
        if os.path.exists(result_path):
            # Read the contents of the "result.csv" file into a pandas DataFrame
            result_df = pd.read_csv(result_path)
            # Add the DataFrame to the list
            df_list.append(result_df)

# Concatenate all DataFrames in the list into a single DataFrame
# Specifically, these are data frames relatedt o
dois = pd.concat(df_list, ignore_index=True)
#dois = pd.read_csv('dois.csv', index_col='Name')
dois= dois.set_index('Name')
dois = dois[~dois.index.duplicated(keep='first')]


# Now we are taking in from input_dir which is
# the directory that has the scientific articles to be 
# processed
pd.set_option('display.max_columns', 999)
for filename in os.listdir(input_dir)[:1]:
    total_nodes = pd.DataFrame()
    total_edges = pd.DataFrame()
    try:
        filepath = os.path.join(input_dir, filename)
        print(f"Filepath is: {filepath}")
        
        #print(type(dois.loc[filename[:-4]]['DOI']))
        with open(filepath, 'r') as file:
            data_lines = ''.join([line.strip() for line in file.readlines()])
            #print("readlines finished")

        # print("DOI cols are: ")
        # print(dois.head())
        # print("filename is: ")
        # print(filename)
        article_fname = filename[:-4] # Minus the '.txt' at the end of a file, for example.
        doi = dois.loc[dois['PDF Name'] == article_fname]['DOI']

        # From the data lines, get the sentences
        df, sentence_fname = retrieve_sentences(str(data_lines), doi)

        # Here is the step for calling the extraction functions
        results = get_extractions(df)
        
        # This is to format the results
        temp_node_csv, temp_edge_csv = URANUS_redone(results)
        temp_node_csv["~label"] = "Scientific Literature"
        temp_edge_csv["~label"] = "Scientific Literature"

        total_nodes = total_nodes.append(temp_node_csv, ignore_index=True)
        total_edges = total_edges.append(temp_edge_csv, ignore_index=True)
        print(total_nodes.shape)
        print(total_edges.shape)
        #data_lines.close()
        

        if total_edges.shape[0] > 0: #remove after testing
            # specify the file path and file name
            #file_path_nodes = output_dir+'/df_nodes_{filename}.csv'
            #file_path_edges = output_dir+'/df_edges_{filename}.csv'

            # save the DataFrame to a CSV file
            #total_nodes.to_csv(file_path_nodes, index=False)
            #total_edges.to_csv(file_path_edges, index=False)
            # Save processed data to file in output_dir
            output_filename = f"{filename[:-4]}_processed.csv" # strip ".txt" and add "_processed.csv"
            output_filepath_nodes = os.path.join(output_dir, f"df_nodes_{output_filename}")
            output_filepath_edges = os.path.join(output_dir, f"df_edges_{output_filename}")
            total_nodes.to_csv(output_filepath_nodes, index=False)
            total_edges.to_csv(output_filepath_edges, index=False)
    except Exception as e:
        print(f"Error processing {filename}: {e}")
        tb_str = traceback.format_exc()
        print(tb_str)
