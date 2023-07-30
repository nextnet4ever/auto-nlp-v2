from functools import partial
import pandas as pd
import sys
import warnings
from tqdm import tqdm
import re
import threading
from func_timeout import func_timeout, FunctionTimedOut
import traceback
import os
import json

#from pyopenie import OpenIE5
#extractor = OpenIE5('http://localhost:9000')
#extractors = [OpenIE5('http://localhost:8000'), OpenIE5('http://localhost:9000')]

import nltk.data
tokenizer = nltk.data.load('tokenizers/punkt/english.pickle')

import ctypes


total_arash = []
total_rows = []


# In the original implementation using OpenIE5, this was what is returned by HELPER_to_extract:


''' "The result is a JSON list of extractions with confidence, offset and other properties."

  extractions
[
    {
        "confidence": 0.38089450366724514,
        "sentence": "The U.S. president Barack Obama gave his speech on Tuesday to thousands of people.",
        "extraction": {
            "arg1": {
                "text": "Barack Obama",
                "offsets": [...]
            },
            "rel": {
                "text": "[is] president [of]",
                "offsets": [...]
            },
            "arg2s": [
                {
                    "text": "United States",
                    "offsets": [...]
                }
            ],
            "context": null,
            "negated": false,
            "passive": false
        }
    },
    {
        "confidence": 0.9168198459177435,
        "sentence": "The U.S. president Barack Obama gave his speech on Tuesday to thousands of people.",
        "extraction": {
            "arg1": {
                "text": "The U.S. president Barack Obama",
                "offsets": [...]
            },
            "rel": {
                "text": "gave",
                "offsets": [...]
            },
            "arg2s": [
                {
                    "text": "his speech",
                    "offsets": [...]
                },
                {
                    "text": "on Tuesday",
                    "offsets": [...]
                },
                {
                    "text": "to thousands of people",
                    "offsets": [...]
                }
            ],
            "context": null,
            "negated": false,
            "passive": false
        }
    }
]'''




# Original implementation using open-ie standalone package. Which was dead.

# def HELPER_to_extract(sent, row, extractor):# = extractors):# None):
#     # global total_arash
#     # global total_rows
#     total_rows = []
#     arash = [] # naming one var arash so the legacy lives on...
#     try:
#         results = extractor.extract(sent)
#         for res in results:
#             for arg2 in res["extraction"]["arg2s"]:
#                 arash.append([res["extraction"]["arg1"]["text"], \
#                     res["extraction"]["rel"]["text"], \
#                         arg2["text"], res["confidence"], res["extraction"]["context"], \
#                             res["extraction"]["negated"], res["extraction"]["passive"]])
#     except:
#         return []

#     # #with lock:
#     #     total_arash.extend(arash)
#     for i in range(len(arash)):
#         total_rows.append(row)
#     return arash, total_rows

# New implementation using OpenIE 6

def HELPER_to_extract(infile_name, outfile_name, row):# = extractors):# None):

    print('Extractor called')
    # global total_arash
    # global total_rows
    total_rows = []
    compendium = [] # naming one var arash so the legacy lives on...
    try:
        #results = core_NLP_client.annotate(sent)
        #results = extractor.extract(sent)
        # New implementation using OpenIE6
    
        # Call OpenIE6 on our saved files
        os.system(f'/home/derek/anaconda3/envs/openie6/bin/python run.py --save models/oie_model --mode splitpredict --model_str bert-base-cased --task oie --gpus 0 --inp {infile_name} --out {outfile_name}' )
        
        results = None
        with open(outfile_name + ".oie.json", 'r') as f:

            results = json.load(f)


        # Iterate over the results
        for res in results:

            this_sentence = res['sentence']
            this_context = res['context']
            this_extractions = res['extractions']

            for ext in this_extractions:

                this_confidence = ext['confidence']
                this_arg1 = ext['arg1']
                this_arg2 = ext['arg2']
                this_rel = ext['rel']
            
                this_row = {'sentence': this_sentence,
                            'context': this_context,
                            'arg1': this_arg1,
                            'rel': this_rel,
                            'arg2': this_arg2,
                            'confidence': this_confidence}
                
                compendium.append(this_row)


    except:
        return []

    # #with lock:
    #     total_arash.extend(arash)
    for i in range(len(arash)):
        total_rows.append(row)
    return compendium, total_rows




def HELPER_clean_sentence(text):
    # TO DO: Refactor with other punctuations...
    text = text.replace('”','"').replace('’','\'').replace(',',',').replace('“','"')
    text1 = re.sub(r'[^\x00-\x7f]',r'', text) 
    return text1.replace("\n"," ")


def HELPER_rels_quick_clean(text):
    # expand appostrophes
    return text#.replace("'s","is").replace("'m","am").replace("'re","are")

# Method to implement extractions(?)
def OPTION1(df = None):
    # global total_arash
    # global total_rows

    df_new = pd.DataFrame()
    headers = ["postnum", "sentencenum", "relnum", "arg1", "rel", "arg2", "confidence", "context", "negated", "passive"]
    partial_sents = []
    partial_rows = []

    for i, row in tqdm(df.iterrows(), total=df.shape[0]):
            
            # This does string preprocessing so it's not gigafucked from trying to adapt pdfs to text.
            sent = str(row["sentence:String"])
            sent1 = sent.replace('”','"').replace('’','\'').replace(',',',').replace('“','"').replace("\n","")
            sent1 = re.sub(r'[^\x00-\x7f]',r'', sent1) 
            if len(sent1) > 300 or "This file" in sent1 or "copyright law" in sent1 or "contributed equally" in sent1 or sent1.count('\n')>1:
                continue

            partial_sents.append(sent1)
            partial_rows.append(row)


            if len(partial_sents) < 1:
                continue
            else:
                # There was some global lock threading code 
                # here from a previous implementation that
                # was removed on 7/29/2023?
                try:

                    # Save the sentences to disk. 

                    print(partial_sents[0])
                    with open("test_file", "w") as f:

                        for thing in partial_sents[0]:
                            f.write(thing)
                    
                    


                    # Here is where the money is made. This is the function that does the relationship extraction. 
                    total_arash, total_rows = HELPER_to_extract(partial_sents[0], partial_rows[0])
                    partial_sents = []
                    partial_rows = []
                except Exception as e:
                    print(e)
                    break#continue

            relationships = total_arash
            total_arash = []

            for k, (rel,r) in enumerate(zip(relationships, total_rows)):
                

                # This step has some ad-hocs to deal with exceptions we know of.
                # TODO: REFACTOR in simpler terms.
                dictionary = {}
                if rel[0] in ["Tcell","T-cell","T-cells","Tcells","t cell","t-cell","T cell", "T cells"]:
                    rel[0] = "T-cell"
                if rel[2] in ["Tcell","T-cell","T-cells","Tcells","t cell","t-cell","T cell", "T cells"]:
                    rel[2] = "T-cell"


                # Constructing the relationships into a format that will eventually be put into a dataframe for neptune upload
                dictionary["arg1:String"] = rel[0]
                dictionary["rel:String"] = HELPER_rels_quick_clean(rel[1])
                dictionary["arg2:String"] = rel[2]
                #dictionary["context"] = sent
                dictionary["confidence:String"] = rel[3]
                dictionary["context_rel:String"] = rel[4]
                dictionary["negated:String"] = rel[5]
                dictionary["passive:String"] = rel[6]

                for k,v in r.items():
                    if k not in ["text","postnum","sentencenum"]:
                        dictionary[k] = v
                try:        
                    df_new = df_new.append(dictionary, ignore_index=True)
                except Exception as e:
                    print(e)
                    continue

            total_rows = []

    #df_new = df_new.reindex(headers, axis=1)
    return df_new



def kill_bill_and_get_extractions(df):

    print('Kill bill called')
    try:
        df_new = OPTION1(df)
    except Exception as e:
        df_new = pd.DataFrame()
        #tb_str = traceback.format_exc()
        #print(tb_str)
        print(e)
    return df_new
