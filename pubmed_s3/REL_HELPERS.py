from functools import partial
import pandas as pd
import sys
import warnings
from tqdm import tqdm
import re
import threading
from func_timeout import func_timeout, FunctionTimedOut
import traceback

from pyopenie import OpenIE5
#extractor = OpenIE5('http://localhost:9000')
extractors = [OpenIE5('http://localhost:8000'), OpenIE5('http://localhost:9000')]

import nltk.data
tokenizer = nltk.data.load('tokenizers/punkt/english.pickle')

import ctypes


total_arash = []
total_rows = []






def HELPER_to_extract(sent, row, extractor):# = extractors):# None):
    # global total_arash
    # global total_rows
    total_rows = []
    arash = [] # naming one var arash so the legacy lives on...
    try:
        results = extractor.extract(sent)
        for res in results:
            for arg2 in res["extraction"]["arg2s"]:
                arash.append([res["extraction"]["arg1"]["text"], \
                    res["extraction"]["rel"]["text"], \
                        arg2["text"], res["confidence"], res["extraction"]["context"], \
                            res["extraction"]["negated"], res["extraction"]["passive"]])
    except:
        return []

    # #with lock:
    #     total_arash.extend(arash)
    for i in range(len(arash)):
        total_rows.append(row)
    return arash, total_rows




def HELPER_clean_sentence(text):
    # TO DO: Refactor with other punctuations...
    text = text.replace('”','"').replace('’','\'').replace(',',',').replace('“','"')
    text1 = re.sub(r'[^\x00-\x7f]',r'', text) 
    return text1.replace("\n"," ")


def HELPER_rels_quick_clean(text):
    # expand appostrophes
    return text#.replace("'s","is").replace("'m","am").replace("'re","are")


def OPTION1(df = None):
    # global total_arash
    # global total_rows

    df_new = pd.DataFrame()
    headers = ["postnum", "sentencenum", "relnum", "arg1", "rel", "arg2", "confidence", "context", "negated", "passive"]
    partial_sents = []
    partial_rows = []

    for i, row in tqdm(df.iterrows(), total=df.shape[0]):
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
                # global lock
                # lock = threading.Lock()
                # threads = [threading.Thread(target=HELPER_to_extract, args=(sent, row, extractor)) for (sent,row,extractor) in zip(partial_sents, partial_rows, extractors)]
                # for t in threads: 
                #     t.start()
                # for t in threads:
                #     t.join()
                try:
                    total_arash, total_rows = HELPER_to_extract(partial_sents[0], partial_rows[0], extractors[0])
                    partial_sents = []
                    partial_rows = []
                except Exception as e:
                    print(e)
                    break#continue

            relationships = total_arash
            total_arash = []

            for k, (rel,r) in enumerate(zip(relationships, total_rows)):
                
                # TODO: REFACTOR in simpler terms.
                dictionary = {}
                if rel[0] in ["Tcell","T-cell","T-cells","Tcells","t cell","t-cell","T cell", "T cells"]:
                    rel[0] = "T-cell"
                if rel[2] in ["Tcell","T-cell","T-cells","Tcells","t cell","t-cell","T cell", "T cells"]:
                    rel[2] = "T-cell"
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
    try:
        df_new = OPTION1(df)
    except Exception as e:
        df_new = pd.DataFrame()
        #tb_str = traceback.format_exc()
        #print(tb_str)
        print(e)
    return df_new
