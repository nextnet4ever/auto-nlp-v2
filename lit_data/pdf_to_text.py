import os
import pandas as pd
import PyPDF2
import requests
from multiprocessing import Pool
from itertools import combinations

#query = ['Lung adenocarcinoma', 'Trastuzumab', 'EGFR', 'GLUT1', 'Warburg effect', 'SLC2A1']

#query = ['Warburg Effect','Trastuzumab']

#query = ['ADAR', 'Adenosine deaminase acting on RNA', 'ADAR1', 'ADAR2', 
#         'dsRNA binding domains', 'A-to-I editing', 'ADAR Liver', 'ADAR Central Nervous System', 
#         'ADAR CNS', 'Alpha-1 Antitrypsin Deficiency']

query = ['BRAF']

combinations_list = [' '.join(subset) for r in range(1, len(query)+1) for subset in combinations(query, r)]

def process_query_item(item):
    print(item)
    # #select line below for sorted literature and DOI by published date or by the best match to the query. rows=number of DOIs
    # #url = f"https://api.crossref.org/works?query={item}&sort=published&order=desc&rows=200"
    url = f"https://api.crossref.org/works?query={item}&rows=5"

    response = requests.get(url)
    data = response.json()
    dois = [item["DOI"] for item in data["message"]["items"]]
    dois = pd.DataFrame(dois)
    csv_file = f"text{item}.txt"
    dois.to_csv(csv_file, index=False, header=False)
    
    #input_dir = f'{item}_pdf'
    input_dir = f'/home/derek/auto-nlp/auto-nlp-drive/{item}_pdf'
    # pdf_path = f"/home/ubuntu/lit_data/{item}_pdf/"
    pdf_path = f"/home/derek/auto-nlp/auto-nlp-drive/{item}_pdf"
    
    if not os.path.exists(input_dir):
        os.makedirs(input_dir)
    
    if not os.path.exists(pdf_path):
        os.makedirs(pdf_path)

        
    os.system(f'python -m PyPaperBot --doi-file="{csv_file}" --dwn-dir="{pdf_path}"')# --scihub-mirror="https://sci-hub.ru"')# --restrict 1 --proxy "http://1.1.1.1::8080"')# --scihub-mirror="https://sci-hub.ru"')

    # Convert the PDFs to text for NLP
    # Define the paths to the input and output directories
    output_dir = '/home/derek/auto-nlp/auto-nlp-drive/text'

    # Create the output directory if it doesn't already exist
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # Loop through each PDF file in the input directory
    for filename in os.listdir(input_dir):
        if filename.endswith('.pdf'):
            # Define the paths to the input and output files
            input_path = os.path.join(input_dir, filename)
            output_path = os.path.join(output_dir, filename[:-4] + '.txt')# will need this and the .csv for the dois to run the nlp properly

            # Open the input PDF file
            with open(input_path, 'rb') as pdf_file:
                try:
                    # Create a PDF reader object
                    pdf_reader = PyPDF2.PdfReader(pdf_file)

                    # Get the total number of pages in the PDF file
                    num_pages = len(pdf_reader.pages)

                    # Initialize a variable to hold the text
                    text = ""

                    # Loop through each page and extract the text
                    for i in range(num_pages):
                        page = pdf_reader.pages[i]
                        text += page.extract_text()

                    # Save the extracted text to a file
                    with open(output_path, 'w') as text_file:
                        text_file.write(text)
                except Exception as e:
                    print(e)
                    pass

if __name__ == '__main__':
    queries = combinations_list  # define your list of queries here
    with Pool(processes=4) as pool:  # adjust the number of processes to your liking
        pool.map(process_query_item, queries)

#run nlp pipeline however, use the concat csv from all the directories to get the proper dois for each literature source.
