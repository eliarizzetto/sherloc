import requests
import xmltodict
import json
import time
from tqdm import tqdm

# Function to convert DOIs to PMIDs using the EFetch endpoint
def fetch_pmid_from_doi(doi_list):
    base_url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi"
    pmid_list = []
    
    for doi in tqdm(doi_list):
        params = {
            "db": "pubmed",
            "term": doi,
            "field": "DOI",
            "retmode": "json"
        }
        response = requests.get(base_url, params=params)
        data = response.json()
        
        # Extract PMID from the response
        pmids = data['esearchresult']['idlist']
        if pmids:
            pmid_list.extend(pmids)
        time.sleep(0.90)
    
    return pmid_list

# Function to fetch full records from PubMed using PMIDs
def fetch_full_records_from_pmid(pmids):
    base_url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi"
    records_list = []

    # Loop over each PMID and make a separate request
    for pmid in pmids:
        params = {
            "db": "pubmed",
            "id": pmid,
            "retmode": "json",
            # "rettype": "abstract"
        }
        
        response = requests.get(base_url, params=params)
        print(response.url)
        
        # # Convert the XML response to JSON
        # record = xmltodict.parse(response.content)
        # json_record = json.loads(json.dumps(record))
        json_record = response.content
        print(json_record)
        
        # Add the JSON record to the list
        records_list.append(json_record)
        time.sleep(0.33)

    return records_list


# Main function to get full PubMed records from a list of DOIs
def get_pubmed_records_from_dois(doi_list):
    # Step 1: Convert DOIs to PMIDs
    pmids = fetch_pmid_from_doi(doi_list)
    
    if not pmids:
        return "No PMIDs found for the provided DOIs."
    
    # Step 2: Fetch full records from PubMed using PMIDs
    full_records = fetch_full_records_from_pmid(pmids)
    
    return full_records

# Example usage

data = {'10.1016/j.surneu.2003.09.012': 'doi',
 '10.1016/j.surneu.2003.10.021': 'doi',
 '10.1016/j.surneu.2003.11.010': 'doi',
 '10.1016/j.surneu.2003.12.003': 'doi',
 '10.1016/j.surneu.2004.03.007': 'doi',
 '10.1016/j.surneu.2004.04.013': 'doi',
 '10.1016/j.surneu.2004.06.007': 'doi',
 '10.1016/j.surneu.2004.07.019': 'doi',
 '10.1016/j.surneu.2004.08.077': 'doi',
 '10.1016/j.surneu.2004.09.024': 'doi',
 '10.1016/j.surneu.2004.10.026': 'doi',
 '10.1016/j.surneu.2004.11.019': 'doi',
 '10.1016/j.surneu.2004.12.007': 'doi',
 '10.1016/j.surneu.2005.01.011': 'doi',
 '10.1016/j.surneu.2005.02.004': 'doi',
 '10.1016/j.surneu.2005.03.015': 'doi',
 '10.1016/j.surneu.2005.04.023': 'doi',
 '10.1016/j.surneu.2005.05.004': 'doi',
 '10.1016/j.surneu.2005.06.012': 'doi',
 '10.1016/j.surneu.2005.07.045': 'doi',
 '10.1016/j.surneu.2005.08.005': 'doi',
 '10.1016/j.surneu.2005.09.007': 'doi',
 '10.1016/j.surneu.2005.10.015': 'doi',
 '10.1016/j.surneu.2005.11.054': 'doi',
 '10.1016/j.surneu.2005.12.018': 'doi',
 '10.1016/j.surneu.2006.01.015': 'doi',
 '10.1016/j.surneu.2006.02.027': 'doi',
 '10.1016/j.surneu.2006.04.001': 'doi',
 '10.1016/j.surneu.2006.05.015': 'doi',
 '10.1016/j.surneu.2006.05.044': 'doi',
 '10.1016/j.surneu.2006.06.045': 'doi',
 '10.1016/j.surneu.2006.08.004': 'doi',
 '10.1016/j.surneu.2006.08.051': 'doi',
 '10.1016/j.surneu.2006.10.024': 'doi',
 '10.1016/j.surneu.2006.11.031': 'doi',
 '10.1016/j.surneu.2006.12.043': 'doi',
 '10.1016/j.surneu.2007.01.012': 'doi',
 '10.1016/j.surneu.2007.01.067': 'doi',
 '10.1016/j.surneu.2007.02.053': 'doi',
 '10.1016/j.surneu.2007.03.036': 'doi',
 '10.1016/j.surneu.2007.05.002': 'doi',
 '10.1016/j.surneu.2007.05.044': 'doi',
 '10.1016/j.surneu.2007.07.004': 'doi',
 '10.1016/j.surneu.2007.08.001': 'doi',
 '10.1016/j.surneu.2007.09.001': 'doi',
 '10.1016/j.surneu.2007.10.006': 'doi',
 '10.1016/j.surneu.2007.11.007': 'doi',
 '10.1016/j.surneu.2008.01.018': 'doi',
 '10.1016/j.surneu.2008.02.022': 'doi',
 '10.1016/j.surneu.2008.04.005': 'doi',
 '10.1016/j.surneu.2008.04.020': 'doi',
 '10.1016/j.surneu.2008.05.021': 'doi',
 '10.1016/j.surneu.2008.07.015': 'doi',
 '10.1016/j.surneu.2008.09.024': 'doi',
 '10.1016/j.surneu.2008.10.013': 'doi',
 '10.1016/j.surneu.2008.11.010': 'doi',
 '10.1016/j.surneu.2009.03.017': 'doi',
 '10.1016/j.surneu.2009.04.028': 'doi',
 '10.1016/j.surneu.2009.06.022': 'doi',
 '10.1016/j.surneu.2009.07.047': 'doi',
 '10.1016/s0090-3019(01)00711-x': 'doi',
 '10.1016/s0090-3019(01)00712-1': 'doi',
 '10.1016/s0090-3019(02)00667-5': 'doi',
 '10.1016/s0090-3019(02)00733-4': 'doi',
 '10.1016/s0090-3019(02)00739-5': 'doi',
 '10.1016/s0090-3019(02)00759-0': 'doi',
 '10.1016/s0090-3019(02)00795-4': 'doi',
 '10.1016/s0090-3019(02)00889-3': 'doi',
 '10.1016/s0090-3019(02)00969-2': 'doi',
 '10.1016/s0090-3019(02)00978-3': 'doi',
 '10.1016/s0090-3019(03)00009-0': 'doi',
 '10.1016/s0090-3019(03)00073-9': 'doi',
 '10.1016/s0090-3019(03)00147-2': 'doi',
 '10.1016/s0090-3019(03)00175-7': 'doi',
 '10.1016/s0090-3019(03)00233-7': 'doi',
 '10.1016/s0090-3019(03)00568-8': 'doi',
 '15639508': 'pmid',
 '15936360': 'pmid',
 '16050995': 'pmid',
 '16378837': 'pmid',
 '16427394': 'pmid',
 '16488237': 'pmid',
 '16531185': 'pmid',
 '17210283': 'pmid',
 '17445598': 'pmid',
 '17512322': 'pmid',
 '17586209': 'pmid',
 '18424297': 'pmid',
 '18486693': 'pmid',
 '19055951': 'pmid',
 '19084681': 'pmid',
 '19427937': 'pmid',
 '19559923': 'pmid'}

doi_list = [value for value, scheme in data.items() if scheme=='doi']


# doi_list = doi_list[:10]

# print(fetch_pmid_from_doi(doi_list))

# records = get_pubmed_records_from_dois(doi_list[:10])
# print(json.dumps(records, indent=4))

# with open('provina.json', 'w', encoding='utf-8') as outfp:
#     outfp.write(json.dumps(records, indent=4))

def get_pmids_for_doi(doi_list):

    base_url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi"
    pmid_dict_out = dict()
    
    for doi in tqdm(doi_list):
        params = {
            "db": "pubmed",
            "term": doi,
            "field": "DOI",
            "retmode": "json"
        }
        try:
            response = requests.get(base_url, params=params)
            data = response.json()
            
            # Extract PMID from the response
            pmids = data['esearchresult']['idlist']
            if pmids:
                if pmid_dict_out.get(doi):
                    pmid_dict_out[doi] += pmids
                else:
                    pmid_dict_out[doi] = pmids
            time.sleep(0.60)
        except Exception as e:
            print(e)
            time.sleep(5)
            response = requests.get(base_url, params=params)
            data = response.json()
            
            # Extract PMID from the response
            pmids = data['esearchresult']['idlist']
            if pmids:
                if pmid_dict_out.get(doi):
                    pmid_dict_out[doi] += pmids
                else:
                    pmid_dict_out[doi] = pmids

    
    return pmid_dict_out

doi_to_pmids_dict = get_pmids_for_doi(doi_list)
print(doi_to_pmids_dict)
with open('doi_to_pmids_register_br061903839782.json', 'w', encoding='utf-8') as outfp:
    outfp.write(json.dumps(doi_to_pmids_dict, indent=4))



## Vedi ad esempio https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=pubmed&term=10.1016%2Fj.surneu.2003.09.012&field=DOI&retmode=json
## così vengono formulate le chiamate all'API. Osserva anche il risultato: ci sono molti PMID per questo DOI.