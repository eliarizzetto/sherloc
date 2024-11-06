import requests
import csv
from SPARQLWrapper import SPARQLWrapper, JSON
from string import Template
from pprint import pprint


fp = 'C:/Users/media/Downloads/external_ids_of_br061903839782.csv'


# query_template = Template('''
# PREFIX literal: <http://www.essepuntato.it/2010/06/literalreification/>
# PREFIX fabio: <http://purl.org/spar/fabio/>
# PREFIX datacite: <http://purl.org/spar/datacite/>

# SELECT ?value ?scheme {
#   $br_uri datacite:hasIdentifier ?id .
#   ?id datacite:usesIdentifierScheme ?scheme ;
#     literal:hasLiteralValue ?value .
# }
# ''')


# out = dict()
# brs_to_check = [
#     'https://w3id.org/oc/meta/br/061903839782',
# ]


# sparql = SPARQLWrapper('https://k8s.opencitations.net/meta/sparql')


# for br in brs_to_check:
#     query = query_template.substitute(br_uri=f'<{br}>')
#     sparql.setQuery(query)
#     sparql.setReturnFormat(JSON)
#     results = sparql.query().convert()
#     for result in results["results"]["bindings"]:
#         id_value = result['value']['value']
#         id_scheme = result['scheme']['value'].removeprefix('http://purl.org/spar/datacite/')
#         out[id_value] = id_scheme

# pprint(out)

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




## Esempio di chiamata all'API di OpenAire: https://api.openaire.eu/search/researchProducts?doi=10.1038/502295a&format=json (articolo di shotton)



# openaire_api = "https://api.openaire.eu/search/researchProducts"

# for v, s in data.items():
#     params = {
#     "doi": v,
#     "format": "json"
#     }

#     response = requests.get(openaire_api, params=params)

#     if response.status_code == 200:
#         response = response.json()
#         something_returned = response['response']['results']
#         if something_returned:
#             returned_info = something_returned['result']
#             for el in returned_info:
#                 pids = el['metadata']['oaf:entity']['oaf:result']['pid']
#             print(pids)


#     else:
#         print(f"Error: {response.status_code}")
    
    

## Questi sono gli unici unici 2 (DUE!) record che otteniamo mandando all'API di OpenAire ciascuno dei 93 ID collegati a https://w3id.org/oc/meta/br/061903839782 in Meta

# [{'@classid': 'doi', '@classname': 'Digital Object Identifier', '@schemeid': 'dnet:pid_types', '@schemename': 'dnet:pid_types', '$': '10.1016/s0090-3019(02)00969-2'}, {'@classid': 'pmid', '@classname': 'PubMed ID', '@schemeid': 'dnet:pid_types', '@schemename': 'dnet:pid_types', '@inferred': False, '@provenanceaction': 'sysimport:actionset', '@trust': '0.9', '$': 12638558}, {'@classid': 'mag_id', '@classname': 'Microsoft Academic Graph Identifier', '@schemeid': 'dnet:pid_types', '@schemename': 'dnet:pid_types', '$': 2414224654}]
# [{'@classid': 'doi', '@classname': 'Digital Object Identifier', '@schemeid': 'dnet:pid_types', '@schemename': 'dnet:pid_types', '$': '10.1097/bsd.0b013e31816a9ebd'}, {'@classid': 'pmid', '@classname': 'PubMed ID', '@schemeid': 'dnet:pid_types', '@schemename': 'dnet:pid_types', '@inferred': False, '@provenanceaction': 'sysimport:actionset', '@trust': '0.9', '$': 19559923}, {'@classid': 'mag_id', '@classname': 'Microsoft Academic Graph Identifier', '@schemeid': 'dnet:pid_types', '@schemename': 'dnet:pid_types', '$': 1978828566}]