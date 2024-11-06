from utils import MultiFileWriter
from SPARQLWrapper import SPARQLWrapper, JSON, CSV
import csv
import os



results_dir = './query_results'

# # Define the SPARQL endpoint URL (replace this with your specific SPARQL endpoint URL)
# index_endpoint = "https://k8s.opencitations.net/index/sparql"
#
# # Define the SPARQL query
# query = """
# PREFIX cito: <http://purl.org/spar/cito/>
# SELECT ?citation WHERE {
# 	?citation a cito:Citation ;
# 		cito:hasCitingEntity ?entity ;
# 		cito:hasCitedEntity ?entity .
# }
# """
#
# # Create a SPARQLWrapper object and set the endpoint and query
# sparql = SPARQLWrapper(index_endpoint)
# sparql.setQuery(query)
# sparql.setReturnFormat(JSON)
#
# # Execute the query and get the results
# results = sparql.query().convert()
# print("Running SPARQL query. This might take a while...")
#
#
#
# # Extract the bindings from the results
# bindings = results['results']['bindings']
#
# with MultiFileWriter(results_dir, fieldnames=['Citation']) as writer:
#     for res in bindings:
#         citation = res['citation']['value']
#         writer.write_row({'Citation': citation})
#
# print(f"Results have been written to files inside {results_dir} directory.")

distinct_rows = set()
for fp in [os.path.join(results_dir, p) for p in os.listdir(results_dir)]:
    with open(fp, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            distinct_rows.add(row['Citation'])

print(len(distinct_rows))

with open('circular_citations_list.txt', 'w', encoding='utf-8') as ofp:
    for cit in sorted(distinct_rows):
        ofp.write(cit + '\n')