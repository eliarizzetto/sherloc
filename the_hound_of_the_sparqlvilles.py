from SPARQLWrapper import SPARQLWrapper, JSON
import json
from pprint import pprint


class SPARQLVillesHound:

    def __init__(self,
                 config_file,
                 meta_endpoint='https://k8s.opencitations.net/meta/sparql',
                 index_endpoint='https://k8s.opencitations.net/index/sparql'
                 ):

        with open(config_file, 'r', encoding='utf-8') as f:
            self.config = json.load(f)

        self.meta_endpoint = meta_endpoint
        self.index_endpoint = index_endpoint

    def run_tests(self):
        output = {'meta': [], 'index': []}
        count_meta = 0
        count_index = 0
        if self.meta_endpoint:
            print('Querying Meta endpoint...')
            sparql = SPARQLWrapper(self.meta_endpoint)
            for issue in self.config['meta']:
                label = issue['label']
                query = issue['query']
                descr = issue['description']

                sparql.setQuery(query)
                sparql.setReturnFormat(JSON)
                try:
                    response = sparql.query().convert()
                    result_value = response['boolean']
                    count_meta += 1
                    if result_value:
                        error = f'FAILED {label}: {descr}'
                        # print(error)
                        output['meta'].append(error)
                except Exception as e:
                    output['meta'].append(f'{label} NOT RUN, AN ERROR OCCURED: {e.__repr__()}')
                    continue # todo: implement better error handling strategy

            print(f"{count_meta} tests ran for the data in Meta. {count_meta-len(output['meta'])} tests passed successfully. "
                  f"{len(output['meta'])} test failed.\n")
            for e in output['meta']:
                print(e)

        if self.index_endpoint:
            print('Querying Index endpoint...')
            sparql = SPARQLWrapper(self.index_endpoint)
            for issue in self.config['index']:
                label = issue['label']
                query = issue['query']
                descr = issue['description']

                sparql.setQuery(query)
                sparql.setReturnFormat(JSON)
                try:
                    response = sparql.query().convert()
                    result_value = True if response['results']['bindings'] else False # 'bindings' list is empty if no results
                    count_index += 1
                    if result_value:
                        error = f'FAILED {label}: {descr}'
                        # print(error)
                        output['index'].append(error)
                except Exception as e:
                    output['index'].append(f'{label} NOT RUN, AN ERROR OCCURED: {e.__repr__()}')
                    continue # todo: implement better error handling strategy

            print(f"\n{count_index} tests ran for the data in Index. {count_index-len(output['index'])} tests passed successfully. "
                  f"{len(output['index'])} test failed.\n")
            for e in output['index']:
                print(e)

        return output



if __name__ == '__main__':
    h = SPARQLVillesHound('testing_queries.json')
    out_res = h.run_tests()
