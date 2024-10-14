from SPARQLWrapper import SPARQLWrapper, JSON
import json
from pprint import pprint
import time
from datetime import datetime


class DataMonitor:

    def __init__(self,
                 config:str,
                 out_path:str):

        with open(config, 'r', encoding='utf-8') as f:
            self.config = json.load(f)
        self.out_path = out_path

    def monitor(self):
        # run checks on collection-specific endpoint
        # writes monitoring output to json file
        return None
    

class MetaMonitor:

    def __init__(self,
                 config_path:str,
                 out_path:str):

        with open(config_path, 'r', encoding='utf-8') as f:
            self.config = json.load(f)

        self.endpoint = self.config['endpoint']
        # TODO: possibility to overwrite the endpoint in config via kwargs?
        self.out_path = out_path

    def run_tests(self):

        output_dict = {
            'endpoint': self.endpoint,
            'datetime': datetime.now().strftime('%d/%m/%Y, %H:%M:%S'), # TODO: extract datetime of the run
            'running_time' : 0.0,
            'monitored_data': []
            }
        
        tests_count = 0
        general_start = time.time()
        print('Querying Meta endpoint...')
        sparql = SPARQLWrapper(self.endpoint)
        for issue in self.config['tests']:
            if issue['to_run']: # specific tests can be switched off in config file
                label = issue['label']
                query = issue['query']
                descr = issue['description']

                sparql.setQuery(query)
                sparql.setReturnFormat(JSON)
                try:
                    res_start = time.time()
                    response = sparql.query().convert()
                    result_value = response['boolean']
                    tests_count += 1
                    test_res = {
                        'label': label,
                        'description': descr,
                        'run': {
                            'got_result': True,
                            'running_time': time.time()-res_start,
                            'error': None}}
                    
                    if result_value is True: # did NOT pass the test
                        test_res['passed']= False
                    else:
                        test_res['passed']= True
                    output_dict['monitored_data'].append(test_res)

                except Exception as e:
                    test_res = {
                        'label': label,
                        'description': descr,
                        'run': {
                            'got_result': False,
                            'running_time': time.time()-res_start,
                            'error': str(e)}}
                    
                    output_dict['monitored_data'].append(test_res)
                finally:
                    continue # TODO: implement better error handling strategy?
        
        output_dict['running_time'] = time.time()-general_start
        with open(self.out_path, 'w', encoding='utf-8') as outf:
            json.dump(output_dict, outf, indent=4)
        return output_dict



# class IndexMonitor:

#     def __init__(self,
#                  config_file,
#                  endpoint='https://k8s.opencitations.net/meta/sparql',
#                  index_endpoint='https://k8s.opencitations.net/index/sparql'
#                  ):

#                     response = sparql.query().convert()
#                    result_value = True if response['results']['bindings'] else False # 'bindings' list is empty if no results


class IndexMonitor:

    def __init__(self,
                 config_path:str,
                 out_path:str):

        with open(config_path, 'r', encoding='utf-8') as f:
            self.config = json.load(f)

        self.endpoint = self.config['endpoint']
        # TODO: possibility to overwrite the endpoint in config via kwargs?
        self.out_path = out_path

    def run_tests(self):

        output_dict = {
            'endpoint': self.endpoint,
            'datetime': datetime.now().strftime('%d/%m/%Y, %H:%M:%S'), # TODO: extract datetime of the run
            'running_time' : 0.0, # overwritten later on
            'monitored_data': []
            }
        
        tests_count = 0
        general_start = time.time()
        print('Querying Index endpoint...')
        sparql = SPARQLWrapper(self.endpoint)
        for issue in self.config['tests']:
            if issue['to_run']: # specific tests can be switched off in config file
                label = issue['label']
                query = issue['query']
                descr = issue['description']

                sparql.setQuery(query)
                sparql.setReturnFormat(JSON)
                try:
                    res_start = time.time()
                    response = sparql.query().convert()
                    result_value = True if response['results']['bindings'] else False # 'bindings' list is empty if no results
                    tests_count += 1
                    test_res = {
                        'label': label,
                        'description': descr,
                        'run': {
                            'got_result': True,
                            'running_time': time.time()-res_start,
                            'error': None}}
                    
                    if result_value is True: # did NOT pass the test
                        test_res['passed']= False
                    else:
                        test_res['passed']= True
                    output_dict['monitored_data'].append(test_res)

                except Exception as e:
                    test_res = {
                        'label': label,
                        'description': descr,
                        'run': {
                            'got_result': False,
                            'running_time': time.time()-res_start,
                            'error': str(e)}}
                    
                    output_dict['monitored_data'].append(test_res)
                finally:
                    continue # TODO: implement better error handling strategy?
        
        output_dict['running_time'] = time.time()-general_start
        with open(self.out_path, 'w', encoding='utf-8') as outf:
            json.dump(output_dict, outf, indent=4)
        return output_dict