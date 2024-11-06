from csv import DictWriter
import logging
import re
from datetime import datetime
from utils import read_compressed_meta_dump, MultiFileWriter

class DataError:
    def __init__(self):
        pass


class IdentifierValuesError(DataError):
    def __init__(self):
        super(DataError, self).__init__()



path_meta_csv_v8 = 'E:/meta_csv_v8.zip'
path_meta_csv_v9 = 'E:/meta_2024_06_20_csv_v9.tar.gz'


def check_orcids_consistency(row:dict):
    full_authors = row['author'].split('; ') # list of all authors in their full representation (last name, first name and IDs)
    result = dict()
    if full_authors:
        for a in full_authors:
            match = re.search(r'\[.*]', a)

            if match:
                ids = match.group().replace('[', '', 1).replace(']', '', 1).strip().split()
                name = a[:a.index('[')].strip()
                orcids = {o for o in ids if o.startswith('orcid')}
                try:
                    omid = [i for i in ids if i.startswith('omid')][0]
                except IndexError:
                    # if no OMID is found inside square brackets it's because suare brackets are used in the author's name string
                    logging.warning(f'Square brackets inside author name string! {row}')
                    continue

                # # todo: tieni registro di autori con più di un orcid
                # if len(orcids) > 1:
                #     print('woooooooooooo qui ci sono troppi orcids!')
                #     print(row, '\n')

                if name and orcids:
                    for orcid in orcids:
                        if result.get(orcid):
                            result[orcid]['name'].update({name})
                            result[orcid]['omid'].update({omid})
                        else:
                            result[orcid] = {'name': {name}, 'omid': {omid}}
    return result


def find_errors(zip_path):
    orcid_dict = dict()
    # orcid_omids_dict = dict()

    for row in read_compressed_meta_dump(zip_path):


        # # ----- Check IDs -----

        ids = row['id'].split()
        # if len(ids) > 1:  # every br has an OMID as internal ID, and optionally other external IDs
        #     type = row['type']
        #     duplicates: bool = len(ids) != len(set(ids))
        #     if duplicates:
        #         print('DUPLICATE:', row)
        #         logging.warning(f'DUPLICATE: {row}')
        #         continue
        #
        #     dois = [i for i in ids if i.startswith('doi')]
        #     pmids = [i for i in ids if i.startswith('pmid')]
        #     pmcids = [i for i in ids if i.startswith('pmcid')]
        #     openalexids = [i for i in ids if i.startswith('openalex')]
        #     omids = [i for i in ids if i.startswith('omid')]
        #     issns = [i for i in ids if i.startswith('issn')]
        #     isbns = [i for i in ids if i.startswith('isbn')]
        #     wikidataids = [i for i in ids if i.startswith('wikidata')]

            # if len(dois) == 1 and len(pmids) > 1:
            #     print('Multiple PMIDs for the same DOI')
            #     ...  # instantiate appropriate Error
            # if len(dois) > 1 and len(pmids) > 1:
            #     print('Multiple DOIs and multiple PMIDs')
            # if len(dois) > 3:
            #     print('Many DOIs')
            # if len(issns) > 2:
            #     print('Many ISSNs')
            # if len(isbns) > 2:
            #     print('Many ISBNs')


        # # ----- Check authors -----

        # authors_orcids_in_row = check_orcids_consistency(row).items()
        # for orcid, value in authors_orcids_in_row:
        #     if orcid_dict.get(orcid):
        #         orcid_dict[orcid]['name'].update(value['name'])
        #         orcid_dict[orcid]['omid'].update(value['omid'])
        #         # if len(orcid_dict[orcid]['name']) > 1:
        #         #     print(f"Stesso ORCID ({orcid}) per nomi diversi: {orcid_dict[orcid]['name']}.")
        #         # if len(orcid_dict[orcid]['omid']) > 1:
        #         #     print(f"Stesso ORCID ({orcid}) per OMID diversi: {orcid_dict[orcid]['omid']}.")
        #     else:
        #         orcid_dict[orcid] = {'name': value['name'], 'omid':value['omid']}


        if 'omid:/ar' in row['author']:
            print(row['author'])

    return orcid_dict


def get_agent_roles(fp, outdir):
    """
    Solo per controllare se ci sono OMID di agent roles (omid:ar/) invece che di responsible agents (omid:ra/) nel campo
    author del CSV.
    """
    ar_count = 0
    with MultiFileWriter(outdir, fieldnames=["br_omid", "author"]) as writer:
        for row in read_compressed_meta_dump(fp):

            if 'omid:ar/' in row['author']:
                br = [i for i in row['id'].split() if i.startswith('omid')][0]
                outrow = {'br_omid': br, 'author': row['author']}
                ar_count += 1
                # print('id:', row['id'], 'author: ', row['author'], 'publisher: ',row['publisher'],'editor: ', row['editor'])
                writer.write_row(outrow)

    print(f'Number of BRs with agent role instead of responsible agent OMID: {ar_count}')
    return ar_count


if __name__ == '__main__':
    log_file = f'find_errors_{datetime.now().strftime("%Y-%m-%d")}.log'  # 'count_brs_ids.log'
    logging.basicConfig(filename=log_file, encoding='utf-8', level=logging.WARNING)
    # with open('orcids_authors_test.json', 'w', encoding='utf-8') as f:
    #     orcids = get_orcids(path_meta_csv_v9)
    #     final_orcids = {k: {ik: list(iv) for ik, iv in v.items()} for k, v in orcids.items()}
    #     json.dump(final_orcids, f, indent=2)
    find_errors(path_meta_csv_v9)