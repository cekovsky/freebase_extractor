"""
Second part of the test suite.
"""

import json
import os
import requests
from src.freebase.api import *
from src.freebase.parser import *

def main():
    """
    Main function of the test program. It first parses the file created
    by the first part of the test suite. Then, for each entity ID, the
    test program executes a live query and compares these query results
    with the results obtaining from parsing the file from first part.
    """
    api_key = load_api_key_from_file_or_die('api_key.txt') 
    print("running tests\n")
    with open('test/test_config.json', 'r') as config_file:
        config = json.loads(config_file.read())

    input_file_name = os.path.join(
        config['test_data_directory'], 'sample_data.rdf')
    with open(input_file_name, 'rt') as input_file:
        lines = input_file.readlines()
        tuples = extract_tuples(lines, config)
        
    entity_info = {}
    for t in tuples:
        new_tuple = (t.lang, t.predicate_id, t.object)
        if entity_info.get(t.subject, None) is None:
            entity_info[t.subject] = [new_tuple,]
        else:
            entity_info[t.subject].append(new_tuple)

    lang_list = config['lang_list']
    predicate_url_list = [p['url'] for p in config['target_predicates']]
    mql_service_url = config['mql_service_url']
    for id, parsed_info in entity_info.items():
        print("Entity ID: {}".format(id))
        queries = create_queries_for_entity(
            lang_list, predicate_url_list,
            mql_service_url, api_key, id)
        result_list = execute_freebase_queries(queries)
        queried_info = query_result_to_entity_info(result_list, config)
        matching_items, missing_items, extra_items = (
            compare_two_lists(queried_info, parsed_info))
        print("Number of matching items: {}"
            .format(len(matching_items)))
        if len(missing_items) > 0:
            print("missing items:")
            print('\n'.join([str(mi) for mi in missing_items]))
        if len(extra_items) > 0:
            print("extra items:")
            print('\n'.join([str(ei) for ei in extra_items]))
        print("")
    print("tests ended")
                 
def extract_tuples(rdf_lines, config):
    """
    Transforms RDF lines into a list of tuples. Keeps only those tuples
    which meets the criteria specified in the configuration dict.
    """
    localized_triples = []
    for line in rdf_lines:
        localized_triple = parse_and_localize(line, config)
        if localized_triple is not None:
            localized_triples.append(localized_triple)
    filtered_triples = filter_triples(localized_triples, config)
    return filtered_triples
    
def compare_two_lists(benchmark_list, compared_list):
    """
    Compares a list with a benchmark list. The result of this
    comparison is:
    - a list of matching items (those that are in both lists)
    - a list of missing items (those that are in the original list, but
      are missing from the compared list)
    - a list of extra items (those that are not in the original list,
      but are in the compared list)
    The function does not check for or remove duplicates.
    """
    matching_items = [benchmark_item
        for benchmark_item in benchmark_list
        if benchmark_item in compared_list]
    missing_items = [benchmark_item
        for benchmark_item in benchmark_list
        if benchmark_item not in compared_list]
    extra_items = [compared_item
        for compared_item in compared_list
        if compared_item not in benchmark_list]
    return matching_items, missing_items, extra_items

if __name__ == "__main__":
    main()