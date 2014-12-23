"""
Example program which uses the src.freebase.parser module to parse and
filter data from a Freebase data dump. It supports both text files for
small testing files and gzip archives for parsing all of Freebase.
"""

import gzip
import json
import time
from src.freebase.parser import *
  
def main():
    """
    Main function of the program. Reads the input file line by line,
    parsing the lines as it proceeds. When it finds that it has read
    all lines for a given entity ID, it filters the parsed lines
    according to the configuration file and either writes or avoids
    writing the parsed data into the output file. It then repeats this
    process of reading, parsing, filtering and possibly writing data
    until all lines of the input file have been processed.
    """
    with open('src/config.json', 'r') as config_file:
        config = json.loads(config_file.read())
    try:
        if config['input_file_name'].endswith('.gz'):
            print("Input file's name ends with .gz.")
            print("Processing it as a gzip archive.")
            input_file = gzip.open(config['input_file_name'], 'rt', encoding='utf-8')
        else:
            print("Input file's name does not end with .gz")
            print("Processing it as a text file.")
            input_file = open(config['input_file_name'], 'rt', encoding='utf-8')
        output_file = open(config['output_file_name'], 'wt', encoding='utf-8')
        processing_begin = time.time()
        first_tuple = None
        processed_lines = 0
        while first_tuple is None:
            line = input_file.readline()
            first_tuple = parse_and_localize(line, config)
            processed_lines += 1
        current_entity_id = first_tuple.subject
        entity_tuples = [first_tuple,]
        condition = (
            config['condition']['predicate_id'],
            config['condition']['predicate_value'])
        for line in input_file:
            tuple = parse_and_localize(line, config)
            processed_lines += 1
            if tuple is None: continue
            if tuple.subject == current_entity_id:
                entity_tuples.append(tuple)
            else:
                if (filter_and_write(entity_tuples, config, condition, output_file)):
                    print("Entity ID: {}".format(current_entity_id))
                    print(processed_lines, (time.time() - processing_begin))
                entity_tuples = [tuple,]
                current_entity_id = tuple.subject
        if (filter_and_write(entity_tuples, config, condition, output_file)):
            print("Entity ID: {}".format(current_entity_id))
            print(processed_lines, (time.time() - processing_begin))
    except FileNotFoundError:
        print("{} not found.".format(config['input_file_name']))
    else:
        input_file.close()
        output_file.close()
  
def triples_to_string(localized_triples):
    """
    Transforms a list of localized triples into a string, where the
    respective triples are separated with new line characters, while
    the fields of each triple are separated with tabs.
    """
    string_list = []
    for t in localized_triples:
        subject = str(t[0].encode('utf-8')).lstrip('b').strip('\'')
        predicate_id = t[1]
        object = str(t[2].encode('utf-8')).lstrip('b').strip('\'')
        lang = t[3]
        item_list = [subject, predicate_id, object, lang]
        string_list.append('\t'.join(item_list))
    return '\n'.join(string_list) + '\n'

def filter_and_write(entity_tuples, config, condition, output_file):
    """
    Filters a list of parsed entity tuples according to a configuration
    dict. It applies two separate filters:
    - 1. it only keeps those tuples which meet the extraction criteria
    - 2. it only keeps those entities which meet the condition
         specified in the configuration dict
    If it was decided that the entity should be kept and its parsed
    data was written to a file, it returns True. Otherwise the function
    returns False.
    """
    filtered_triples = filter_triples(entity_tuples, config)
    predicate_id_value_list = [(t[1], t[2]) for t in filtered_triples]
    if condition in predicate_id_value_list:
        output_file.write(triples_to_string(filtered_triples))
        return True
    else:
        return False
        
if __name__ == "__main__":
    main()
       