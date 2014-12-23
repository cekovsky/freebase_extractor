"""
A sample console application which uses the data parsed from Freebase
to create a very simple search engine.
"""

import json
import os
import sys
import whoosh.fields
import whoosh.index
import whoosh.qparser

def main():
    """
    Main function of the sample application. Anything that the user
    writes on the input is searched for in the object data of all
    indexed entries. However, if user input starts with #all_about,
    then the next token (separated by a space) is taken to be the
    ID of an entity, and all information about this entity is looked
    up and displayed. If there is another space after the entity ID,
    this token is taken to be the language code, and in that case,
    the results are limited to the specified language. For displaying
    only information which link to other entities, specify "link" as
    the filter language.
    """
    with open('src/config.json', 'r') as config_file:
        config = json.loads(config_file.read())
    if whoosh_index_exists_in(config['index_directory']):
        print("index already exists, not re-creating it")
    else:
        print("index does not yet exist, creating it")
        create_whoosh_index(
            config['output_file_name'],
            config['index_directory'])
    print("type #exit to exit, #help for help")
    while True:
        user_input = input(">> ")
        if user_input.startswith('#exit'):
           break
        elif user_input.startswith('#help'):
            print("#all_about id [lang]")
        elif user_input.startswith('#all_about'):
            tokens = user_input.split(' ', maxsplit=2)
            search_term = tokens[1]
            if len(tokens) == 3:
                lang = tokens[2]
                filter_function = (lambda x: x[3] == lang)
            else:
                filter_function = (lambda _: True)
            results = search_whoosh_index(
                search_term, 'entity_id', config['index_directory'])
            filtered_results = filter(filter_function, results)
            print('\n'.join([str(x) for x in filtered_results]))
        else:
            results = search_whoosh_index(
                user_input, 'object', config['index_directory'])
            print('\n'.join([str(x) for x in results]))
            
def whoosh_index_exists_in(index_directory):
    """
    Checks whether a directory exists, and if so, if it also contains
    a Whoosh index.
    """
    if os.path.isdir(index_directory) is False:
        return False
    if whoosh.index.exists_in(index_directory) is False:
        return False
    return True

def create_whoosh_index(parse_file_name, index_directory):
    """
    Creates a Whoosh index from data stored in an input file.
    """
    if os.path.isdir(index_directory) is False:
        print("{} is not a directory."
            .format(index_directory))
        print("Please create it as an empty directory.")
        sys.exit(3)
    whoosh_schema = whoosh.fields.Schema(
        entity_id = whoosh.fields.ID(stored=True),
        predicate_id = whoosh.fields.STORED,
        object = whoosh.fields.NGRAM(stored=True),
        lang = whoosh.fields.STORED)
    whoosh_index = whoosh.index.create_in(
        index_directory, whoosh_schema)
    index_writer = whoosh_index.writer()
    with open(parse_file_name, 'rt', encoding='utf-8') as input_file:
        for line in input_file:
            tokens = line.rstrip('\n').split('\t')
            assert(len(tokens) == 4)
            index_writer.add_document(
                entity_id = tokens[0],
                predicate_id = tokens[1],
                object = tokens[2],
                lang = tokens[3])
    index_writer.commit()

def search_whoosh_index(search_term, searched_field, index_directory):
    """
    Searches a Whoosh index and returns the result as a list of tuples.
    """
    print("searching for {} in {}".format(search_term, searched_field))
    whoosh_index = whoosh.index.open_dir(index_directory)
    with whoosh_index.searcher() as index_searcher:
        query_parser = whoosh.qparser.QueryParser(
            searched_field, whoosh_index.schema)
        query = query_parser.parse(search_term)
        results = index_searcher.search(query)
        # "results" is a generator which must be evaluated
        # while the index_searcher is still open, in order
        # avoid the whoosh.reading.ReaderClosed exception.
        result_list = [
            (
                result['entity_id'],
                result['predicate_id'],
                result['object'],
                result['lang']
            )
            for result in results]
    return result_list
    
if __name__ == "__main__":
    main()
