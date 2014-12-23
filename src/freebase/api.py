"""
The Freebase API module contains a set of functions for:
1. transforming the Turtle format into N-triples RDF
2. building MQL queries for an entity
3. executing and processing the results of these queries
"""

from collections import namedtuple
import json
import requests
import sys
from parse import *

_PredicateObjectPair = namedtuple('_PredicateObjectPair', 'predicate, object')
   
def turtle_lines_to_rdf_lines(turtle_lines):
    """Transforms the Turtle format into N-Triples RDF."""
    prefix_lines, subject, po_lines = (
        _split_turtle_lines(turtle_lines))

    token_list = (
        [
            parse("@prefix {} <{}>.", line).fixed
            for line in prefix_lines
        ])
    pt_dict =  {token[0]: token[1] for token in token_list}
    
    full_subject =_replace_prefix(subject, pt_dict)
    po_pairs =_build_predicate_object_pairs(po_lines)
    
    full_triples = []
    for po_pair in po_pairs:
        full_triple = (
            _format_as_rdf_line(
                full_subject,
                po_pair, pt_dict))
        full_triples.append(full_triple)
    return full_triples
    
def create_queries_for_entity(
    lang_list, predicate_url_list,
    mql_service_url, api_key, entity_id):
    """
    Creates a list of tuples according to the input arguments.
    
    Each item in the output list consists of the MQL request link
    as a string and the language code to use for the query.
    """
    mid = '/' + entity_id.translate({ord('.'): '/'})
    # prepare query dictionary with the MID
    query_dict = {'mid': mid}
    # insert predicate MQL keys into the dictionary
    for predicate in predicate_url_list:
        mql_predicate_key = (
            _predicate_url_to_mql_key(predicate))
        query_dict[mql_predicate_key] = '[]'
    queries = []
    # build a list of query tuples
    for lang in lang_list:
        mql_link = (
            _create_mql_request_link(
                mql_service_url,
                api_key, query_dict, lang))
        queries.append((mql_link, lang))
    return queries
           
def execute_freebase_queries(queries):
    """
    Executes a list of queries created by the create_queries_for_entity
    function. Returns a dict created by decoding the returned JSON and
    parsing the results.
    """
    tuple_list = []
    for query in queries:
        query_link = query[0]
        query_lang = query[1]
        http_reply = requests.get(query_link)
        if http_reply.status_code == 400:
            print("Request URL: {}".format(query_link))
            print("Server response: bad request.")
            print("Probably an invalid Google API key?")
            print("Response data:")
            print(http_reply.text)
            sys.exit(2)
        assert(http_reply.status_code == 200)
        result = json.loads(http_reply.text)['result']
        for predicate, object_list in result.items():
            if predicate == "mid" or len(object_list) is 0:
                continue
            for object in object_list:
                stripped_object = (
                    object.lstrip('/').translate({ord('/'): '.'}))
                encoded_object = (str(
                    stripped_object
                        .encode('utf-8'))
                        .lstrip('b')
                        .strip('\''))
                tuple_list.append(
                    (query_lang, predicate, encoded_object))
    return tuple_list
    
def create_turtle_download_link(rdf_service_url, api_key, topic_id):
    """Creates an RDF download link for the specified topic ID."""
    linkable_topic_id = topic_id.translate({ord('.'): '/'})
    return ("{}/{}?key={}".format(
        rdf_service_url,
        linkable_topic_id,
        api_key))

def load_api_key_from_file_or_die(file_name):
    """
    Loads a Google API key from a file, or, if the file does not exist,
    prints an error message and exits the application.
    """
    try:
        api_key_file = open(file_name, 'rt')
        api_key = api_key_file.readline()
    except FileNotFoundError:
        print("{} not found".format(file_name))
        print("Please create a Google API browser key")
        print("and enable the Freebase API.")
        sys.exit(1)
    else:
        api_key_file.close()
    return api_key
           
def _create_mql_query_string(input_dict):
    query = '{'
    for key, val in input_dict.items():
        query += '\"{key}\":'.format(key=key)
        if (val == '[]' or val == 'null'):
            query += val
        else:
            query += '\"' + val + '\"'
        query += ','
    return query[:-1] + '}'

def _predicate_url_to_mql_key(predicate_url):
    mql_key = (
        predicate_url
            .strip('<>')
            .rsplit('/', maxsplit=1)
            [1])
    return '/' + mql_key.translate({ord('.'): '/'})
    
def _create_mql_request_link(mql_service_url, api_key, input_dict, lang = 'en'):
    query_string = _create_mql_query_string(input_dict)
    return ("{}read?query={}&lang=/lang/{}&key={}"
            .format(
                mql_service_url,
                query_string,
                lang, api_key))
    
def _build_predicate_object_pairs(predicate_object_lines):
    token_list = [line.strip().split(None, 1)
                 for line in predicate_object_lines]
    return (
        [
            _PredicateObjectPair(
                token[0],
                token[1].rstrip(';'))
            for token in token_list
        ])

def _replace_prefix(str, prefix_translation_dict):
    for p, r in prefix_translation_dict.items():
        if str.startswith(p):
            prefix, replacement = p, r
            break
    else:
        prefix, replacement = None, None
    return (str if prefix is None
            else str
                .replace(prefix, replacement)
                .rstrip()) # remove newline

def _tag_as_link_if_http(str):
    if str.startswith('http://'):
        return '<{}>'.format(str)
    else:
        return str

def _format_as_rdf_line(subject, po_pair, pt_dict):
    # expand prefixes
    predicate =_replace_prefix(po_pair.predicate, pt_dict)
    object =_replace_prefix(po_pair.object, pt_dict)
    # format as valid links
    rdf_subject = _tag_as_link_if_http(subject)
    rdf_predicate = _tag_as_link_if_http(predicate)
    rdf_object = _tag_as_link_if_http(object)
    return '{}\t.\n'.format(
        '\t'.join([rdf_subject, rdf_predicate, rdf_object]))

def _split_turtle_lines(turtle_lines):
    # Prefix lines end at the first empty line.
    prefix_lines_end = turtle_lines.index("")
    prefix_lines = turtle_lines[:prefix_lines_end]
    # The subject ID should be right after
    # the empty line where prefix lines end.
    subject = turtle_lines[prefix_lines_end+1]
    # The rest of the RDF lines are
    # predicates paired with objects.
    po_begin = prefix_lines_end+2
    po_lines = turtle_lines[po_begin:]
    return prefix_lines, subject, po_lines