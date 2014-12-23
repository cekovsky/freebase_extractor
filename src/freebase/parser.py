"""
The Freebaser parser module contains a set of functions for:
1. Transforming RDF lines into named tuples.
2. Filtering these transformed triples.
3. Transforming MQL result lists for further processing.

All of these actions are performed according to a configuration dict.
A sample configuration dict in JSON format can look like this:

{
    "lang_list": ["en", "de", "sk"],
    "main_lang": "en",
    "target_predicates":
    [
        {
            "id": "name",
            "url": "<http://rdf.freebase.com/ns/type.object.name>",
            "localizable_subject": true
        },
        {
            "id": "alias",
            "url": "<http://rdf.freebase.com/ns/common.topic.alias>",
            "localizable_subject": true
        },
        {
            "id": "type",
            "url": "<http://rdf.freebase.com/ns/type.object.type>",
            "localizable_subject": false
        }
    ],
    "main_predicate_url": "<http://rdf.freebase.com/ns/type.object.name>",
}

"""

from collections import namedtuple
from parse import *

_Triple = namedtuple('_Triple', 'subject, predicate, object')
_LocalizedTriple = namedtuple(
    '_LocalizedTriple', 'subject, predicate_id, object, lang')
          
def parse_and_localize(rdf_line, config):
    """
    Parses an RDF line according to a configuration dict.
    
    The result is a named tuple which contains:
    - the subject (entity ID),
    - the predicate ID (according to the config dict),
    - actual object data (string data or a link key),
    - language code, or 'link' for entires without a language.
    
    In case the RDF line could not be parsed, the function
    returns None.
    """
    t = _parse_line(rdf_line)
    if t is None: return None
    return (_LocalizedTriple
        (
            subject=_extract_link_key(t[0]),
            predicate_id=_predicate_url_to_predicate_id(t[1], config),
            object=_extract_string_data_or_link_key(t[2]),
            lang=_extract_lang(t[2], 'link')
        ))

def filter_triples(triples, config):
    """
    Filters a list of localized triples. Only keeps those which the
    configuration dict specifies as "target".
    """
    lang_predicate_tuples = _config_to_lang_predicate_tuples(config)
    filter_function = (lambda t:
        (t.lang, t.predicate_id) in lang_predicate_tuples)
    return [x for x in filter(filter_function, triples)]
  
def query_result_to_entity_info(result_list, config):
    """
    Takes a list of tuples, where each item consists of:
    - the language code,
    - the predicate key,
    - actual object data,
    and transforms it into a similar list of tuples, except that
    predicate keys are mapped into corresponding predicate ID's
    based on the input configuration dict, and linked predicates
    from multiple languages are replaced with a single predicate
    entry per (predicate ID, value) pair and value. Their language
    is then set to 'link'.
    """
    linked_predicates = filter_config_predicates(False, config)
    linked_predicate_id_list = [
        predicate['id']
        for predicate in linked_predicates]
    entity_info = []
    for tuple in result_list:
        lang = tuple[0]
        predicate_key = tuple[1]
        object = tuple[2]
        predicate_id = (
            predicate_key_to_predicate_id(predicate_key, config))
        if predicate_id in linked_predicate_id_list:
            new_tuple = ('link', predicate_id, object)
        else:
            new_tuple = (lang, predicate_id, object)
        if new_tuple not in entity_info:
            entity_info.append(new_tuple)
    return entity_info
        
def filter_config_predicates(localizable, config):
    """
    Filters and returns predicates from a configuration dict based on
    whether their 'localizable_subject' key is True or False.
    """
    localizable_predicates = []
    for predicate in config['target_predicates']:
        if predicate['localizable_subject'] is localizable:
            localizable_predicates.append(predicate)
    return localizable_predicates
    
def predicate_key_to_predicate_id(predicate_key, config):
    """
    Maps a predicate key to a predicate ID based on the configuration dict.
    """
    key_string = predicate_key.lstrip('/').translate({ord('/'): '.'})
    for predicate in config['target_predicates']:
        if predicate['url'].rstrip('>').endswith(key_string):
            return predicate['id']
    else:
        return None     
 
def _extract_lang(object, not_found_val):
    try:
        if object[-3] == '@':
            return object[-2:]
    except IndexError:
        return not_found_val
    else:
        return not_found_val

def _parse_line(line):
    tokens = line.rstrip('\t.\n').split('\t')
    if len(tokens) is 3:
        return _Triple(
            tokens[0],
            tokens[1],
            tokens[2].rstrip(' \t\n'))
    else:
        return None

def _extract_string_data_or_link_key(object):
    if _extract_lang(object, None):
        return object[:-3].strip('\"')
    else:
        return _extract_link_key(object)

def _extract_link_key(str):
    return (str.strip('<>')            # remove link marks
               .rsplit('/', 1)[-1])    # string after last '/'

def _predicate_url_to_predicate_id(predicate_url, config):
    for predicate in config['target_predicates']:
        if predicate['url'] == predicate_url:
            return predicate['id']
    else:
        return None
        
def _find_id_of_main_predicate(config):
    for predicate in config['target_predicates']:
        if predicate['url'] == config['main_predicate_url']:
            return predicate['id']
    else:
        return None
    
def _config_to_lang_predicate_tuples(config):
    lang_predicate_tuples = []
    # 1. for the main language, all predicates with a localizable subject
    localizable_predicates = filter_config_predicates(True, config)
    for predicate in localizable_predicates:
        lang_predicate_tuples.append((config['main_lang'], predicate['id']))
    # 2. for the main predicate, all target languages
    main_predicate_id = _find_id_of_main_predicate(config)
    assert(main_predicate_id is not None)
    for lang in config['lang_list']:
        lang_predicate_tuples.append((lang, main_predicate_id))
    # 3. for predicates with linked objects, one file per predicate
    linked_predicates = filter_config_predicates(False, config)
    for predicate in linked_predicates:
        lang_predicate_tuples.append(('link', predicate['id']))
    # remove duplicates
    return list(set(lang_predicate_tuples))