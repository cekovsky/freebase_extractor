"""First part of the test suite."""

import json
import os
import requests
from src.freebase.api import *
from src.freebase.parser import *

def main():
    """
    Main function of the test program. For each topic MID specified
    in the test config file, it downloads the first 100 information
    entries in Turtle format. It then transforms these into N-Triple
    RDF format and saves the aggregated result into a file.
    """
    api_key = load_api_key_from_file_or_die('api_key.txt')
    with open('test/test_config.json', 'r') as config_file:
        config = json.loads(config_file.read())

    output_lines = []
    for topic_id in sorted(config['test_topic_id_list']):
        print("starting download for {topic_id}"
              .format(topic_id=topic_id))
        rdf_url = (
            create_turtle_download_link(
                config['rdf_service_url'],
                api_key, topic_id))
        print(rdf_url)
        http_reply = requests.get(rdf_url)
        if http_reply.status_code != 200:
            print(http_reply.text)
            sys.exit(4)
        print("download OK")
        rdf_lines = http_reply.text.split('\n')
        full_triples = turtle_lines_to_rdf_lines(rdf_lines)
        output_lines.extend(sorted(full_triples))
    
    # saved preprocessed data
    output_file_name = os.path.join(
        config['test_data_directory'], 'sample_data.rdf')
    with open(output_file_name, 'wt') as output_file:
        output_file.writelines(output_lines)
    print("downloaded and prepared sample data into {}"
        .format(output_file_name))

if __name__ == "__main__":
    main()