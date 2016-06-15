import json

from elasticsearch import Elasticsearch


# Handles all the communication with Elasticsearch - adding and searching Linkedin profiles
class ElasticsearchConnector(Elasticsearch):
    es = Elasticsearch()

    esIndexName = 'profile'
    esTypeName = 'user'

    def add_json(self, json_element):
        self.es.create(self.esIndexName, self.esTypeName, json_element)

    def get_users_results_string(self, key, value):
        results = self.es.search(self.esIndexName, self.esTypeName, body={
            'query': {
                'match': {
                    key: value
                }
            }
        })
        return self.get_elastic_results_string(results)

    def get_users_top_score_results_string(self):
        results = self.es.search(self.esIndexName, self.esTypeName, body={
            'query': {
                'function_score': {
                    'functions': [
                        {
                            'field_value_factor': {
                                'field': 'RECOMMENDATIONS_NUMBER',
                                'factor': 1.5
                            }
                        }
                    ],
                    'query': {
                        'match': {
                            'EDUCATION.NAME': 'degree university college academy'
                        }
                    },
                    'score_mode': 'avg'
                }
            }
        })
        return self.get_elastic_results_string(results)

    def get_elastic_results_string(self, results):
        # Start response by the number of results from the Elastic's JSON response
        response = '[{"Number.of.results": %s},' % results['hits']['total']

        # Print results nicely, without ElasticSearch's metadata on the query result
        for doc in results['hits']['hits']:
            # Concat to final response each result profile
            response += '%s,' % json.dumps(doc['_source'])

        # Return result, ended by a valid JSON string (ending ',' needs {})
        return response + '{}]'
