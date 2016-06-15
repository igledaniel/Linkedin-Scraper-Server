import json

from flask import Flask

from ElasticsearchConnector import ElasticsearchConnector
from LinkedinController import LinkedinController

# Using Flask for RestAPIs
app = Flask(__name__)

esConn = ElasticsearchConnector()
linkedinTool = LinkedinController()
linkedinProfileUrlPrefix = 'https://www.linkedin.com/in/'


@app.route('/add/user/<username>')
def add_user_profile(username):
    profile = linkedinTool.extractProfile(linkedinProfileUrlPrefix + username)
    # Add the profile to Elasticsearch if it is a valid profile
    if linkedinTool.isProfileValid():
        esConn.add_json(profile)
    # Convert to printable JSON and show it as output
    return json.dumps(profile)


@app.route('/get/users_by/<property>/<value>')
def get_users_by(property, value):
    # Get users from Elasticsearch by the search of a profile property and its value
    return esConn.get_users_results_string(property, value)


@app.route('/get/users_by/top_score')
def get_users_by_top_score():
    # Get users from Elasticsearch's scoring query
    return esConn.get_users_top_score_results_string()


# Run this app only if chosen to run this file
if __name__ == '__main__':
    app.run()
