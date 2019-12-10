
# ---
# name: web-extract-link
# deployed: true
# title: Website Link Extraction
# description: Returns the url and domain for all links on one-or-more webpages matching a search string.
# params:
# - name: url
#   type: string
#   description: Url of webpage to search
#   required: true
# - name: search
#   type: string
#   description: String
#   required: true
# - name: properties
#   type: array
#   description: The properties to return (defaults to all properties). See "Notes" for a listing of the available properties.
#   required: false
# - name: paginator
#   type: string
#   description: Optional paginator pattern string for extracting links on pages with similar web pages.
#   required: true
# examples:
# - '"https://www.flex.io", "Contact Us"'
# - '"https://news.ycombinator.com/", "Contact, link"'
# notes: |
#   The following properties are allowed:
#     * `link`: the link corresponding to the matched item
#     * `domain`: the domain corresponding to the matched item
# ---

import json
import requests
import urllib
from datetime import *
from cerberus import Validator
from collections import OrderedDict
from bs4 import BeautifulSoup

def flexio_handler(flex):

    # get the input
    input = flex.input.read()
    try:
        input = json.loads(input)
        if not isinstance(input, list): raise ValueError
    except ValueError:
        raise ValueError

    # define the expected parameters and map the values to the parameter names
    # based on the positions of the keys/values
    params = OrderedDict()
    params['url'] = {'required': True, 'type': 'string'}
    params['search'] = {'required': True, 'type': 'string'}
    params['properties'] = {'required': False, 'validator': validator_list, 'coerce': to_list, 'default': '*'}
    input = dict(zip(params.keys(), input))

    # validate the mapped input against the validator
    v = Validator(params, allow_unknown = True)
    input = v.validated(input)
    if input is None:
        raise ValueError

    property_map = OrderedDict()
    property_map['link'] = 'link'
    property_map['domain'] = 'domain'

    # get the properties to return and the property map
    properties = [p.lower().strip() for p in input['properties']]

    # if we have a wildcard, get all the properties
    if len(properties) == 1 and properties[0] == '*':
        properties = list(property_map.keys())

    try:

        # search url and text
        search_url = input['url']
        search_expression = input['search']

        # build up the result
        result = []
        result.append(properties)

        row = getPage(search_url, search_expression, properties)
        if row is not False:
            result.append(row)

        # return the result
        flex.output.content_type = "application/json"
        flex.output.write(result)

    except:
        flex.output.content_type = 'application/json'
        flex.output.write([['']])

def getPage(search_url, search_expression, properties):

    try:

        # get the contents from a URL
        response = requests.get(search_url)
        response.raise_for_status()
        content = response.text

        # extract the info and build up the result
        soup = BeautifulSoup(content, "lxml")

        for item in soup.findAll(True, text=search_expression):
            link, domain = '',''
            if item is not None and item.name == 'a':
                link = item.get('href','')
            else:
                parent = item.find_parent('a')
                if parent is not None and parent.name == 'a':
                    link = parent.get('href','')
            if len(link) == 0:
                continue

            # get a complete url; see here for info on urllib.parse: https://docs.python.org/3/library/urllib.parse.html
            link = urllib.parse.urljoin(search_url, link)
            domain = urllib.parse.urlparse(search_url)[1] # second item is the network location part of the url
            available_properties = {'link': link, 'domain': domain}

        row = [available_properties.get(p,'') for p in properties]
        return row

    except:
        return False

def validator_list(field, value, error):
    if isinstance(value, str):
        return
    if isinstance(value, list):
        for item in value:
            if not isinstance(item, str):
                error(field, 'Must be a list with only string values')
        return
    error(field, 'Must be a string or a list of strings')

def to_string(value):
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, (Decimal)):
        return str(value)
    return value

def to_list(value):
    # if we have a list of strings, create a list from them; if we have
    # a list of lists, flatten it into a single list of strings
    if isinstance(value, str):
        return value.split(",")
    if isinstance(value, list):
        return list(itertools.chain.from_iterable(value))
    return None
