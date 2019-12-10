
# ---
# name: web-newspaper
# deployed: true
# title: Newspaper
# description: Returns content information from a web page article
# params:
# - name: url
#   type: string
#   description: Url for the article for which to get the info
#   required: true
# - name: properties
#   type: array
#   description: The properties to return (defaults to all properties). See "Notes" for a listing of the available properties.
#   required: false
# examples:
# - '"https://www.flex.io"'
# - '"https://www.flex.io", "text"'
# - '"https://www.flex.io", "title, top_image"'
# notes: |
#   The following properties are allowed:
#     * `title`: the main title of the page article
#     * `authors`: the authors of the page article
#     * `publish_date`: the publish date of the page article
#     * `text`: the text of the page article
#     * `top_image`: the top image url for the page article
#     * `images`: a comma-delimited list of image urls for the page article
#     * `movies`: a comma-delimited list of movie urls for the page article
# ---

import json
import requests
import itertools
from datetime import *
from cerberus import Validator
from collections import OrderedDict
from newspaper import Article

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
    params['properties'] = {'required': False, 'validator': validator_list, 'coerce': to_list, 'default': 'title'}
    input = dict(zip(params.keys(), input))

    # validate the mapped input against the validator
    v = Validator(params, allow_unknown = True)
    input = v.validated(input)
    if input is None:
        raise ValueError

    property_map = OrderedDict()
    property_map['title'] = 'title'
    property_map['authors'] = 'authors'
    property_map['publish_date'] = 'publish_date'
    property_map['text'] = 'text'
    property_map['top_image'] = 'top_image'
    property_map['images'] = 'images'
    property_map['movies'] = 'movies'

    # get the properties to return and the property map
    properties = [p.lower().strip() for p in input['properties']]

    # if we have a wildcard, get all the properties
    if len(properties) == 1 and properties[0] == '*':
        properties = list(property_map.keys())

    try:

        # get the article
        url = input['url']
        headers = {
            'User-Agent': 'Flex.io'
        }
        response = requests.get(url, headers=headers)  # fetch the content manually; this will let us easily extend the example to make multiple async requests
        response.encoding = response.apparent_encoding # use the apparent encoding when accessing response text

        article = Article(response.url, language='en')
        article.download(input_html=response.text)
        article.parse()

        # return the result
        info = {}
        info['title'] = article.title
        info['authors'] = ','.join(article.authors)
        info['publish_date'] = article.publish_date
        info['text'] = article.text
        info['top_image'] = article.top_image
        info['images'] = ','.join(article.images)
        info['movies'] = ','.join(article.movies)

        #article.nlp()
        #info['summary'] = article.summary
        #info['keywords'] = ';'.joins(article.keywords)

        # limit the results to the requested properties
        result = [[info.get(property_map.get(p,''),'') or '' for p in properties]]

        # return the results
        result = json.dumps(result, default=to_string)
        flex.output.content_type = "application/json"
        flex.output.write(result)

    except:
        raise RuntimeError

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
