
# ---
# name: newspaper
# deployed: true
# title: Newspaper
# description: Returns content information from a web page
# params:
# - name: url
#   type: string
#   description: Url for which to get the info
#   required: true
# - name: columns
#   type: array
#   description: Information to return; default returns 'title'
#   required: false
# examples:
# - '"https://www.flex.io"'
# - '"https://www.flex.io", "text"'
# - '"https://www.flex.io", "title, top_image"'
# - '"https://www.flex.io", A1:A3'
# notes:
#   The following columns are allowed: title, authors, publish_date, text, top_image, images, movies
# ---

import json
import requests
from datetime import *
from decimal import *
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
    params['columns'] = {'required': False, 'validator': validator_list, 'coerce': to_list, 'default': 'title'}
    input = dict(zip(params.keys(), input))

    # validate the mapped input against the validator
    v = Validator(params, allow_unknown = True)
    input = v.validated(input)
    if input is None:
        raise ValueError

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

        # limit the results to the requested columns
        columns = [c.lower().strip() for c in input['columns']]
        result = [info.get(c,'') for c in columns]

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
