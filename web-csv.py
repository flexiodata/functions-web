
# ---
# name: web-csv
# deployed: true
# title: CSV Reader
# description: Returns the data for the CSVs given by the URLs
# params:
# - name: url
#   type: array
#   description: Urls for which to get the info
#   required: true
# examples:
# - '"https://raw.githubusercontent.com/flexiodata/data/master/sample/sample-contacts.csv"'
# notes:
# ---

import csv
import json
import tempfile
import io
import aiohttp
import asyncio
import itertools
from cerberus import Validator
from contextlib import closing
from collections import OrderedDict

def flexio_handler(flex):

    # get the input
    input = flex.input.read()
    input = json.loads(input)
    if not isinstance(input, list):
        raise ValueError

    # define the expected parameters and map the values to the parameter names
    # based on the positions of the keys/values
    params = OrderedDict()
    params['urls'] = {'required': True, 'validator': validator_list, 'coerce': to_list}
    #params['columns'] = {'required': True, 'validator': validator_list, 'coerce': to_list}
    input = dict(zip(params.keys(), input))

    # validate the mapped input against the validator
    v = Validator(params, allow_unknown = True)
    input = v.validated(input)
    if input is None:
        raise ValueError

    urls = input['urls']
    loop = asyncio.get_event_loop()
    temp_fp_all = loop.run_until_complete(fetch_all(urls))

    flex.output.content_type = 'application/json'
    flex.output.write('[')

    # get the columns for each of the input urls
    properties = []
    for temp_fp in temp_fp_all:
        try:
            fp = io.TextIOWrapper(temp_fp, encoding='utf-8-sig')
            reader = csv.DictReader(fp, delimiter=',', quotechar='"')
            for row in reader:
                properties = list(row.keys())
                break
        finally:
            fp.seek(0)
            fp.detach()

    flex.output.write(json.dumps(properties))

    for temp_fp in temp_fp_all:
       fp = io.TextIOWrapper(temp_fp, encoding='utf-8-sig')
       reader = csv.DictReader(fp, delimiter=',', quotechar='"')
       for row in reader:
           row = ',' + json.dumps([(row.get(p) or '') for p in properties])
           flex.output.write(row)
       temp_fp.close()

    flex.output.write(']')

async def fetch_all(urls):
    tasks = []
    async with aiohttp.ClientSession() as session:
        for url in urls:
            tasks.append(fetch(session, url))
        temp_fp_all = await asyncio.gather(*tasks)
        return temp_fp_all

async def fetch(session, url):
    # stream the data from the url into a temporary file and return
    # it for processing, after which it'll be closed and deleted
    temp_fp = tempfile.TemporaryFile()
    async with session.get(url) as response:
        while True:
            data = await response.content.read(1024)
            if not data:
                break
            temp_fp.write(data)
        temp_fp.seek(0) # rewind to the beginning
        return temp_fp

def validator_list(field, value, error):
    if isinstance(value, str):
        return
    if isinstance(value, list):
        for item in value:
            if not isinstance(item, str):
                error(field, 'Must be a list with only string values')
        return
    error(field, 'Must be a string or a list of strings')

def to_list(value):
    # if we have a list of strings, create a list from them; if we have
    # a list of lists, flatten it into a single list of strings
    if isinstance(value, str):
        return value.split(",")
    if isinstance(value, list):
        return list(itertools.chain.from_iterable(value))
    return None
