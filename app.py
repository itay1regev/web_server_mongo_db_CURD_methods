"""
How to use (Postman instruction): Please check the multi-line comment below.

:return:  key:value pairs (Dictionary/ Json - String Representation) with the keys:
* msg - relevance to the specific request (get/put/post/delete)
* is_formula_true - Boolean answer, whether the formula is True or False.

Only for get request:
* get_by_type_lst - The results, filter by sample_type.
* formula_agg - a json instance (String representation) , some aggregation functions (group by sample_type)
"""

"""
Postman instruction:

- Choose one form the list : ['GET', 'POST', 'UPDATE', 'DELETE'], (upper left corner).
- Enter the URL http://127.0.0.1:5000/db_api
- Choose 'Body', Choose 'form-data', Enter the key:val pairs.
>> e.g 'form-data': {'formula':'{pressure} < 100 or {volume} < 100', 'time_stamp':1545730073, 'sample_type': 'volume', 'vallue': 1]}
'time_agg': the time to take into account for the aggregation functions, multiply by 1 hour. 

* 'time_agg' examples: 1 >> 1 hour , 0.5 >> 30 min , 0.25 >>>> 15 min)
* 'formula' examples : {pressure} < 100 or {volume} < 100  >>>> boolean expression to check whether (T/F) pressure
is smaller than 100 and volume is smaller than 100. The value to be evaluated is the latest values that exist in 
the Time-Series DB.

- Our server returns a Json instance (String representation).
"""

"""
LINK For DB:
https://cloud.mongodb.com/v2/5e2bfc61ff7a25715ff87f29#metrics/replicaSet/5e2bfeff79358ee85c4189de/explorer/test/posts/find

Login to mongo:
Email: itay.regev@hotmail.com
Pass: mongomongo098

mongodb details:
AWS
North America - N.virginia
Cluster tier - 512 MB storage, shared RAM
Cluste name - cluste0
user name: itay , user pass: mongo1 , atlas admin 
Network access: allowed access from anywhere

URI node js: mongodb+srv://itay:<password>@cluster0-zcmuz.mongodb.net/test?retryWrites=true&w=majority
URI python 3.4 or later - MONGO_URI

Mongo db doc' : https://api.mongodb.com/python/current/tutorial.html
"""

from flask import Flask, jsonify, request
from flask_pymongo import PyMongo
from bson.objectid import ObjectId
from flask_cors import CORS
import pandas as pd
import pprint
import datetime
import sys

GET_MSG_STR = "You are using GET (Filter by sample_type)"
PUT_MSG_STR = "You are using PUT"
POST_MSG_STR = "You are using POST"
DELETE_MSG_STR = "You are using DELETE (Deletes all documents from the inventory collection)"

app = Flask(__name__)

app.config['MONGO_URI'] = 'mongodb://itay:mongo1@cluster0-shard-00-00-zcmuz.mongodb.net:27017,cluster0-shard-00-01' \
                          '-zcmuz.mongodb.net:27017,cluster0-shard-00-02-zcmuz.mongodb.net:27017/test?ssl=true&' \
                          'replicaSet=Cluster0-shard-0&authSource=admin&retryWrites=true&w=majority'

mongo = PyMongo(app)

CORS(app)

data_samples = mongo.db.data_samples



@app.route('/')
def index():
    """
    :return: msg to show - app.route
    """
    return '<h1> Please add to the url: /db_api  <h1>'


def get_from_db(dict_row):
    """
    :param dict_row: dictionary which represents the new data point.
    :return: The results - filter by sample_type.
    """
    lst_result = []
    if dict_row['sample_type'] is not None:
        for data_sample_i in data_samples.find({'sample_type': dict_row['sample_type']}):
            lst_result.append(data_sample_i)
    return lst_result


def delete_from_db():
    """
    Deletes all documents from the data_samples collection
    """

    mongo.db.data_samples.remove({})


def put_db(dict_row):
    """
    Put- add row
    :param dict_row: dictionary which represents the new data point.
    """

    data_samples.insert_one({'time_stamp': dict_row['time_stamp'], 'sample_type': dict_row['sample_type'],
                             'value': dict_row['value']}).inserted_id


def post_db(dict_row):
    """
    post- replace table with new row
    :param dict_row: A Dictionary which represents the new data point.
    """
    mongo.db.data_samples.remove({})
    data_samples.insert_one({'time_stamp': dict_row['time_stamp'], 'sample_type': dict_row['sample_type'],
                             'value': dict_row['value']}).inserted_id


def get_data():
    """
    :return: The data from the DB in a sorted DataFrame instance. (by DateTime instance)
    """
    df = pd.DataFrame(list(data_samples.find()))
    if len(df) > 0:
        df['date_time'] = df['time_stamp'].apply(
            lambda time_stamp: datetime.datetime.fromtimestamp(int(time_stamp)).isoformat())
        df = df.sort_values(by=['date_time'], ascending=False)
        return df
    return df


def xrange(x):
    """
    :param x: input for range function. (int)
    :return: Iterator from 0 to x, not including x.
    """
    return iter(range(x))


def get_last_values(type_str, df):
    """
    :param type_str: the type (string) which we would like to search.
    :param df: an instance of DataFrame.
    :return: The last data-point's value of the type_str. or (-1) if can't find it.
    """
    for i in xrange(len(df)):
        sample_type_cur = df.iloc[i, :]['sample_type']
        if sample_type_cur == type_str:
            return df.iloc[i, :]['value']
    return -1


def get_boolean_from_formula(formula_str_cur, df):
    """
    :param formula_str_cur: the received formula.
    :param df: a DataFrame instance.
    :return: Boolean answer, whether the formula is True or False.
    """
    if df is not None:
        if 'volume' in formula_str_cur:
            volume = float(get_last_values('volume', df))
        if 'pressure' in formula_str_cur:
            pressure = float(get_last_values('pressure', df))
        if 'temperature' in formula_str_cur:
            temperature = float(get_last_values('temperature', df))

        formula_to_eval = formula_str_cur.replace('{pressure}', 'pressure').replace('{volume}', 'volume').replace(
            '{temperature}', 'temperature')
        return eval(formula_to_eval)
    return True


def agg_formula(dict_agg_formula):
    """
    :param dict_agg_formula: a dictionary which represents the demanded aggregation formula.
    :return: an instance of json which we will later convert to String.
    """
    df = get_data()
    # The default aggregation time is 1 hour.
    time_agg = '1H'
    if 'time_agg' in dict_agg_formula.keys():
        time_agg = str(dict_agg_formula['time_agg']) + 'H'

    if df is not None:
        df = df.dropna(subset=['sample_type'])

        if len(df) == 0:
            return
        # get rows from the last {time_agg}.
        df['date_time'] = pd.to_datetime(df['date_time'])
        df = (df.loc[df.date_time >= pd.datetime.now() - pd.Timedelta(time_agg), :])
        try:
            if len(df) > 0:
                df = df.loc[:, ['sample_type', 'value']]
                # To add some more agg' methods such as: 'mean' and 'std'.
                df = df.groupby('sample_type').agg(['min', 'max', 'count'])
                return df.to_json()
        except:
            print("Unexpected error:", sys.exc_info()[0])
            return


def check_na_formula(df, formula_str):
    """
    :param df: The collected data - from the DB. DataFrame instance.
    :param formula_str: The formula which we would like to check.
    :return: True if the formula is based on 'NA parameters'. Otherwise, False.
    """
    sample_type_lst = ['pressure', 'volume', 'temperature']
    for type in sample_type_lst:
        if (len(df.loc[df.sample_type == type, :]) == 0) and type in formula_str:
            return True
    return False


def parse_request_form(dict_form):
    """
    :param dict_form: a dictionary which represents the request's form.
    :return: the request's form values: time_stamp, sample_type, value.
    """
    if 'time_stamp' in dict_form.keys():
        time_stamp = request.form.to_dict()['time_stamp']
    if 'sample_type' in dict_form.keys():
        sample_type = request.form.to_dict()['sample_type']
    if 'value' in dict_form.keys():
        value = request.form.to_dict()['value']
    return time_stamp, sample_type, value


@app.route("/db_api", methods=['GET', 'POST', 'PUT', 'DELETE'])
def request_handler():
    """
    How to use (Postman instruction): Please check the multi-line comment below.

    :return:  key:value pairs (Dictionary/ Json - String Representation) with the keys:
    * msg - relevance to the specific request (get/put/post/delete)
    * is_formula_true - Boolean answer, whether the formula is True or False.

    Only for get request:
    * get_by_type_lst - The results, filter by sample_type.
    * formula_agg - a json instance (String representation) , some aggregation functions (group by sample_type)
    """

    """
    Postman instruction:
    
    - Choose one form the list : ['GET', 'POST', 'UPDATE', 'DELETE'], (upper left corner).
    - Enter the URL http://127.0.0.1:5000/db_api
    - Choose 'Body', Choose 'form-data', Enter the key:val pairs.
    >> e.g 'form-data': {'formula':'{pressure} < 100 or {volume} < 100', 'time_stamp':1545730073, 'sample_type': 'volume', 'vallue': 1]}
    'time_agg': the time to take into account for the aggregation functions, multiply by 1 hour. 
    
    * 'time_agg' examples: 1 >> 1 hour , 0.5 >> 30 min , 0.25 >>>> 15 min)
    * 'formula' examples : {pressure} < 100 or {volume} < 100  >>>> boolean expression to check whether (T/F) pressure
    is smaller than 100 and volume is smaller than 100. The value to be evaluated is the latest values that exist in 
    the Time-Series DB.

    - Our server returns a Json instance (String representation).
    """

    df = get_data()
    if 'time_agg' in request.form.to_dict().keys():
        time_agg = request.form.to_dict()['time_agg']

    formula_str = request.form.to_dict()['formula']
    if check_na_formula(df, formula_str):
        return 'Please check the formula'

    boolean_answer = get_boolean_from_formula(formula_str, df)
    time_stamp, sample_type, value = parse_request_form(request.form.to_dict())

    # Put-add row, Post-replace table with new row, Delete- delete all the rows from the table.
    if request.method == 'POST':
        if time_stamp is None or sample_type is None or value is None:
            return 'Please check the request.form params'
        post_db({'time_stamp': time_stamp, 'sample_type': sample_type, 'value': value})
        return {'msg': POST_MSG_STR, 'is_formula_true': str(boolean_answer)}
    elif request.method == 'PUT':
        if time_stamp is None or sample_type is None or value is None:
            return 'Please check the request.form params'
        put_db({'time_stamp': time_stamp, 'sample_type': sample_type, 'value': value})
        return {'msg': PUT_MSG_STR, 'is_formula_true': str(boolean_answer)}
    elif request.method == 'DELETE':
        delete_from_db()
        return {'msg': DELETE_MSG_STR, 'is_formula_true': str(boolean_answer)}
    elif request.method == 'GET':
        json_agg_formula = agg_formula({'time_agg': time_agg})
        get_by_type_lst = get_from_db({'time_stamp': time_stamp, 'sample_type': sample_type, 'value': value})
        return {'msg': GET_MSG_STR, 'is_formula_true': str(boolean_answer),
                'get_by_type_lst': str(get_by_type_lst), 'formula_agg': str(json_agg_formula)}


if __name__ == '__main__':
    app.run()
