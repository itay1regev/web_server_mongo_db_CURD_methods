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
