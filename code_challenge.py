from flask import Flask, request, jsonify
import requests
import json
import pandas as pd

app = Flask(__name__)

class InvalidUsage(Exception):
    status_code = 400

    def __init__(self, message, status_code=None, payload=None):
        Exception.__init__(self)
        self.message = message
        if status_code is not None:
            self.status_code = status_code
        self.payload = payload

    def to_dict(self):
        rv = dict(self.payload or ())
        rv['message'] = self.message
        return rv

@app.errorhandler(InvalidUsage)
def handle_invalid_usage(error):
    response = jsonify(error.to_dict())
    response.status_code = error.status_code
    return response

@app.route('/signals/norm/<id>', methods=['GET'])
def norm(id):
    try:
        id_num = int(id)
    except ValueError:
        raise InvalidUsage('id must be an integer', status_code=400)

    if id_num < 1 or id_num > 6:
        raise InvalidUsage('id must be between 1 and 6 inclusive', status_code=400)

    response = requests.get("http://predata-challenge.herokuapp.com/signals/" + str(id_num))

    df = pd.DataFrame.from_dict(response.json(), orient="columns")
    
    #turn data column into index:
    date = df.pop("date")
    df.index = date
    
    #Min-max normalization:
    norm_df = 100*(df-df.min())/(df.max()-df.min()) 
    
    #data json is formatted according to table schema, has extra key "schema":
    data = norm_df.to_json(orient="table") 
    
    #convert to dict and extract the desired "data" dict:
    data_dict = json.loads(data)["data"] 
    
    #convert back to json
    return json.dumps(data_dict) 

@app.route('/signals/combine/', methods=['GET'])
def lin_comb():
    id_weight_pairs = request.args.getlist('signal') #list of strings of the form "<id>,<weight>"

    #create dict mapping signal ids to weights:
    weights = {}
    for elem in id_weight_pairs:
        pair = elem.split(",") #list of the form [<id>, <weight>]

        try:
            id_num = int(pair[0])
        except ValueError:
            raise InvalidUsage('id must be an integer', status_code=400)

        if id_num < 1 or id_num > 6:
            raise InvalidUsage('id must be between 1 and 6 inclusive', status_code=400)


        try:
            weight= float(pair[1])
        except ValueError:
            raise InvalidUsage('weight must be a float', status_code=400)
        
        weights[id_num] = weight

    num_signals = len(id_weight_pairs)
    
    #create dict mapping signal ids to dataframes:
    df_dict = {}
    for i in range(num_signals):
        id_num = i+1 #indexing starts at 0, shift id_num by 1

        response = requests.get("http://predata-challenge.herokuapp.com/signals/" + str(id_num))

        df = pd.DataFrame.from_dict(response.json(), orient="columns")
    
        #turn data column into index:
        date = df.pop("date")
        df.index = date
    
        df_dict[id_num] = df

    #set new_df to the weighted dataframe corresponding to first id:
    new_df = weights[1] * df_dict[1]

    #add desired weighted dataframes to new_df:
    for i in range(0, num_signals-1): #exclude last id to avoid over-indexing
        id_num = i+2 #indexing starts at 0, shift id_num by 1; exclude 1st id since it has been added
        
        df_add = weights[id_num] * df_dict[id_num] #weighted dataframe that is being added
        
        new_df += df_add
        
    #data json is formatted according to table schema of the form {schema: {}, data: {}}:
    data = new_df.to_json(orient="table") 
    
    #convert to dict and extract the desired "data" dict:
    data_dict = json.loads(data)["data"] 
    
    #convert back to json
    return json.dumps(data_dict)

