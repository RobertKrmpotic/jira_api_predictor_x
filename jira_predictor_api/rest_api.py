import datetime
import os
import warnings
from loguru import logger
from flask import Flask
from flask_restful import Api, Resource, reqparse, abort, fields, marshal_with
from sqlalchemy.sql import text
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import types,create_engine
from sqlalchemy.pool import StaticPool
import pandas as pd
import json_to_df, model

warnings.simplefilter(action='ignore', category=FutureWarning)
app = Flask(__name__)
api = Api(app)
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///database.db'
table_name= "prediction_table"

db = SQLAlchemy(app)
engine = create_engine(
    "sqlite:///database.db", 
    connect_args={"check_same_thread": False}, 
    poolclass=StaticPool
)

class ResolveFake(Resource):
    def get (self,issue_key):
        result = pd.read_sql_query(f"SELECT issue_key from {table_name} WHERE issue_key = '{issue_key}'", app.config['SQLALCHEMY_DATABASE_URI'])
        print(result)
        if len (result) <1:
            abort(404, message="Could not find issue with that issue_key") 
        return {'issue_key' : f'{issue_key}','predicted_date' : '1970-01-01T00:00:00.000+0000'}, 200

class LoadData(Resource):
    def get (self):
        main()
        return {'dataload' : 'successful'}, 200
    
class ResolvePrediction(Resource):
    def get (self,issue_key):
        result = pd.read_sql_query(f"SELECT issue_key, predicted_resolved  from {table_name} WHERE issue_key = '{issue_key}'", app.config['SQLALCHEMY_DATABASE_URI'])
        if len (result) <1:
            abort(404, message="Could not find issue with that issue_key") 
        result = result.reset_index(drop=True)
        result["predicted_resolved"] = pd.to_datetime(result["predicted_resolved"] )
        predicted_date = result.at[0,"predicted_resolved"].to_pydatetime().isoformat()
        return {
            'issue' : f'{issue_key}','predicted_resolution_date' : f'{predicted_date}'
            }, 200

def check_date_format(provided_date):
    pass

class ResolvedSinceNow(Resource):
    def get (self,provided_date):
        check_date_format(provided_date)
        result = pd.read_sql_query(f"SELECT issue_key, predicted_resolved  from {table_name} WHERE issue_key != 'Done'", app.config['SQLALCHEMY_DATABASE_URI'])
        result = result.loc[result["predicted_resolved"]<provided_date]
        print(result)
        dict_list = []
        result = result.reset_index(drop=True)
        for i in range(0,len(result)):
            t_dict = {"issue":result.at[i,"issue_key"], "predicted_resolution_date": result.at[i,"predicted_resolved"] }
            dict_list.append(t_dict)
        if len (result) <1:
            abort(404, message="Could not find issue with that issue_key") 

        return {
                'now' : datetime.datetime.now().isoformat(),
                'issues' : dict_list
                }, 200

def write_predictions(predictions):
    predictions.to_sql(name=f'{table_name}', con=engine, index=False, dtype={'issue_key': types.VARCHAR(length=30),
                   'created': types.DateTime(),
                   'status': types.VARCHAR(length=30),
                   'predicted_resolved': types.DateTime()})
    logger.debug("created database")

def main():
    logger.debug("Running main.main")
    directory = os.getcwd()
    json_location = f"{directory}/jira_predictor_api/data/data.json"
    issues_df, transitions_df,counts_df  = json_to_df.get_dataframes_from_json(json_location)
    database_df = model.model_workflow(issues_df, transitions_df,counts_df)
    write_predictions(database_df)
    logger.debug("Ready to accept requests!")

issue_put_args = reqparse.RequestParser()
issue_put_args.add_argument("issue_key", type=str, help="issue_key of the tickets is required", required=True)
issue_put_args.add_argument("provided_date", type=str, help="provided_date is required (ISO format)", required=True)

api.add_resource(ResolveFake, "/api/issue/<string:issue_key>/resolve_fake")
api.add_resource(ResolvePrediction, "/api/issue/<string:issue_key>/resolve_prediction")
api.add_resource(ResolvedSinceNow, "/api/release/<string:provided_date>/resolved_since_now")
api.add_resource(LoadData, "/api/data/load")

if __name__ == "__main__":
    app.run(debug=True , host="0.0.0.0" ,port=8050)
    