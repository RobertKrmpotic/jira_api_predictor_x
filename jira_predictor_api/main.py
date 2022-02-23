import json_to_df, model
from loguru import logger
from flask import Flask
from flask_restful import Api, Resource, reqparse, abort, fields, marshal_with
from sqlalchemy.sql import text
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import create_engine
from sqlalchemy.pool import StaticPool
import pandas as pd
import os


app = Flask(__name__)
api = Api(app)
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///database.db'
db = SQLAlchemy(app)
engine = create_engine(
    "sqlite:///database.db", 
    connect_args={"check_same_thread": False}, 
    poolclass=StaticPool
)
class IssueModel(db.Model):
	issue_key = db.Column(db.String(100), primary_key=True)
	created = db.Column(db.String(100), nullable=True)
	status = db.Column(db.String(100), nullable=True)
	predicted_date = db.Column(db.String(100), nullable=True)

	def __repr__(self):
		return f"Issue (issue_key = {issue_key}, status = {status}, predicted_date = {predicted_date})"

resource_fake_fields = {
	'issue_key': fields.String,
	'predicted_date': fields.String}

resource_fields = {
	'issue_key': fields.String,
	'created': fields.String,
	'status': fields.String,
	'predicted_date': fields.String}

def add_data_to_database():
    #args = issue_put_args.parse_args()
    issue = IssueModel(issue_key="ALGO-9999", created="TEST", status="test", predicted_date="tested date")
    db.session.add(issue)
    db.session.commit()

class ResolveFake(Resource):
    def get (self,issue_key):
        result = pd.read_sql_query(f"SELECT * from predictions_table2 WHERE issue_key = '{issue_key}'", app.config['SQLALCHEMY_DATABASE_URI'])
        #result = IssueModel.query.filter_by(issue_key=issue_key).first()
        print(result)
        if not result:
            abort(404, message="Could not find issue with that issue_key")
        return {'issue_key' : f'{issue_key}','predicted_date' : '1970-01-01T00:00:00.000+0000'}, 200

class LoadData(Resource):
    def get (self):
        main()
        return {'dataload' : 'successful'}, 200
    
class ResolvePrediction(Resource):
    def get (self,issue_key):
        return {
            'issue' : f'{issue_key}','predicted_resolution_date' : '2010-01-01T00:00:00.000+0000'
            }, 200

def check_date_format(provided_date):
    pass
class ResolvedSinceNow(Resource):
    def get (self,provided_date):
        check_date_format(provided_date)
        return {
                'now' : '2013-05-27T09:33:23.123+0200',
                'issues' : 
                [
                {
                'issue' : 'AVRO-1333',
                'predicted_resolution_date' : '2013-09-07T09:24:31.761+0000'
                },
                {
                'issue' : 'AVRO-1335',
                'predicted_resolution_date' : '2013-09-12T09:24:31.761+0000'
                }
                ]
                }, 200

def write_predictions(predictions):
    db.drop_all()
    predictions.to_sql(name='predictions_table2', con=engine, index=False )
    logger.debug("created database")

def main():
    logger.debug("Running main.main")
    directory = os.getcwd()
    json_location = f"directory/data/data.json"
    issues_df, transitions_df,counts_df  = json_to_df.get_dataframes_from_json(json_location)
    database_df = model.model_workflow(issues_df, transitions_df,counts_df)
    write_predictions(database_df)

#db.drop_all()
#db.create_all()
issue_put_args = reqparse.RequestParser()
issue_put_args.add_argument("issue_key", type=str, help="issue_key of the tickets is required", required=True)
issue_put_args.add_argument("provided_date", type=str, help="provided_date is required (ISO format)", required=True)

api.add_resource(ResolveFake, "/api/issue/<string:issue_key>/resolve_fake")
api.add_resource(ResolvePrediction, "/api/issue/<string:issue_key>/resolve_prediction")
api.add_resource(ResolvedSinceNow, "/api/release/<string:provided_date>/resolved_since_now")
api.add_resource(LoadData, "/api/data/load")

if __name__ == "__main__":
    app.run(debug=True , port=8050)
    