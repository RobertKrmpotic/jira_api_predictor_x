### About

This package includes a machine learning model which predicts completion time of a JIRA ticket.
This can be accessed using the API
The whole process can be understood by following main function in rest_api
- API is started and awaits for load_data api call
- Data is loaded in json format and converted to pandas dataframe
- Data is preprocessed and selected for training
- Features are generated and selected
- Multiple model are test and the best on is selected
- That model has its hyperparameters tuned and is trained
- Predictions are generated for entire dataset and are written in a database
- API is ready to take other calls

### Requirements
Since the entire program is packaged in docker there should only be 1 requirement:
Having docker installed and running 
https://docs.docker.com/get-docker/

### How to get the API running
1. build docker image
open command prompt and cd into the this folder and run: 
docker build -t flask-rest-api .

Grab a drink, this might take a minute.

2. run the docker image:
docker run -d -p 8050:8050 flask-rest-api

this will start a container from the image in the background and provide the output of container id
in case you wish to check on progress you can run
docker logs <containerID>

3. Load the data
In your browser run:
http://localhost:8050/api/data/load
This starts the whole flow where the data is transformed and loaded, model is trained and predictions are written to the database
Ths will take a few minutes and you should receive sucessful response once its done.
Check the logs if you wish to see the progress

4. Test the api connection
In your browser run:

http://localhost:8050/api/issue/AVRO-1/resolve_fake

5. Use the endpoints:

/api/issue/<issue_key>/resolve_fake
  
This is a basic call that runs a (fake) hardcoded resolution for a single issue.
replace issue key with string corresponding to jira issue

/api/issue/<issue_key>/resolve_prediction
  
This call returns a predicted 

/api/release/<date provided>/resolved_since_now
  
date can be in iso format or YYYY-MM-DD for example:

2015-10-01T11:25:13.635193
or
2015-10-01


### How to change the data
if you wish to change the data all you have to do is change the data.json file in the data folder.
Then go back to step one do steps until step 5

The program expects json file to be in the same format meaning {}{}{}
To retrieve issues from jira you can use 
(https://github.com/godatadriven/jiraview).

### Notes
Note: the container will say it is running on http://172.17.0.2:8050/, but this does not map to the same address outside of container.

you can stop the container by command
docker stop <containerID>
