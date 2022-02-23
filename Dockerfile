FROM python:3.8

WORKDIR /app

ADD ./jira_predictor_api ./jira_predictor_api  
ADD poetry.lock .
ADD pyproject.toml .

RUN pip install .
CMD ["python3", "jira_predictor_api/rest_api.py"]