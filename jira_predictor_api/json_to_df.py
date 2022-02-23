import json
import pandas as pd
import datetime
from loguru import logger

def load_and_correct_json(location:str="data/data.json")->json:
    ''' Takes a location and loads json. The json is also corrected from {}{}{} to [{},{},{}]'''
    with open(location) as json_file:
        json_str = json_file.read()
        #correct the format
        new_str = json_str.replace('{"operations"', ',{"operations"')
        new_str = "[" + new_str[1:] + "]"
        contents = json.loads(new_str)
    return contents

def json_to_dataframe(contents:json) ->pd.DataFrame:
    ''' Takes a json and returns a dataframe'''
    issues_full_df = pd.json_normalize(contents)
    issues_full_df = issues_full_df.dropna(axis=1, how="all")
    return issues_full_df
    
def generate_transitions(df:pd.DataFrame) ->pd.DataFrame:
    ''' Takes a dataframe which contains history of the transitions locked in a columns and returns a dataframe where each row is a transition'''
    transitions_df = pd.DataFrame()
    transitions_cols=["created", "key", "fromString", "toString", "author.name"]
    for i, row in df.iterrows():
        #create initial transition from none to open
        first_status = [row["fields.created"], row["key"],"None","Open",row["fields.creator.key"]]
        transitions_df = transitions_df.append(pd.Series(first_status, index = transitions_cols), ignore_index=True)
        #load other transitions
        changes = pd.json_normalize(row["changelog.histories"])
        if len(changes) > 0:
            changes["key"] = row.loc["key"]
            items = pd.json_normalize(changes["items"])
            df_to_append=pd.DataFrame()
            for col in items.columns:
                temp_df = pd.json_normalize(items[col])
                df_to_append = pd.concat([df_to_append,temp_df],axis=0)
            df_to_append = df_to_append.loc[df_to_append["field"]=="status"]
            changes = pd.merge(changes,df_to_append,how="inner", left_index=True, right_index=True).loc[:,transitions_cols]
            transitions_df= pd.concat([transitions_df, changes], axis=0)

    transitions_df.columns=["when", "key", "from_status", "to_status", "reporter"]
    transitions_df = transitions_df.sort_values(by="when").reset_index(drop=True)
    transitions_df["when"] = pd.to_datetime(transitions_df["when"] )
    transitions_df["when_date"] = pd.to_datetime(transitions_df["when"]).dt.date
    
    transitions_df["when_date"] = pd.to_datetime(transitions_df["when_date"] )
    return transitions_df

def get_known_statuses(transitions_df:pd.DataFrame)->set:
    ''' Takes dataframe and returns a set of all unique status from both categories (from and to)'''
    known_statuses = set()
    unique_from = list(transitions_df["from_status"].unique())
    unique_to = list(transitions_df["to_status"].unique())

    for transition in unique_from:
        known_statuses.add(transition)
    for transition in unique_to:
        known_statuses.add(transition)
    return known_statuses

def generate_counts(transitions_df:pd.DataFrame)->pd.DataFrame:
    ''' Takes dataframe of transitions and gives back a dataframe with count of issues in a status per day'''
    count_df = pd.DataFrame()
    count_cols= ["day", "status", "count"]
    if len(transitions_df) > 0:

        known_statuses = get_known_statuses(transitions_df)
        issue_counts = { s : 0 for s in known_statuses }
        one_day = datetime.timedelta(days = 1)
        day = pd.Timestamp(transitions_df['when_date'].min())
        iloc_counter = 0
        line = transitions_df.iloc[iloc_counter,:]
        while day <= transitions_df['when_date'].max(): #before last day
            while line['when_date'] < day:
                issue_counts[line['from_status']] -= 1
                issue_counts[line['to_status']] += 1
                iloc_counter += 1
                line = transitions_df.iloc[iloc_counter,:]

            rows = [
                {
                    'day' : day.isoformat(),
                    'status' : k,
                    'count' : v
                } for k,v in issue_counts.items() if k != "None" ]
            for row in rows:
                count_df = count_df.append(pd.Series(row, index = count_cols), ignore_index=True)

            day += one_day
    return count_df

def trim_df(issues_full_df:pd.DataFrame)->pd.DataFrame:
    ''' Take a dataset and only loc the useful columns'''

    useful_cols=["key", "id","fields.creator.key", "fields.assignee.key", "fields.reporter.name", "fields.status.statusCategory.name" ,"changelog.total",  
            "fields.description", "fields.summary" , "fields.issuetype.name", "changelog.histories", "fields.resolutiondate", 	
            "fields.priority.name", "fields.watches.watchCount", "fields.created", "fields.updated" ,"fields.resolution.name"]
    issues_df = issues_full_df.loc[:,useful_cols]
    
    new_names=["key", "id","creator", "assignee", "reporter", "status" ,"changelog.total",  
            "description", "summary" , "issue_type", "changelog.histories", "resolutiondate",	
            "priority", "watch_count", "created", "updated" ,"resolution.name"]
    issues_df.columns = new_names
    return issues_df


def get_dataframes_from_json(json_location:str):#gives back tuple of dfs
    ''' Takes a json location and returns a dataframe containing issues information as well as one containing status count per day'''
    logger.debug("Loading json")
    issues_json = load_and_correct_json(json_location)
    issues_full_df = json_to_dataframe(issues_json)
    issues_df = trim_df(issues_full_df)
    changelog_df = issues_full_df.loc[:,["key","changelog.histories", "fields.created", "fields.creator.key"]]
    logger.debug("Generating transitions")
    transitions_df = generate_transitions(changelog_df)
    logger.debug("Generating counts")
    counts_df=generate_counts(transitions_df)
    logger.debug("Dataframes generated successfully!")
    return (issues_df, transitions_df,counts_df)