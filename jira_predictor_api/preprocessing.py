import pandas as pd
from scipy import stats
import numpy as np

#Careful some of these are stateful transformations

def loc_cols(df:pd.DataFrame, cols:list) ->pd.DataFrame:
    ''' Returns dataframe with only columns that have been provided in a list'''
    return df.loc[:,[cols]]

def cols_to_datetime(df:pd.DataFrame,cols:list) ->pd.DataFrame:
    ''' Transforms columns dtype to datetime, rest of the columns are passed through'''
    for col in cols:
        df.loc[:,col] = pd.to_datetime(df[col])
    return df

def filter_values_is(df:pd.DataFrame, column:str, values:list) ->pd.DataFrame:
    ''' Filters dataframe so that values is in column'''
    return df.loc[df[column].isin(values)]

def filter_values_is_not(df:pd.DataFrame, column:str, values:list) ->pd.DataFrame:
    ''' Filters dataframe so that values is NOT in column'''
    return df.loc[~df[column].isin(values)]

def replace_with_other(df:pd.DataFrame, column:str, top:int=3) ->pd.DataFrame:
    ''' Replaces all values in the column with Other except for top 3 most often'''
    top_list = df[column].value_counts()[:top].index.tolist()
    df.loc[~df[column].isin(top_list), column] = "Other"
    return df

def sutract_date_cols_hrs(df:pd.DataFrame, date_cols:tuple, new_col:str ) ->pd.DataFrame:
    ''' Takes a tuple of 2 dates and creates a new column which is a hours difference of col1 - col2 '''
    df[new_col] = (df[date_cols[0]] - df[date_cols[1]]).dt.total_seconds()/3600
    return df

def date_only_col(df:pd.DataFrame, date_col:str, new_col:str ) ->pd.DataFrame:
    ''' Takes datetime column and removes hours, minutes etc..'''
    df[new_col] =df[date_col].dt.date
    return df

def sort_values(df:pd.DataFrame, by:str, ascending=True) ->pd.DataFrame:
    ''' Sorts values in df by a column'''
    return df.sort_values(by=by, ascending=ascending)

def weekday_name(df:pd.DataFrame, col:str, new_col:str) ->pd.DataFrame:
    ''' Take datetime column and gives back the name of the weekday'''
    df[new_col] = df[col].dt.day_name()
    return df

def len_col(df:pd.DataFrame, column:str)->pd.DataFrame:
    df[f"{column}_length"] = df[column].str.len()
    return df

def add_lagging_features(df:pd.DataFrame, day_count:pd.DataFrame) ->pd.DataFrame:
    ''' Adds lagging features to the data, expects to have columns created_date'''
    #double check logic
    day_count["day"] = pd.to_datetime(day_count["day"]).dt.date
    opened_per_day = day_count.loc[day_count["status"].isin(["Open", "Reopened"])]
    resolved_per_day = day_count.loc[day_count["status"].isin(["Resolved","Closed"])]
    currently_open = opened_per_day.pivot_table(index="day", aggfunc="sum", values="count").fillna(0)

    resolved_or_closed = resolved_per_day.pivot_table(index="day", aggfunc="sum", values="count")
    resolved_yesterday = resolved_or_closed.diff().fillna(0)
    resolved_7_days = resolved_yesterday.rolling(7).sum()#.fillna(0)
    resolved_28_days = resolved_yesterday.rolling(28).sum()#.fillna(0)

    new_yesterday = day_count.pivot_table(index="day", values="count", aggfunc="sum").diff().fillna(0)
    new_7_days = new_yesterday.rolling(7).sum()#.fillna(0)
    #number of tickets in open status
    df["open_yesterday"] = df["created_date"].map(currently_open.to_dict()["count"])
    df["resolved_yesterday"] = df["created_date"].map(resolved_yesterday.to_dict()["count"])
    df["resolved_7_days"] = df["created_date"].map(resolved_7_days.to_dict()["count"])
    df["resolved_28_days"] = df["created_date"].map(resolved_28_days.to_dict()["count"])
    df["new_yesterday"] = df["created_date"].map(new_yesterday.to_dict()["count"])
    df["new_7_days"] = df["created_date"].map(new_7_days.to_dict()["count"])
    return df

def get_reopened_list(transitions:pd.DataFrame) -> list:
    reopened = list(transitions.loc[transitions["to_status"]=="Reopened"]["key"].unique())
    return reopened

def remove_reopened(issues_prep:pd.DataFrame,transitions:pd.DataFrame):
    reopened = get_reopened_list(transitions)
    issues_prep = issues_prep.loc[~issues_prep["key"].isin(reopened)]
    return issues_prep

def remove_outliers(issues_prep:pd.DataFrame, z_treshold=1) ->pd.DataFrame:
    issues_prep["z_score"] = np.abs(stats.zscore(issues_prep['ticket_duration']))
    issues_prep = issues_prep.loc[issues_prep["z_score"]<z_treshold]
    return issues_prep

def get_X_y(issues_prep):
    '''Takes a dataframe and only gives back the columns needed for model training'''
    X_cols = ["priority", "issue_type", "description_length", "summary_length", "watch_count", 'open_yesterday',
        'resolved_yesterday', 'resolved_7_days', 'resolved_28_days','new_yesterday', 'new_7_days', "created_weekday"]
    X = issues_prep.loc[:,X_cols]
    X["description_length"] = X["description_length"].fillna(0)
    y = issues_prep["ticket_duration"]
    return (X,y)

def run_issues_pipeline(issues:pd.DataFrame,day_count:pd.DataFrame) -> pd.DataFrame:
    print("running issues pipeline")
    print(f"original df shape = {issues.shape}")
    issues_prep = (
        issues
        #.pipe(filter_values_is, "status", ["Closed", "Resolved"])
        .pipe(cols_to_datetime, ["updated", "created", "resolutiondate"], )
        .pipe(sort_values, "created", False )
        .pipe(date_only_col, "created", "created_date")
        .pipe(weekday_name, "created", "created_weekday")
        .pipe(replace_with_other, "reporter", 5)
        .pipe(len_col,"description")
        .pipe(len_col,"summary")
        .pipe(sutract_date_cols_hrs, ("resolutiondate", "created"), "ticket_duration" )
        .pipe(add_lagging_features, day_count )
        )

    print(f"prepared df shape = {issues_prep.shape}")
    return issues_prep

