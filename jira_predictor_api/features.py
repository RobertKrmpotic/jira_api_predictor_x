import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer

from sklearn.feature_selection import VarianceThreshold

from sklearn.metrics import mean_absolute_error, mean_absolute_percentage_error



def generate_features(X,y, X_test) ->pd.DataFrame: #train
    ''' Takes the X and y, imputes missing values and scales numeric features, for categorial features it implutes nas and one hot encodes the rest '''
    numeric_features = ['description_length', "summary_length", "open_yesterday", "resolved_yesterday" , "resolved_7_days", 
                    "resolved_28_days", "new_yesterday", "new_7_days"]
    numeric_transformer = Pipeline(steps=[
        ('imputer', SimpleImputer(strategy='median')),
        ('scaler', StandardScaler())])

    categorical_features = ["priority", "issue_type","created_weekday"]
    categorical_transformer = Pipeline(steps=[
        ('imputer', SimpleImputer(strategy='constant', fill_value='missing')),
        ('onehot', OneHotEncoder(handle_unknown='ignore'))])

    preprocessor = ColumnTransformer(
        transformers=[
            ('num', numeric_transformer, numeric_features),
            ('cat', categorical_transformer, categorical_features)])
    preprocessing_pipeline = Pipeline(steps=[('preprocessor', preprocessor)])
    preprocessing_pipeline.fit(X,y)
    X_trans = preprocessing_pipeline.transform(X)
    X_test_trans = preprocessing_pipeline.transform(X_test)
    X_trans_cols =  numeric_features +list(preprocessing_pipeline.named_steps['preprocessor'].transformers_[1][1].named_steps['onehot'].get_feature_names_out(categorical_features))  
    X_trans = pd.DataFrame(X_trans, columns=X_trans_cols)
    X_test_trans = pd.DataFrame(X_test_trans, columns=X_trans_cols)
    return (X_trans, X_test_trans)

def select_features(X_trans,y, threshold=0.2):
    ''' Selects featurs based on Variance treshold'''
    var = VarianceThreshold(threshold=threshold)
    var = var.fit(X_trans,y)
    X_trans = X_trans[X_trans.columns[var.get_support(indices=True)]]
    return X_trans

