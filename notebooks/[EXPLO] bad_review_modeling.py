# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ### Proactive Customer Service with Review Prediction
# MAGIC
# MAGIC In the competitive world of e-commerce, customer satisfaction is paramount. Negative reviews can significantly impact brand reputation and sales. This project proposes a novel approach to proactively address potential customer dissatisfaction and prevent negative reviews.
# MAGIC
# MAGIC #### The Challenge:
# MAGIC
# MAGIC Traditional customer service models often rely on a reactive approach, waiting for customers to reach out with complaints.  Additionally, current systems frequently pick customers at random for post-purchase satisfaction surveys, potentially missing valuable feedback from high-risk customers.
# MAGIC
# MAGIC #### The Solution:
# MAGIC
# MAGIC This project introduces a system that leverages a machine learning model capable of predicting customers at high risk of leaving a negative review. This predictive power, combined with in-depth customer reviews analysis, empowers us to:
# MAGIC
# MAGIC - Identify at-risk customers: The model flags customers with a high likelihood of leaving a bad review based on purchase data and historical trends.
# MAGIC - Understand potential issues: By analyzing customer reviews, we can identify patterns and factors associated with low review scores.
# MAGIC - Prioritize outreach: We can prioritize outreach efforts to customers with the highest risk and most pressing concerns.
# MAGIC - Personalized communication: Insights from review analysis allow for tailoring communication to address specific customer concerns.
# MAGIC
# MAGIC #### Benefits:
# MAGIC
# MAGIC - Improved customer experience: Proactive outreach demonstrates a commitment to customer satisfaction and can lead to positive resolution before dissatisfaction escalates.
# MAGIC  - Reduced negative reviews: Addressing potential issues head-on can significantly decrease the number of negative reviews posted online.
# MAGIC - Increased customer retention: By preventing negative experiences and enhancing customer satisfaction, we can foster customer loyalty and encourage repeat business.
# MAGIC
# MAGIC This project represents a significant step forward  in proactive customer service strategies. By leveraging predictive models and customer insights, we can move beyond a random selection process and create a more targeted approach to address customer concerns before they escalate into negative reviews.

# COMMAND ----------

# MAGIC  %pip install imblearn

# COMMAND ----------

# MAGIC %pip install scikit-plot

# COMMAND ----------

# MAGIC %pip install feature-engine

# COMMAND ----------

# MAGIC %md
# MAGIC ### 0. Imports

# COMMAND ----------

from sklearn.model_selection import StratifiedKFold, KFold, GroupKFold, StratifiedGroupKFold
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.impute import SimpleImputer
from sklearn.tree import DecisionTreeClassifier



# COMMAND ----------

import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import numpy as np

from feature_engine.selection import RecursiveFeatureElimination

from sklearn.model_selection import StratifiedKFold, GridSearchCV, train_test_split
from sklearn.feature_selection import RFE
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import OneHotEncoder
from sklearn.metrics import (accuracy_score, precision_score, recall_score, 
                             f1_score, roc_auc_score, average_precision_score, 
                             confusion_matrix, classification_report)
from sklearn.utils.class_weight import compute_class_weight
from sklearn.dummy import DummyClassifier
from sklearn.ensemble import RandomForestClassifier
from sklearn.tree import DecisionTreeClassifier
from xgboost import XGBClassifier
import scikitplot as skplt
import mlflow
from databricks import feature_store


# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Functions

# COMMAND ----------

def evaluate_model(pipeline, X_test, y_test):
    """
    Evaluate a machine learning model using various evaluation metrics and plot the confusion matrix.

    Args:
        pipeline: Trained scikit-learn pipeline.
        X_test: Test features.
        y_test: Test labels.
    """
    # Make predictions on the test set
    y_pred = pipeline.predict(X_test)
    y_prob = pipeline.predict_proba(X_test)[:, 1]

    # Calculate evaluation metrics
    accuracy = accuracy_score(y_test, y_pred)
    precision = precision_score(y_test, y_pred)
    recall = recall_score(y_test, y_pred)
    f1 = f1_score(y_test, y_pred)
    roc_auc = roc_auc_score(y_test, y_prob)
    pr_auc = average_precision_score(y_test, y_prob)
    conf_matrix = confusion_matrix(y_test, y_pred)

    # Print evaluation metrics
    print(f"Accuracy: {accuracy:.4f}")
    print(f"Precision: {precision:.4f}")
    print(f"Recall: {recall:.4f}")
    print(f"F1 Score: {f1:.4f}")
    print(f"ROC-AUC: {roc_auc:.4f}")
    print(f"PR-AUC: {pr_auc:.4f}")
    print("\nConfusion Matrix:")
    print(conf_matrix)

    # Plot the confusion matrix
    sns.heatmap(conf_matrix, annot=True, fmt="d", cmap="Blues", cbar=False)
    plt.xlabel("Predicted Labels")
    plt.ylabel("True Labels")
    plt.title("Confusion Matrix")
    plt.show()

# COMMAND ----------



def log_run(gridsearch, features: list, experiment_name: str, run_name: str, run_index: int):
    """
    Logs the results of a GridSearchCV run to MLflow.

    Args:
        gridsearch (GridSearchCV): The GridSearchCV object.
        experiment_name (str): Name of the MLflow experiment.
        run_name (str): Name of the run.
        run_index (int): Index of the run in the cross-validation results.
    """
    # Check if the experiment exists, if not, create it
    experiment = mlflow.get_experiment_by_name(experiment_name)
    if experiment is None:
        mlflow.create_experiment(experiment_name)
        experiment = mlflow.get_experiment_by_name(experiment_name)
        print(f"Experiment '{experiment_name}' created.")
    
    cv_results = gridsearch.cv_results_
    with mlflow.start_run(run_name=run_name, experiment_id=experiment.experiment_id) as run:  
        mlflow.log_param("folds", gridsearch.cv)

        print("Logging parameters")
        params = list(gridsearch.param_grid.keys())
        for param in params:
            mlflow.log_param(param, cv_results["param_%s" % param][run_index])

        print("Logging Features")
        mlflow.log_param('features', features)


        print("Logging metrics")
        for score_name in [score for score in cv_results if "mean_test" in score]:
            mlflow.log_metric(score_name, cv_results[score_name][run_index])
            mlflow.log_metric(score_name.replace("mean","std"), cv_results[score_name.replace("mean","std")][run_index])

        run_id = run.info.run_uuid
        experiment_id = run.info.experiment_id
        print("runID: %s" % run_id)


# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Reading Data

# COMMAND ----------

# MAGIC %md
# MAGIC #### Reading Training Set

# COMMAND ----------

df_target = spark.sql(
"""

WITH tb as (
                SELECT DISTINCT t1.order_purchase_timestamp,
                                    CAST(date_format(t1.order_purchase_timestamp, 'yyyy-MM-dd') as DATE) as fs_reference_timestamp,
                                    t2.customer_unique_id,
                                    t1.order_id,
                                    CASE WHEN t3.review_score < 3 THEN 1 ELSE 0 END as bad_review_flag

                FROM silver.olist_orders AS t1
                LEFT JOIN silver.olist_customers AS t2
                ON t1.customer_id = t2.customer_id
                LEFT JOIN silver.olist_order_reviews as t3
                ON t1.order_id = t3.order_id
                WHERE t2.customer_unique_id IS NOT NULL

                )

  SELECT t1.*

  FROM tb as t1
  INNER JOIN feature_store.olist_orders_features as t2
  ON t1.order_id = t2.order_id
  INNER JOIN feature_store.olist_customer_features as t3
  ON t1.fs_reference_timestamp = t3.fs_reference_timestamp
  AND t1.customer_unique_id = t3.customer_unique_id


  WHERE t1.order_purchase_timestamp >= '2017-02-01'
  AND t1.order_purchase_timestamp < '2018-06-01'


    

"""
)

# COMMAND ----------

feature_lookups_orders =    feature_store.FeatureLookup(
                                table_name='feature_store.olist_orders_features',
                                lookup_key=['order_id'],
                                output_name='orders'
                                )

feature_lookups_customers = feature_store.FeatureLookup(
                                table_name='feature_store.olist_customer_features',
                                lookup_key=['fs_reference_timestamp', 'customer_unique_id'],
                                output_name='customers'
                                )
    

feature_lookups = [feature_lookups_orders, feature_lookups_customers]

# COMMAND ----------

from pyspark.sql.functions import col

feature_lookups = [
 
    feature_store.FeatureLookup(
        table_name='feature_store.olist_customer_features',
        lookup_key=['fs_reference_timestamp', 'customer_unique_id'],
        output_name='customers'
    ),
       feature_store.FeatureLookup(
        table_name='feature_store.olist_orders_features',
        lookup_key=['order_id'],
        output_name='orders'
    )
]
fs = feature_store.FeatureStoreClient()


training_set = fs.create_training_set(
    df=df_target,
    feature_lookups=feature_lookups,
    label='bad_review_flag'

)
df = training_set.load_df()

# COMMAND ----------

df = df.toPandas()

# COMMAND ----------

df.head()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Data Description

# COMMAND ----------

print("Number of rows: ", df.shape[0])
print("Number of columns: ", df.shape[1])

# COMMAND ----------

(df.isna().sum().sort_values(ascending = False) / len(df))

# COMMAND ----------

df.describe()

# COMMAND ----------

df['bad_review_flag'].value_counts(normalize = True)

# COMMAND ----------

df['month'] = df['order_purchase_timestamp'].dt.month
df['month'] = df['month'].astype(str)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Split Train-Test

# COMMAND ----------

target = 'bad_review_flag'

variables = list(set(df.columns.tolist()) - set([target]))


# COMMAND ----------

# Use train_test_split with stratification based on the target variable
train_data, test_data, train_labels, test_labels = train_test_split(
    df[variables], df[target], test_size=0.2, random_state=42
)

# COMMAND ----------

# Put the train and label into dataset for training
X_train, y_train= train_data.drop('customer_unique_id', axis = 1), train_labels

X_test, y_test= test_data.drop('customer_unique_id', axis = 1), test_labels


# COMMAND ----------

print('Shape of training:', X_train.shape, y_train.shape)
print('Shape of test:', X_test.shape, y_test.shape)

# COMMAND ----------

print('Train balance:')
print(y_train.value_counts(normalize = True))


# COMMAND ----------

# Compute class weights
class_weights = compute_class_weight('balanced', classes=np.unique(y_train), y=y_train)
class_weight_dict = dict(enumerate(class_weights))

# COMMAND ----------

class_weight_dict

# COMMAND ----------

print('Test balance:')
print(y_test.value_counts(normalize = True))


# COMMAND ----------

# MAGIC %md
# MAGIC ### 5. Feature Selection

# COMMAND ----------

cols_to_remove = ['order_id', 'customer_unique_id' 'fs_reference_timestamp', 'order_purchase_timestamp']
cols_to_remove = cols_to_remove + [var for var in variables if 'total' in var and ('items_90_days' in var or 'items_180_days' in var)]



variables = list(set(variables) - set(cols_to_remove))

# COMMAND ----------

# Features to use imputer and scaler
numeric_features = df[variables].select_dtypes(exclude = 'object').columns.tolist()



# Features to conver to ordinal encoding
categorical_features = ['customer_state', 'month']


# Numerical transformations
numeric_transformer = Pipeline(
    steps=[("imputer", SimpleImputer(strategy="median"))]
)

# Categorical transformations
categorical_transformer = Pipeline(
    steps=[
        ("encoder", OneHotEncoder(handle_unknown='ignore'))
]
)

# Preprocessing pipeline
preprocessor = ColumnTransformer(
    transformers=[
        ("num", numeric_transformer, numeric_features, ),
        ("cat", categorical_transformer, categorical_features),
    ],
    remainder='passthrough'
    
)


# COMMAND ----------

features_to_select = numeric_features + categorical_features

# COMMAND ----------

# Number of features to select with RFE
num_features_to_select = 15  # You can adjust this number based on your preference

# Updated pipeline
pipeline = Pipeline(steps=[
    ('preprocessor', preprocessor),
    ('feature_selection', RFE(estimator=DecisionTreeClassifier(), 
                              step = 1,    
                              n_features_to_select=num_features_to_select)),
])

# Fit the training data
pipeline.fit(X_train[features_to_select], y_train)

# COMMAND ----------

selected_features_mask = pipeline.named_steps['feature_selection'].support_


# Extract selected features from the original feature list
selected_features = [pipeline[0].get_feature_names_out()[i] for i, selected in enumerate(selected_features_mask) if selected]

# Display or use the selected features as needed
print("Selected Features:", selected_features)

# COMMAND ----------

selected_features = [feat.split('__')[1] for feat in selected_features if 'customer_state' not in feat.split('__')[1]]

selected_features = selected_features + ['customer_state', 'late_order']

# COMMAND ----------

selected_features = ['total_orders_late_delivery',
 'item_price',
 'total_spent_freight_180_days',
 'avg_days_to_deliver',
 'total_spent_price_180_days',
 'freight_price',
 'total_spent_freight_90_days',
 'total_spent_price_90_days',
 'total_price',
 'total_items',
 'total_orders_delivered_90_days',
 'pct_item_price',
 'pct_freight_price',
 'month',
 'customer_state',
 'late_order']

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### 5. Baseline

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Our baseline is based on the customer service actual method, which is basically a random sampling from all customers that bought an item and didn't review it. 

# COMMAND ----------

# Create and fit a dummy classifier
dummy_classifier = DummyClassifier(strategy="stratified")
dummy_classifier.fit(X_train, y_train)

# Make predictions
y_pred_dummy = dummy_classifier.predict(X_test[selected_features])

# COMMAND ----------

evaluate_model(dummy_classifier, X_test[selected_features], y_test)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6. Modeling

# COMMAND ----------

# MAGIC %md
# MAGIC #### Pre-processing

# COMMAND ----------

# Features to use imputer and scaler
numeric_features = X_train[selected_features].select_dtypes(exclude = 'object').columns.tolist()

categoric_features = ['month']


# Numerical transformations
numeric_transformer = Pipeline(
    steps=[("imputer", SimpleImputer(strategy="median"))]
)

# Categorical transformations
categorical_transformer = Pipeline(
    steps=[
        ("encoder", OneHotEncoder(handle_unknown='ignore'))
]
)

# Preprocessing pipeline
preprocessor = ColumnTransformer(
    transformers=[
        ("num", numeric_transformer, numeric_features, ),
        #("cat", categorical_transformer, categoric_features),
      
    ],
    remainder='passthrough'
    
)



# COMMAND ----------

selected_features_final = numeric_features

# COMMAND ----------

# MAGIC %md
# MAGIC #### Fit

# COMMAND ----------

skf = StratifiedKFold(n_splits=3, shuffle=True, random_state=42)

# Compute class weights
class_weights = compute_class_weight('balanced', classes=np.unique(y_train), y=y_train)
class_weight_dict = dict(enumerate(class_weights))


# Define your parameter grid for Random Forest
param_grid = {
    'max_depth': [5, 10],
    'min_samples_split': [2, 5, 10],
    'min_samples_leaf': [1, 2, 4],
}


# Create a Random Forest classifier
model = DecisionTreeClassifier(class_weight=class_weight_dict)


# Grid Search for hyperparameters tuning
grid_search = GridSearchCV( model, 
							param_grid,
							n_jobs = 1,
							cv = skf,
							scoring = 'roc_auc',
							verbose = 3,
							refit = True)

# Pipeline de dados
pipeline = Pipeline( steps = [('preprocessor', preprocessor),
							  ('gridsearch', grid_search)])

# Fit treino
pipeline.fit(X_train[selected_features_final], y_train)

# COMMAND ----------

grid_cv_res = pd.DataFrame(grid_search.cv_results_).sort_values('rank_test_score')
grid_cv_res

# COMMAND ----------

# MAGIC %md
# MAGIC #### Evaluation

# COMMAND ----------

evaluate_model(pipeline, X_train[selected_features_final], y_train)

# COMMAND ----------

evaluate_model(pipeline, X_test[selected_features_final], y_test)

# COMMAND ----------

y_probas = pipeline.predict_proba(X_test)

# COMMAND ----------

skplt.metrics.plot_roc(y_test, y_probas)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Feature Importance

# COMMAND ----------

pipeline[:-1].get_feature_names_out()

# COMMAND ----------

feature_names = [feat.split('__')[1] for feat in pipeline[:-1].get_feature_names_out()]

# COMMAND ----------


# Access the best estimator from the grid search
best_model = pipeline.named_steps['gridsearch'].best_estimator_

# Get feature importance from the best XGBoost model
feature_importance = best_model.feature_importances_

# Create a DataFrame with feature names and their importance scores
feature_importance_df = pd.DataFrame({'Feature': feature_names, 'Importance': feature_importance})

# Sort the DataFrame by importance scores in descending order
feature_importance_df = feature_importance_df.sort_values(by='Importance', ascending=False)

# Plot the feature importance
plt.figure(figsize=(10, 6))
sns.barplot(x='Importance', y='Feature', data=feature_importance_df, palette='viridis')
plt.title('Feature Importance')
plt.show()


# COMMAND ----------

# MAGIC %md
# MAGIC #### Tracking Experiment with MLFlow

# COMMAND ----------

experiment_name = "/Users/joaopaulo_brum@hotmail.com/mlflow-runs/Bad Review Model"
run_description = 'Final DT'


# Run the experiment in mlflow
log_run(pipeline[-1], selected_features_final, experiment_name, run_description, grid_cv_res.index[0])

# COMMAND ----------

# MAGIC %md
# MAGIC #### Validation by Folds

# COMMAND ----------

best_model = pipeline.named_steps['gridsearch'].best_estimator_
pipeline = Pipeline( steps = [('preprocessor', preprocessor),
							  ('model', best_model)])

# COMMAND ----------

grid_cv_res.iloc[0]['params']

# COMMAND ----------

# Initialize lists to store validation results
roc_auc_valid = []

X_train['month_year'] = X_train['order_purchase_timestamp'].dt.strftime('%m-%Y')
grouping_feature = 'month_year'



# Initialize StratifiedKFold
skf = StratifiedKFold(shuffle=True, n_splits=5).split(X=X_train, y=y_train)
skf = StratifiedGroupKFold().split(X=X_train, y=y_train, groups=X_train[grouping_feature])
# Iterate over each fold
for fold, (index_train, index_valid) in enumerate(skf):
    # validation models
    model_fit = pipeline.fit(X_train.iloc[index_train, :-1][selected_features_final], y_train.iloc[index_train])
    
    # Calculate ROC AUC for validation set
    roc_valid = roc_auc_score(y_train.iloc[index_valid], model_fit.predict_proba(X_train.iloc[index_valid, :-1][selected_features_final])[:, 1])
    
    # Store validation ROC AUC score
    roc_auc_valid.append(roc_valid)

# Calculate mean and standard deviation of validation ROC AUC scores
mean_roc_valid = np.mean(roc_auc_valid)
std_roc_valid = np.std(roc_auc_valid)

# Calculate ±3 sigma
lower_bound = mean_roc_valid - 3 * std_roc_valid
upper_bound = mean_roc_valid + 3 * std_roc_valid

# Print results
print("Mean ROC AUC:", mean_roc_valid)
print("Standard deviation of ROC AUC:", std_roc_valid)
print("Lower bound:", lower_bound)
print("Upper bound:", upper_bound)


# COMMAND ----------

# MAGIC %md
# MAGIC #### Out of Time Evaluation

# COMMAND ----------

df_oot = spark.sql(
"""


WITH tb as (
                SELECT DISTINCT t1.order_purchase_timestamp,
                                    CAST(date_format(t1.order_purchase_timestamp, 'yyyy-MM-dd') as DATE) as fs_reference_timestamp,
                                    t2.customer_unique_id,
                                    t1.order_id,
                                    CASE WHEN t3.review_score < 3 THEN 1 ELSE 0 END as bad_review_flag

                FROM silver.olist_orders AS t1
                LEFT JOIN silver.olist_customers AS t2
                ON t1.customer_id = t2.customer_id
                LEFT JOIN silver.olist_order_reviews as t3
                ON t1.order_id = t3.order_id
                WHERE t2.customer_unique_id IS NOT NULL

                )

            SELECT t1.*

            FROM tb as t1
            INNER JOIN feature_store.olist_orders_features as t2
            ON t1.order_id = t2.order_id
            INNER JOIN feature_store.olist_customer_features as t3
            ON t1.fs_reference_timestamp = t3.fs_reference_timestamp
            AND t1.customer_unique_id = t3.customer_unique_id


            WHERE t1.order_purchase_timestamp > '2018-06-01'


"""
)

# COMMAND ----------

from pyspark.sql.functions import col

feature_lookups = [
 
    feature_store.FeatureLookup(
        table_name='feature_store.olist_customer_features',
        lookup_key=['fs_reference_timestamp', 'customer_unique_id'],
        output_name='customers'
    ),
       feature_store.FeatureLookup(
        table_name='feature_store.olist_orders_features',
        lookup_key=['order_id'],
        output_name='orders'
    )
]
fs = feature_store.FeatureStoreClient()


oot_set = fs.create_training_set(
    df=df_oot,
    feature_lookups=feature_lookups,
    label='bad_review_flag'

)
validation = oot_set.load_df().toPandas()

# COMMAND ----------

evaluate_model(pipeline, validation[selected_features_final], validation['bad_review_flag'])

# COMMAND ----------

def fold_validation(pipeline, cv, X_train, y_train):

    # Initialize lists to store validation results
    roc_auc_valid = []

    X_train['month_year'] = X_train['order_purchase_timestamp'].dt.strftime('%m-%Y')
    grouping_feature = 'month_year'


    # Initialize StratifiedKFold
    cv = StratifiedGroupKFold().split(X=X_train, y=y_train, groups=X_train[grouping_feature])
    # Iterate over each fold
    for fold, (index_train, index_valid) in enumerate(cv):
        # validation models
        model_fit = pipeline.fit(X_train.iloc[index_train, :-1][selected_features_final], y_train.iloc[index_train])
        
        # Calculate ROC AUC for validation set
        roc_valid = roc_auc_score(y_train.iloc[index_valid], model_fit.predict_proba(X_train.iloc[index_valid, :-1][selected_features_final])[:, 1])
        
        # Store validation ROC AUC score
        roc_auc_valid.append(roc_valid)

    # Calculate mean and standard deviation of validation ROC AUC scores
    mean_roc_valid = np.mean(roc_auc_valid)
    std_roc_valid = np.std(roc_auc_valid)

    # Calculate ±3 sigma
    lower_bound = mean_roc_valid - 3 * std_roc_valid
    upper_bound = mean_roc_valid + 3 * std_roc_valid

    results_dict = {
        "Validation Mean ROC AUC": mean_roc_valid,
        "Standard deviation of ROC AUC": std_roc_valid,
        "Lower bound": lower_bound,
        "Upper bound": upper_bound
    }

    return results_dict


# COMMAND ----------

fold_validation(pipeline, X_train, y_train)
