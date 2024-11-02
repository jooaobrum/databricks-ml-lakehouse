# Databricks notebook source
# MAGIC  %pip install imblearn

# COMMAND ----------

# MAGIC %pip install scikit-plot
# MAGIC %pip install feature-engine
# MAGIC %pip install --upgrade pandas 
# MAGIC %pip install xgboost
# MAGIC %pip install mlflow
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 0. Imports

# COMMAND ----------

import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import numpy as np

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
from imblearn.under_sampling import RandomUnderSampler
from imblearn.pipeline import Pipeline as Pipeline
from sklearn.dummy import DummyClassifier
from sklearn.ensemble import RandomForestClassifier
from sklearn.tree import DecisionTreeClassifier
import scikitplot as skplt
import mlflow
import os

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
        params_to_log = ['split0_test_score','split1_test_score','split2_test_score','split3_test_score','split4_test_score', 'split5_test_score', 'mean_test_score', 'std_test_score']

        for score_name in [score for score in params_to_log if score in cv_results.keys()]:
            mlflow.log_metric(score_name, cv_results[score_name][run_index])
            mlflow.log_metric(score_name.replace("mean","std"), cv_results[score_name.replace("mean","std")][run_index])

        run_id = run.info.run_uuid
        experiment_id = run.info.experiment_id
        print("runID: %s" % run_id)

# COMMAND ----------

cv_results.keys()

# COMMAND ----------

cv_results = pipeline[-1].cv_results_

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Reading Data

# COMMAND ----------

# MAGIC %md
# MAGIC #### Reading Training Set

# COMMAND ----------

# DBTITLE 1,Build Dataset
query_features = """

with table_order as (

  SELECT t1.order_delivered_customer_timestamp as dt_ref,
        t1.order_id,
        AVG(t3.prod_average_review_score) as product_avg_review_score,
        AVG(t4.seller_total_orders) as seller_total_orders,
        AVG(t4.seller_average_review_score) as seller_avg_review_score
      
        

  FROM olist_silver.olist_orders as t1
  LEFT JOIN olist_silver.olist_order_items as t2
  ON t1.order_id = t2.order_id
  LEFT JOIN olist_feature_stores.olist_product_review_hist_features as t3
  ON t1.order_delivered_customer_timestamp = t3.dt_ref
  AND t2.product_id = t3.product_id
  LEFT JOIN olist_feature_stores.olist_seller_review_hist_features as t4
  ON t1.order_delivered_customer_timestamp = t4.dt_ref
  AND t2.seller_id = t4.seller_id

  WHERE t1.order_status = 'delivered'


  GROUP BY t1.order_delivered_customer_timestamp, t1.order_id
)

SELECT DISTINCT t1.dt_ref,
       t1.order_id,
       t1.product_avg_review_score,
       t1.seller_total_orders,
       t1.seller_avg_review_score,
       t2.days_to_estimation as order_day_to_estimation,
      t2.delivery_status as order_delivery_status,
      t2.days_after_carrier as order_days_after_carrier,
      t2.days_to_shipping_limit as order_days_to_shipping_limit,
      t2.shipping_status as order_shipping_status,
      t2.n_items as order_n_items,
      t2.items_price as order_items_price,
      t2.freight_price as order_freight_price,
      t2.total_order_price as order_total_price,
      t2.freight_proportion_price as order_freight_proportion_price,
      t2.payment_type as order_payment_type,
      t2.payment_installments as order_payment_installments,
      t3.beauty_health,
      t3.arts_entertainment,
      t3.sports_leisure,
      t3.baby_kids,
      t3.home_furniture,
      t3.electronics_appliances,
      t3.technology_gadgets,
      t3.construction_tools,
      t3.automotive_industry,
      t3.fashion_accessories,
      t3.avg_order_prod_description_length,
      t3.avg_order_photos_qty,
      t3.total_order_weight



FROM table_order as t1
INNER JOIN olist_feature_stores.olist_orders_delivered_features as t2
ON t1.dt_ref = t2.dt_ref
AND t1.order_id = t2.order_id
INNER JOIN olist_feature_stores.olist_products_features as t3
ON t1.dt_ref = t3.dt_ref
AND t1.order_id = t3.order_id

"""

df_features = spark.sql(query_features)


query_target = """
SELECT t1.order_delivered_customer_timestamp,
       t1.order_id,
       CASE WHEN t2.review_score < 3 THEN 1 ELSE 0 END as bad_review
FROM olist_silver.olist_orders as t1
LEFT JOIN olist_silver.olist_order_reviews as t2
ON t1.order_id = t2.order_id

WHERE t2.review_score IS NOT NULL
"""

df_target = spark.sql(query_target)

df = df_features.join(df_target, ['order_id'], 'inner')

df = df.toPandas()

# COMMAND ----------

df.head()

# COMMAND ----------

df['dt_ref'] = df['dt_ref'].astype(str)

# COMMAND ----------

df_train = df[df['dt_ref'] < '2018-06-01'] 
df_oot = df[df['dt_ref'] > '2018-06-01'] 

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Data Description

# COMMAND ----------

print("Number of rows: ", df_train.shape[0])
print("Number of columns: ", df_train.shape[1])

# COMMAND ----------

(df_train.isna().sum().sort_values(ascending = False) / len(df_train))

# COMMAND ----------

df_train.describe()

# COMMAND ----------

df_train['bad_review'].value_counts(normalize = True)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pre-Processing

# COMMAND ----------

df_train = df_train.drop('order_delivered_customer_timestamp', axis = 1)
df_oot = df_oot.drop('order_delivered_customer_timestamp', axis = 1)

df_train['order_n_items'] = df_train['order_n_items'].astype(int)
df_oot['order_n_items'] = df_oot['order_n_items'].astype(int)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Split Train-Test

# COMMAND ----------

target = 'bad_review'

variables = list(set(df_train.columns.tolist()) - set([target]))


# COMMAND ----------

# Use train_test_split with stratification based on the target variable
train_data, val_data, train_labels, val_labels = train_test_split(
    df_train[variables], df_train[target], test_size=0.3, random_state=42, stratify=df_train[target]
)

# COMMAND ----------

# Put the train and label into dataset for training
X_train, y_train= train_data.drop(['dt_ref', 'order_id'], axis = 1), train_labels

X_val, y_val= val_data.drop(['dt_ref', 'order_id'], axis = 1), val_labels


# COMMAND ----------

print('Shape of training:', X_train.shape, y_train.shape)
print('Shape of test:', X_val.shape, y_val.shape)

# COMMAND ----------

print('Train balance:')
print(y_train.value_counts(normalize = True))


# COMMAND ----------

print('Validation balance:')
print(y_val.value_counts(normalize = True))


# COMMAND ----------

# MAGIC %md
# MAGIC ### 5. Feature Selection

# COMMAND ----------

X_train.select_dtypes(exclude = 'object').columns

# COMMAND ----------

from feature_engine.imputation import ArbitraryNumberImputer
from sklearn.preprocessing import OneHotEncoder, StandardScaler

# COMMAND ----------

# Features to use imputer and scaler
numeric_features = ['construction_tools', 'automotive_industry',
                    'order_freight_proportion_price', 'technology_gadgets',
                    'order_total_price', 'total_order_weight', 'order_payment_installments',
                    'order_day_to_estimation', 'avg_order_photos_qty', 'home_furniture',
                    'seller_total_orders', 'product_avg_review_score',
                    'fashion_accessories', 'arts_entertainment', 'order_items_price',
                    'baby_kids', 'order_n_items', 'seller_avg_review_score',
                    'order_freight_price', 'avg_order_prod_description_length',
                    'order_days_after_carrier', 'order_days_to_shipping_limit',
                    'electronics_appliances', 'sports_leisure', 'beauty_health']

# Features to conver to ordinal encoding
categorical_features = ['order_delivery_status', 'order_payment_type', 'order_shipping_status']

def create_preprocessor(numeric_features, categorical_features):
    

    # Numerical transformations
    numeric_transformer = Pipeline(
        steps=[
            ("imputer", ArbitraryNumberImputer(arbitrary_number=0)),
            ("scaler", StandardScaler())
        ]
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
            ("num", numeric_transformer, numeric_features),
            ("cat", categorical_transformer, categorical_features),
        ],
        remainder='passthrough'
    )
    
    return preprocessor

# COMMAND ----------

preprocessor = create_preprocessor(numeric_features, categorical_features)

# COMMAND ----------

from sklearn.model_selection import StratifiedKFold

# Updated pipeline with StratifiedKFold
pipeline_rfe = Pipeline(steps=[
    ('preprocessor', preprocessor),
    ('feature_selection', RFECV(estimator=DecisionTreeClassifier(), 
                                min_features_to_select=10,
                                step=2,
                                scoring = 'roc_auc',
                                cv=StratifiedKFold(5)))
])

# Fit the training data
pipeline_rfe.fit(X_train, y_train)

# COMMAND ----------

# Access the feature selection step in the pipeline
rfe_step = pipeline_rfe.named_steps['feature_selection']

# Get the mask of selected features (True for selected, False for not selected)
selected_features_mask = rfe_step.support_

# If you want to extract the names of the selected features:
# Assuming you have the feature names in a list called `feature_names`
selected_features = [feature for feature, selected in zip(X_train.columns, selected_features_mask) if selected]

print("Selected Features: ", selected_features)

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
dummy_classifier = DummyClassifier(strategy="uniform")
dummy_classifier.fit(X_train, y_train)

# Make predictions
y_pred_dummy = dummy_classifier.predict(X_val[selected_features])

# COMMAND ----------

evaluate_model(dummy_classifier, X_val[selected_features], y_val)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6. Modeling

# COMMAND ----------

# MAGIC %md
# MAGIC #### Pre-processing

# COMMAND ----------

X_train[selected_features].select_dtypes(exclude = 'object').columns

# COMMAND ----------

# Features to use imputer and scaler

numeric_features = ['order_freight_proportion_price', 'technology_gadgets',
                    'order_total_price', 'total_order_weight', 'order_payment_installments',
                    'order_day_to_estimation', 'avg_order_photos_qty', 'home_furniture',
                    'product_avg_review_score', 'arts_entertainment', 'order_items_price',
                    'baby_kids', 'order_n_items', 'seller_avg_review_score',
                    'order_freight_price', 'beauty_health']

categorical_features = ['order_delivery_status']

preprocessor = create_preprocessor(numeric_features, categorical_features)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Fit

# COMMAND ----------

from sklearn.linear_model import LogisticRegression

# COMMAND ----------

skf = StratifiedKFold(n_splits=5, shuffle=True, random_state=42)

# Compute class weights
class_weights = compute_class_weight('balanced', classes=np.unique(y_train), y=y_train)
class_weight_dict = dict(enumerate(class_weights))


# Define your parameter grid for Random Forest
param_grid = {
    'C': [0.001, 0.01, 0.1, 1, 10],
    'penalty': ['l1', 'l2'],
    'solver': ['liblinear']
}


# Create a Random Forest classifier
model = LogisticRegression(class_weight=class_weight_dict)


# Grid Search for hyperparameters tuning
grid_search = GridSearchCV( model, 
							param_grid,
							n_jobs = -1,
							cv = skf,
							scoring = 'roc_auc',
							verbose = 3,
							refit = True)

# Pipeline de dados
pipeline = Pipeline( steps = [('preprocessor', preprocessor),
							  ('gridsearch', grid_search)])

# Fit treino
pipeline.fit(X_train[selected_features], y_train)

# COMMAND ----------

grid_cv_res = pd.DataFrame(grid_search.cv_results_).sort_values('rank_test_score')
grid_cv_res

# COMMAND ----------

# MAGIC %md
# MAGIC #### Evaluation

# COMMAND ----------

evaluate_model(pipeline, X_train[selected_features], y_train)

# COMMAND ----------

evaluate_model(pipeline, X_val[selected_features], y_val)

# COMMAND ----------

evaluate_model(pipeline, df_oot[selected_features], df_oot[target])

# COMMAND ----------

y_probas = pipeline.predict_proba(X_val[selected_features])

# COMMAND ----------

skplt.metrics.plot_roc(y_val, y_probas)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Feature Importance

# COMMAND ----------

feature_names = [feat.split('__')[1] for feat in pipeline[:-1].get_feature_names_out()]

# COMMAND ----------


# Access the best estimator from the grid search
best_model = pipeline.named_steps['gridsearch'].best_estimator_

# Get feature importance from the best XGBoost model
feature_importance = best_model.coef_[0]

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

experiment_name = "/Users/joaopaulo_brum@hotmail.com/mlflow_runs/bad_review_experiments"
run_description = 'DT Model Initial'


# Run the experiment in mlflow
log_run(pipeline[-1], selected_features, experiment_name, run_description, grid_cv_res.index[0])

# COMMAND ----------

df_oot['prob'] = pipeline.predict_proba(df_oot[selected_features])[:, 1]

# COMMAND ----------

df_oot['Month'] = pd.to_datetime(df_oot['dt_ref']).dt.to_period('M')

# COMMAND ----------

df_oot.groupby('Month').apply(lambda x: roc_auc_score(x['bad_review'], x['prob']))

# COMMAND ----------


