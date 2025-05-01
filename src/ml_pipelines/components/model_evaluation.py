import numpy as np
from sklearn.metrics import (
    accuracy_score,
    average_precision_score,
    f1_score,
    precision_score,
    recall_score,
    roc_auc_score,
)


class ModelEvaluation:
    def evaluate_model(pipeline, X_test, y_test, suffix):
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

        evaluation_metrics = {
            f"accuracy_{suffix}": accuracy,
            f"precision_{suffix}": precision,
            f"recall_{suffix}": recall,
            f"f1 score_{suffix}": f1,
            f"roc_auc_{suffix}": roc_auc,
            f"pr_auc_{suffix}": pr_auc,
        }

        return evaluation_metrics

    def fold_validation(pipeline, cv, X_train, y_train):
        # Initialize lists to store validation results
        roc_auc_valid = []

        # Iterate over each fold
        for fold, (index_train, index_valid) in enumerate(cv.split(X_train, y_train)):
            # validation models
            model_fit = pipeline.fit(X_train.iloc[index_train], y_train.iloc[index_train])

            # Calculate ROC AUC for validation set
            roc_valid = roc_auc_score(
                y_train.iloc[index_valid], model_fit.predict_proba(X_train.iloc[index_valid])[:, 1]
            )

            # Store validation ROC AUC score
            roc_auc_valid.append(roc_valid)

        # Calculate mean and standard deviation of validation ROC AUC scores
        mean_roc_valid = np.mean(roc_auc_valid)
        std_roc_valid = np.std(roc_auc_valid)

        # Calculate ±3 sigma
        lower_bound = mean_roc_valid - 3 * std_roc_valid
        upper_bound = mean_roc_valid + 3 * std_roc_valid

        validation_metrics = {
            "Validation Mean ROC AUC": mean_roc_valid,
            "Standard deviation of ROC AUC": std_roc_valid,
            "Lower bound": lower_bound,
            "Upper bound": upper_bound,
        }

        return validation_metrics
