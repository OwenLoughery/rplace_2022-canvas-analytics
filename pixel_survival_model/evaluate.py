"""
Evaluates the existing pixel survival model more completely.

Uses the same train/test files, features, Random Forest settings, random_state and
default 0.5 threshold as research.py. Nothing is tuned or changed; this only adds
metrics that research.py didn't save (confusion matrix, baseline, ROC-AUC, average
precision).

Usage:
    python evaluate.py [folder with train.parquet and test.parquet]

Writes results/evaluation.json and images/precision_recall_curve.png next to this script.
"""

import json
import sys
from pathlib import Path

import duckdb
import matplotlib
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import sklearn
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import (
    accuracy_score,
    average_precision_score,
    classification_report,
    confusion_matrix,
    precision_recall_curve,
    roc_auc_score,
)

data_dir = Path(sys.argv[1]) if len(sys.argv) > 1 else Path(".")
out_dir = Path(__file__).resolve().parent

SURVIVE_MIN = 30
LOOKBACK_MIN = 10

con = duckdb.connect()
train = con.execute(f"SELECT * FROM read_parquet('{(data_dir / 'train.parquet').as_posix()}')").df()
test = con.execute(f"SELECT * FROM read_parquet('{(data_dir / 'test.parquet').as_posix()}')").df()

features = [
    "minutes_since_start",
    "prior_changes_at_pixel",
    "time_since_last_change_sec",
    f"local_event_count_{LOOKBACK_MIN}m",
    f"local_unique_users_{LOOKBACK_MIN}m",
    f"local_unique_colors_{LOOKBACK_MIN}m",
]
target = f"y_survives_{SURVIVE_MIN}m"

X_train, y_train = train[features], train[target]
X_test, y_test = test[features], test[target]

# same model as research.py
model = RandomForestClassifier(
    n_estimators=150,
    random_state=42,
    n_jobs=-1,
    class_weight="balanced",
)
model.fit(X_train, y_train)

preds = model.predict(X_test)  # default 0.5 threshold, same as research.py
scores = model.predict_proba(X_test)[:, 1]

base_rate = float(y_test.mean())
report = classification_report(y_test, preds, output_dict=True)
cm = confusion_matrix(y_test, preds, labels=[0, 1])

results = {
    "description": "Existing Random Forest from research.py, evaluated on the time-based test set (last 20% of the sample). No tuning.",
    "rows": {"train": len(train), "test": len(test)},
    "survivor_base_rate": {"train": float(y_train.mean()), "test": base_rate},
    "threshold": 0.5,
    "confusion_matrix": {
        "layout": "rows = actual, columns = predicted, order [overwritten (0), survived (1)]",
        "values": cm.tolist(),
    },
    "accuracy": {
        "model": accuracy_score(y_test, preds),
        "majority_class_baseline": 1 - base_rate,
    },
    "per_class": {
        name: {k: report[key][k] for k in ("precision", "recall", "f1-score", "support")}
        for name, key in (("overwritten (0)", "0"), ("survived (1)", "1"))
    },
    "roc_auc": roc_auc_score(y_test, scores),
    "average_precision": {
        "model": average_precision_score(y_test, scores),
        "survivor_prevalence_baseline": base_rate,
    },
    "model_settings": {"n_estimators": 150, "random_state": 42, "class_weight": "balanced", "features": features},
    "versions": {
        "python": sys.version.split()[0],
        "scikit-learn": sklearn.__version__,
        "numpy": np.__version__,
        "pandas": pd.__version__,
        "duckdb": duckdb.__version__,
        "matplotlib": matplotlib.__version__,
    },
}

(out_dir / "results").mkdir(exist_ok=True)
(out_dir / "results" / "evaluation.json").write_text(json.dumps(results, indent=2) + "\n", encoding="utf-8")
print(classification_report(y_test, preds))
print(json.dumps({k: results[k] for k in ("survivor_base_rate", "confusion_matrix", "accuracy", "roc_auc", "average_precision")}, indent=2))

# precision-recall curve for the survived class
precision, recall, _ = precision_recall_curve(y_test, scores)
op_precision = report["1"]["precision"]
op_recall = report["1"]["recall"]

plt.rcParams.update({"font.family": "DejaVu Sans", "font.size": 11})
fig, ax = plt.subplots(figsize=(7.5, 5.5), dpi=150)
ax.plot(recall, precision, color="#2a78d6", linewidth=2, label=f"Random Forest (AP = {results['average_precision']['model']:.2f})")
ax.axhline(base_rate, color="#52514e", linewidth=1.2, linestyle="--", label=f"Survivor prevalence ({base_rate:.0%})")
ax.plot(op_recall, op_precision, "o", color="#eb6834", markersize=9, label="Default 0.5 threshold")
ax.set_xlim(0, 1)
ax.set_ylim(0, 1)
ax.set_xlabel("Recall (share of actual survivors caught)")
ax.set_ylabel("Precision (share of predicted survivors that survived)")
ax.set_title("Precision-Recall Curve, Survived Class (Test Set)", loc="left", fontweight="bold")
ax.grid(color="#e4e3df", linewidth=0.8)
ax.set_axisbelow(True)
for side in ("top", "right"):
    ax.spines[side].set_visible(False)
ax.legend(frameon=False, loc="upper right")
fig.tight_layout()

(out_dir / "images").mkdir(exist_ok=True)
fig.savefig(out_dir / "images" / "precision_recall_curve.png")
print("Saved results/evaluation.json and images/precision_recall_curve.png")
