import duckdb
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report, confusion_matrix
import numpy as np
import plotly.express as px
import shap
import kaleido
import os
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', None)

full_path = r"place_preprocessed.parquet"
sampled_path = r"place_sampled_timebins.parquet"
labeled_path = r"place_sampled_labeled.parquet"
features_path = r"place_sampled_features.parquet"
train_path = r"train.parquet"
test_path  = r"test.parquet"

SURVIVE_MIN = 30
GRID = 20
LOOKBACK_MIN = 10

con = duckdb.connect()


train = con.execute("SELECT * FROM read_parquet('train.parquet')").df()
test = con.execute("SELECT * FROM read_parquet('test.parquet')").df()

features = [
    "minutes_since_start",
    "prior_changes_at_pixel",
    "time_since_last_change_sec",
    f"local_event_count_{LOOKBACK_MIN}m",
    f"local_unique_users_{LOOKBACK_MIN}m",
    f"local_unique_colors_{LOOKBACK_MIN}m",
]

X_train = train[features]
y_train = train[f"y_survives_{SURVIVE_MIN}m"]

X_test = test[features]
y_test = test[f"y_survives_{SURVIVE_MIN}m"]

model = RandomForestClassifier(
    n_estimators=150,
    random_state=42,
    n_jobs=-1,
    class_weight="balanced" # training set unbalanced so used balanced feature of random forest to add more weight/importance to the y outcomes that are less
)


model.fit(X_train, y_train)

preds = model.predict(X_test)

# precision, recall, f1 scores + confusion matrix
print(classification_report(y_test, preds))
print(confusion_matrix(y_test, preds))


#Shap for feature importance

n_shap = min(1000, len(X_test))
X_shap = X_test.sample(n=n_shap, random_state=42)

explainer = shap.TreeExplainer(model, feature_perturbation="tree_path_dependent")
shap_values = explainer.shap_values(X_shap, approximate=True)


if isinstance(shap_values, list):
    sv = shap_values[1]
else:
    sv = shap_values[:, :, 1] if shap_values.ndim == 3 else shap_values

shap_importance = np.abs(sv).mean(axis=0)

shap_imp_df = (
    pd.DataFrame({"feature": features, "importance": shap_importance})
    .sort_values("importance", ascending=True)
)

print(shap_imp_df.sort_values("importance", ascending=False))



test["ts"] = pd.to_datetime(test["ts"])


test["survival_prob"] = model.predict_proba(test[features])[:, 1]

# Feature importance bar chart
fig_imp = px.bar(
    shap_imp_df,
    x="importance",
    y="feature",
    orientation="h",
    title="SHAP Feature Importance"
)
fig_imp.show()

shap.summary_plot(sv, X_shap, feature_names=features, show=True)

# Scatter map for a 3 hour time window from the test set
t0 = test["ts"].min() + (test["ts"].max() - test["ts"].min()) / 2
window = pd.Timedelta(minutes=180)

slice_df = test[(test["ts"] >= t0) & (test["ts"] < t0 + window)].copy()

fig_scatter = px.scatter(
    slice_df,
    x="x",
    y="y",
    color="survival_prob",
    color_continuous_scale="Viridis",
    opacity=0.7,
    title="Predicted Pixel Survival Probability Map (For Test Window of 3 hours)",
    labels={"survival_prob": "Predicted Survival Probability", "x": "Canvas X", "y": "Canvas Y"}
)
fig_scatter.update_yaxes(autorange="reversed")
fig_scatter.show()

# 3) Binned heatmap
n = min(150_000, len(test))
plot_df = test.sample(n=n, random_state=42).copy()


bins = 120
plot_df["x_bin"] = pd.cut(plot_df["x"], bins=bins, labels=False)
plot_df["y_bin"] = pd.cut(plot_df["y"], bins=bins, labels=False)

heat = (
    plot_df.groupby(["x_bin", "y_bin"], as_index=False)["survival_prob"]
    .mean()
    .rename(columns={"survival_prob": "avg_survival_prob"})
)

fig_heat = px.density_heatmap(
    heat,
    x="x_bin",
    y="y_bin",
    z="avg_survival_prob",
    color_continuous_scale="Viridis",
    title="Predicted Pixel Survival Probability Map (Binned Heatmap)",
    labels={"avg_survival_prob": "Avg Predicted Survival Probability", "x_bin": "X Bin", "y_bin": "Y Bin"}
)
fig_heat.update_yaxes(autorange="reversed")
fig_heat.show()




os.makedirs("images", exist_ok=True)
os.makedirs("docs", exist_ok=True)


fig_heat.write_image("images/survival_heatmap.png")


fig_scatter.write_image("images/survival_scatter.png")


fig_imp.write_image("images/shap_importance.png")



fig_heat.write_html(
    "docs/survival_heatmap.html",
    include_plotlyjs="cdn",   # OR True
    full_html=True
)

fig_scatter.write_html(
    "docs/survival_scatter.html",
    include_plotlyjs="cdn",
    full_html=True
)

fig_imp.write_html(
    "docs/shap_importance.html",
    include_plotlyjs="cdn",
    full_html=True
)

