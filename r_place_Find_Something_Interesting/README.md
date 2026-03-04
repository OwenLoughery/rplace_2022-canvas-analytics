# Week 5 Analysis — Predicting Pixel Survival

## What I did
I trained a machine learning model to predict whether a pixel placement in r/place 2022 would **survive at least 30 minutes** (not be overwritten). This model shows that pixel death is highly predictable from local conflict signals, while survival is harder and often depends on broader coordination. Using SHAP for interpretation, the strongest predictors of survival were:

- **How many times that pixel had already been changed** (high = much less likely to survive)
- **When in the event the pixel was placed** (later = more likely to survive)
- **How active the local area was in the last 10 minutes** (more activity/users = less likely to survive)

The reason I chose to build this model and look into this was to build a method so that in the r/place if this event were hosted again, you could find what areas of the canvas would give you the best chance at creating an artwork on your own or just with a smaller community by using the model.



## Research Question
**Can we predict whether a pixel will survive for 30 minutes, using only information available at the moment it was placed?**

### Why this is interesting
r/place looks like a chaotic and very random thing, but it’s also a system of coordinated behavior. If survival is predictable, that implies there are measurable “rules” to stable places (quiet zones, defended regions) versus conflict areas (borders, mass scale community takeovers, ongoing battles between communities).



## Method Overview

### Prediction target (label)
For each pixel placement event, I labeled whether it **survived 30 minutes**:

- **1** = survived ≥ 30 minutes before being overwritten  
- **0** = overwritten within 30 minutes  

To compute this, I used LEAD(ts) over (x,y) to find the next time the same pixel was changed.



## Sampling Strategy
The full dataset is huge and is to big to all be used in a ML algorithm, so I used **stratified sampling across time**:

- I divided the event into **24 equal time bins**
- I sampled an equal number of events from each bin to make a total sample size of **720,000**

This helps prevent the sample from being biased and only including times where there was a ton of placement in the r/place so the model learns across the whole time scheme and improves generalization.



## Feature Engineering
All features were computed using only information from **before** the pixel placement time tp make sure there was no future peeking. Also I did not include the actual coordinates x and y in the features because I did not want the model to just memorize pixels and instead be able to predict based on what actually makes the pixel survivable or not.

For each sampled pixel placement, I computed:

### 1) Pixel history features (exact same location)
- **prior_changes_at_pixel**: how many times this pixel had been changed before  
- **time_since_last_change_sec**: seconds since the previous change at the same (x,y)

I decided to use these features since pixels on borders and conflict zones get overwritten repeatedly whereas stable/safe pixels change rarely.

### 2) Local neighborhood activity (approximate region)
To approximate a neighborhood (area in canvas of pixels grouped together), I assigned each pixel to a grid cell:

- gx = floor(x/20)  
- gy = floor(y/20)

Then I measured activity in the same cell during the **previous 10 minutes**:

- **local_event_count_10m**: number of placements nearby  
- **local_unique_users_10m**: number of unique users nearby  
- **local_unique_colors_10m**: number of unique colors nearby  

I chose these features because I thought high activity and many users/colors would indicate conflict (lower pixel survivability).

### 3) Global time feature
- **minutes_since_start**: minutes since the dataset start  

I chose to make this feature since I figured the early event is more chaotic whereas later event stabilizes.



## Train/Test Split (80/20 split)
To avoid data leakage and mimic real prediction, I split by time:

- first **80%** of sampled events (earlier timestamps) = **train**  
- last **20%** (later timestamps) = **test**

This evaluates whether the model can generalize from earlier event behavior to later event behavior.



## Model Choice
I used a **Random Forest classifier** because:

- The r/place dynamics are nonlinear  
- Random forests work well on structured input data features  
- It provides strong performance without extensive tuning and variable   

I used **class_weight = "balanced"** because survival is less common than overwriting as I found in my training data set there was roughly 75% of the pixels not having survived while only 25% had survived so this was an unbalanced data set.



## Results

### Model performance

Here are the **precision, recall, and f1-score for each class**:

| Class | Meaning | Precision | Recall | f1-score |
|------|---------|-----------|--------|--------|
| 0 | Pixel overwritten | 0.77 | **0.98** | 0.86 |
| 1 | Pixel survived ≥30 min | 0.53 | **0.08** | 0.13 |

The model is extremely good at identifying pixels that will be overwritten.  
A recall of **0.98** for class 0 means that almost all pixels that were eventually overwritten are correctly flagged as high-risk.

However, the model struggles to confidently identify surviving pixels.  
A recall of **0.08** for class 1 means the model only labels a small fraction of true survivors as “likely to survive.”

This asymmetry reveals an important behavioral insight:

> Pixel death is much easier to predict from local conflict signals than pixel survival is to predict from local stability signals.

Overwritten pixels tend to occur in highly active, contested areas that leave strong patterns (frequent prior changes, many nearby users, high recent activity). Survival, on the other hand, often depends on broader coordinated defense and community-scale structure that cannot be fully captured by purely local features.

Although this may make it seem like the model is not useful for finding places to create artwork in the r/place, it still is as it gives really good insight into what places your work will definitely be overwritten (places of conflict and high activity) and you can still see the probabilities that a pixel survives for 30 minutes which we will see in the graph soon which help a ton with finding safe places in the r/place.



## Interpretation with SHAP

To understand what the model learned, I used **SHAP** values. The first image shows direction and second interactive one shows overall feature importance (click on link for second image to view)

### SHAP feature importance

![SHAP Importance](images/shap_importance_direction.png)

**[View Interactive SHAP Plot](https://OwenLoughery.github.io/CSC-369-Week-1-Assignment/shap_importance.html)**

### Direction of effects

- High **prior changes** = strongly decreases survival probability  
- Later **minutes_since_start** = increases survival probability  
- High **local activity** and many **users** = decreases survival probability  
- Long time since last change = increases survival probability  

**Key behavioral conclusion:**  
Pixels survive when placed in regions with historical stability and low ongoing conflict, especially later in the event.



## Visualizations: “Safe Zones” on the Canvas

I visualized predicted survival probability on the canvas (click on interactive versions to zoom into certain zones and see predicted pixel survival odds:

- A **scatter map** of predicted survival probability for a test-time window

![Survival Scatter](images/survival_scatter.png)

**[View Interactive Scatter Map](https://OwenLoughery.github.io/CSC-369-Week-1-Assignment/survival_scatter.html)**

- A **binned heatmap** showing average predicted survival probability by region

![Survival Heatmap](images/survival_heatmap.png)

**[View Interactive Heatmap](https://OwenLoughery.github.io/CSC-369-Week-1-Assignment/survival_heatmap.html)**

Interpretation:

- Bright areas = more likely to survive  
- Dark areas = conflict zones  

These maps act like a **risk map** for pixel survival.



## Practical Takeaway

Even though the model struggles to identify all survivors, it is highly effective at identifying **high-risk zones**.

If a small group wants their art to survive:

- Avoid pixels with **many prior changes**
- Avoid regions with **high recent activity**
- Build **later** in the event when stability increases

The model works best as a risk-avoidance tool and can be very useful for deciding where to work on some art in the r/place subreddit!
