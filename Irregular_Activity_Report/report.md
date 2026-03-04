## Bucket 1

### Automated Territorial Defense / Pixel Warfare

#### Human description:

Certain regions of the canvas experienced extreme pixel placement in a specific pixel, with some individual pixels being repainted over 90,000 times. This level of sustained, repetitive activity far exceeds normal human interaction patterns and suggests the use of automated tools or scripts to defend some type of territory. *(See Bucket_1.py for script)*

Evidence found that the irregular activity could exist:

- Table of top pixels

|   x |   y |   edits |
|----:|----:|--------:|
|   0 |   0 |   98807 |
| 359 | 564 |   69198 |
| 349 | 564 |   55230 |
| 859 | 766 |   52261 |
| 860 | 766 |   51485 |

- Table contains pixels with >50,000 edits

- Top users contributing hundreds of edits to the same tiny set of pixels (table evidence):

|     user_id |   edits_on_hot_pixels |
|------------:|----------------------:|
| 5.93665e+18 |                   653 |
| 1.99475e+17 |                   570 |
| 1.10989e+19 |                   554 |
| 1.60994e+18 |                   534 |
| 1.3479e+19  |                   533 |

#### Time Between Placement Exploration Evidence:

This graph shows the time between placement distribution for users heavily involved in heavily contested pixels shows a sharp spike at a consistent interval, corresponding closely to the platform’s cooldown period. Unlike the broad, irregular timing expected from human interaction, this pattern suggests scripted or tool-assisted placement synchronized with the cooldown timer.

![Time Between Placements for Contested-Pixel Users](images/time_between_placements.png)

#### Session Length Exploration Evidence

This graph shows many of the users responsible for the highest placed pixels remained continuously active for sessions lasting 10–25 hours without breaks longer than 30 minutes. This type of sustained activity is inconsistent with normal human usage patterns and suggests automated or tool-assisted behavior as people live lives still and need sleep. Comparing this to a baseline graph of the session length for all r/place users it is clear the session lengths are much higher for the users in the top contestested pixel areas than general users.

**Session lengths for heavily contested-pixel users:**

![Session Lengths for Contested-Pixel Users](images/session_lengths_top_users.png)

**Session lengths across all r/place users:**

![Session Lengths for All Users](images/session_lengths_overall.png)

#### Spatial Concentration Exploration Evidence

Looking at edit concentration shows that while many users placed pixels across a wide range of locations, a subset of the users in the high pixel placement areas directed a majority of their activity toward a very small set of heavily contested pixels. In several cases, more than half and sometimes nearly all of a user’s placements occurred in these high placement area regions. This level of spatial focus is inconsistent with typical human participation patterns and suggests automated or tool-assisted territorial defense behavior trying to maintain a specific art work or control of a contested area.

![Fraction of Edits in Hot Pixels](images/hot_edit_ratio.png)

#### Conclusion

Taken together, these behavioral patterns: 

extreme pixel churn, cooldown-synchronized timing, unusually long continuous activity sessions, and highly concentrated spatial focus 

all form a consistent profile of automated or tool-assisted territorial defense rather than typical human participation.


## Bucket 2  

### Low Color Diversity / Task-Specialized Automation

#### Human description:

A subset of highly active accounts exhibited extremely low color diversity, placing hundreds of pixels while using only one or two colors. Unlike typical participants who switch colors while contributing to artwork, these accounts repeatedly placed the same color in highly localized areas. This pattern suggests task-focused, repetitive behavior consistent with automated scripts or tool-assisted placement rather than normal human artistic participation. *(See Bucket_2.py for script)*

Evidence found that the irregular activity could exist:

- Distribution of color diversity ratios per user  
- Table of highly active users with only 1–2 total colors used  
- Per-user spatial maps showing tightly localized placement patterns  

#### Color Diversity Exploration Evidence:

Looking at per-user color usage shows that several accounts made between 200 and 700 placements while using only one or two colors. These users have color diversity ratios below 0.01, meaning over 99% of their placements were the same color. Human contributors typically use a broader range of colors when creating or modifying artwork, making this extreme level of repetition highly unusual. Table to support:

| user_id              | total_colors | total_edits | color_diversity_ratio |
|----------------------|--------------|-------------|------------------------|
| 13973723605168343779  | 1            | 406         | 0                      |
| 1357144649154069865   | 1            | 403         | 0                      |
| 8684514774390412613   | 1            | 396         | 0                      |
| 14155905291761609074  | 2            | 692         | 0.002890               |
| 3949342267876374367   | 1            | 334         | 0                      |

#### Color Dominance Exploration Evidence

Further analysis of dominant color usage reveals that many of these accounts placed nearly all of their pixels using a single color. In several cases, more than 98–100% of a user’s placements were the same color. Such extreme color specialization is consistent with automated or tool-assisted behavior focused on repetitive maintenance tasks, such as border reinforcement or template correction.

#### Spatial Pattern Exploration Evidence

Visual inspection of the most extreme low color usage users shows highly localized placement patterns. Each account concentrated its activity in small clusters or narrow boundary regions rather than contributing broadly across the canvas. Some users appear to maintain straight edges or repeatedly correct the same limited set of pixels. This spatial behavior aligns with automated maintenance roles rather than creative human participation.

![Pixel Placement Patterns of Top Low Color Diversity Users](images/low_entropy_user_patterns.png)

*Figure: Spatial placement patterns for the five most extreme low color-diversity users. Each panel shows that activity is concentrated in small, highly localized regions rather than spread across broader artwork, consistent with task-focused automated or tool-assisted maintenance behavior.*

#### Conclusion

Taken together, these behavioral patterns:

extremely low color diversity, overwhelming dominance of a single color, and tightly localized spatial placement patterns

form a consistent profile of task-specialized, repetitive behavior. These signals strongly suggest automated or tool-assisted accounts performing targeted maintenance functions rather than typical human artistic contribution.


## Bucket 3  

### Mass First-Time User Spike

#### Human description:

During several short time windows, the platform experienced unusually large surges of first-time participants placing their very first pixel. These spikes represent moments where thousands of new accounts became active almost simultaneously. While high activity is expected during major events, the scale and sharpness of these bursts looked like it could be organized influxes of users rather than gradual organic growth. Leading to the hypothesis that this could be due to mass amount of bot users being made to take over a canvas. *(See Bucket_3.py for script)*


### Evidence found that the irregular activity could exist:

- Minute-level placement activity time series showing sharp spikes  
- Table of the highest-activity minutes  
- Table of first-time user counts and statistical comparison  
- Lifespan statistics of users who joined during the spike  


### First-Time User Spike Exploration Evidence

This overall minute-level activity time series shows several sharp placement spikes across the event.

![Minute-Level Placement Activity](images/activity_time_series.png)

One of the largest spikes occurs at **2022-04-03 21:00**, which appears in the table of top placement minutes. Although this is only the second highest in placements I chose to look into this time because the top one not only had all the other highest placements just less than it but also was right at the end of the r/place timeframe so a super extreme spike would be expected:

| minute              | placements |
|---------------------|-----------:|
| 2022-04-04 21:47:00 |     158506 |
| 2022-04-03 21:00:00 |     155955 |
| 2022-04-04 21:53:00 |     145168 |
| 2022-04-04 21:43:00 |     143402 |
| 2022-04-04 21:37:00 |     142327 |


To determine whether this represented *new participants*, I examined the timestamp of each user’s **first-ever placement**. The spike minute stands out as statistically extreme:

| Spike Minute       | First-Time Users in Spike | Average First-Time Users per Minute | Std Dev per Minute | Z-Score of Spike |
|-------------------|--------------------------:|------------------------------------:|-------------------:|-----------------:|
| 2022-04-03 21:00  |                      5965 |                             2076.23 |            907.051 |          4.28727 |

Nearly **6,000 accounts** placed their first-ever pixel within this single minute, compared to an average of roughly **2,076 new users per minute**. A Z-score above **4** confirms this is a statistically extreme new user surge, leading to evidence of my hypothesis about this being bot activity.


### User Lifespan Exploration Evidence

I next examined how long these new users remained active after their first placement. Rather than disappearing immediately, many continued participating for extended periods of time.

| spike_new_users | avg_lifespan_min | std_lifespan_min | median_lifespan_min | p90_lifespan_min |
|----------------:|-----------------:|-----------------:|--------------------:|-----------------:|
|            5965 |         981.97   |         660.28   |              1364   |            1590  |

On average, these users remained active for over **16 hours**, with many continuing for nearly a full day. This weakened my hypothesis that this irregular event was due to bot activity as if these were all created for just a specific moment not many of the users would have had long lifespans.

![Lifespan Distribution of Spike-Onboarded Users](images/lifespan_histogram.png)


### Time-Based Clustering Exploration Evidence

Looking more into this the new user surges occurred in narrow, sharply defined time windows rather than gradually over longer periods.

![Zoomed View of Spike Activity Window](images/spike_zoom_timeseries.png)

The rapid rise in first-time user counts suggests a large participation driven by some type of external coordination such as a famous streamer doing an event on r/place or maybe r/place gaining a popular post on some other reddit thread trying to make an image.

A heatmap of pixel placements during the spike minute and then looking at those that stayed for a long time shows that these users contributed broadly across the canvas after this event rather than concentrating in one location the whole lifespan. This further makes it clear that this was actually **NOT** bot activity as they went from working on one specific spot where the event probably was focused then the people that stayed spread out to all other parts of the r/place which would be expected human behavior:

![Figure 4 — Pixel Placement Heatmap During Onboarding Spike](images/spike_heatmap.png)

![Figure 4 — Pixel Placement Heatmap During Onboarding Spike](images/spike_heatmap_long.png)

So this pattern is consistent with mass participation rather than some type of bot automated behavior.


### Conclusion

These behavioral patterns indicate a large scale external event causing new participants to join the canvas. I decided to include this as a bucket because although not bot behavour, It was something that looked like it should have been so it was interesting to discover that this was actually human activity that created an irregular form of activity in the r/place.
