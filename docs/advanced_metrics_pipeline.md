"""# NBA Advanced Metrics Pipeline 

## Overview

This DAG pulls data from the NBA Player and Team stats API, transforms, and stores data for downstream analytics.

### Covers the following Player/Team stat categories and loads to respective table(s): 
- __Advanced__ -> adv.PlayerBox, adv.TeamBox
- __Defense__ -> def.PlayerBox, def.TeamBox
- __Violations__ -> violations.PlayerBox, violations.TeamBox
- __Usage__ _(player only)_ -> usage.PlayerBox
- __Four Factors__ _(team only)_ -> ffactors.TeamBox

---

## APIs
- **leaguedashplayerstats**
- **leaguedashteamstats**

## Walkthrough

First, we intialize *LeagueDashAPI* and *ScheduleForAPI* Classes.
- These classes are configured to be initialized the once, then re-initialized with specific parameters for what we need at time of execution

We then enter the following loop:
```python
for pt in ['Player','Team']:
        for schema, measure_type in {
            'adv': 'Advanced',
            'misc': 'Misc',
            'def': 'Defense',
            'violations': 'Violations',
            'usage': 'Usage',
            'ffactors': 'Four Factors',
        }.items():
```
We'll use the *pt* value and the *schema* value to get our table name: f'{schema}.{pt}Box'

_get_schedule_
- With that, we query Schedule and PlayerBox/TeamBox, with a left join to our table to see which dates we need to check in our run
- This will also give us data for every game on the dates we need to do so we can match Player and Team stats to the games in which they were recorded
- Hourly will only look at the past two days while daily will check all dates with data potentially missing

_get_measure_type_data_
- Knowing the dates to do, we'll loop through each date and pass the date value to the API as the *DateFrom* and *DateTo* params
- We'll also pass our _measure_type_ value

- Once data is returned, match GameIDs, finish formatting and load."""