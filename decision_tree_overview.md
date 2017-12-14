Decision Tree Workflow
-----------------

Our workflow is divided into several jobs, which can be run one after another automatically when called by `sh run_decisiontree_pipeline.sh`; each job uses data from the latest output of the step before. The workflow looks like this: 
<img src="https://user-images.githubusercontent.com/8107614/33561247-72463dd0-d912-11e7-8485-b40585da8434.png" width="500" height="500">




### Step 1: Denormalization (`src/merger.py`)
We denormalize the data because machine learning algorithms typically prefer one input matrix. Denormalization turns n > 1 tables into 1 table. This one table is not typically how you'd like to store data in a database -- we're undoing the entire benefit gained from relational databases. 

Suppose there is a table called sales and a table called stores, which we combine into a table which communicates the same data but less efficiently. 
#### Sales:

|Transaction Id| Item | Store Name |
|-|-|-|
|1|Cheese|Zimmerstrasse Store|
|2|Cabbage|Erich-Fromm Platz Store|
|3|Carrots|Zimmerstrasse Store|

#### Stores

| Store Name | City|
|-|-|
|Erich-Fromm Platz Store|Frankfurt|
|Zimmerstrasse Store|Berlin|

#### De-normalized

|Transaction Id| Item | Store Name | City |
|-|-|-|-|
|1|Cheese|Zimmerstrasse Store|Berlin|
|2|Cabbage|Erich-Fromm Platz Store|Frankfurt|
|3|Carrots|Zimmerstrasse Store|Berlin|

Now we have a table that's ready be analyzed. So.

`src/merger.py`:
1. downloads raw data from `s3://twde-datalab/raw/` or loads it from your local harddrive
2. joins files together based on columns they have in common
3. adds columns to the DataFrame which are extracted out of the other columns
    - extrapolating from dates (`2015-08-10`) to  day of the week (Mon, Tues, ...)
    - extrapolating from `date` and `city` information to get the city's population at the time, and whether it rained that day
4. saves its output to `merger/bigTable.csv`


### Step 2: Validation Preparation (`src/splitter.py`)
We split the data into training data and validation data each time we run the pipeline. Training data is used to make our model, and validation data is then compared to the model, as if we've been provided new data points. This prevents us from overfitting our model, and gives us a sanity check for whether we're improving the model or not.

Consider the following graphs; think of each trend line as a model of the data.

![image](https://user-images.githubusercontent.com/8107614/33661598-f91a92c6-da88-11e7-8a69-8c83fdf44ab1.png)

- The first model, a linear model on the left, fails to capture the convex shape of the data. It wont be predictive for new data points.
  - **This is called underfitting.**
- The second model captures the general trend of the data and is likely to continue generally describing the data even as new data points are provided. 
  - **This model is neither underfit nor overfit, it's just right.**
- The third model, the polynomial trend line all the way on the right, describes the data we have perfectly, but it's unlikely to be accurate for data points further down the x axis, if it were ever provided new data. 
  - **This is called overfitting.**
  - It's tempting to overfit a model because of how well it describes the data we already have, but it's much better to have a generally-right-but-never-perfectly-right model than a right-all-the-time-but-only-for-the-data-we-already-have model. 

If we don't randomly withhold some of the data from ourselves and then evaluate our model against that withheld data, we will inevitably overfit the model and lose our general predictivity.

### Step 3: Machine Learning Models (`src/decision_tree.py`)
Step 3 of the pipeline is to supply data to a machine learning algorithm (or several) and make predictions by generalizing the data with a model. We provide our decision tree with the `train` data from `splitter`, and the algorithm learns to map different values of the various column in the data to the `unit_sales` column. This mapping is the model. Then we use the model to make predictions on the `validation` data that `splitter` created for us, and see how well the model performed.
