# TWDE-Datalab
This is the onboarding document for the TWDE Datalab. If you want to get involved, find something confusing, or just want to say hi, [please open an issue](https://github.com/emilyagras/kaggle-favorita/issues).


![](http://i0.kym-cdn.com/photos/images/original/001/268/288/04a.gif)
###### (Pictured above: the android named Data, from Star Trek - The Next Generation)

1. [Introduction](https://github.com/emilyagras/kaggle-favorita/blob/master/README.md#introduction)
1. [Data](https://github.com/emilyagras/kaggle-favorita/blob/master/README.md#data)
1. [Infrastructure](https://github.com/emilyagras/kaggle-favorita/blob/master/README.md#infrastructure)
1. [Algorithms](https://github.com/emilyagras/kaggle-favorita/blob/master/README.md#algorithms)
1. [Next Steps](https://github.com/emilyagras/kaggle-favorita/blob/master/README.md#next-steps)
1. [Getting Started](https://github.com/emilyagras/kaggle-favorita/blob/master/README.md#getting-started)


## Introduction
The purpose of this project is to help build a foundational knowledge pool around the fields of data science, machine learning, and intelligence empowerment for ThoughtWorkers in Germany. To do so, we've selected a competition from kaggle.com that, broadly speaking, compares to a realistic problem we would tackle for our clients. The specific problem is demand forecasting for an Ecuadorian grocery store company. For specifics, see [the Favorita Grocery Sales Forecasting Kaggle competition](https://www.kaggle.com/c/favorita-grocery-sales-forecasting)

## Data

We've been provided [4 years of purchasing history](https://www.kaggle.com/c/favorita-grocery-sales-forecasting/data) in the competition itself. Our goal is to analyze this data, plus any other data we acquire (see the [external data discussion on kaggle](https://www.kaggle.com/c/favorita-grocery-sales-forecasting/discussion/41537)), and produce an estimated `unit sales` for each item in each store on each day for a two week period in 2017. 


## Infrastructure

Our workflow is divided into several jobs, which can be deployed one after another automatically on Amazon Web Services. Let's look at Arif's amazing diagram to illustrate the workflow.

![](https://user-images.githubusercontent.com/8107614/33561247-72463dd0-d912-11e7-8485-b40585da8434.png)

Our workflow is divided into several discrete steps, each downloading data from the latest output of the step before.

### Step 1: Denormilization (`src/merger.py`)
Our first step is to denormalize our data. We do this for: 
  1. Consistent encoding of variables when we convert features from from `{True, False, NaN}` to `{0, 1, -1}` (or else we might end up with True mapping to 1 or 0 inconsistently)
  2. Machine learning algorithms, which typically prefer one input matrix

`src/merger.py`:
  - 1. downloads raw data from `s3://twde-datalab/raw/`
  - 2. joins files together based on columns they have in common
    - one dataset maps date + item sales to store numbers, a second dataset maps store numbers to city, and a third dataset maps city to weather information
    - joining these data together, we can now associate the weather in the city on the day items were sold
  - 3. adds columns to the DataFrame which are extracted out of the other columns
    - for example, extrapolating from dates (`2015-08-10`) to  day of the week (Mon, Tues, ...)
  - 4. uploads its (two file) output to `s3://twde-datalab/merger/<timestamp>/{bigTable.hdf,bigTestTable.hdf}`
    - bigTable is the training for our machine learning algorithms
    - bigTestTable is the test data we are to predict sales for, now enriched with data like weather and prices

### Step 2: Validation Preparation (`src/splitter.py`)
blah


## Algorithms

## Next Steps

## Getting Started

# notes for setup
I recommend using anaconda to set up environment using environment.yml

Yet, sometimes necessary latest version is not available. Then please use pip in addition. 
for example,
pip install notebook==5.1.0
