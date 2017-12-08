# TWDE-Datalab
This is the onboarding document for the TWDE Datalab. If you want to get involved, find something confusing, or just want to say hi, [please open an issue](https://github.com/ThoughtWorksInc/twde-datalab/issues).


![](http://i0.kym-cdn.com/photos/images/original/001/268/288/04a.gif)
###### (Pictured above: the android named Data, from Star Trek - The Next Generation)

1. [Introduction](https://github.com/ThoughtWorksInc/twde-datalab/blob/master/README.md#introduction)
1. [Data](https://github.com/ThoughtWorksInc/twde-datalab/blob/master/README.md#data)
1. [Infrastructure](https://github.com/ThoughtWorksInc/twde-datalab/blob/master/README.md#infrastructure)
1. [Algorithms](https://github.com/ThoughtWorksInc/twde-datalab/blob/master/README.md#algorithms)
1. [Next Steps](https://github.com/ThoughtWorksInc/twde-datalab/blob/master/README.md#next-steps)
1. [Ways To Get Involved](https://github.com/ThoughtWorksInc/twde-datalab/blob/master/README.md#ways-to-get-involved)
1. [Getting Started](https://github.com/ThoughtWorksInc/twde-datalab/blob/master/README.md#getting-started)


## Introduction
The purpose of this project is to help build a foundational knowledge pool around the fields of data science, machine learning, and intelligent empowerment for ThoughtWorkers in Germany. To do so, we've selected a competition from kaggle.com that, broadly speaking, compares to a realistic problem we would tackle for our clients. The specific problem is demand forecasting for an Ecuadorian grocery store company. For specifics, see [the Favorita Grocery Sales Forecasting Kaggle competition](https://www.kaggle.com/c/favorita-grocery-sales-forecasting)

## Data

We've been provided [4 years of purchasing history](https://www.kaggle.com/c/favorita-grocery-sales-forecasting/data) in the competition itself. Our goal is to analyze this data, plus any other data we acquire (see the [external data discussion on kaggle](https://www.kaggle.com/c/favorita-grocery-sales-forecasting/discussion/41537)), and produce an estimated `unit sales` for each item in each store on each day for a two week period in 2017. 


## Infrastructure

Our workflow is divided into several jobs, which can be deployed one after another automatically on Amazon Web Services; each job downloads data from the latest output of the step before.

### Step 1: Denormalization (`src/merger.py`)
We denormalize the data for: 
  1. Consistent encoding of variables when we convert features from `{True, False, NaN}` to `{0, 1, -1}` (or else we might end up with True mapping to 1 or 0 inconsistently)
  2. Machine learning algorithms, which typically prefer one input matrix

`src/merger.py`:
1. downloads raw data from `s3://twde-datalab/raw/`
2. joins files together based on columns they have in common
    - one dataset maps date + item sales to store numbers, a second dataset maps store numbers to city, and a third dataset maps city to weather information
    - joining these data together, we can now associate the weather in the city on the day items were sold
3. adds columns to the DataFrame which are extracted out of the other columns
    - for example, extrapolating from dates (`2015-08-10`) to  day of the week (Mon, Tues, ...)
4. uploads its (two file) output to `s3://twde-datalab/merger/<timestamp>/{bigTable.hdf,bigTestTable.hdf}`
    - bigTable is the training for our machine learning algorithms
    - bigTestTable is the test data we are to predict sales for, now enriched with data like weather and prices

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
Step 3 of the pipeline is to supply data to a machine learning algorithm (or several) and made predictions on the data asked of us from `test.csv`, as provided by the kaggle competition. See the [algorithms section](https://github.com/ThoughtWorksInc/twde-datalab/blob/master/README.md#algorithms) below for more details on what we've implemented.

## Algorithms
We implement one machine learning model for the time being, which creates a model based on the training data, rates its own accuracy using the validation data, and creates predictions, ready to be submitted to kaggle.com from the `train.csv` file that was provided through the competiton.

Decision trees are one of the simplest algorithms to implement, which is why we've chosen it for our first approach. More complex variations of decision trees can be used to combate the downsides of decision trees, which maybe you, dear reader, would like to implement for us?

At the end of the day, we chose to start with a decision tree because it is relatively light weight, it handles categorical and numerical data well, and it is robust against co-linearity, which our data has a lot of at the moment. 


## Next Steps

## Ways To Get Involved
In the short time we had to start the TWDE-Datalab, we had to gloss over a lot of important parts of the science and engineering that are involved in a good data science project. There are many low hanging fruit ready to be picked by you, dear reader, if you want to get involved in the Data Science world at ThoughtWorks. You should look to the [issues](https://github.com/ThoughtWorksInc/twde-datalab/issues) on this repository for specifics or to ask for guidance, but generally some of the next steps include:
  - More Features
  - Better Data for Existing Features
  - Hyperparameterize the Existing Machine Learning Algorithms
  - Try Different Models
  - Use GridSearch to Compare Model Efficacy
  - Improve Validation Strategy 
  - Improving the pipeline setup
  
The maintainers of the repository will be happy to help you get started.

## Getting Started
This project expects python3 to be used.

1. `git clone https://github.com/ThoughtWorksInc/twde-datalab && cd twde-datalab`
~~1. `pip3 install -r requirements.txt`~~
1. `python3 merger.py`
1. `python3 splitter.py`
1. `python3 decision_tree.py`

# notes for setup
I recommend using anaconda to set up environment using environment.yml

Yet, sometimes necessary latest version is not available. Then please use pip in addition. 
for example,
pip install notebook==5.1.0
