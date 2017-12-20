# TWDE-Datalab
This is the onboarding document for the TWDE Datalab. If you want to get involved, find something confusing, or just want to say hi, [please open an issue](https://github.com/ThoughtWorksInc/twde-datalab/issues).

1. [Introduction](https://github.com/ThoughtWorksInc/twde-datalab/blob/master/README.md#introduction)
1. [Data](https://github.com/ThoughtWorksInc/twde-datalab/blob/master/README.md#data)
1. [Algorithms](https://github.com/ThoughtWorksInc/twde-datalab/blob/master/README.md#algorithms)
1. [Getting Started](https://github.com/ThoughtWorksInc/twde-datalab/blob/master/README.md#getting-started)
1. [Ways To Get Involved](https://github.com/ThoughtWorksInc/twde-datalab/blob/master/README.md#ways-to-get-involved)
1. [Next Steps (Get really involved)](https://github.com/ThoughtWorksInc/twde-datalab/blob/master/README.md#next-steps)


## Introduction
It's our goal to onboard you to the basics of data science as quickly and thoroughly as possible. We've selected a challenge from kaggle.com that, broadly speaking, compares to a realistic problem we would tackle for clients. The specific problem is demand forecasting for an Ecuadorian grocery company. For details, see [the Favorita Grocery Sales Forecasting Kaggle competition](https://www.kaggle.com/c/favorita-grocery-sales-forecasting).

## Data
The competition provides [4 years of purchasing history](https://www.kaggle.com/c/favorita-grocery-sales-forecasting/data) along with data about things like the price of oil (Ecuador is a net exporter of oil), and public holidays in Ecuador. Our goal is to analyze this data, plus any other data we acquire (see the [external data discussion on kaggle](https://www.kaggle.com/c/favorita-grocery-sales-forecasting/discussion/41537)), and produce an estimated `unit sales` for each item in each store for a given time period. 

To make it easier to get started, we provide a data set that is a subset of the original data. The sample consists of only one type of store in one city (Quito), and only includes transaction data from the last year. This dramatically reduces the size of the data, which limits our predictive capabilities, but will be more than enough to get started with thorough analysis.

## Algorithms
We provide one functioning machine learning model: a simple decision tree. Check out the [description for our decision tree pipeline](https://github.com/ThoughtWorksInc/twde-datalab/blob/master/decision_tree_overview.md) for details about our implementation. 

Decision trees are one of the simplest algorithms to use, which is why we've chosen it for our first approach. More complex variations of decision trees can be used to combate the downsides of decision trees, which maybe you, dear reader, would like to try out for us?

At the end of the day, we chose to start with a decision tree because it is relatively light weight, it handles categorical and numerical data well, and it is robust against co-linearity, which our data has a lot of at the moment. 

## Getting Started
This project expects Python 3 to be used. The easiest way to get Python 3 is by using [Anaconda](https://www.anaconda.com/download).

1. `git clone https://github.com/ThoughtWorksInc/twde-datalab && cd twde-datalab`
1. `pip install -r requirements.txt`
1. `sh run_decisiontree_pipeline.sh`

After running the pipeline, which can take a while, the output data will be stored in folders in the corresponding to the file that created them, e.g.
- `data/merger/bigTable.csv`
- `data/splitter/train.csv`
- `data/splitter/validation.csv`
- `data/decision_tree/model.pkl`
- `data/decision_tree/score_and_metadata.csv`

If running the decision tree pipeline worked without error, you are ready to start science-ing on your own! Next, you can consider:
- [Reading about how (and why) we implement the pipeline for the decision tree the way we do](https://github.com/ThoughtWorksInc/twde-datalab/blob/master/decision_tree_overview.md)
- [Doing some exploratory analysis and document what you find](https://github.com/ThoughtWorksInc/twde-datalab/blob/master/jupyter_notebooks)
- [Coming up with a hypothesis about some feature engineering tasks and test your hypothesis](https://github.com/ThoughtWorksInc/twde-datalab/blob/master/jupyter_notebooks/Feature_Engineering.ipynb)
- See also "Ways To Get Involed" below, or search our issues for more things!


## Ways To Get Involved
There are many low hanging fruit ready to be picked by you, dear reader, if you want to get involved in the Data Science world at ThoughtWorks. You should look to the [issues](https://github.com/ThoughtWorksInc/twde-datalab/issues) on this repository for specifics or to ask for guidance. Categorically, some of the possible next steps include:
  - Use more features for existing algorithms
    - Daily weather
    - Price of oil
    - Natural disasters
    - Political unrest
  - Hyperparameterize the Existing Machine Learning Algorithms
  - Try Different Models
    - Random forest
    - Neural network
    - Time series regression for each item
  - Improve Validation Strategy 
    - Use 30% of the data to validate
    - Don't only validate on the last time period of the training data
  - Improving the pipeline setup
    - Can data preprocessing be used for multiple algorithms?
    - Streamlining deployment on AWS
  
The maintainers of the repository will be happy to help you get started.

Let's get started!

![](http://i0.kym-cdn.com/photos/images/original/001/268/288/04a.gif)
###### (Pictured above: the android named Data, from Star Trek - The Next Generation)


## Next Steps (Get really involved)

The default master branch represents a simplified version of our work that is optimised for comprehensibility and for local development, which is also why a dramatically downsized dataset is used.
For really validating whether an improvement or alternative approach you applied significantly increases prediction quality you probably want to run the training on the entire dataset and you probably don't want to run it locally. 
That is what the branch https://github.com/ThoughtWorksInc/twde-datalab/tree/run-on-aws is all about (a.k.a. the 'pro' branch).

We have  been exploring different ways to deploy the code on AWS. Our first approach was through creating Elastic Map Reduce clusters, but since we haven't been doing distributed computing very much, we're using single EC2 machines being provisioned through AWS Data Pipeline. 
If you want to get access to AWS resources or want to contribute to the pro branch get in touch with us through an issue.
