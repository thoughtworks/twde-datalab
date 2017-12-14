# TWDE-Datalab
This is the onboarding document for the TWDE Datalab. If you want to get involved, find something confusing, or just want to say hi, [please open an issue](https://github.com/ThoughtWorksInc/twde-datalab/issues).

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
1. `pip3 install -r requirements.txt`
1. `python3 merger.py`
1. `python3 splitter.py`
1. `python3 decision_tree.py`

After the long wait for each file to compute, the output data will be uploaded to the latest folder on S3. 
- `/merger/<latest timestamp>/bigTable2016-2017.hdf`
- `/merger/<latest timestamp>/bigTestTable.hdf`
- `/splitter/<latest timestamp>/train.hdf`
- `/splitter/<latest timestamp>/test.hdf`
- `/decision_tree/<latest timestamp>/submission.hdf`
- `/decision_tree/<latest timestamp>/model.pkl`
- `/decision_tree/<latest timestamp>/score_and_metadata.csv`
where each `<latest timestamp>` can be found in each directory's `latest` file. (i.e. `merger/latest`, `splitter/latest`, and `decision_tree/latest`)

We have also been exploring different ways to deploy the code on AWS. Our first approach was through creating Elastic Map Reduce clusters, but since we haven't been doing distributed computing very much, we're using AWS Data Pipeline. You can deploy a job or multiple jobs as a Data Pipeline from the `deployment` directory, using `sh deploy-pipeline -j JOB [-n PIPELINE_NAME]`. Get in touch with us if you'd like to be given access via AWS IAM.

![](http://i0.kym-cdn.com/photos/images/original/001/268/288/04a.gif)
###### (Pictured above: the android named Data, from Star Trek - The Next Generation)
