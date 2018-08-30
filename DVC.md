# Executing the Datalab pipeline with dvc

This document will help you get started using dvc as a tool for integrating Continuous Intelligence workflows and productionizable data science.

## Installing dvc

dvc is installed alongside the other python packages required for executing the Datalab models. **It is recommended to use a virtual environment when installing python packages.** To install the required packages, run `pip install -r requirements.txt`.

If you just want to install dvc, run:
```sh
pip install dvc[gs]
```

dvc supports python 2.7 and python 3.6+ as of version 0.18.2. The `[gs]` tells the installer to include support for Google Cloud Storage. To install dvc with Amazon S3 support, for instance, run `pip install dvc[s3]`. To add support for all supported cloud storage options, `pip install dvc[all]`.

## Setting up a local remote

dvc requires a storage target. For the Datalab demo, this will be Google Cloud Storage; however, this is not yet configured. In the meanwhile, one will need to configure a local remote, i.e. a local storage option. This is currently configured as `/tmp/dvc/twde`. To create this local remote, run:

```sh
mkdir -p /tmp/dvc/twde
```

## Explaining dvc pipelines

One of the core features of dvc is the ability to define, execute, and reproduce data processing pipelines. A typical data science pipeline might look something like this:

- collect data
- process/clean data into usable formats
- extract features
- split data into training/validation sets
- train model
- evaluate model
- serialize model
- integrate into production app

dvc allows the user to chain together these steps in such a way that changes are automatically propagated. This simplifies iteration cycles and reduces the need for every user to be an expert on every step. Furthermore, it allows for parallel development and continuous improvement.

The way it does this is by building a "manifest" file for the output of each step. This manifest file specifies all the inputs and outputs of each step, contains hashes for the data/files required to execute the step, and is itself version controlled in git. The input files need not be version controlled; dvc will offload any data/models to the remote storage, thereby avoiding problems with using git to manage large files.

Each subsequent step builds on the previous step(s). This way, a change to one step can be propagated through version control without having to rebuild the entire pipeline. So if you change the data (e.g. by trimming it to make training faster), then you can simply rebuild the manifest for the corresponding step, and the splitting/training/serialization steps will remain the same. You can also switch between branches to test multiple changes more easily using this technique.

## Using dvc to build the Datalab pipeline

The datalab pipeline is encoded into three major steps:

- Data fetching/Feature extraction
  - Input: a single python script
  - Output: bigTable.csv (and .csv files for the fetched data)
- Data splitting
  - Input: bigTable.csv
  - Output: train.csv, validation.csv
- Model training/evaluation
  - Input: train.csv, validation.csv
  - Output: model.pkl, score_and_metadata.csv
  
Currently this is managed by a shell script, but we can use dvc instead.

### Running a step in a dvc pipeline

To run any step of a dvc pipeline, we will use `dvc run`. The syntax is simple: use a `-d` flag for every dependency, a `-o` flag for every output, and a `-M` flag for every metric output, followed by the actual command needed to execute the step. For example:

```sh
dvc run -d src/merger.py -o data/merger/bigTable.csv python src/merger.py
```

This will create a file in the repo root called `bigTable.csv.dvc`.

If we execute `dvc push`, we will push `bigTable.csv` to our remote storage, along with all of the necessary information for another user to re-run the pipeline.

Next, we'll run the splitter:

```sh
dvc run -d data/merger/bigTable.csv -d src/splitter.py -o data/splitter/validation.csv -o data/splitter/train.csv python src/splitter.py
```

This will output `train.csv.dvc`, and we repeat the above steps.

### Reproducing a dvc pipeline

The real benefit of DVC comes from the ability to reproduce steps. Once you have pushed your dvc files to Github, and your data files to your remote storage, the user simply needs to perform

```sh
git pull origin master --rebase
dvc pull
dvc repro [step].dvc
```

where `[step]` represents the name of the dvc file for the step we wish to reproduce.

For instance, the following steps should reproduce the entire Datalab pipeline from scratch (assuming you're in a virtual environment):

```sh
git clone https://github.com/ThoughtWorksInc/twde-datalab.git
pip install -r requirements.txt
dvc pull
dvc repro model.pkl.dvc
```

If we want to change the model parameters, this is simple. We make our changes to the python script, re-generate the `model.csv.dvc` file, and push to Github and dvc. No additional steps are required.
