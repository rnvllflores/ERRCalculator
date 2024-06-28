<div align="center">

# OneBase Data Ingestion

</div>

<a href="https://www.python.org/"><img alt="Python" src="https://img.shields.io/badge/-Python 3.9-blue?style=for-the-badge&logo=python&logoColor=white"></a>
<a href="https://black.readthedocs.io/en/stable/"><img alt="Code style: black" src="https://img.shields.io/badge/code%20style-black-black.svg?style=for-the-badge&labelColor=gray"></a>

<br/>
<br/>


# 📜 Description

Documents data processing and calculations as prescribed by Verra methodologies

<br/>
<br/>


# ⚙️ Local Setup for Development

This repo assumes the use of [conda](https://docs.conda.io/en/latest/miniconda.html) for simplicity in installing GDAL.


## Requirements

1. Python 3.9
2. make
3. conda


## 🐍 One-time Set-up
Run this the very first time you are setting-up the project on a machine to set-up a local Python environment for this project.

1. Install [miniconda](https://docs.conda.io/en/latest/miniconda.html) for your environment if you don't have it yet.
```bash
wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh
bash Miniconda3-latest-Linux-x86_64.sh
```

2. Create a local conda env and activate it. This will create a conda env folder in your project directory.
```
make conda-env
conda activate ./env
```

3. Run the one-time set-up make command.
```
make setup
```

## Pre-commit
Pre-commit is necessary to run linters and generate .py files from .ipynb notebooks whenever you perform `git commit`
1. Install [pre-commit](https://pre-commit.com/#installation)
2. Run `pre-commit install` to set up the git hook scripts
3. Verify if pre-commit runs after committing in git

## 🐍 Testing
To run automated tests, simply run `make test`.

## 📦 Dependencies

Over the course of development, you will likely introduce new library dependencies. This repo uses [pip-tools](https://github.com/jazzband/pip-tools) to manage the python dependencies.

There are two main files involved:
* `requirements.in` - contains high level requirements; this is what we should edit when adding/removing libraries
* `requirements.txt` - contains exact list of python libraries (including depdenencies of the main libraries) your environment needs to follow to run the repo code; compiled from `requirements.in`


When you add new python libs, please do the ff:

1. Add the library to the `requirements.in` file. You may optionally pin the version if you need a particular version of the library.

2. Run `make requirements` to compile a new version of the `requirements.txt` file and update your python env.

3. Commit both the `requirements.in` and `requirements.txt` files so other devs can get the updated list of project requirements.

Note: When you are the one updating your python env to follow library changes from other devs (reflected through an updated `requirements.txt` file), simply run `pip-sync requirements.txt`


## 📁 Data File Trees
Outline the necessary file structure before running the notebooks. You can create a file tree [here](https://tree.nathanfriend.io/).
### GitHub repo
Within your local copy of the copied template, create the `data/` folder with the following structure prior to running the notebooks (change `datasetX` and `fileX` as applicable):
```
data/
├── 02_dataset_alignment/
│   ├── dataset1/
│   │   └── file1
│   ├── dataset2/
│   │   └── file2
└── 03_analytics/
```
Mention file sources.

### GCP
Within the project GCP (add link), make sure the following folders and files are present (change structure as applicable):
```
project-gcp/
├── 02_dataset1/
├── 02_dataset2/
└── 03_analytics/
```
Mention table sources if any are needed prior to running the notebooks.
