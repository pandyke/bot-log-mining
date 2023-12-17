# Bot Log Mining: An Approach to Integrated Analysis in Robotic Process Automation and Process Mining

This repository contains the source code and online appendix for our paper 'Bot Log Mining: An Approach to Integrated Analysis in Robotic Process Automation and Process Mining'. 
For a more comprehensive overview of the approach we refer to the paper.


## Installation

Execute the following commands to check out the repository, create a new Python virtual environment, and install the dependencies:

```
git clone https://github.com/pandyke/bot-log-mining
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```


## Bot Log Parser

The file `bot_log_parser.py` parses the...
The resulting DataFrames are saved to the folder `results`.
To run the file, execute the following command:
```
python3 bot_log_parser.py
```


## Log Merger

The file `log_merger.py` merges the...
The resulting DataFrames are saved to the folder `results`.
To run the file, execute the following command:
```
python3 log_merger.py
```


## Measures

The file `measures.py` includes the...
The resulting DataFrames are saved to the folder `results`.
The specific measure that should be executed can be adapted in the code.
To run the file, execute the following command:
```
python3 measures.py
```
In the following an explanation of all 12 measures is provided that is based on the algorithms in the file `measures.py`:
![Alt text](https://github.com/pandyke/bot-log-mining/blob/main/measure_formalizations/Measure_formalizations.JPG?raw=true "Measure formalizations")
