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

The file `bot_log_parser.py` parses bot logs into the XES process mining format.
It uses several bot logs from the `data` folder and the resulting XES files are saved to the `results` folder.
Depending on the use case and on the specific format of a bot log, different attribute values are needed as input and can be configured in the file.
To run the file, execute the following command:
```
python3 bot_log_parser.py
```


## Log Merger

The file `log_merger.py` merges XES-parsed bot logs with corresponding business process event logs.
It uses business process event logs from the `data` folder and XES-parsed bot logs from the `results` folder.
The resulting merged logs are saved to the `results` folder.
As input attributes the name of the connecting attribute in the business process event log as well as the name of the connecting attribute in the bot log are needed and can be configured in the file.
To run the file, execute the following command:
```
python3 log_merger.py
```


## Measures

The file `measures.py` includes 12 measures that are specifically tailored to analyze merged bot and process event logs to enable an end-to-end analysis of RPA-enabled business processes.
It uses a merged log from the `results` folder and outputs a directly-follows graph or a CSV file, depending on the selected measure. The output is saved to the `results` folder.
The exact measure that should be executed as well as the exact merged log that the selected measure should be applied on can be selected at the end of the file.
To run the file, execute the following command:
```
python3 measures.py
```
In the following an explanation of all 12 measures is provided that is based on the algorithms in the file `measures.py`:
![Alt text](https://github.com/pandyke/bot-log-mining/blob/main/measure_formalizations/Measure_formalizations_legend.JPG?raw=true "Definitions")

![Alt text](https://github.com/pandyke/bot-log-mining/blob/main/measure_formalizations/Measure_formalizations.JPG?raw=true "Measure formalizations")
