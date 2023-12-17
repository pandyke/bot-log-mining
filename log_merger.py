#Log Merger

#Imports
import pm4py
import pandas as pd
import numpy as np
from datetime import timezone, datetime, timedelta
import json
from os import listdir
from pm4py.objects.log.importer.xes import importer as xes_importer
from pm4py.objects.log.exporter.xes import exporter as xes_exporter
from pm4py.objects.conversion.log import converter as log_converter

#Define merging function
def merge_logs(df_log_business_process, df_log_bot,
               connecting_attribute_business_process, connecting_attribute_bot, show_progress=True):
    """
    Merges a business process log with a bot log
    To one activity of the business process several bot activities can be merged.
    The connecting attribute serves as an identifier which bot activities belong to a business process activity.
    Depending on the lifecycle state of the business process activity, the bot activities are either placed before
    or after the business process activity
    
    Parameters
    -----------
    df_log_business_process
        The business process log as dataframe
    df_log_bot
        The bot log as dataframe
    connecting_attribute_business_process
        The connecting attribute in the business process log (e.g. 'eventId')
    connecting_attribute_bot
        The connecting attribute in the bot log (e.g. 'businessActivityId')
    show_progress
        Whether a progress update every 100 events should be printed out or not
    Returns
    -----------
    df_merged
        The merged log as a dataframe
    """
    
    df_merged = df_log_business_process.copy()
    additional_columns = list(set(df_log_business_process.columns) - set(df_log_bot.columns))
    progress_counter = 0
    bot_connecting_attribute_values = set(df_log_bot[connecting_attribute_bot])
    for bp_index, bp_event in df_log_business_process.iterrows():
        current_connAttr_bp = bp_event[connecting_attribute_business_process]
        if current_connAttr_bp in bot_connecting_attribute_values:
            bot_events = df_log_bot.loc[df_log_bot[connecting_attribute_bot] == current_connAttr_bp]
            step = 1/(len(bot_events)+1)
            current_bp_lifecycle = bp_event["lifecycle:transition"]
            if current_bp_lifecycle == "start":
                #Add business process event first, then the corresponding bot events
                index_list = np.arange(bp_index+step, bp_index+1-0.0001, step).tolist()
                bot_events.index = index_list
            elif current_bp_lifecycle == "complete":
                #Add corresponding bot events first, then the actual business process event
                index_list = np.arange(bp_index-1+step, bp_index-0.0001, step).tolist()
                bot_events.index = index_list
            else:
                print("else")
            
            for add_col in additional_columns:
                add_col_value = bp_event[add_col]
                #bot_events[add_col] = add_col_value
                bot_events.insert(0, add_col, add_col_value)
                
            df_merged = pd.concat([df_merged, bot_events],ignore_index=False)
            #df_merged.append(bot_events, ignore_index=False)
    
        if show_progress:
            progress_counter = progress_counter + 1
            if progress_counter % 100 == 0:
                print(progress_counter, " of ", len(df_log_business_process), " business process events")
                    
    df_merged = df_merged.sort_index().reset_index(drop=True)
    
    
    return df_merged


#Merge BPI Challenge Logs

#Load and preprocess business process event log and parsed bot log from BPI challenge
path_business_process_log = 'data/BPI_BusinessProcess_Log.xes'
path_bot_log = 'results/BPI_Bot_Log_UiPath_Parsed.xes'

#Load the business process event log
log_bp = xes_importer.apply(path_business_process_log)
df_log_business_process = log_converter.apply(log_bp, variant=log_converter.Variants.TO_DATA_FRAME)
#Preprocess
df_log_business_process.rename(columns={"eventid": "eventId", "docid_uuid": "caseId"}, inplace=True)

#Load the bot log
log_bot = xes_importer.apply(path_bot_log)
df_log_bot = log_converter.apply(log_bot, variant=log_converter.Variants.TO_DATA_FRAME)
#Preprocess
df_log_bot.rename(columns={"case:caseId": "botCaseId"}, inplace=True)
df_log_bot.drop(['case:concept:name'], axis=1, inplace=True)
df_log_bot["bot"] = True

#Merge logs
df_merged_log = merge_logs(df_log_business_process, df_log_bot, 'eventId', 'businessActivityId', show_progress=True)

#Save
parameters = {log_converter.Variants.TO_EVENT_LOG.value.Parameters.CASE_ID_KEY: 'caseId'}
merged_log = log_converter.apply(df_merged_log, parameters=parameters, variant=log_converter.Variants.TO_EVENT_LOG)
xes_exporter.apply(merged_log, 'results/BPI_Merged_Log.xes')


#Merge Real World Log from Company

#Load and preprocess business process event log and parsed bot log from company
path_business_process_log = 'data/Company_BusinessProcess_Log.xes'
path_bot_log = 'results/Company_Bot_Log_UiPath_Parsed.xes'

#Load the business process event log
log_bp = xes_importer.apply(path_business_process_log)
df_log_business_process = log_converter.apply(log_bp, variant=log_converter.Variants.TO_DATA_FRAME)

#Load the bot log
log_bot = xes_importer.apply(path_bot_log)
df_log_bot = log_converter.apply(log_bot, variant=log_converter.Variants.TO_DATA_FRAME)
#Preprocess
df_log_bot.rename(columns={"case:caseId": "botCaseId"}, inplace=True)
df_log_bot.drop(['case:concept:name'], axis=1, inplace=True)
df_log_bot["bot"] = True

#Merge logs
df_merged_log = merge_logs(df_log_business_process, df_log_bot, 'RPA_Exec_Nr', 'Ordnungsbegriff', show_progress=True)

df_merged_log['time:timestamp'] = pd.to_datetime(df_merged_log['time:timestamp'], format='mixed')
df_merged_log.sort_values(by='time:timestamp')

#Save
parameters = {log_converter.Variants.TO_EVENT_LOG.value.Parameters.CASE_ID_KEY: 'caseId'}
merged_log = log_converter.apply(df_merged_log, parameters=parameters, variant=log_converter.Variants.TO_EVENT_LOG)
xes_exporter.apply(merged_log, 'results/Company_Merged_Log.xes')

#Visualize as directly follows graph
#dfg, start_activities, end_activities = pm4py.discover_dfg(merged_log)
#pm4py.view_dfg(dfg, start_activities, end_activities)