# Bot Log Parser

#imports
import pm4py
import pandas as pd
import numpy as np
from datetime import timezone, datetime, timedelta
import pytz
import json
from os import listdir

from pm4py.objects.log.exporter.xes import exporter as xes_exporter
from pm4py.objects.conversion.log import converter as log_converter

#UiPath to .xes
def uipath_log_to_df(log_lines, connecting_attribute, attr_conceptName, attr_timestamp, attr_lifecycle, valuesLifecycle,
                     standardValueLifecycle, attr_eventId, attr_caseId, attr_resource, attr_botProcessName,
                     attr_botProcessVersionNumber, attr_succcess, valueNoSuccess, traceLevelOnly):
    """
    Converts a UiPath log to a dataframe.
    The function is aborted, if one of the attributes given as inputs is not found in the log.

    Parameters
    -----------
    log_lines
        A list of lines of a UiPath log
    connecting_attribute
        The name of the connecting attribute that is later used to merge the bot log with a business process log
    attr_conceptName
        The name of the attribute whose value is used for the concept:name attribute in the resulting xes log
    attr_timestamp
        The name of the attribute whose value is used for the time:timestamp attribute in the resulting xes log
    attr_lifecycle
        The name of the attribute whose value is used for the lifecycle:transition attribute in the resulting xes log
    valuesLifecycle
        The values of the attr_lifecycle which indicate the status 'ate:abort', 'start', 'complete' respectively,
        e.g. 'Faulted', 'Executing', 'Closed'
    standardValueLifecycle
        The fallback standard value for attr_lifecycle if no valuesLifecycle matches (e.g. 'start' or 'complete')
    attr_eventId
        The name of the attribute whose value is used for the eventId attribute in the resulting xes log
    attr_caseId
        The name of the attribute whose value is used for the case:caseId attribute in the resulting xes log
    attr_resource
        The name of the attribute whose value is used for the org:resource attribute in the resulting xes log
    attr_botProcessName
        The name of the attribute whose value is used for the botProcessName attribute in the resulting xes log
    attr_botProcessVersionNumber
        The name of the attribute whose value is used for the botProcessVersionNumber attribute in the resulting xes log
    attr_succcess
        The name of the attribute whose value is used for the success attribute in the resulting xes log
    valueNoSuccess
        The string value of the attr_succcess which indicates that success is false (e.g. 'Faulted' or 'Error')
    traceLevelOnly
        A boolean that indicates if only trace level log entries should be considered
        or others as well (e.g. Info or Error level log entries)
    

    Returns
    -----------
    df_log
        The UiPath log converted to a dataframe
    """
    
    json_entries_list = []
    for line in log_lines:
        jsonString = "{" + line.split("{",1)[1]
        currentEntryJson = json.loads(jsonString)
        json_entries_list.append(currentEntryJson)
    df_log_initial = pd.json_normalize(json_entries_list, record_prefix=False)
    
    #only take trace level log entries if desired
    if traceLevelOnly:
        df_log = df_log_initial.loc[df_log_initial["level"] == "Trace"]
    else:
        df_log = df_log_initial
    
    column_names = df_log.columns
    attributes_not_found = {connecting_attribute, attr_conceptName, attr_timestamp, attr_eventId, attr_caseId, attr_resource,
                           attr_botProcessName, attr_botProcessVersionNumber, attr_lifecycle, attr_succcess}
    for column in column_names:
        #Check if name of attribute is a substring in the original column name (which includes the "."-separated json path)
        if connecting_attribute in column:
            df_log.rename(columns={column: connecting_attribute}, inplace=True)
            attributes_not_found.discard(connecting_attribute)
        elif attr_conceptName in column:
            df_log.rename(columns={column: 'concept:name'}, inplace=True)
            attributes_not_found.discard(attr_conceptName)
        elif attr_timestamp in column:
            df_log.rename(columns={column: 'time:timestamp'}, inplace=True)
            attributes_not_found.discard(attr_timestamp)
        elif attr_eventId in column:
            df_log.rename(columns={column: 'eventId'}, inplace=True)
            attributes_not_found.discard(attr_eventId)
        elif attr_caseId in column:
            df_log.rename(columns={column: 'case:caseId'}, inplace=True)
            attributes_not_found.discard(attr_caseId)
        elif attr_resource in column:
            df_log.rename(columns={column: 'org:resource'}, inplace=True)
            attributes_not_found.discard(attr_resource)
        elif attr_botProcessName in column:
            df_log.rename(columns={column: 'botProcessName'}, inplace=True)
            attributes_not_found.discard(attr_botProcessName)
        elif attr_botProcessVersionNumber in column:
            df_log.rename(columns={column: 'botProcessVersionNumber'}, inplace=True)
            attributes_not_found.discard(attr_botProcessVersionNumber)
        
        if attr_lifecycle in column:
            #the column name containing the infos for the lifecycle attribute
            lifecycle_col = column
            attributes_not_found.discard(attr_lifecycle)
        if attr_succcess in column:
            #the column name containing the infos for the success attribute. Normally is the same than the lifecycle column
            success_col = column
            attributes_not_found.discard(attr_succcess)

    if len(attributes_not_found) > 0:
        print("The following attributes were not found in the log, function is aborted: ", attributes_not_found)
        return
    else:
        print("Found all attributes in the log that were provided as inputs")
            
    df_log['success'] = df_log.apply(lambda x: False if x[success_col] == valueNoSuccess else True, axis=1)
    
    valueAbort, valueStart, valueComplete = valuesLifecycle
    #df_log['lifecycle:transition'] = df_log.apply(lambda x: "ate:abort" if x[lifecycle_col] == valueAbort else
    #                                              "start" if x[lifecycle_col] == valueStart else 
    #                                              "complete" if x[lifecycle_col] == valueComplete else
    #                                              standardValueLifecycle, axis=1)
    #New version where lifecycle is not set to ate:abort
    df_log['lifecycle:transition'] = df_log.apply(lambda x: "start" if x[lifecycle_col] == valueStart else 
                                                            "complete" if x[lifecycle_col] == valueComplete else
                                                            standardValueLifecycle, axis=1)
    
    if lifecycle_col == success_col:
        df_log.drop([lifecycle_col], axis=1, inplace=True)
    else:
        df_log.drop([lifecycle_col, success_col], axis=1, inplace=True)

    df_log = df_log[['case:caseId', 'concept:name', 'time:timestamp', 'eventId', 'org:resource', 'botProcessName',
                     'botProcessVersionNumber', 'success', 'lifecycle:transition', connecting_attribute]]
        
    return df_log

# Parse UiPath log from BPI challenge (Bot_Log_UiPath.log)
path_uiPath_bot_log = "data/Bot_Log_UiPath.log"

file = open(path_uiPath_bot_log, 'r')
lines = file.read().splitlines()
file.close()

connecting_attribute = 'businessActivityId'
attr_conceptName = 'DisplayName'
attr_timestamp = 'timeStamp'
attr_lifecycle = 'State'
valuesLifecycle = ['Faulted', 'Executing', 'Closed']
standardValueLifecycle = "complete"
attr_eventId = 'fingerprint'
attr_caseId = 'jobId'
attr_resource = 'robotName'
attr_botProcessName = 'processName'
attr_botProcessVersionNumber = 'processVersion'
attr_succcess = 'State'
valueNoSuccess = "Faulted"
traceLevelOnly = True

df_log = uipath_log_to_df(lines, connecting_attribute, attr_conceptName, attr_timestamp, attr_lifecycle, valuesLifecycle, standardValueLifecycle, attr_eventId, attr_caseId,
                          attr_resource, attr_botProcessName, attr_botProcessVersionNumber, attr_succcess, valueNoSuccess, traceLevelOnly)

parameters = {log_converter.Variants.TO_EVENT_LOG.value.Parameters.CASE_ID_KEY: 'case:caseId'}
log = log_converter.apply(df_log, parameters=parameters, variant=log_converter.Variants.TO_EVENT_LOG)

xes_exporter.apply(log, 'results/Bot_Log_UiPath_parsed.xes')

dfg, start_activities, end_activities = pm4py.discover_dfg(log)
pm4py.view_dfg(dfg, start_activities, end_activities)

### Parse UiPath real world log from company in financial industry (0522_UiPath_RealWorld_Execution_Log.txt)
path_uiPath_bot_log = "data/0522_UiPath_RealWorld_Execution_Log_2.txt"

file = open(path_uiPath_bot_log, 'r')
lines = file.read().splitlines()
file.close()

connecting_attribute = 'Ordnungsbegriff'
attr_conceptName = 'message'
attr_timestamp = 'timeStamp'
attr_lifecycle = 'level'
valuesLifecycle = ['Error', 'Info', '']
standardValueLifecycle = "start"
attr_eventId = 'fingerprint'
attr_caseId = 'jobId'
attr_resource = 'robotName'
attr_botProcessName = 'processName'
attr_botProcessVersionNumber = 'processVersion'
attr_succcess = 'level'
valueNoSuccess = "Error"
traceLevelOnly = False

df_log = uipath_log_to_df(lines, connecting_attribute, attr_conceptName, attr_timestamp, attr_lifecycle, valuesLifecycle,standardValueLifecycle, attr_eventId, attr_caseId,
                          attr_resource, attr_botProcessName, attr_botProcessVersionNumber, attr_succcess, valueNoSuccess, traceLevelOnly)

parameters = {log_converter.Variants.TO_EVENT_LOG.value.Parameters.CASE_ID_KEY: 'case:caseId'}
log = log_converter.apply(df_log, parameters=parameters, variant=log_converter.Variants.TO_EVENT_LOG)

xes_exporter.apply(log, 'results/0522_UiPath_RealWorld_Execution_Log_parsed.xes')

dfg, start_activities, end_activities = pm4py.discover_dfg(log)
pm4py.view_dfg(dfg, start_activities, end_activities)