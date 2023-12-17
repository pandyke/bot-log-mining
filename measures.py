#Integrated Measures for Analysis

#Imports
import pm4py
import tempfile
from copy import copy
import pandas as pd
import numpy as np
from datetime import timezone, datetime, timedelta
from graphviz import Digraph
from IPython.display import display
from pm4py.objects.log.util import interval_lifecycle
from pm4py.statistics.attributes.log import get as attr_get
from pm4py.objects.dfg.utils import dfg_utils
from pm4py.util import xes_constants as xes
from pm4py.visualization.common.utils import *
from pm4py.util import exec_utils
from pm4py.visualization.dfg.parameters import Parameters
from pm4py.visualization.dfg import visualizer as dfg_visualization
from pm4py.visualization.common import gview
from pm4py.visualization.common import save as gsave
from enum import Enum
from pm4py.util import exec_utils
from pm4py.objects.conversion.log import converter as log_converter
from pm4py.algo.discovery.dfg import algorithm as dfg_discovery

#Customized functions for directly follows graph (dfg) visualization based on pm4py standard functions
def own_variant_measure_get_min_max_value(dfg):
    """
    Gets min and max value assigned to edges
    in DFG graph

    Parameters
    -----------
    dfg
        Directly follows graph

    Returns
    -----------
    min_value
        Minimum value in directly follows graph
    max_value
        Maximum value in directly follows graph
    """
    min_value = 9999999999
    max_value = -1

    for edge in dfg:
        if dfg[edge] < min_value:
            min_value = dfg[edge]
        if dfg[edge] > max_value:
            max_value = dfg[edge]

    return min_value, max_value
def own_variant_measure_assign_penwidth_edges(dfg):
    """
    Assign penwidth to edges in directly-follows graph

    Parameters
    -----------
    dfg
        Direcly follows graph

    Returns
    -----------
    penwidth
        Graph penwidth that edges should have in the direcly follows graph
    """
    penwidth = {}
    min_value, max_value = own_variant_measure_get_min_max_value(dfg)
    for edge in dfg:
        v0 = dfg[edge]
        v1 = get_arc_penwidth(v0, min_value, max_value)
        penwidth[edge] = str(v1)

    return penwidth
def own_variant_measure_graphviz_visualization(activities_count, activities_color, activities_labels, dfg, show_edge_labels=True, image_format="png", measure="own_measure_1",
                           max_no_of_edges_in_diagram=170, start_activities=None, end_activities=None):
    """
    Do GraphViz visualization of a DFG graph

    Parameters
    -----------
    activities_count
        Count of attributes in the log (may include attributes that are not in the DFG graph)
    activities_color
        color values (hex) for the visualization of each activity
    activities_labels
        labels (str) for the visualization of each activity
    dfg
        DFG graph
    show_edge_labels
        A boolean indicating whether the labels of the edges of the graph should be displayed or not
    max_no_of_edges_in_diagram
        Maximum number of edges in the diagram allowed for visualization

    Returns
    -----------
    viz
        Digraph object
    """
    if start_activities is None:
        start_activities = []
    if end_activities is None:
        end_activities = []

    filename = tempfile.NamedTemporaryFile(suffix='.gv')
    viz = Digraph("", filename=filename.name, engine='dot', graph_attr={'bgcolor': 'transparent'})

    # first, remove edges in diagram that exceeds the maximum number of edges in the diagram
    dfg_key_value_list = []
    for edge in dfg:
        dfg_key_value_list.append([edge, dfg[edge]])
    # more fine grained sorting to avoid that edges that are below the threshold are
    # undeterministically removed
    dfg_key_value_list = sorted(dfg_key_value_list, key=lambda x: (x[1], x[0][0], x[0][1]), reverse=True)
    dfg_key_value_list = dfg_key_value_list[0:min(len(dfg_key_value_list), max_no_of_edges_in_diagram)]
    dfg_allowed_keys = [x[0] for x in dfg_key_value_list]
    dfg_keys = list(dfg.keys())
    for edge in dfg_keys:
        if edge not in dfg_allowed_keys:
            del dfg[edge]

    # calculate edges penwidth
    penwidth = own_variant_measure_assign_penwidth_edges(dfg)
    activities_in_dfg = set()
    # activities_count_int is then just the same as activities_count
    activities_count_int = copy(activities_count)

    for edge in dfg:
        activities_in_dfg.add(edge[0])
        activities_in_dfg.add(edge[1])

    # represent nodes
    viz.attr('node', shape='box')

    if len(activities_in_dfg) == 0:
        activities_to_include = sorted(list(set(activities_count_int)))
    else:
        # take unique elements as a list not as a set (in this way, nodes are added in the same order to the graph)
        activities_to_include = sorted(list(set(activities_in_dfg)))

    activities_map = {}

    for act in activities_to_include:
        if act in activities_count_int:
            #viz.node(str(hash(act)), act + " (" + str(activities_count_int[act]) + ")", style='filled',
            #         fillcolor=activities_color[act])
            #customized:
            viz.node(str(hash(act)), activities_labels[act], style='filled',
                     fillcolor=activities_color[act])
            activities_map[act] = str(hash(act))
        else:
            viz.node(str(hash(act)), act)
            activities_map[act] = str(hash(act))

    # make edges addition always in the same order
    dfg_edges = sorted(list(dfg.keys()))

    # represent edges
    for edge in dfg_edges:
        label = str(dfg[edge])
        #label = human_readable_stat(dfg[edge])
        if show_edge_labels:
            viz.edge(str(hash(edge[0])), str(hash(edge[1])), label=label, penwidth=str(penwidth[edge]))
        else:
            viz.edge(str(hash(edge[0])), str(hash(edge[1])), label="", penwidth=str(penwidth[edge]))
        
    start_activities_to_include = [act for act in start_activities if act in activities_map]
    end_activities_to_include = [act for act in end_activities if act in activities_map]

    if start_activities_to_include:
        viz.node("@@startnode", "@@S", style='filled', shape='circle', fillcolor="#32CD32", fontcolor="#32CD32")
        for act in start_activities_to_include:
            viz.edge("@@startnode", activities_map[act])

    if end_activities_to_include:
        viz.node("@@endnode", "@@E", style='filled', shape='circle', fillcolor="#FFA500", fontcolor="#FFA500")
        for act in end_activities_to_include:
            viz.edge(activities_map[act], "@@endnode")

    viz.attr(overlap='false')
    viz.attr(fontsize='11')

    viz.format = image_format

    return viz
def custom_variant_measure_apply(dfg, activities_color, activities_labels, show_edge_labels=True, log=None, parameters=None,
                                 activities_count=None, max_no_of_edges=200):
    if parameters is None:
        parameters = {}

    activity_key = exec_utils.get_param_value(Parameters.ACTIVITY_KEY, parameters, xes.DEFAULT_NAME_KEY)
    image_format = exec_utils.get_param_value(Parameters.FORMAT, parameters, "png")
    max_no_of_edges_in_diagram = exec_utils.get_param_value(Parameters.MAX_NO_EDGES_IN_DIAGRAM, parameters, max_no_of_edges)
    start_activities = exec_utils.get_param_value(Parameters.START_ACTIVITIES, parameters, [])
    end_activities = exec_utils.get_param_value(Parameters.END_ACTIVITIES, parameters, [])

    if activities_count is None:
        if log is not None:
            activities_count = attr_get.get_attribute_values(log, activity_key, parameters=parameters)
        else:
            activities = dfg_utils.get_activities_from_dfg(dfg)
            activities_count = {key: 1 for key in activities}

    return own_variant_measure_graphviz_visualization(activities_count, activities_color, activities_labels, dfg, show_edge_labels, image_format=image_format,
                                  max_no_of_edges_in_diagram=max_no_of_edges_in_diagram,
                                  start_activities=start_activities, end_activities=end_activities)

#Functions for loading and preprocessing the merged log 
def load_merged_log_and_preprocess(path, attr_lifecycle, attr_timestamp, show_progress=True):
    """
    Load a merged log in xes format here and preprocess.
        Affordances to the merged xes log before applying the measures on it in the next steps:
        Events are sorted by timestamp in ascending order
        Every event in the log has to contain at least the following attributes for the measures to function    
            attr_activity
                The name/key of the attribute in the log which contains the name of an activity. Example value: 'concept:name'
            attr_traceID
                The name/key of the attribute in the log which contains the traceID,
                i.e. the identifier that matches every event to a specific trace. Example value: 'docid_uuid'
            attr_timestamp
                The name/key of the attribute in the log which contains the timestamp. Example value: 'time:timestamp'
            attr_success
                The name/key of the attribute in the log which contains the information
                whether the event was successfull or not (true / false). Example value: 'success'
            attr_bot
                The name/key of the attribute in the log which contains the information
                whether the event was executed by a bot or not (true / false). Example value: 'true'
            attr_eventid
                The name/key of the attribute in the log which contains the unique id of an event. Example value: 'eventid'
            attr_lifecycle
                The name/key of the attribute in the log which contains the lifecycle ('start' or 'complete').
                Example value: 'lifecycle:transition'
            
        Further needed attributes as defined in the paper
            org:resource
            botProcessName
            botProcessVersionNumber
        Note: A connecting attribute was only needed in the previous steps to create a merged log
        
        Converts a lifecycle log (each event is instantaneous, has lifecycles) to an
        interval log (events may be associated to two timestamps - start and end timestamp)
        The respective timestamps are saved in the 'start_timestamp' and 'end_timestamp' column
        Note that if the log basically includes 'start' and 'complete' events, the code will try to convert the log to an
        interval log. If an event misses a corresponding event (e.g. only the 'start' event exists but the corresponding
        'complete' event is missing) then this event will just be deleted/not considered anymore
        So make sure that the log either always has corresponding 'start' and 'complete' events for every activity (as 'pairs')
        or that the log uses only one type of 'lifecycle:transition' in every case (e.g. only 'complete' events)
    
    Parameters
    -----------
    path
        the path to the merged xes log
    attr_lifecycle
        The name/key of the attribute in the log which contains the lifecycle ('start' or 'complete').
        Example value: 'lifecycle:transition'
    attr_timestamp
        The name/key of the attribute in the log which contains the timestamp. Example value: 'time:timestamp'
    show_progress
        Whether a progress update every 1000 events should be printed out or not
    Returns
    -----------
    log_final, df_log_final, dfg_final
        The log loaded in pm4py, the log as dataframe and the directly follows graph
    """
    
    log_initial = pm4py.read_xes(path)
    df_log_initial = log_converter.apply(log_initial, variant=log_converter.Variants.TO_DATA_FRAME)
    
    #Check if the log includes 'start' AND 'complete' events
    all_lifecycles = list(df_log_initial[attr_lifecycle].unique())
    print("All lifecylces in log:", all_lifecycles)
    if 'start' in all_lifecycles and 'complete' in all_lifecycles:
        print("This log includes 'start' and 'complete' events")
        #If yes convert lifecycle log to interval log
        log_final = interval_lifecycle.to_interval(log_initial)
        df_log_final = log_converter.apply(log_final, variant=log_converter.Variants.TO_DATA_FRAME)
    else:
        print("This log does not include 'start' AND 'complete' events")
        #not 'start' AND 'complete' events included
        log_final = log_initial
        #add/adjust the respective columns 'start_timestamp' and 'time:timestamp' accordingly for every event
        df_log_final =df_log_initial.copy()
        df_log_final['start_timestamp'] = pd.NaT
        progress_counter = 0
        for index, event in df_log_final.iterrows():
            if event[attr_lifecycle] == 'start':
                #the timestamps are written in the 'start_timestamp' column
                df_log_final.at[index,'start_timestamp'] = event[attr_timestamp]
                #and the value in the 'time:timestamp' column is set to nat
                df_log_final.at[index, attr_timestamp] = pd.NaT
            #Note: When lifecylce is 'complete' then the timestamp already is in the right column ('time:timestamp') and the
            #'start_timestamp' column is already set to NaT, so nothing to change here
            elif event[attr_lifecycle] != 'complete':
                #Delete row since lifecycle attribute is not 'start' or 'complete'
                df_log_final.drop(index, inplace=True)
            if show_progress:
                progress_counter = progress_counter + 1
                if progress_counter % 1000 == 0:
                    print(progress_counter, " of ", len(df_log_final), " events preprocessed")
    
    df_log_final.rename(columns={attr_timestamp: 'end_timestamp'}, inplace=True)
    df_log_final['end_timestamp'] =  pd.to_datetime(df_log_final['end_timestamp'], utc=True)
    df_log_final['start_timestamp'] =  pd.to_datetime(df_log_final['start_timestamp'], utc=True)
    dfg_final = dfg_discovery.apply(log_final)

    return log_final, df_log_final, dfg_final
def preprocess_add_columns(df_log, attr_traceID, attr_timestamp, attr_activity, attr_eventid, attr_bot):
    """
    Adds several new columns to the log dataframe (one row in df equals one event):
        path: The path of the current trace, i.e. the sequence in which the activities are executed in this trace
        trace_start: The time when the current trace started
        trace_end: The time when the current trace ended
        trace_execution_time: The execution time of the current trace (trace_end-trace_start)
        time_until_end: The time it takes from the current event to the end of the trace
        act_exe_time: The exact execution time of the current event (can only be calcualted if the start and end time
            of the current event are known)
        act_exe_time_appr: The approximated execution time of the current event (uses start or end times only,
            depending on which values are given)
        followed_by: indicates by whom the following event was executed ('bot', 'human' or if it was the 'end_of_trace') 

    Parameters
    -----------
    df_log
        The log dataframe
    attr_traceID
        The name/key of the attribute in the log which contains the traceID,
        i.e. the identifier that matches every event to a specific trace. Example value: 'docid_uuid'
    attr_timestamp
        The name/key of the attribute in the log which contains the timestamp. Example value: 'time:timestamp'
    attr_activity
        The name/key of the attribute in the log which contains the name of an activity. Example value: 'concept:name'
    attr_eventid
        The name/key of the attribute in the log which contains the unique id of an event. Example value: 'eventid'
    attr_bot
        The name/key of the attribute in the log which contains the information
        whether the event was executed by a bot or not (true / false). Example value: 'bot'

    Returns
    -----------
    df
        The dataframe with the 4 new columns
    """
    df = df_log.copy()
    all_traceIDs = list(df[attr_traceID].unique())
    all_traceIDs = [x for x in all_traceIDs if str(x) != 'nan']
    
    counter = 0
    df['is_first_event_in_trace'] = False
    for trace in all_traceIDs:
        eventsInTrace = df.loc[(df[attr_traceID] == trace)]
        first_event_id = eventsInTrace[attr_eventid].iloc[0]
        df.loc[df[str(attr_eventid)] == first_event_id, 'is_first_event_in_trace'] = True
        
        trace_path = eventsInTrace[attr_activity].tolist()
        trace_path_str = ','.join(trace_path)
        df.loc[df[attr_traceID] == trace, 'path'] = trace_path_str
        
        min_start_timestamp = eventsInTrace['start_timestamp'].min()
        min_end_timestamp = eventsInTrace['end_timestamp'].min()
        if not pd.isnull(min_start_timestamp):
            trace_start = min_start_timestamp
        else:
            #If only complete events are in the log then the trace start time is approximately set to
            #the end/complete timestamp of the first event in the trace
            trace_start = min_end_timestamp
        
        max_start_timestamp = eventsInTrace['start_timestamp'].max()
        max_end_timestamp = eventsInTrace['end_timestamp'].max()
        if not pd.isnull(max_end_timestamp):
            trace_end = max_end_timestamp
        else:
            #If only start events are in the log then the trace end time is approximately set to
            #the start timestamp of the last event in the trace
            trace_end = max_start_timestamp
        df.loc[df[attr_traceID] == trace, 'trace_start'] = trace_start
        df.loc[df[attr_traceID] == trace, 'trace_end'] = trace_end
        df['trace_start'] =  pd.to_datetime(df['trace_start'], utc=True)
        df['trace_end'] =  pd.to_datetime(df['trace_end'], utc=True)
        df['trace_execution_time'] = df['trace_end'] - df['trace_start']
        
        counter = counter + 1
        if counter%100 == 0:
            print(counter, "of", len(all_traceIDs), "traces")
    
    df['time_until_end'] = df.apply(lambda x: x['trace_end'] - x['end_timestamp'] if not pd.isnull(x['end_timestamp']) else x['trace_end'] - x['start_timestamp'], axis=1)
    df['time_until_end'] = df.apply(lambda x: x['time_until_end'] if x['time_until_end'] >= timedelta(days = 0) else timedelta(days = 0), axis=1)
    
    df['act_exe_time'] = df['end_timestamp'] - df['start_timestamp']
            
    all_start_timestamps = list(df['start_timestamp'].unique())
    if all_start_timestamps == [pd.NaT] or pd.isnull(all_start_timestamps).all():
        #i.e only complete/end events are in the log
        df['act_exe_time_appr'] = df['end_timestamp'] - df['end_timestamp'].shift(1)
        df.loc[df['is_first_event_in_trace'] == True, 'act_exe_time_appr'] = pd.NaT
    else:
        df['act_exe_time_appr'] = df['start_timestamp'] - df['start_timestamp'].shift(1)
        df.loc[df['is_first_event_in_trace'] == True, 'act_exe_time_appr'] = pd.NaT
    
    
    df['following_trace'] = df[attr_traceID].shift(-1)
    df['following_resource'] = df[attr_bot].shift(-1)
    df['followed_by'] = df.apply(lambda x: 'bot' if x['following_resource'] == True else 'human', axis=1)
    df['followed_by'] = df.apply(lambda x: 'end_of_trace' if x[attr_traceID] != x['following_trace'] else x['followed_by'], axis=1)
    df.drop(['following_trace', 'following_resource'], axis=1, inplace=True)
    
    return df

#Customized helper functions based on pm4py standard functions
def get_color_hex(color_as_string, color_intensity):
    """
    Tranforming a color inputed as string and a wanted intensity to a respective color hex value.
    Beginning not with the darkest shade of the respective color for better visualization

    Parameters
    -----------
    color_as_string
        The wanted color formulated as string
    color_intensity
        An intensity value between 0 and 1
        
    Returns
    -----------
    color_hex
        The hex value of the specific color
    """
    if color_as_string == 'yellow':
        #color_hex = '#FFFF00'
        r = int((color_intensity*100)+155)
        g = int((color_intensity*100)+155)
        b = 0
    elif color_as_string == 'blue':
        #color_hex = '#0000FF'
        r = 0
        g = 0
        b = int((color_intensity*100)+155)
    elif color_as_string == 'green':
        #color_hex = '#00FF00'
        r = 0
        g = int((color_intensity*100)+155)
        b = 0
    else:
        #grey
        #color_hex = '#808080'
        r = 128
        g = 128
        b = 128
        
    color_hex = "#{:02x}{:02x}{:02x}".format(r,g,b)
    return color_hex
def get_color_intensity(dict_values):
    """
    Depending on the calculated measure values for every activity, the color intensities are calcualted.
    A higher measure value means a higher color intensity (brighter).
    If no fails occured at a specific activity (e.g. relative fails measure), or an activity was always followed by
        a human (e.g. bot human handover impact measure) the intensity is set to 0.0.
    If only fails occured at a specific activity (e.g. relative fails measure), or an activity was always followed by
        a bot (e.g. bot human handover impact measure), the intensity is set to 1.0.
    If no data was available, the intensity is set to 0.0.
    If all activities have the same measure value (min=max), the intensities of all activities are set to 0.5

    Parameters
    -----------
    dict_values
        Contains for every activity the value of the specific measure, calculated in advance (e.g. for 'relative fails measure')
        Some special values can be 'no fails' or 'only fails', which lead to a special coloring
        
    Returns
    -----------
    intensity
        A color intensity value between 0 and 1
    """
    dict_non_str_only = {}
    for activity, value in dict_values.items():
        if not isinstance(value,str):
            dict_non_str_only[activity] = value
    if len(dict_non_str_only) == 0:
        #This case occurs if all dict_values are strings
        min_value = 0
        max_value = 0
    else:
        min_value = min(dict_non_str_only.values())
        max_value = max(dict_non_str_only.values())
    
    intensity = {}
    for activity, value in dict_values.items():
        if value == "no fails" or value == "always followed by human" or value == "once followed by bot":
            intensity[activity] = 0
        elif value == "only fails" or value == "always followed by bot" or value == "once followed by human":
            intensity[activity] = 1
        elif value == "no data":
            intensity[activity] = 0
        else:
            #If all activities have the same measure values, all color intensities are set to the middle value 0.5
            if min_value == max_value:
                intensity[activity] = 0.5
            else:
                intensity[activity] = (value-min_value)/(max_value-min_value)
    return intensity
def get_coloring_by_resource(performed_by, color_intensities):
    """
    Get final coloring for every activity, based on by whom it was performed (bot, human, both) and
    by the values of the calculated measure (e.g. 'relative fails measure')

    Parameters
    -----------
    performed_by
        Indicates for every activity by whom it was performed. Can have the values 'manual_and_bot', 'bot_only' or 'manual_only'
    color_intensities
        A color intensity value between 0 and 1
        
    Returns
    -----------
    coloring
        For every activity the respective color hex value for visualization
    """
    coloring = {}
    for activity, value in performed_by.items():
        color_intensity = color_intensities[activity]
        if value == "manual_and_bot":
            coloring[activity] = get_color_hex('yellow', color_intensity)
        elif value == "bot_only":
            coloring[activity] = get_color_hex('blue', color_intensity)
        elif value == "manual_only":
            coloring[activity] = get_color_hex('green', color_intensity)
        else:
            coloring[activity] = get_color_hex('grey', color_intensity)
    
    return coloring
def timeFormatter(timedelta):
    """
    Formatting a timedelta to a string showing days, hours, minutes and seconds

    Parameters
    -----------
    timedelta
        a timedelta that should be formatted
        
    Returns
    -----------
    timedelta_as_string
        a formatted timedelta showing days, hours, minutes and seconds
    """
    total_seconds = timedelta.total_seconds()
    
    days, remainder1 = divmod(total_seconds, 3600*24)
    
    hours, remainder2 = divmod(remainder1, 3600)
    minutes, seconds = divmod(remainder2, 60)
    timedelta_as_string = '{:03}days {:02}h {:02}m {:02}s'.format(int(days), int(hours), int(minutes), int(seconds))
    return timedelta_as_string
def timeFormatter_seconds_input(total_seconds):
    """
    Formatting seconds to a string showing days, hours, minutes and seconds

    Parameters
    -----------
    total_seconds
        seconds that should be formatted
        
    Returns
    -----------
    seconds_as_string
        a formatted timedelta showing days, hours, minutes and seconds
    """
    if np.isnan(total_seconds):
        return "no data"
    
    days, remainder1 = divmod(total_seconds, 3600*24)
    
    hours, remainder2 = divmod(remainder1, 3600)
    minutes, seconds = divmod(remainder2, 60)
    seconds_as_string = '{:03}days {:02}h {:02}m {:02}s'.format(int(days), int(hours), int(minutes), int(seconds))
    return seconds_as_string


#Defining measures with a directly follows graph (dfg) visualization as output
def measure_relative_fails(df_log, round_decimals, attr_activity, attr_success, attr_bot):
    """
    Measure: Calculates the relative exception rate of every activity by dividing the number of events of a specific activity
    that failed by the total number of occurrences of that activity in the log

    Parameters
    -----------
    df_log
        The log dataframe
    round_decimals
        The number of decimals the relative fails value should be rounded to before visualization
    attr_activity
        The name/key of the attribute in the log which contains the name of an activity. Example value: 'concept:name'
    attr_success
        The name/key of the attribute in the log which contains the information
        whether the event was successfull or not (true / false). Example value: 'success'
    attr_bot
        The name/key of the attribute in the log which contains the information
        whether the event was executed by a bot or not (true / false). Example value: 'bot'
        
    Returns
    -----------
    labels, coloring
        The labels and coloring for every activity, needed for visualization and the name of the measure ('relative fails')
    """
    fail_rates = {}
    performed_by = {}
    activities_list = list(df_log[attr_activity].unique())
    for activity in activities_list:
        df_activity_only = df_log.loc[(df_log[attr_activity] == activity)]
        df_fails = df_activity_only.loc[df_activity_only[attr_success] == False]
        
        if len(df_fails) == 0:
            #no fails
            fail_rates[str(activity)] = "no fails"
        elif len(df_fails) == len(df_activity_only):
            #only fails
            fail_rates[str(activity)] = "only fails"
        else:
            fail_rates[str(activity)] = len(df_fails)/len(df_activity_only)
        
        performed_by_list = list(df_activity_only[attr_bot].unique())
        if (True in performed_by_list) and (False in performed_by_list):
            performed_by[str(activity)] = "manual_and_bot"
        elif True in performed_by_list:
            performed_by[str(activity)] = "bot_only"
        else:
            performed_by[str(activity)] = "manual_only"
        
    color_intensities = get_color_intensity(fail_rates)
    coloring = get_coloring_by_resource(performed_by, color_intensities)
    
    labels = {}
    for activity, value in fail_rates.items():
        if not isinstance(value, str):
            value_str = str(round(value*100,round_decimals)) + " %"
        else:
            value_str = value
        labels[activity] = activity + "\n" + value_str
    
    return labels, coloring, 'relative fails'
def measure_exception_time_impact(df_log, attr_activity, attr_success, attr_bot):
    """
    Measure: Calculates the average impact (in terms of time) which an activity has on the process, if the activity fails.
    It compares the remaining duration of the whole process in cases where the activity under observation failed
    to the remaining duration of the whole process in cases where the activity under observation did not fail.
    Calculation: mean_time_to_end_fails - mean_time_to_end_no_fails. This means a negative value indicates that the process
    on average ends faster when the respective activity fails

    Parameters
    -----------
    df_log
        The log dataframe
    attr_activity
        The name/key of the attribute in the log which contains the name of an activity. Example value: 'concept:name'
    attr_success
        The name/key of the attribute in the log which contains the information
        whether the event was successfull or not (true / false). Example value: 'success'
    attr_bot
        The name/key of the attribute in the log which contains the information
        whether the event was executed by a bot or not (true / false). Example value: 'bot'
        
    Returns
    -----------
    labels, coloring
        The labels and coloring for every activity, needed for visualization
        and the name of the measure ('exception time impact')
    """
    exception_time_impact = {}
    performed_by = {}
    activities_list = list(df_log[attr_activity].unique())
    for activity in activities_list:
        df_activity_only = df_log.loc[(df_log[attr_activity] == activity)]
        df_act_fails = df_activity_only.loc[df_activity_only[attr_success] == False]
        df_act_no_fails = df_activity_only.loc[df_activity_only[attr_success] == True]
        
        if len(df_act_fails) == 0:
            #no fails
            exception_time_impact[str(activity)] = "no fails"
        elif len(df_act_no_fails) == 0:
            #only fails
            exception_time_impact[str(activity)] = "only fails"
        else:
            mean_time_to_end_fails = df_act_fails.time_until_end.mean()
            mean_time_to_end_no_fails = df_act_no_fails.time_until_end.mean()
            eti_value = mean_time_to_end_fails - mean_time_to_end_no_fails
            exception_time_impact[str(activity)] = eti_value
        
        performed_by_list = list(df_activity_only[attr_bot].unique())
        if (True in performed_by_list) & (False in performed_by_list):
            performed_by[str(activity)] = "manual_and_bot"
        elif True in performed_by_list:
            performed_by[str(activity)] = "bot_only"
        else:
            performed_by[str(activity)] = "manual_only"
        
    color_intensities = get_color_intensity(exception_time_impact)
    coloring = get_coloring_by_resource(performed_by, color_intensities)
    
    labels = {}
    for activity, value in exception_time_impact.items():
        if not isinstance(value, str):
            value_str = timeFormatter(value)
        else:
            value_str = value
        labels[activity] = activity + "\n" + value_str
    
    return labels, coloring, 'exception time impact'
def measure_exception_time_variance(df_log, attr_activity, attr_success, attr_bot):
    """
    Measure: Calculates the standard deviation of the time it takes to end the process when an activity fails
    and when it does not fail.
    Note: For more interpretable results we use the standard deviation instead of the variance
    Calculates for every activity the std of the time it takes to end the process when the observed activity fails,
    and analogously the std when the observed activity does not fail.
    A higher gap between these two differences means a brighter coloring in the visualization

    Parameters
    -----------
    df_log
        The log dataframe
    attr_activity
        The name/key of the attribute in the log which contains the name of an activity. Example value: 'concept:name'
    attr_success
        The name/key of the attribute in the log which contains the information
        whether the event was successfull or not (true / false). Example value: 'success'
    attr_bot
        The name/key of the attribute in the log which contains the information
        whether the event was executed by a bot or not (true / false). Example value: 'bot'
        
    Returns
    -----------
    labels, coloring
        The labels and coloring for every activity, needed for visualization
        and the name of the measure ('exception_time_variance')
    """
    
    exception_time_variance_no_fail = {}
    exception_time_variance_fail = {}
    exception_time_variance_diff = {}
    performed_by = {}
    activities_list = list(df_log[attr_activity].unique())
    for activity in activities_list:
        df_activity_only = df_log.loc[(df_log[attr_activity] == activity)]
        df_act_fails = df_activity_only.loc[df_activity_only[attr_success] == False]
        df_act_no_fails = df_activity_only.loc[df_activity_only[attr_success] == True]
        
        if len(df_act_fails) == 0:
            #no fails
            exception_time_variance_no_fail[str(activity)] = df_act_no_fails.time_until_end.dt.total_seconds().std()
            exception_time_variance_fail[str(activity)] = "no fails"
            exception_time_variance_diff[str(activity)] = "no fails"
        elif len(df_act_no_fails) == 0:
            #only fails
            exception_time_variance_no_fail[str(activity)] = "only fails"
            exception_time_variance_fail[str(activity)] = df_act_fails.time_until_end.dt.total_seconds().std()
            exception_time_variance_diff[str(activity)] = "only fails"
        else:
            variance_time_to_end_fails = df_act_fails.time_until_end.dt.total_seconds().std()
            variance_time_to_end_no_fails = df_act_no_fails.time_until_end.dt.total_seconds().std()
            exception_time_variance_fail[str(activity)] = variance_time_to_end_fails
            exception_time_variance_no_fail[str(activity)] = variance_time_to_end_no_fails
            var_diff_value = variance_time_to_end_fails - variance_time_to_end_no_fails
            exception_time_variance_diff[str(activity)] = var_diff_value
        
        performed_by_list = list(df_activity_only[attr_bot].unique())
        if (True in performed_by_list) & (False in performed_by_list):
            performed_by[str(activity)] = "manual_and_bot"
        elif True in performed_by_list:
            performed_by[str(activity)] = "bot_only"
        else:
            performed_by[str(activity)] = "manual_only"
        
    color_intensities = get_color_intensity(exception_time_variance_diff)
    coloring = get_coloring_by_resource(performed_by, color_intensities)
    
    labels = {}
    for activity, value_no_fail in exception_time_variance_no_fail.items():
        if not isinstance(value_no_fail, str):
            value_str_no_fails = timeFormatter_seconds_input(value_no_fail)
        else:
            value_str_no_fails = value_no_fail
            
        value_fail = exception_time_variance_fail[activity]
        if not isinstance(value_fail, str):
            value_str_fail = timeFormatter_seconds_input(value_fail)
        else:
            value_str_fail = value_fail
            
        labels[activity] = activity + "\n" + "no fail: " + value_str_no_fails + "\n" + "fail: "+ value_str_fail
    
    return labels, coloring, 'exception_time_variance'
def measure_relative_execution_time(df_log_input, round_decimals, attr_activity, attr_bot):
    """
    Measure: Calculates the average execution time of an activity, compared to the average execution time of the whole process

    Parameters
    -----------
    df_log
        The log dataframe
    round_decimals
        The number of decimals the relative_execution_time value should be rounded to before visualization
    attr_activity
        The name/key of the attribute in the log which contains the name of an activity. Example value: 'concept:name'
    attr_bot
        The name/key of the attribute in the log which contains the information
        whether the event was executed by a bot or not (true / false). Example value: 'bot'
        
    Returns
    -----------
    labels, coloring
        The labels and coloring for every activity, needed for visualization
        and the name of the measure ('relative_execution_time')
    """
    df_log = df_log_input.copy()
    mean_exe_time_process = df_log.trace_execution_time.mean()
    #print("mean_exe_time_process:", mean_exe_time_process)
        
    relative_execution_time = {}
    approximated_bool = {}
    performed_by = {}
    activities_list = list(df_log[attr_activity].unique())
    for activity in activities_list:
        df_activity_only = df_log.loc[(df_log[attr_activity] == activity)]
        
        all_act_exe_times = list(df_activity_only['act_exe_time'].unique())
        if all_act_exe_times == [pd.NaT] or pd.isnull(all_act_exe_times):
            #This means the exact activity execution times (end_timestamp-start_timestamp) could not be calculated
            #because there are not both 'start' and 'complete' lifecycle events
            #Therefore take the approximated activity execution times
            mean_exe_time_activity = df_activity_only.act_exe_time_appr.mean()
            approximated_bool[str(activity)] = True
        else:
            #This means there is at least one exact activity execution time, i.e. there were some corresponding
            #'start' and 'complete' lifecycle event pairs
            #Therefore take the exact activity execution times
            mean_exe_time_activity = df_activity_only.act_exe_time.mean()
            approximated_bool[str(activity)] = False
        
        #print("mean_exe_time_activity",activity,mean_exe_time_activity)
        ret_value = mean_exe_time_activity / mean_exe_time_process
        if pd.isnull(ret_value):
            relative_execution_time[str(activity)] = "no data"
        else:
            relative_execution_time[str(activity)] = ret_value
        
        performed_by_list = list(df_activity_only[attr_bot].unique())
        if (True in performed_by_list) & (False in performed_by_list):
            performed_by[str(activity)] = "manual_and_bot"
        elif True in performed_by_list:
            performed_by[str(activity)] = "bot_only"
        else:
            performed_by[str(activity)] = "manual_only"
    
    color_intensities = get_color_intensity(relative_execution_time)
    coloring = get_coloring_by_resource(performed_by, color_intensities)
    
    labels = {}
    for activity, value in relative_execution_time.items():
        if not isinstance(value, str):
            value_str = str(round(value*100,round_decimals)) + " %"
        else:
            value_str = value
        if approximated_bool[activity] and value_str != "no data":
            labels[activity] = activity + "\n" + "appr. " + value_str
        else:
            labels[activity] = activity + "\n" + value_str
    
    return labels, coloring, 'relative_execution_time'
def measure_execution_time_variance(df_log_input, round_decimals, attr_activity, attr_bot):
    """
    Measure: Calculates the standard deviation of the execution times of every activity
    Note: For more interpretable results we use the standard deviation instead of the variance

    Parameters
    -----------
    df_log
        The log dataframe
    round_decimals
        The number of decimals the execution_time_variance value should be rounded to before visualization
    attr_activity
        The name/key of the attribute in the log which contains the name of an activity. Example value: 'concept:name'
    attr_bot
        The name/key of the attribute in the log which contains the information
        whether the event was executed by a bot or not (true / false). Example value: 'bot'
        
    Returns
    -----------
    labels, coloring
        The labels and coloring for every activity, needed for visualization
        and the name of the measure ('execution_time_variance')
    """
    df_log = df_log_input.copy()
        
    execution_time_variance = {}
    approximated_bool = {}
    performed_by = {}
    activities_list = list(df_log[attr_activity].unique())
    for activity in activities_list:
        df_activity_only = df_log.loc[(df_log[attr_activity] == activity)]
        all_act_exe_times = list(df_activity_only['act_exe_time'].unique())
        if all_act_exe_times == [pd.NaT] or pd.isnull(all_act_exe_times):
            #This means the exact activity execution times (end_timestamp-start_timestamp) could not be calculated
            #because there are not both 'start' and 'complete' lifecycle events
            #Therefore take the approximated activity execution times
            std_exe_time_activity = df_activity_only.act_exe_time_appr.dt.total_seconds().std()
            approximated_bool[str(activity)] = True
        else:
            #This means there is at least one exact activity execution time, i.e. there were some corresponding
            #'start' and 'complete' lifecycle event pairs
            #Therefore take the exact activity execution times
            std_exe_time_activity = df_activity_only.act_exe_time.dt.total_seconds().std()
            approximated_bool[str(activity)] = False
        
        if pd.isnull(std_exe_time_activity):
            execution_time_variance[str(activity)] = "no data"
        else:
            execution_time_variance[str(activity)] = std_exe_time_activity
        
        performed_by_list = list(df_activity_only[attr_bot].unique())
        if (True in performed_by_list) & (False in performed_by_list):
            performed_by[str(activity)] = "manual_and_bot"
        elif True in performed_by_list:
            performed_by[str(activity)] = "bot_only"
        else:
            performed_by[str(activity)] = "manual_only"
    
    color_intensities = get_color_intensity(execution_time_variance)
    coloring = get_coloring_by_resource(performed_by, color_intensities)
    
    labels = {}
    for activity, value in execution_time_variance.items():
        if not isinstance(value, str):
            value_str = timeFormatter_seconds_input(value)
        else:
            value_str = value
        if approximated_bool[activity] and value_str != "no data":
            labels[activity] = activity + "\n" + "appr. " + value_str
        else:
            labels[activity] = activity + "\n" + value_str
    
    return labels, coloring, 'execution_time_variance'
def measure_bot_human_handover_count(df_log, attr_activity, attr_bot):
    """
    Measure: Calculates for every activity how often it is followed by a bot or human activity. All color intensities are same

    Parameters
    -----------
    df_log
        The log dataframe
    attr_activity
        The name/key of the attribute in the log which contains the name of an activity. Example value: 'concept:name'
    attr_bot
        The name/key of the attribute in the log which contains the information
        whether the event was executed by a bot or not (true / false). Example value: 'bot'
        
    Returns
    -----------
    labels, coloring
        The labels and coloring for every activity, needed for visualization and the name of the measure ('bot_human_handover_count')
    """
    followed_by_bot = {}
    followed_by_human = {}
    performed_by = {}
    color_intensities = {}
    activities_list = list(df_log[attr_activity].unique())
    for activity in activities_list:
        df_activity_only = df_log.loc[(df_log[attr_activity] == activity)]
        df_followed_by_bot = df_activity_only.loc[df_activity_only['followed_by'] == 'bot']
        df_followed_by_human = df_activity_only.loc[df_activity_only['followed_by'] == 'human']
        followed_by_bot[activity] = len(df_followed_by_bot)
        followed_by_human[activity] = len(df_followed_by_human)
        
        performed_by_list = list(df_activity_only[attr_bot].unique())
        if (True in performed_by_list) & (False in performed_by_list):
            performed_by[str(activity)] = "manual_and_bot"
        elif True in performed_by_list:
            performed_by[str(activity)] = "bot_only"
        else:
            performed_by[str(activity)] = "manual_only"
    
        color_intensities[activity] = 0.5
        
    coloring = get_coloring_by_resource(performed_by, color_intensities)
    
    labels = {}
    for activity, value in followed_by_bot.items():
        value_2 = followed_by_human[activity]
        if value == 0:
            #always followed by a human activity
            labels[activity] = activity + "\n" + "always followed by human"
        elif value_2 == 0:
            #always followed by a bot activity
            labels[activity] = activity + "\n" + "always followed by bot"
        else:
            labels[activity] = activity + "\n" + "followed by bot: " + str(value) + "\n" + "followed by human: " + str(value_2)
    
    return labels, coloring, 'bot_human_handover_count'
def measure_bot_human_handover_impact(df_log, attr_activity, attr_bot):
    """
    Measure: Calculates for every activity how much longer it takes on average to end the process,
        when the activity is followed by a bot activity, compared to when it is followed by a human activity.
        Calculation: mean_time_to_end_followed_bot - mean_time_to_end_followed_human.
        This means a positive value indicates that on average the process takes longer to end when the respective
        activity is followed by a bot

    Parameters
    -----------
    df_log
        The log dataframe
    attr_activity
        The name/key of the attribute in the log which contains the name of an activity. Example value: 'concept:name'
    attr_bot
        The name/key of the attribute in the log which contains the information
        whether the event was executed by a bot or not (true / false). Example value: 'bot'
        
    Returns
    -----------
    labels, coloring
        The labels and coloring for every activity, needed for visualization
        and the name of the measure ('bot_human_handover_impact')
    """
    bot_human_handover_impact = {}
    performed_by = {}
    activities_list = list(df_log[attr_activity].unique())
    for activity in activities_list:
        df_activity_only = df_log.loc[(df_log[attr_activity] == activity)]
        df_act_followed_bot = df_activity_only.loc[df_activity_only['followed_by'] == 'bot']
        df_act_followed_human = df_activity_only.loc[df_activity_only['followed_by'] == 'human']
        
        if len(df_act_followed_bot) == 0:
            #always followed by human
            bot_human_handover_impact[str(activity)] = "always followed by human"
        elif len(df_act_followed_human) == 0:
            #always followed by bot
            bot_human_handover_impact[str(activity)] = "always followed by bot"
        else:
            mean_time_to_end_followed_bot = df_act_followed_bot.time_until_end.mean()
            mean_time_to_end_followed_human = df_act_followed_human.time_until_end.mean()
            bhhi_value = mean_time_to_end_followed_bot - mean_time_to_end_followed_human
            bot_human_handover_impact[str(activity)] = bhhi_value
        
        performed_by_list = list(df_activity_only[attr_bot].unique())
        if (True in performed_by_list) & (False in performed_by_list):
            performed_by[str(activity)] = "manual_and_bot"
        elif True in performed_by_list:
            performed_by[str(activity)] = "bot_only"
        else:
            performed_by[str(activity)] = "manual_only"
        
    color_intensities = get_color_intensity(bot_human_handover_impact)
    coloring = get_coloring_by_resource(performed_by, color_intensities)
    
    labels = {}
    for activity, value in bot_human_handover_impact.items():
        if not isinstance(value, str):
            value_str = timeFormatter(value)
        else:
            value_str = value
        labels[activity] = activity + "\n" + value_str
    
    return labels, coloring, 'bot_human_handover_impact'
def measure_bot_human_handover_variance(df_log, attr_activity, attr_bot):
    """
    Measure: Calculates for every activity the standard deviation of the time it takes to end the process when the activity
        is followed by a bot activity and analogously the standard deviation when the activity is followed by a human activity
    Note: For more interpretable results we use the standard deviation instead of the variance
    Calculates for every activity the std of the time it takes to end the process when the observed activity is
        followed by a bot, and analogously the std when the observed activity is followed by a human.
        A higher gap between these two differences means a brighter coloring in the visualization

    Parameters
    -----------
    df_log
        The log dataframe
    attr_activity
        The name/key of the attribute in the log which contains the name of an activity. Example value: 'concept:name'
    attr_bot
        The name/key of the attribute in the log which contains the information
        whether the event was executed by a bot or not (true / false). Example value: 'bot'
        
    Returns
    -----------
    labels, coloring
        The labels and coloring for every activity, needed for visualization
        and the name of the measure ('bot_human_handover_variance')
    """
    
    bot_human_handover_variance_followed_human = {}
    bot_human_handover_variance_followed_bot = {}
    bot_human_handover_variance_diff = {}
    performed_by = {}
    activities_list = list(df_log[attr_activity].unique())
    for activity in activities_list:
        df_activity_only = df_log.loc[(df_log[attr_activity] == activity)]
        df_act_followed_bot = df_activity_only.loc[df_activity_only['followed_by'] == 'bot']
        df_act_followed_human = df_activity_only.loc[df_activity_only['followed_by'] == 'human']
                
        if len(df_act_followed_bot) == 0 and len(df_act_followed_human) == 0:
            #no data
            bot_human_handover_variance_followed_human[str(activity)] = "no data"
            bot_human_handover_variance_followed_bot[str(activity)] = "no data"
            bot_human_handover_variance_diff[str(activity)] = "no data"
        
        elif len(df_act_followed_bot) == 0:
            #always followed by a human activity
            bot_human_handover_variance_followed_human[str(activity)] = df_act_followed_human.time_until_end.dt.total_seconds().std()
            bot_human_handover_variance_followed_bot[str(activity)] = "always followed by human"
            bot_human_handover_variance_diff[str(activity)] = "always followed by human"
        elif len(df_act_followed_human) == 0:
            #always followed by a bot activity
            bot_human_handover_variance_followed_human[str(activity)] = "always followed by bot"
            bot_human_handover_variance_followed_bot[str(activity)] = df_act_followed_bot.time_until_end.dt.total_seconds().std()
            bot_human_handover_variance_diff[str(activity)] = "always followed by bot"
        
        elif len(df_act_followed_bot) == 1:
            #only once followed by a bot
            bot_human_handover_variance_followed_human[str(activity)] = df_act_followed_human.time_until_end.dt.total_seconds().std()
            bot_human_handover_variance_followed_bot[str(activity)] = "once followed by bot"
            bot_human_handover_variance_diff[str(activity)] = "once followed by bot"
        elif len(df_act_followed_human) == 1:
            #only once followed by a human
            bot_human_handover_variance_followed_human[str(activity)] = "once followed by human"
            bot_human_handover_variance_followed_bot[str(activity)] = df_act_followed_bot.time_until_end.dt.total_seconds().std()
            bot_human_handover_variance_diff[str(activity)] = "once followed by human"
        
        else:
            variance_time_to_end_followed_bot = df_act_followed_bot.time_until_end.dt.total_seconds().std()
            variance_time_to_end_followed_human = df_act_followed_human.time_until_end.dt.total_seconds().std()
            bot_human_handover_variance_followed_bot[str(activity)] = variance_time_to_end_followed_bot
            bot_human_handover_variance_followed_human[str(activity)] = variance_time_to_end_followed_human
            var_diff_value = variance_time_to_end_followed_bot - variance_time_to_end_followed_human
            bot_human_handover_variance_diff[str(activity)] = var_diff_value
        
        performed_by_list = list(df_activity_only[attr_bot].unique())
        if (True in performed_by_list) & (False in performed_by_list):
            performed_by[str(activity)] = "manual_and_bot"
        elif True in performed_by_list:
            performed_by[str(activity)] = "bot_only"
        else:
            performed_by[str(activity)] = "manual_only"
    
    color_intensities = get_color_intensity(bot_human_handover_variance_diff)
    coloring = get_coloring_by_resource(performed_by, color_intensities)
    
    labels = {}
    for activity, value_followed_human in bot_human_handover_variance_followed_human.items():
        if not isinstance(value_followed_human, str):
            value_str_followed_human = timeFormatter_seconds_input(value_followed_human)
        else:
            value_str_followed_human = value_followed_human
            
        value_followed_bot = bot_human_handover_variance_followed_bot[activity]
        if not isinstance(value_followed_bot, str):
            value_str_followed_bot = timeFormatter_seconds_input(value_followed_bot)
        else:
            value_str_followed_bot = value_followed_bot
            
        labels[activity] = activity + "\n" + "followed by human: " + value_str_followed_human + "\n" + "followed by bot: "+ value_str_followed_bot
    
    return labels, coloring, 'bot_human_handover_variance'

#Defining measures with a dataframe as output
def measure_relative_case_fails(df_log, round_decimals, attr_traceID, attr_success, attr_bot, show_progress=True):
    """
    Measure: Calculates for every path (i.e. for every occuring sequence of activities in the traces) the fail rate and bot share
        fail rate: dividing the number of traces with that specific path that include at least one event that failed
        by the total number of traces with that specific path
        bot share: dividing the number of events with that specific path that were performed by a bot
        by the total number of events with that path

    Parameters
    -----------
    df_log
        The log dataframe
    round_decimals
        The number of decimals the relative fails value should be rounded to before visualization
    attr_traceID
        The name/key of the attribute in the log which contains the traceID,
        i.e. the identifier that matches every event to a specific trace. Example value: 'docid_uuid'
    attr_success
        The name/key of the attribute in the log which contains the information
        whether the event was successfull or not (true / false). Example value: 'success'
    attr_bot
        The name/key of the attribute in the log which contains the information
        whether the event was executed by a bot or not (true / false). Example value: 'bot'
    show_progress
        Whether a progress update every 100 paths should be printed out or not
        
    Returns
    -----------
    results_df, 'relative_case_fails'
        The results as a dataframe, containing for every path the fail rate and bot share of that path
        and the name of the measure ('relative_case_fails')
    """
    fail_rate = []
    bot_share = []
    paths_list = list(df_log['path'].unique())
    paths_list = [x for x in paths_list if str(x) != 'nan']
    
    progress_counter = 0
    for path in paths_list:
        df_path_activities_only = df_log.loc[(df_log['path'] == path)]
        traces_in_path_list = list(df_path_activities_only[attr_traceID].unique())
        number_traces_of_path = len(traces_in_path_list)
        number_failed_traces_of_path = 0
        for trace in traces_in_path_list:
            df_failed_trace_activities_only = df_path_activities_only.loc[(df_path_activities_only[attr_traceID] == trace) & 
                                                                  (df_path_activities_only[attr_success] == False)]
            if len(df_failed_trace_activities_only) > 0:
                #trace includes at least one failed activity
                number_failed_traces_of_path = number_failed_traces_of_path + 1
        current_path_fail_rate = number_failed_traces_of_path/number_traces_of_path
        current_path_fail_rate_str = str(round(current_path_fail_rate*100,round_decimals))
        fail_rate.append(current_path_fail_rate_str)
        
        #of all performed activities on that path x were performed by a bot
        df_bot_activities = df_path_activities_only.loc[(df_path_activities_only[attr_bot] == True)]
        current_bot_share = len(df_bot_activities) / len(df_path_activities_only)
        current_bot_share_str = str(round(current_bot_share*100,round_decimals))
        bot_share.append(current_bot_share_str)
        
        if show_progress:
            progress_counter = progress_counter + 1
            if progress_counter % 100 == 0:
                print(progress_counter, " of ", len(paths_list), " paths")
        
    results_df = pd.DataFrame(
                {'path': paths_list,
                 'fail rate in %': fail_rate,
                 'bot share in %': bot_share
                })
    results_df.sort_values(by=['fail rate in %'], ascending=False, inplace=True)
    
    return results_df, 'relative_case_fails'
def measure_automation_rate(df_log, round_decimals, attr_activity, attr_bot):
    """
    Measure: Calculates the number of activities that are performed by bots, humans or both,
        compared to the total number of activities

    Parameters
    -----------
    df_log
        The log dataframe
    round_decimals
        The number of decimals the relative fails value should be rounded to before visualization
    attr_activity
        The name/key of the attribute in the log which contains the name of an activity. Example value: 'concept:name'
    attr_bot
        The name/key of the attribute in the log which contains the information
        whether the event was executed by a bot or not (true / false). Example value: 'bot'
        
    Returns
    -----------
    results_df, 'automation_rate'
        The results as a dataframe, showing automation rate details and the name of the measure ('automation_rate')
    """
    bot_share_total = []
    bot_share_relative = []
    human_share_total = []
    human_share_relative = []
    
    activities_list = list(df_log[attr_activity].unique())
    for activity in activities_list:
        df_activity_only = df_log.loc[(df_log[attr_activity] == activity)]
        df_bot_share = df_activity_only.loc[df_activity_only[attr_bot] == True]
        df_human_share = df_activity_only.loc[df_activity_only[attr_bot] == False]
        
        bot_share_total.append(len(df_bot_share))
        current_bot_share_relative = len(df_bot_share)/len(df_activity_only)
        current_bot_share_relative_str = str(round(current_bot_share_relative*100,round_decimals))
        bot_share_relative.append(current_bot_share_relative_str)
        
        human_share_total.append(len(df_human_share))
        current_human_share_relative = len(df_human_share)/len(df_activity_only)
        current_human_share_relative_str = str(round(current_human_share_relative*100,round_decimals))
        human_share_relative.append(current_human_share_relative_str)
        
    results_df = pd.DataFrame(
                {'activity': activities_list,
                 'performed by bot': bot_share_total,
                 'performed by bot in %': bot_share_relative,
                 'performed manually': human_share_total,
                 'performed manually in %': human_share_relative
                })
    results_df.sort_values(by=['performed by bot in %'], ascending=False, inplace=True)
    
    return results_df, 'automation_rate'
def measure_case_activities_execution_time(df_log, attr_activity, show_progress=True):
    """
    Measure: Calculates for every case/path the average execution times of the single activites in the case/path

    Parameters
    -----------
    df_log
        The log dataframe
    attr_activity
        The name/key of the attribute in the log which contains the name of an activity. Example value: 'concept:name'
    show_progress
        Whether a progress update every 100 paths should be printed out or not
        
    Returns
    -----------
    results_df, 'case_activities_execution_time'
        The results as a dataframe, containing for every path the average execution times of the single activites
        and the name of the measure ('case_activities_execution_time')
    """
    
    activities_list = list(df_log[attr_activity].unique())

    paths_list = list(df_log['path'].unique())
    paths_list = [x for x in paths_list if str(x) != 'nan']

    empty_list = [np.nan] * len(paths_list)
    act_dict = {}
    act_dict['path'] = paths_list
    for activity in activities_list:
        act_dict[activity] = empty_list
    results_df = pd.DataFrame(act_dict)
    
    execution_times = []
    progress_counter = 0
    for path in paths_list:
        df_path_activities_only = df_log.loc[(df_log['path'] == path)]
        activities_in_path_list = list(df_path_activities_only[attr_activity].unique())
        
        for activity in activities_in_path_list:
            df_activity_only = df_path_activities_only.loc[(df_path_activities_only[attr_activity] == activity)]
            all_act_exe_times = list(df_activity_only['act_exe_time'].unique())
            if all_act_exe_times == [pd.NaT] or pd.isnull(all_act_exe_times):
                #This means the exact activity execution times (end_timestamp-start_timestamp) could not be calculated
                #because there are not both 'start' and 'complete' lifecycle events
                #Therefore take the approximated activity execution times
                mean_exe_time_activity = df_activity_only.act_exe_time_appr.mean()
            else:
                #This means there is at least one exact activity execution time, i.e. there were some corresponding
                #'start' and 'complete' lifecycle event pairs
                #Therefore take the exact activity execution times
                mean_exe_time_activity = df_activity_only.act_exe_time.mean()
            
            if pd.isnull(mean_exe_time_activity):
                mean_exe_time_activity_formatted = "no data"
            else:
                mean_exe_time_activity_formatted = timeFormatter(mean_exe_time_activity)
            current_index = results_df.index[results_df['path'] == path]
            results_df.at[current_index, activity] = mean_exe_time_activity_formatted
        
        if show_progress:
            progress_counter = progress_counter + 1
            if progress_counter % 100 == 0:
                print(progress_counter, " of ", len(paths_list), " paths")
        
    return results_df, 'case_activities_execution_time'
def measure_case_activities_execution_time_variance(df_log, attr_activity, show_progress=True):
    """
    Measure: Calculates for every case/path the standard deviation of the execution times
        of the single activites in the case/path
    Note: For more interpretable results we use the standard deviation instead of the variance

    Parameters
    -----------
    df_log
        The log dataframe
    attr_activity
        The name/key of the attribute in the log which contains the name of an activity. Example value: 'concept:name'
    show_progress
        Whether a progress update every 100 paths should be printed out or not
        
    Returns
    -----------
    results_df, 'case_activities_execution_time_variance'
        The results as a dataframe, containing for every path the average execution times of the single activites
        and the name of the measure ('case_activities_execution_time_variance')
    """
    
    activities_list = list(df_log[attr_activity].unique())

    paths_list = list(df_log['path'].unique())
    paths_list = [x for x in paths_list if str(x) != 'nan']

    empty_list = [np.nan] * len(paths_list)
    act_dict = {}
    act_dict['path'] = paths_list
    for activity in activities_list:
        act_dict[activity] = empty_list
    results_df = pd.DataFrame(act_dict)
    
    execution_times = []
    progress_counter = 0
    for path in paths_list:
        df_path_activities_only = df_log.loc[(df_log['path'] == path)]
        activities_in_path_list = list(df_path_activities_only[attr_activity].unique())
        
        for activity in activities_in_path_list:
            df_activity_only = df_path_activities_only.loc[(df_path_activities_only[attr_activity] == activity)]
            all_act_exe_times = list(df_activity_only['act_exe_time'].unique())
            if all_act_exe_times == [pd.NaT] or pd.isnull(all_act_exe_times):
                #This means the exact activity execution times (end_timestamp-start_timestamp) could not be calculated
                #because there are not both 'start' and 'complete' lifecycle events
                #Therefore take the approximated activity execution times
                variance_exe_time_activity = df_activity_only.act_exe_time_appr.dt.total_seconds().std()
            else:
                #This means there is at least one exact activity execution time, i.e. there were some corresponding
                #'start' and 'complete' lifecycle event pairs
                #Therefore take the exact activity execution times
                variance_exe_time_activity = df_activity_only.act_exe_time.dt.total_seconds().std()
            
            if pd.isnull(variance_exe_time_activity):
                variance_exe_time_activity_formatted = "no data"
            else:
                variance_exe_time_activity_formatted = timeFormatter_seconds_input(variance_exe_time_activity)
            current_index = results_df.index[results_df['path'] == path]
            results_df.at[current_index, activity] = variance_exe_time_activity_formatted
        
        if show_progress:
            progress_counter = progress_counter + 1
            if progress_counter % 100 == 0:
                print(progress_counter, " of ", len(paths_list), " paths")
        
    return results_df, 'case_activities_execution_time_variance'

#Function for applying the measures
def apply_measure(df_log, dfg, log, measure_name, attr_activity, attr_success, attr_bot, attr_traceID,
                  save_result=False, round_decimals=2, show_edge_labels=True, show_progress=True, max_no_of_edges=200):
    """
    Applies a measure identified by its name and returns either a visualization or a dataframe, depending on the measure

    Parameters
    -----------
    df_log
        The log dataframe
    dfg
        The directly follows graph
    log
        The log that was loaded
    measure_name
        The name of the measure which should be applied (e.g. 'relative_fails')
    attr_activity
        The name/key of the attribute in the log which contains the name of an activity. Example value: 'concept:name'
    attr_success
        The name/key of the attribute in the log which contains the information
        whether the event was successfull or not (true / false). Example value: 'success'
    attr_bot
        The name/key of the attribute in the log which contains the information
        whether the event was executed by a bot or not (true / false). Example value: 'bot'
    attr_traceID
        The name/key of the attribute in the log which contains the traceID,
        i.e. the identifier that matches every event to a specific trace. Example value: 'docid_uuid'
    save_result
        A boolean indicating for graphical measures whether the resulting visualization should be saved as .png or not
    round_decimals
        The number of decimals the relative fails value should be rounded to before visualization
    show_edge_labels
        A boolean indicating whether the labels of the edges of the graph should be displayed or not
    show_progress
        Whether a progress update every 100 paths should be printed out or not
    max_no_of_edges
        The maximum number of edges shown in the visualization. More edges show a more detailed picture of the process
    
    Returns
    -----------
    gviz or result_df
        The gviz for measures with a dfg visualization or the result_df for measures with a dataframe
    """
    # Measures with a dfg visualization as output
    is_graphical_measure = False
    if measure_name == 'relative_fails':
        is_graphical_measure = True
        activity_labeling, activity_coloring, measure = measure_relative_fails(df_log, round_decimals, attr_activity,
                                                                           attr_success, attr_bot)
    elif measure_name == 'exception_time_impact':
        is_graphical_measure = True
        activity_labeling, activity_coloring, measure = measure_exception_time_impact(df_log, attr_activity,
                                                                           attr_success, attr_bot)
    elif measure_name == 'exception_time_variance':
        is_graphical_measure = True
        activity_labeling, activity_coloring, measure = measure_exception_time_variance(df_log, attr_activity,
                                                                           attr_success, attr_bot)
    elif measure_name == 'relative_execution_time':
        is_graphical_measure = True
        activity_labeling, activity_coloring, measure = measure_relative_execution_time(df_log, round_decimals, attr_activity,
                                                                           attr_bot)
    elif measure_name == 'execution_time_variance':
        is_graphical_measure = True
        activity_labeling, activity_coloring, measure = measure_execution_time_variance(df_log, round_decimals, attr_activity,
                                                                           attr_bot)
    elif measure_name == 'bot_human_handover_count':
        is_graphical_measure = True
        activity_labeling, activity_coloring, measure = measure_bot_human_handover_count(df_log, attr_activity, attr_bot)
    
    elif measure_name == 'bot_human_handover_impact':
        is_graphical_measure = True
        activity_labeling, activity_coloring, measure = measure_bot_human_handover_impact(df_log, attr_activity, attr_bot)
        
    elif measure_name == 'bot_human_handover_variance':
        is_graphical_measure = True
        activity_labeling, activity_coloring, measure = measure_bot_human_handover_variance(df_log, attr_activity, attr_bot)
    
    # Measures with a dataframe as output
    elif measure_name == 'relative_case_fails':
        df_relative_case_fails, measure = measure_relative_case_fails(df_log, round_decimals, attr_traceID,
                                                                           attr_success, attr_bot, show_progress=show_progress)
        result_df = df_relative_case_fails
        display(df_relative_case_fails)
        
    elif measure_name == 'automation_rate':
        df_automation_rate, measure = measure_automation_rate(df_log, round_decimals, attr_activity, attr_bot)
        result_df = df_automation_rate
        display(df_automation_rate)
    
    elif measure_name == 'case_activities_execution_time':
        df_case_activities_execution_time, measure = measure_case_activities_execution_time(df_log, attr_activity,
                                                                                            show_progress=show_progress)
        result_df = df_case_activities_execution_time
        display(df_case_activities_execution_time)
        
    elif measure_name == 'case_activities_execution_time_variance':
        df_case_activities_execution_time_variance, measure = measure_case_activities_execution_time_variance(df_log,
                                                                                                              attr_activity,
                                                                                            show_progress=show_progress)
        result_df = df_case_activities_execution_time_variance
        display(df_case_activities_execution_time_variance)
        
    else:
        print("unknown measure")
        
    if is_graphical_measure:
        gviz = custom_variant_measure_apply(dfg, activities_color=activity_coloring, activities_labels=activity_labeling,
                                        show_edge_labels=show_edge_labels, log=log, max_no_of_edges=max_no_of_edges)
        dfg_visualization.view(gviz)
        if save_result:
            save_name = 'dfg_' + measure + '.png'
            dfg_visualization.save(gviz, "results/" +save_name)
        return gviz
    else:
        if save_result:
            save_name = 'df_' + measure + '.csv'
            result_df.to_csv("results/" + save_name, index=False, sep=';')
        return result_df


#Applying the measures on a selected log

# Measures with a dfg visualization as output
# 'relative_fails', 'exception_time_impact', 'exception_time_variance', 'relative_execution_time', 'execution_time_variance',
# 'bot_human_handover_count', 'bot_human_handover_impact', 'bot_human_handover_variance'

# Measures with a dataframe as output
# 'relative_case_fails', 'automation_rate', 'case_activities_execution_time', 'case_activities_execution_time_variance'

#choose measure
selected_measure = 'exception_time_impact'
#choose log ('company' or 'bpi')
selected_log = 'company'


#Standard values: Real world log from company
path = "results/Company_Merged_Log.xes"
#Names/keys of the respective attributes in the log
attr_activity = 'concept:name'
attr_timestamp = 'time:timestamp'
attr_traceID = 'caseId'
attr_success = 'success'
attr_bot = 'bot'
attr_eventid = 'eventId'
attr_lifecycle = 'lifecycle:transition'
log_company, df_log_company, dfg_company = load_merged_log_and_preprocess(path, attr_lifecycle, attr_timestamp, True)
df_log_company = preprocess_add_columns(df_log_company, attr_traceID, attr_timestamp, attr_activity, attr_eventid, attr_bot)
df_log, dfg, log = df_log_company, dfg_company, log_company

if selected_log == 'bpi':
    #BPI challenge
    path = "results/BPI_Merged_Log.xes"
    #Names/keys of the respective attributes in the log
    attr_activity = 'concept:name'
    attr_timestamp = 'time:timestamp'
    attr_traceID = 'docid_uuid'
    attr_success = 'success'
    attr_bot = 'bot'
    attr_eventid = 'eventid'
    attr_lifecycle = 'lifecycle:transition'
    log_bpi, df_log_bpi, dfg_bpi = load_merged_log_and_preprocess(path, attr_lifecycle, attr_timestamp, True)
    df_log_bpi = preprocess_add_columns(df_log_bpi, attr_traceID, attr_timestamp, attr_activity, attr_eventid, attr_bot)
    df_log, dfg, log = df_log_bpi, dfg_bpi, log_bpi

measure_result = apply_measure(df_log, dfg, log, selected_measure, attr_activity, attr_success, attr_bot, attr_traceID, save_result=True, round_decimals=2,
                               show_edge_labels=True, show_progress=True, max_no_of_edges=150)

measure_result