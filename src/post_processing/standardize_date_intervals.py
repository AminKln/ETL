import logging
import os
import time
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime, timedelta

import pandas as pd
from tqdm import tqdm

from src.utils import db_conn, db_exec, db_get, db_write

logger = logging.getLogger(__name__)

"""
The priority functions are meant to return results from Low to High priority
i.e. subsequent rows take priority over preceding rows, carving them out.
"""
def priority_latest_start(line, sdt_col, ndt_col):
    return (line[sdt_col], -line[ndt_col].toordinal())

def _intervals_overlap(start_a, end_a, start_b, end_b):
    """
    Return True if the intervals [start_a, end_a) and [start_b, end_b) overlap.
    """
    return not (end_a <= start_b or start_a >= end_b)

def _get_overlap_range(start_a, end_a, start_b, end_b):
    """
    Given two overlapping intervals, return the overlap start and end.
    Assumes intervals are overlapping.
    """
    return max(start_a, start_b), min(end_a, end_b)

def _fully_subsumes(overlap_start, overlap_end, old_start, old_end):
    """
    Return True if the overlap [overlap_start, overlap_end) 
    completely subsumes [old_start, old_end).
    """
    return overlap_start <= old_start and overlap_end >= old_end

def _handle_fully_subsumed(interval, statuses, sdt_col, ndt_col, id_col):
    """
    Mark an existing interval as 'Removed (Fully Subsumed)' 
    and flag it for removal.
    """
    interval['STATUS'] = 'Removed (Fully Subsumed)'
    i_id = interval[id_col]
    i_start = interval[sdt_col]
    i_end = interval[ndt_col]
    statuses[(i_id, i_start, i_end)] = interval['STATUS']
    interval['remove_flag'] = True

def _handle_partial_overlap(interval, overlap_start, overlap_end, statuses, final_list, sdt_col, ndt_col, id_col):
    """
    Handle the case where an existing interval is partially overlapped 
    by the new incoming interval. This may mean trimming the existing 
    interval or splitting it into two intervals.
    """
    f_start = interval[sdt_col]
    f_end   = interval[ndt_col]

    # Left portion
    if overlap_start > f_start:
        old_end = interval[ndt_col]
        interval[ndt_col] = overlap_start
        interval['STATUS'] = 'Trimmed End'
        statuses[(interval[id_col], f_start, old_end)] = interval['STATUS']
    else:
        # Overlap from the start => remove the existing interval
        interval['STATUS'] = 'Removed (Overlapped Start)'
        statuses[(interval[id_col], f_start, f_end)] = interval['STATUS']
        interval['remove_flag'] = True

    # Right portion
    if overlap_end < f_end:
        new_right = dict(interval)  # shallow copy
        # Remove any remove_flag that carried over
        new_right.pop('remove_flag', None)
        new_right[sdt_col] = overlap_end
        new_right[ndt_col]   = f_end
        new_right['STATUS']   = 'Trimmed Start'
        statuses[(new_right[id_col], overlap_end, f_end)] = new_right['STATUS']
        final_list.append(new_right)

def _remove_flagged(final_list):
    """
    Remove any intervals flagged for removal from the final list.
    Returns the cleaned-up list.
    """
    return [item for item in final_list if not item.get('remove_flag')]

def _process_intervals_for_id(df_for_id, sdt_col, ndt_col, id_col, priority_fn, reverse_sort=False):
    """
    Core overlap logic for one ID's dataframe.
    """
    # Create a priority column
    df_for_id['priority'] = df_for_id.apply(lambda row: priority_fn(row, sdt_col, ndt_col), axis=1)
    # Sort ascending unless reverse_sort=True
    df_for_id = df_for_id.sort_values(by='priority', ascending=not reverse_sort)

    final_list = []
    statuses = {}

    for _, row in df_for_id.iterrows():
        new_line = row.to_dict()
        new_start = new_line[sdt_col]
        new_end   = new_line[ndt_col]

        # Check overlap with existing intervals in final_list
        for existing in final_list:
            f_start, f_end = existing[sdt_col], existing[ndt_col]
            if _intervals_overlap(new_start, new_end, f_start, f_end):
                overlap_start, overlap_end = _get_overlap_range(
                    new_start, new_end, f_start, f_end
                )

                if _fully_subsumes(overlap_start, overlap_end, f_start, f_end):
                    _handle_fully_subsumed(existing, statuses, sdt_col, ndt_col, id_col)
                else:
                    _handle_partial_overlap(existing, overlap_start, overlap_end, statuses, final_list, sdt_col, ndt_col, id_col)

        # Remove intervals that were flagged for removal
        final_list = _remove_flagged(final_list)

        # Add the new line
        new_line['STATUS'] = 'Added/Retained'
        statuses[(new_line[id_col], new_start, new_end)] = new_line['STATUS']
        final_list.append(new_line)

    return final_list, statuses

def process_intervals_singlethread(df, sdt_col, ndt_col, id_col, priority_fn, reverse_sort=False):
    """
    Single-threaded standardization operating on a dataframe.
    """
    if id_col is not None:
        grouped = df.groupby(id_col)
    else:
        # If no ID column is given, treat entire df as one group
        grouped = [(None, df)]

    # Process each ID's intervals separately
    final_results = {}
    all_statuses = {}

    if reverse_sort:
        def adjusted_priority_fn(x):
            return -priority_fn(x)
        used_priority_fn = adjusted_priority_fn
    else:
        used_priority_fn = priority_fn

    for i_id, group_df in tqdm(grouped, desc='Processing IDs', position=0):
        final_list, statuses = _process_intervals_for_id(
            group_df.copy(), sdt_col, ndt_col, id_col, used_priority_fn, reverse_sort
        )
        final_results.append(pd.DataFrame(final_list))
        all_statuses[i_id] = statuses

    logger.debug('All IDs processed')
    # Concatenate final pieces for all IDs
    final_df = pd.concat(final_results, ignore_index=True)
    return final_df, all_statuses

# TODO: generalize to work with any table, with or without grouping with ID
def standardize_date_intervals(table_name, conn, sdt_col, ndt_col, id_col=None):
    logger.debug('Starting Date Standardization...')
    df = db_get(conn, f'SELECT * FROM {table_name}')
    df[sdt_col] = pd.to_datetime(df[sdt_col])
    df[ndt_col] = pd.to_datetime(df[ndt_col])

    results_df, statuses = process_intervals_singlethread(df, sdt_col, ndt_col, id_col, priority_latest_start)
    
    # Save to DB
    results_df[ndt_col] -= timedelta(days=1) # convert from [sdt_col, ndt_col) to [sdt_col, ndt_col]
    db_write(results_df, table_name)

    return results_df, statuses