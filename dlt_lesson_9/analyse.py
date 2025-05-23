# ---
# jupyter:
#   jupytext:
#     cell_metadata_filter: -all
#     custom_cell_magics: kql
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.11.2
#   kernelspec:
#     display_name: aida_aws_3_9
#     language: python
#     name: python3
# ---

# %%

############################################################
###### START: CHUNKING AND PARALLELIZATION  ################
############################################################

## CHUNKING
# Resuls for chunking: {True: [[45.93333053588867]], False: [[45.79884648323059]]}
# The results are even worse for chuking. Probably because the data sizes are small. The experiment should be ran more times.
# If the APIs were returning more data chunking would have been more useful.


## PARALELIZATION
# Results: times=[[67.84752178192139], [73.70786118507385]] (2 runs with same parameters)
# Here results are worse than the ones we saw in the previous experiment that was not parallelized.
# It seems that the extraction jobs are relatively quick so the overhead of managing thread is not worth it.
# This would be more useful if we had resources that were bigger. Now the extract for customers and products
# finish way before the one for orders. so parallelization looses point as there are no more resources to cycle
# between.

### CONCLUSIONS
# Chunking really did not show significant change in run time. Parallelization made things worse.
# small data sizes also play a role. Chunking may have much higher impect with bigger data sizes. 
# We only have  <1000 rows in customers, < 100 in products, <8400 in orders
# Paralellisation would have helped if resources customers and products were yielding more cause
# now everything is consumed by orders.
# Moving forwards: parallelized = False, chuking = True (because of cleaner code)


############################################################
###### END: CHUNKING AND PARALLELIZATION    ################
############################################################

############################################################
###### START: OPTIMIZE BUFFER SIZE AND FILE ROTATIONS  #####
############################################################

# experiments were ran with following parameters:
# buffer_sizes = [10,  1000, 10000]  # os.environ['DATA_WRITER__BUFFER_MAX_ITEMS'] 
# extract_rotations = [10,  1000, 10000] # os.environ["EXTRACT__DATA_WRITER__FILE_MAX_ITEMS"
# normalize_rotations = [10,  1000, 10000] # os.environ['NORMALIZE__DATA_WRITER__FILE_MAX_ITEMS']
# Above were kept at max of 10k because that's the highest amount of rows the biggest resource has.

# %%
############################################################
###### START: OPTIMIZE BUFFER SIZE AND FILE ROTATIONS  #####
############################################################

# Results are in optimization_results_20250523_081157.csv
import pandas as pd
import matplotlib.pyplot as plt

# Load the CSV file
results_file = "optimization_results_20250523_081157.csv"
df = pd.read_csv(results_file)

# Plot extract_time vs buffer_size
plt.figure(figsize=(10, 6))
for rotation in df['extract_rotation'].unique():
    subset = df[df['extract_rotation'] == rotation]
    plt.plot(subset['buffer_size'], subset['extract_time'], label=f"Extract Rotation: {rotation}")

plt.title("Extract Time vs Buffer Size")
plt.xlabel("Buffer Size")
plt.ylabel("Extract Time (s)")
plt.legend()
plt.grid()
plt.show()

# Plot normalize_time vs normalize_rotation
plt.figure(figsize=(10, 6))
for buffer_size in df['buffer_size'].unique():
    subset = df[df['buffer_size'] == buffer_size]
    plt.plot(subset['normalize_rotation'], subset['normalize_time'], label=f"Buffer Size: {buffer_size}")

plt.title("Normalize Time vs Normalize Rotation")
plt.xlabel("Normalize Rotation")
plt.ylabel("Normalize Time (s)")
plt.legend()
plt.grid()
plt.show()

# Plot total_time vs buffer_size and extract_rotation
plt.figure(figsize=(10, 6))
for rotation in df['extract_rotation'].unique():
    subset = df[df['extract_rotation'] == rotation]
    plt.plot(subset['buffer_size'], subset['total_time'], label=f"Extract Rotation: {rotation}")

plt.title("Total Time vs Buffer Size")
plt.xlabel("Buffer Size")
plt.ylabel("Total Time (s)")
plt.legend()
plt.grid()
plt.show()

### CONCLUSIONS
# - Extract time is influenced by buffer size and extract rotation, however it is really hard to see what is happenings here
# as this also depends on the API as at some point it seems that the API was being throttled. 
# - Normalize time remains relatively stable across different buffer sizes and rotations. Probably real difference would have
# been seen if we also chose to do experiments with setting = 1 for extract_rotation and normalize_rotation as that would have
# drastically increased write and read calls.
# - Load time showed high dependency on the normalize_rotations parameter - this is as expected as the bigger this parameter is
# the less load into the db calls have to be made. The deceas was big: from 42 sec when the parameter is 10, to 0.75 secs
# when the parameter is 1000. With 10 000 the loading became slower, so smaller load chunks are benefitials. 1000 here seems to be
# a reasonable option.
# - Total time is primarily driven by extract time.

# Parameters I'd select: 
# DATA_WRITER__BUFFER_MAX_ITEMS=10000, EXTRACT__DATA_WRITER__FILE_MAX_ITEMS=1000, NORMALIZE__DATA_WRITER__FILE_MAX_ITEMS=1000
# %%
