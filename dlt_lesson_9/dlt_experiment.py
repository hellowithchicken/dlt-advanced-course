# %%
import time
from threading import current_thread

import os
os.cpu_count()

import dlt
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.rest_api import rest_api_source
from dlt.sources.helpers.rest_client.paginators import HeaderLinkPaginator
import uuid

import os
print(os.cpu_count()) # 12 cpus


os.environ["EXTRACT__WORKERS"] = "3" # we have three resources, so let's use max here.
os.environ["NORMALIZE__WORKERS"] = "12" # this is cpu bound, so set to the max as we have 12 cpus
os.environ["LOAD__WORKERS"] = "20" # let's keep it at default

# %%
# with parallelizing
def measure_extract_time_parallel(chunking=True, parallelized=True, measure="extract"):
  @dlt.source
  def jaffle_store_source(chunking=chunking, parallelized=parallelized):
      client = RESTClient(
          base_url="https://jaffle-shop.scalevector.ai/api/v1",
          paginator=HeaderLinkPaginator(
              links_next_key="next"
              ),
      )

      @dlt.resource(parallelized=parallelized)
      def customers():
          yield_no = 0
          for page in client.paginate("customers", params={"page_size": 100, "page": "1"}):
              print(f"customers in thread {current_thread().name}")
              if chunking:
                yield_no += 1
                print(f"""customers: Yielding no {yield_no}""")
                yield page
              else:
                for i in page:
                  yield_no += 1
                  print(f"""customers: Yielding no {yield_no}""")
                  yield i

      @dlt.resource(parallelized=parallelized)
      def orders():
          yield_no = 0
          for page in client.paginate("orders", params={"page_size": 100, 
                                                        "page": "1",
                                                        "start_date": "2017-05-01T00:00:00",
                                                        "end_date": "2017-06-01T00:00:00"}):
              print(f"orders in thread {current_thread().name}")
              if chunking:
                yield_no += 1
                print(f"""orders: Yielding no {yield_no}""")
                yield page
              else:
                for i in page:
                  yield_no += 1
                  print(f"""orders: Yielding no {yield_no}""")
                  yield i

      @dlt.resource(parallelized=parallelized)
      def products():
          yield_no = 0
          for page in client.paginate("products", params={"page_size": 100, "page": "1"}):
              print(f"products in thread {current_thread().name}")
              if chunking:
                yield_no += 1
                print(f"""products: Yielding no {yield_no}""")
                yield page
              else:
                for i in page:
                  yield_no += 1
                  print(f"""products: Yielding no {yield_no}""")
                  yield i


      return customers, orders, products

  pipeline = dlt.pipeline(
      pipeline_name=f"jaffle_store_test_chunk_{str(uuid.uuid4())}",
      destination="duckdb",
      dataset_name=f"jaffle_store_test_chunk_{str(uuid.uuid4())}",
      dev_mode=True,
  )

  start_time = time.time()
  pipeline.extract(jaffle_store_source(chunking= chunking, parallelized=parallelized))
  extract_time = time.time() - start_time

  if measure == "extract":
    return [extract_time]

  start_time = time.time()
  pipeline.normalize()
  normalize_time = time.time() - start_time

  if measure == "normalize":
    return [extract_time, normalize_time]

  start_time = time.time()
  pipeline.load()
  load_time = time.time() - start_time

  if measure == "load":
    return [extract_time, normalize_time, load_time]

#%%

############################################################
###### START: CHUNKING AND PARALLELIZATION  ################
############################################################


time_chunking={}
for chunking in [True, False]:
    time_chunking[chunking] = []
    for i in range(1): # run only once to save time, but should be ran multiple time
        print(i)
        time_ = measure_extract_time_parallel(chunking=chunking, parallelized=False, measure="extract")
        time_chunking[chunking].append(time_)

# Resuls for chunking: {True: [[45.93333053588867]], False: [[45.79884648323059]]}
# The results are even worse for chuking. Probably because the data sizes are small. The experiment should be ran more times.
# If the APIs were returning more data chunking would have been more useful.

# %%
# Try parelelization, twice
times = [measure_extract_time_parallel(chunking=True, parallelized=True, measure="extract") for i in range(2)]
# Results: times=[[67.84752178192139], [73.70786118507385]]
# Here results are again worse than the ones we saw in the previous experiment that was not parallelized.
# It seems that the extraction jobs are relatively quick so the overhead of managing thread is not worth it.
# This would be more useful if we had resources that were bigger. Now the extract for customers and products
# finish way before the one for orders. so parallelization looses point as there are no more resources to cycle
# between.

### Conclusion
# Chunking really did not show significant change in run time. Parallelization made things worse.
# small data sizes also play a role. Chunking may have much higher impect with bigger data sizes. 
# We only have  <1000 rows in customers, < 100 in products, <8400 in orders
# Paralellisation would have helped if resources customers and products were yielding more cause
# now everything is consumed by orders.
# Moving forwards: parallelized = False, chuking = True (because of cleaner code)


############################################################
###### END: CHUNKING AND PARALLELIZATION    ################
############################################################


# %%
############################################################
###### START: OPTIMIZE BUFFER SIZE AND FILE ROTATIONS  #####
############################################################

import csv
from datetime import datetime

def run_optimization_test(buffer_size, extract_rotation, normalize_rotation):
    """Run a test with specific buffer and rotation settings and return the timing results"""
    os.environ['DATA_WRITER__BUFFER_MAX_ITEMS'] = str(buffer_size)
    os.environ["EXTRACT__DATA_WRITER__FILE_MAX_ITEMS"] = str(extract_rotation)
    os.environ['NORMALIZE__DATA_WRITER__FILE_MAX_ITEMS'] = str(normalize_rotation)
    
    print(f"Testing with buffer_size={buffer_size}, extract_rotation={extract_rotation}, normalize_rotation={normalize_rotation}")
    
    # Run with chunking=True, parallelized=False as per conclusion from extract optimization
    results = measure_extract_time_parallel(chunking=True, parallelized=False, measure="load")
    
    return {
        "buffer_size": buffer_size,
        "extract_rotation": extract_rotation,
        "normalize_rotation": normalize_rotation,
        "extract_time": results[0],
        "normalize_time": results[1],
        "load_time": results[2],
        "total_time": sum(results)
    }

# Setup parameters for tests
buffer_sizes = [10,  1000, 10000]  # Keep this up to 10k as orders has up to 8.5k items
extract_rotations = [10,  1000, 10000]
normalize_rotations = [10,  1000, 10000]

# Create a CSV file to store the results
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
results_file = f"/home/iggy/SageMaker/optimization_results_{timestamp}.csv"

with open(results_file, 'w', newline='') as csvfile:
    fieldnames = ['buffer_size', 'extract_rotation', 'normalize_rotation', 
                  'extract_time', 'normalize_time', 'load_time', 'total_time']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()
    
    # For brevity, we'll test a subset of combinations to save time
    for buffer_size in buffer_sizes:
        for extract_rotation in extract_rotations:
            for normalize_rotation in normalize_rotations: 
                result = run_optimization_test(buffer_size, extract_rotation, normalize_rotation)
                writer.writerow(result)
                csvfile.flush()
                
                print(f"Results: Extract: {result['extract_time']:.2f}s, "
                      f"Normalize: {result['normalize_time']:.2f}s, "
                      f"Load: {result['load_time']:.2f}s, "
                      f"Total: {result['total_time']:.2f}s")

print(f"Optimization results saved to {results_file}")
