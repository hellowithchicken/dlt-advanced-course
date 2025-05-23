from threading import current_thread

import dlt
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.paginators import HeaderLinkPaginator

@dlt.source
def jaffle_store_source(chunking=True, parallelized=False):
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
                if yield_no > 1:
                    break # this is to speed things up for testing
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
                if yield_no > 1:
                    break # this is to speed things up for testing
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
                if yield_no > 1:
                    break # this is to speed things up for testing
                print(f"""products: Yielding no {yield_no}""")
                yield page
            else:
                for i in page:
                    yield_no += 1
                    print(f"""products: Yielding no {yield_no}""")
                    yield i


    return customers, orders, products


if __name__ == "__main__":

    pipeline = dlt.pipeline(
        pipeline_name="jaffle_store_1",
        destination="duckdb",
        dataset_name="jaffle_store_12",
    )

    load_info = pipeline.run(jaffle_store_source(chunking=True, parallelized=False))