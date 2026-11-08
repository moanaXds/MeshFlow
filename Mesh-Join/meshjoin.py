# data enrichment = somone buys smth -> trans added -> But they don't contain complete info 
# so u add that info from customer + product master data = via Mesh Join


import csv
from collections import deque
from pathlib import Path


#           STABLE HASH FUNCTION ( assign physical trans to any 4 phyysical stores )


def stable(text, n):                 # trans id ,4 
    import hashlib                   
    text = str(text)
    value = hashlib.md5(text.encode("utf-8")).hexdigest()[:8]          # string -> hash
    return int(value, 16) % n








#           load MASTER DATA from disk + actual partitioning  ( 500 rows = 1 page ) + reading pages



class DiskReader:
    def __init__(self, file_path, page_size=500):
        self.file_path = file_path
        self.page_size = page_size
        self.partitions = []
        self.current_partition_index = 0
        self.load_partitions()

    

    def load_partitions(self):

        with open(self.file_path, "r", encoding="utf-8") as file:

            reader = csv.DictReader(file)       #       reads 1 row as 1 dict
            current_page = []

            for row in reader:
                current_page.append(row)
                if len(current_page) == self.page_size:
                    self.partitions.append(current_page)
                    current_page = []

            # add remaining rows into page if not empty

            if current_page:
                self.partitions.append(current_page)

    # read pages cyclically

    def get_next_partition(self):
        if not self.partitions:
            return []

        partition = self.partitions[self.current_partition_index]
        self.current_partition_index = (self.current_partition_index + 1) % len(self.partitions)
        return partition

    def total_partitions(self):
        return len(self.partitions)






#           MESHJOIN OPERATOR CLASS



class MeshJoin:
    def __init__(self, customer_file, product_file, chunk_size=500, stream_buffer_size=1000):
        
        # receive trans Chunk 1 -> queue -> hash table ( customer and product )

        # now 

        # read  Customer Page 1 -> probe hash -> match n enrich = mark customer_matched
        # read Product Page 1 -> probe hash -> match n enrich = mark product_matched

        # read Customer and Produt Page 2,3,4,5,7,8,9 and join them with the chunk 1 
        # remove chunk 1 from queue and hash tables
        
        # repeat for all chunks in queue
       

        
        
        self.chunk_size = chunk_size
        self.stream_buffer_size = stream_buffer_size


        self.stream_buffer = deque()

        # disk readers for master data

        self.customer_reader = DiskReader(customer_file, page_size=500)
        self.product_reader = DiskReader(product_file, page_size=500)

        # queue keeps chunks in arrival order (FIFO)

        self.queue = deque()


        # hash table H maps join attributes to transactions
    


        self.hash_customer = {}       # customer_id -> list of transactions
        self.hash_product = {}        # product_id -> list of transactions


        # store assignments (1 online, 2-5 physical)

        self.physical_stores = ["2", "3", "4", "5"]

        # statistics

        self.total_enriched = 0
        self.total_stream_in = 0

    # add incoming transaction to stream buffer

    def add_to_stream_buffer(self, transaction):
        if len(self.stream_buffer) < self.stream_buffer_size:
            self.stream_buffer.append(transaction)
            self.total_stream_in += 1
            return True

        # stream buffer full -> transaction shed or queued

        return False

    # step 1 & 2: insert chunk from stream buffer -> hash table & queue

    def load_chunk_to_hash_table(self):
        if not self.stream_buffer:
            return None

        chunk = []
        while self.stream_buffer and len(chunk) < self.chunk_size:
            tx = self.stream_buffer.popleft()

            tx["customer_matched"] = False
            tx["product_matched"] = False

        
            chunk.append(tx)

            # add pointer to customer hash table

            cust_id = str(tx["customer_id"]).strip()
            if cust_id not in self.hash_customer:
                self.hash_customer[cust_id] = []
            self.hash_customer[cust_id].append(tx)

            # add pointer to product hash table

            prod_id = str(tx["product_id"]).strip()
            if prod_id not in self.hash_product:
                self.hash_product[prod_id] = []
            self.hash_product[prod_id].append(tx)

        # track partition lifetimes in queue
        # chunk needs to see all customer partitions and all product partitions

        chunk_node = {
            "chunk": chunk,
            "customer_partitions_left": self.customer_reader.total_partitions(),
            "product_partitions_left": self.product_reader.total_partitions()
        }

        self.queue.append(chunk_node)
        return chunk_node

    # step 3: probe master data partitions into hash table H

    def probe_master_data(self):
        # 1. read customer partition from disk buffer
        customer_page = self.customer_reader.get_next_partition()
        for cust in customer_page:
            cust_id = str(cust["customer_id"]).strip()
            if cust_id in self.hash_customer:
                for tx in self.hash_customer[cust_id]:
                    if not tx["customer_matched"]:
                        # customer attributes added
                        tx["gender"] = cust.get("gender")
                        tx["age_group"] = cust.get("age_group")
                        tx["occupation"] = cust.get("occupation")
                        tx["city_category"] = cust.get("city_category")
                        tx["stay_in_current_city_years"] = cust.get("stay_in_current_city_years")
                        tx["marital_status"] = cust.get("marital_status")
                        tx["customer_matched"] = True

        # 2. read product partition from disk buffer
        product_page = self.product_reader.get_next_partition()
        for prod in product_page:
            prod_id = str(prod["product_id"]).strip()
            if prod_id in self.hash_product:
                for tx in self.hash_product[prod_id]:
                    if not tx["product_matched"]:
                        # product & supplier attributes added
                        tx["product_name"] = prod.get("product_name")
                        tx["product_category"] = prod.get("product_category")
                        tx["supplier_id"] = prod.get("supplier_id")
                        tx["supplier_name"] = prod.get("supplier_name")

                        # assign store
                        if tx.get("sales_mode") == "online":
                            tx["store_id"] = "1"
                        else:
                            # physical store 2, 3, 4, or 5
                            store_index = stable(tx["transaction_id"], 4)
                            tx["store_id"] = self.physical_stores[store_index]

                        # calculate sale = quantity * price
                        qty = float(tx.get("quantity", 0))
                        price = float(tx.get("unit_price", 0.0))
                        tx["sale"] = round(qty * price, 2)

                        tx["product_matched"] = True

        # decrement partition counters for all chunks in queue
        for node in self.queue:
            if node["customer_partitions_left"] > 0:
                node["customer_partitions_left"] -= 1
            if node["product_partitions_left"] > 0:
                node["product_partitions_left"] -= 1


    # step 4: remove completed chunks from queue and hash table H

    def remove_completed_chunks(self):
        enriched_transactions = []

        # remove from queue if work is finished
        while self.queue:
            oldest_node = self.queue[0]

            # chunk has seen all partitions
            if oldest_node["customer_partitions_left"] <= 0 and oldest_node["product_partitions_left"] <= 0:
                self.queue.popleft()
                completed_chunk = oldest_node["chunk"]

                for tx in completed_chunk:
                    # remove pointers from hash table
                    cust_id = str(tx["customer_id"]).strip()
                    if cust_id in self.hash_customer:
                        if tx in self.hash_customer[cust_id]:
                            self.hash_customer[cust_id].remove(tx)
                        if not self.hash_customer[cust_id]:
                            del self.hash_customer[cust_id]

                    prod_id = str(tx["product_id"]).strip()
                    if prod_id in self.hash_product:
                        if tx in self.hash_product[prod_id]:
                            self.hash_product[prod_id].remove(tx)
                        if not self.hash_product[prod_id]:
                            del self.hash_product[prod_id]

                    enriched_transactions.append(tx)
                    self.total_enriched += 1
            else:
                break

        return enriched_transactions

    # one full iteration of meshjoin
    def run_one_iteration(self):
        # 1. read chunk from stream buffer into hash table
        self.load_chunk_to_hash_table()

        # 2. probe master data partitions
        self.probe_master_data()

        # 3. remove completed chunks
        enriched = self.remove_completed_chunks()

        return enriched

    # check if processing is completely finished
    def is_finished(self):
        return len(self.stream_buffer) == 0 and len(self.queue) == 0



#           TEST FUNCTION



if __name__ == "__main__":
    base_dir = Path(__file__).parent.parent / "Data" / "processed"
    customer_csv = base_dir / "customer_master_data.csv"
    product_csv = base_dir / "product_master_data.csv"
    stream_csv = base_dir / "online_sales_stream.csv"

    print("\n--- TESTING MESHJOIN ---")

    mj = MeshJoin(customer_csv, product_csv)

    print("Customer partitions:", mj.customer_reader.total_partitions())
    print("Product partitions:", mj.product_reader.total_partitions())

    # load test sample into stream buffer
    with open(stream_csv, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for i, row in enumerate(reader):
            if i >= 1500:
                break
            mj.add_to_stream_buffer(row)

    print("Stream buffer loaded:", len(mj.stream_buffer))

    iteration = 0
    total_out = 0

    while not mj.is_finished() and iteration < 50:
        iteration += 1
        output = mj.run_one_iteration()
        if output:
            total_out += len(output)

    print("\nCompleted iterations:", iteration)
    print("Total enriched output:", total_out)
    print("Remaining in queue:", len(mj.queue))
    print("Remaining in stream buffer:", len(mj.stream_buffer))
    print("\nMeshJoin test finished successfully!")

