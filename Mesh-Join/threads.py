
import csv
import time
import threading
from collections import deque
from pathlib import Path


#           LOCK FOR SHARED STREAM BUFFER ACCESS


buffer_lock = threading.Lock()



#           THREAD 1: STREAM PRODUCER ( adding trans -> stream buffer ) 



class StreamProducerThread(threading.Thread):
    def __init__(self, stream_file, meshjoin_operator, batch_rate=100, sleep_time=0.01):
        super().__init__()
        self.stream_file = stream_file
        self.mj = meshjoin_operator
        self.batch_rate = batch_rate
        self.sleep_time = sleep_time
        self.running = True
        self.total_stream_pushed = 0

    def run(self):
        print("\n[Online Stream Thread] Started reading online stream...")

        with open(self.stream_file, "r", encoding="utf-8") as file:
            reader = csv.DictReader(file)
            batch = []

            for row in reader:
                if not self.running:
                    break

                batch.append(row)

                if len(batch) >= self.batch_rate:
                    self._push_batch(batch)
                    batch = []
                    time.sleep(self.sleep_time)

            # push remaining rows
            if batch and self.running:
                self._push_batch(batch)

        print(f"[Online Stream Thread] Finished. Total online pushed: {self.total_stream_pushed}")

    def _push_batch(self, batch):
        with buffer_lock:

            for tx in batch:

                # keep trying until room in buffer or stopped

                while self.running:
                    added = self.mj.add_to_stream_buffer(tx)
                    if added:
                        self.total_stream_pushed += 1
                        break

                    # buffer full -> release lock briefly so MESHJOIN can consume

                    buffer_lock.release()
                    time.sleep(0.005)
                    buffer_lock.acquire()

    def stop(self):
        self.running = False



#           THREAD 2: PERIODIC EXTRACTOR & LOAD SHEDDING (adding and removing trans ->  stream buffer) 



class PeriodicExtractorThread(threading.Thread):
    def __init__(self, periodic_file, meshjoin_operator, interval_sec=0.1, total_hours=48):
        super().__init__()
        self.periodic_file = periodic_file
        self.mj = meshjoin_operator
        self.interval_sec = interval_sec
        self.total_hours = total_hours
        self.running = True

        # shed table on disk / in memory

        self.shed_queue = deque()
        self.total_periodic_pushed = 0
        self.total_shed = 0
        self.total_recovered = 0

        # group periodic data by release_hour

        self.hourly_batches = {}
        self._load_periodic_data()

    def _load_periodic_data(self):
        with open(self.periodic_file, "r", encoding="utf-8") as file:
            reader = csv.DictReader(file)
            for row in reader:
                hour = int(row.get("release_hour", 0))
                if hour not in self.hourly_batches:
                    self.hourly_batches[hour] = []
                self.hourly_batches[hour].append(row)

    def run(self):
        print("\n[Periodic Thread] Started periodic extraction...")

        # loop through 48 release periods

        for hour in range(self.total_hours):
            if not self.running:
                break

            batch = self.hourly_batches.get(hour, [])

            with buffer_lock:

                # first try to recover shed transactions if buffer has capacity

                while self.shed_queue and len(self.mj.stream_buffer) < self.mj.stream_buffer_size:
                    recovered_tx = self.shed_queue.popleft()
                    self.mj.add_to_stream_buffer(recovered_tx)
                    self.total_recovered += 1

                # process current hour's periodic transactions

                for tx in batch:

                    # check if MESHJOIN is at full capacity

                    if len(self.mj.stream_buffer) < self.mj.stream_buffer_size:
                        self.mj.add_to_stream_buffer(tx)
                        self.total_periodic_pushed += 1
                    else:
                        
                        # shed periodic transaction to shed table

                        self.shed_queue.append(tx)
                        self.total_shed += 1

            time.sleep(self.interval_sec)

        # recover any remaining shed transactions once all hours finished
        while self.shed_queue and self.running:
            with buffer_lock:
                while self.shed_queue and len(self.mj.stream_buffer) < self.mj.stream_buffer_size:
                    recovered_tx = self.shed_queue.popleft()
                    self.mj.add_to_stream_buffer(recovered_tx)
                    self.total_recovered += 1
            time.sleep(0.01)

        print(f"[Periodic Thread] Finished. Periodic pushed: {self.total_periodic_pushed}, Shed: {self.total_shed}, Recovered: {self.total_recovered}")

    def stop(self):
        self.running = False



#           THREAD 3: MESHJOIN EXECUTION WORKER (joining + giving final)



class MeshJoinWorkerThread(threading.Thread):
    def __init__(self, meshjoin_operator, output_queue, sleep_time=0.001):
        super().__init__()
        self.mj = meshjoin_operator
        self.output_queue = output_queue
        self.sleep_time = sleep_time
        self.running = True
        self.total_iterations = 0
        self.total_output = 0

    def run(self):
        print("\n[MeshJoin Thread] Started MESHJOIN processing...")

        while self.running:
            enriched_records = []

            with buffer_lock:

                # run one cycle if there are transactions in stream buffer or queue
                
                if self.mj.stream_buffer or self.mj.queue:
                    enriched_records = self.mj.run_one_iteration()
                    self.total_iterations += 1

            if enriched_records:
                self.output_queue.extend(enriched_records)
                self.total_output += len(enriched_records)

            time.sleep(self.sleep_time)

        print(f"[MeshJoin Thread] Stopped. Total iterations: {self.total_iterations}, Total enriched: {self.total_output}")

    def stop(self):
        self.running = False



#           TEST FUNCTION



if __name__ == "__main__":
    from meshjoin import MeshJoin

    data_dir = Path(__file__).parent.parent / "Data" / "processed"
    customer_csv = data_dir / "customer_master_data.csv"
    product_csv = data_dir / "product_master_data.csv"
    stream_csv = data_dir / "online_sales_stream.csv"
    periodic_csv = data_dir / "periodic_sales_staging.csv"

    print("\n--- TESTING MULTI-THREADED MESHJOIN ---")

    mj = MeshJoin(customer_csv, product_csv)
    enriched_output = deque()

    # create threads
    stream_thread = StreamProducerThread(stream_csv, mj, batch_rate=200, sleep_time=0.005)
    periodic_thread = PeriodicExtractorThread(periodic_csv, mj, interval_sec=0.02, total_hours=48)
    worker_thread = MeshJoinWorkerThread(mj, enriched_output, sleep_time=0.001)

    # start worker first, then producers
    worker_thread.start()
    stream_thread.start()
    periodic_thread.start()

    # wait for producers to complete
    stream_thread.join()
    periodic_thread.join()

    # wait for MESHJOIN to finish remaining queue & buffer
    print("\nWaiting for MESHJOIN to finish remaining records...")
    while True:
        with buffer_lock:
            if mj.is_finished():
                break
        time.sleep(0.05)

    # stop worker thread
    worker_thread.stop()
    worker_thread.join()

    print("\n--- MULTI-THREADED TEST RESULTS ---")
    print("Total online stream transactions:", stream_thread.total_stream_pushed)
    print("Total periodic transactions:", periodic_thread.total_periodic_pushed)
    print("Total shed transactions:", periodic_thread.total_shed)
    print("Total recovered transactions:", periodic_thread.total_recovered)
    print("Total enriched output tuples:", len(enriched_output))
    print("\nMulti-threading test completed successfully!")

