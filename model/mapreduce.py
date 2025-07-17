import multiprocessing
import os
from collections import defaultdict, Counter

class MapReduce:

    def map_function(self, insects):
        mapped_values = []
        for insect_data in insects:
            insect = insect_data["insect"]
            event = insect_data["event"]
            mapped_values.append(("species:" + insect["species"], 1))
            mapped_values.append(("role:" + insect["role"], 1))
            mapped_values.append(("event:" + event, 1))
        return mapped_values

    def combiner_function(self, mapped_data):
        combined = defaultdict(int)
        for key, count in mapped_data:
            combined[key] += count
        return combined


    def reduce_function(self, combined_data):
        summary = defaultdict(int)
        for data in combined_data:
            for key, count in data.items():
                summary[key] += count
        return dict(summary)

    def map_worker(self, map_queue, reduce_queue):
        pid = os.getpid()
        while True:
            task = map_queue.get()
            if task is None:
                break
            mapped_data = self.map_function(task)
            combined_data = self.combiner_function(mapped_data)
            reduce_queue.put(combined_data)
            print(f"MapWorker PID {pid} processed batch of {len(task)} insects.")

    def reduce_worker(self, reduce_queue, result_queue):
        pid = os.getpid()
        combined_data = []
        while True:
            task = reduce_queue.get()
            if task is None:
                reduced_data = self.reduce_function(combined_data)
                result_queue.put(reduced_data)
                print(f"ReduceWorker PID {pid} produced final result.")
                break
            combined_data.append(task)

    def master_controller(self, insects_dict, num_map_tasks, num_reduce_tasks):
        all_insects = list(insects_dict.values())
        chunk_size = len(all_insects) // num_map_tasks + 1
        chunks = [all_insects[i:i + chunk_size] for i in range(0, len(all_insects), chunk_size)]

        map_queue = multiprocessing.Queue()
        reduce_queue = multiprocessing.Queue()
        result_queue = multiprocessing.Queue()

        map_workers = []
        for _ in range(num_map_tasks):
            worker = multiprocessing.Process(target=self.map_worker, args=(map_queue, reduce_queue))
            map_workers.append(worker)
            worker.start()

        reduce_workers = []
        for _ in range(num_reduce_tasks):
            worker = multiprocessing.Process(target=self.reduce_worker, args=(reduce_queue, result_queue))
            reduce_workers.append(worker)
            worker.start()

        for chunk in chunks:
            map_queue.put(chunk)

        for _ in range(num_map_tasks):
            map_queue.put(None)

        for worker in map_workers:
            worker.join()

        for _ in range(num_reduce_tasks):
            reduce_queue.put(None)

        for worker in reduce_workers:
            worker.join()

        final_result = defaultdict(int)
        for _ in range(num_reduce_tasks):
            result = result_queue.get()
            for key, count in result.items():
                final_result[key] += count

        return dict(final_result)