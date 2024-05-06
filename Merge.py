from Pyro4 import expose
import random
import time
import heapq
from heapq import heappush, heappop
class Solver:
    def __init__(self, workers=None, input_file_name=None, output_file_name=None):
        self.input_file_name = input_file_name
        self.output_file_name = output_file_name
        self.workers = workers
        print("Inited")

    def solve(self):
        print("Job Started")
        print("Workers %d" % len(self.workers))
        start_time = time.time()
        array = self.read_input()
        #array = [6,2,8,3,4,1]
        subarrays = self.divide_array(len(self.workers), array)
        #step = n / len(self.workers)

        # map
        mapped = []
        for i in xrange(0, len(self.workers)):
            mapped.append(self.workers[i].mymap(subarrays[i]))

        print 'Map finished: ', mapped

        # reduce
        reduced = self.myreduce(mapped)
        print("Reduce finished: " + str(reduced))
        total_time = time.time() - start_time
        # output
        self.write_output(reduced,total_time)

        print("Job Finished")

    @staticmethod
    @expose
    def mymap(subarray):
        print (subarray)
        sorted_subarray = Solver.merge_sort(subarray)
        return sorted_subarray

    @staticmethod
    @expose
    def myreduce(mapped):
       merged_array = []
       for item in mapped:
          merged_array.append(item.value)
       return Solver.merge_sorted_arrays(merged_array)

    # def read_input(self):
    #     f = open(self.input_file_name, 'r')
    #     line = f.readline()
    #     f.close()
    #     return int(line)
    def read_input(self):
      with open(self.input_file_name, 'r') as f:
          array = [int(x) for x in f.readline().split()]
      return array
    def write_output(self, output, time_elapsed):
        with open(self.output_file_name, 'w') as f:
            f.write("Sorted Array: {}\n".format(output))
            f.write("Execution Time: {:.3f} seconds\n".format(time_elapsed))
            f.write("Number of Workers: {}\n".format(len(self.workers)))
    def divide_array(self, workersAmount, array):
        n = len(array)
        step = n // workersAmount
        subarrays = [array[i * step:(i + 1) * step] for i in range(workersAmount - 1)]
        subarrays.append(array[(workersAmount - 1) * step:])
        return subarrays

    @staticmethod
    @expose
    def merge_sort(array):
        if len(array) <= 1:
            return array
        mid = len(array) // 2
        left_half = array[:mid]
        right_half = array[mid:]
        left_half = Solver.merge_sort(left_half)
        right_half = Solver.merge_sort(right_half)
        return Solver.merge(left_half, right_half)  

    @staticmethod
    def merge(left, right):
        result = []
        i = j = 0
        while i < len(left) and j < len(right):
            if left[i] < right[j]:
                result.append(left[i])
                i += 1
            else:
                result.append(right[j])
                j += 1
        result.extend(left[i:])
        result.extend(right[j:])
        return result

    @staticmethod
    def merge_sorted_arrays(merged_array):
      result = []
      pq = [(subarray[0], i, 0) for i, subarray in enumerate(merged_array) if subarray]
      heapq.heapify(pq)
      while pq:
          val, array_idx, idx = heapq.heappop(pq)
          result.append(val)
          if idx + 1 < len(merged_array[array_idx]):
              next_val = merged_array[array_idx][idx + 1]
              heapq.heappush(pq, (next_val, array_idx, idx + 1))
      
      return result