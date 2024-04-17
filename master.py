import os
import grpc
import random
from concurrent import futures

import kmeans_pb2
import kmeans_pb2_grpc
import multiprocessing as mp
from concurrent.futures import ThreadPoolExecutor
import time

class Master(kmeans_pb2_grpc.MasterServicer):
    def __init__(self,mappers,reducers,num_centroids,num_iterations):
        self.mapper = kmeans_pb2_grpc.MasterServicer
        self.reducer = kmeans_pb2_grpc.MasterServicer

        self.mappers = mappers
        self.reducers = reducers
        self.num_centroids = num_centroids
        self.centroids = []
        self.all_points = self.read_points_file('points.txt')
        self.num_points = len(self.all_points)
        self.num_iterations = num_iterations
        self.start_indexes=[]
        self.end_indexes=[]
        self.mapper_addresses=[]
        self.reducer_addresses=[]
        self.mapper_pids = []
        self.reducer_pids = []
        self.mapper_responses = []
        self.reducer_responses = []

        


        self.init_centroids()
        self.assign_indexes()
        # self.create_file_structure()
        
        self.run()

    def create_file_structure(self):
        os.mkdir('Mappers')
        os.mkdir('Reducers')

        for i in range(self.mappers):
            os.mkdir(f'Mappers/M{i}')
        for i in range(self.reducers):
            os.mkdir(f'Reducers/R{i}')
        # os.mkdir('partitions')
        # os.mkdir('output')



    def read_points_file(self, filename):
        points = []
        with open(filename, 'r') as file:
            for line in file:
                # print(line)
                # x,y = line.split(',')
                point = list(map(float, line.split(',')))
                points.append(point)
        return points

    # select random points as initial centroids
    def init_centroids(self):
        self.centroids = random.sample(self.all_points, self.num_centroids)

    # assign start and end indexes to each mapper
    def assign_indexes(self):
        points_per_mapper = self.num_points // self.mappers
        for i in range(self.mappers):
            self.start_indexes.append(i * points_per_mapper)
            self.end_indexes.append((i + 1) * points_per_mapper)
        self.end_indexes[-1] = self.num_points
    
    # setup local host address for each mapper
    def assign_mapper_addresses(self):
        for i in range(self.mappers):
            self.mapper_addresses.append('localhost:5005'+str(i+1))
        
    def assign_reducer_addresses(self):
        for i in range(self.reducers):
            self.reducer_addresses.append('localhost:5006'+str(i+1))

    # launch mappers as grpc servers in parallel seperate processes
    def launch_mappers(self):
        self.assign_mapper_addresses()
        for i in range(self.mappers):
            process_name = "mapper"+str(i+1)
            p = mp.Process(target=self.launch_mapper, args=(self.mapper_addresses[i],),name=process_name)
            p.start()
            self.mapper_pids.append(p.pid)
            # mp.Process(target=self.launch_mapper, args=(self.mapper_addresses[i],),name=process_name).start()
    
    def launch_mapper(self, address):
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        mapper = Mapper(self)
        kmeans_pb2_grpc.add_MapperServicer_to_server(mapper, server)
        server.add_insecure_port(address)
        server.start()
        server.wait_for_termination()
    
    def launch_reducers(self):
        self.assign_reducer_addresses()
        for i in range(self.reducers):
            process_name = "reducer"+str(i+1)
            p = mp.Process(target=self.launch_reducer, args=(self.reducer_addresses[i],),name=process_name)
            p.start()
            self.reducer_pids.append(p.pid)
            # mp.Process(target=self.launch_reducer, args=(self.reducer_addresses[i],),name=process_name).start()
    def launch_reducer(self, address):
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        reducer = Reducer(self)
        kmeans_pb2_grpc.add_ReducerServicer_to_server(reducer, server)
        server.add_insecure_port(address)
        server.start()
        server.wait_for_termination()



    def run(self):
        self.launch_mappers()
        self.launch_reducers()
        
        print("Mappers: ", self.mapper_pids)
        print("Reducers: ", self.reducer_pids)
        completed_iterations = 0
        converged = False
        old_centroids=[]
        while completed_iterations < self.num_iterations or not converged:
            old_centroids = self.centroids
            
            # get stubs for all mappers and send request to all in parallel
            with ThreadPoolExecutor(max_workers=self.mappers) as executor:
                futures = []
                for i in range(self.mappers):
                    channel = grpc.insecure_channel(self.mapper_addresses[i])
                    stub = kmeans_pb2_grpc.MapperStub(channel)  
                    mapper_id = i
                    # print the arguments

                    centroids = [kmeans_pb2.Centroid(centroid=c) for c in self.centroids]

                    args = kmeans_pb2.MapperArgs(mapper_id=mapper_id,
                            start_index=self.start_indexes[i],
                            end_index=self.end_indexes[i],
                            centroids = centroids,
                            num_reducers = self.reducers)
                    
                    # Submit the request to the thread pool
                    future = executor.submit(stub.Mapper, args)
                    futures.append(future)


            # Gather all the responses
            for future_num in range(len(futures)):
                response = futures[future_num].result()
                if response.status == 'FAIL':
                    # executor.submit(self.retry_mapper, future_num)
                    print("Retrying mapper: ", future_num)
                else:
                    self.mapper_responses.append(response)
            # print("Mapper responses: ", self.mapper_responses)

        # def retry_mapper(self, future_num):
        #     print("Retrying mapper: ", future_num)
        #     channel = grpc.insecure_channel(self.mapper_addresses[future_num])
        #     stub = kmeans_pb2_grpc.MapperStub(channel)
        #     mapper_id = future_num
        #     centroids = [kmeans_pb2.Centroid(centroid=c) for c in self.centroids]
        #     args = kmeans_pb2.MapperArgs(mapper_id=mapper_id,
        #                     start_index=self.start_indexes[future_num],
        #                     end_index=self.end_indexes[future_num],
        #                     centroids = centroids,
        #                     num_reducers = self.reducers)
        #     response = stub.Mapper(args)
        #     self.mapper_responses.append(response)
            
            # if all mappers return SUCCESS, launch reducers
            # if all([response.status == 'SUCCESS' for response in self.mapper_responses]):

                # get stubs for all reducers and send request to all in parallel
            with ThreadPoolExecutor(max_workers=self.reducers) as reducer_executor:
                reducer_futures = []
                for i in range(self.reducers):
                    channel = grpc.insecure_channel(self.reducer_addresses[i])
                    stub = kmeans_pb2_grpc.ReducerStub(channel)
                    reducer_id = i
                    args = kmeans_pb2.ReducerArgs(reducer_id=reducer_id,
                                                    mapper_addresses=self.mapper_addresses,
                                                    num_centroids=self.num_centroids)
                    reducer_future = reducer_executor.submit(stub.Reducer, args)
                    reducer_futures.append(reducer_future)
            
            # Gather all the responses
            for future_num in range(len(reducer_futures)):
                response = reducer_futures[future_num].result()
                if response.status == 'FAIL':
                    # executor.submit(self.retry_mapper, future_num)
                    print("Retrying reducer: ", future_num)
                else:
                    self.reducer_responses.append(response)

            # print("Reducer responses: ", self.reducer_responses)
            # consolidate the new centroids from all reducers
            new_centroids = {}
            for reducer_response in self.reducer_responses:
                for centroid in reducer_response.computed_centroids:
                    new_centroids[centroid.centroid_key-1] = centroid.centroid

            # print("New centroids: ", new_centroids)
            # convert new_centroids into a list with key as the index
            formatted_new_centroids = [new_centroids[i] for i in range(self.num_centroids)]
            print("Old centroids: ", old_centroids)
            print("New centroids: ", formatted_new_centroids)
            self.centroids = formatted_new_centroids

            # check for convergence upto 2 decimal places
            for i in range(self.num_centroids):
                if int(old_centroids[i][0] * 100) == int(formatted_new_centroids[i][0] * 100) and int(old_centroids[i][1] * 100) == int(formatted_new_centroids[i][1] * 100):
                    converged = True
                else:
                    converged = False
                    break
            completed_iterations += 1

class Mapper(kmeans_pb2_grpc.MapperServicer):
    def __init__(self, master):
        self.master = master
        self.partitions=[]
    def Mapper(self, request, context):
        print("Mapper called", request.mapper_id)
        # start_time = time.time()
        # print("mapper started at: ", start_time)
        # # print(request)
        # time.sleep(random.randint(1, 5))
        # end_time = time.time()
        # print("mapper ended at: ", end_time)
        self.mapper_id = request.mapper_id
        self.num_centroids = len(request.centroids)
        self.start_index = request.start_index
        self.end_index = request.end_index
        self.file_path = 'points.txt'
        self.num_reducers = request.num_reducers
        self.points = self.read_points_file(self.file_path)
        # print(self.points)
        self.partitions = []
        self.point_assignments=[]

        for point in self.points:
            min_distance = float('inf')
            closest_centroid = None
            for i, centroid in enumerate(request.centroids):
                distance = euclidean_distance(point, centroid.centroid)
                if distance < min_distance:
                    min_distance = distance
                    closest_centroid = i
            self.point_assignments.append({closest_centroid:point})
        # print(self.point_assignments)
        self.partitions = self.create_partitions(self.point_assignments, self.num_reducers)
        # print(self.partitions)
        self.write_partitions_to_file(self.partitions)
        return kmeans_pb2.MapperReply(status='Success')



    # read points from file from start_index to end_index
    def read_points_file(self, filename):
        points = []
        with open(filename, 'r') as file:
            for i, line in enumerate(file):
                if i >= self.start_index and i < self.end_index:
                    point = list(map(float, line.split(',')))
                    points.append(point)
        return points

    # create num_reducers partitions of points such that each partition has all points assigned to a centroid and the centroid keys are equally distributed
    def create_partitions(self,centroid_point_assignments, num_reducers):
    
        partitions = [[] for _ in range(num_reducers)]

        for assignment in centroid_point_assignments:
            for centroid_key, point in assignment.items():
                partition_index = centroid_key % num_reducers
                partitions[partition_index].append((centroid_key, point))

        return partitions
    
    def write_partitions_to_file(self,partitions):
        for i, partition in enumerate(partitions):
            # print("Partition: ", partition)
            with open(f'Mappers/M{self.mapper_id}/partition_{i}.txt', 'w') as file:
                for centroid_key, point in partition:
                    file.write(f'{centroid_key},{",".join(map(str, point))}\n')


    def PartitionReq(self, request, context):
        partition_id = request.reducer_id
        # print("Got request")
        # read the partition from file corresponding to partition_id
        partition = []
        with open(f'Mappers/M{self.mapper_id}/partition_{partition_id}.txt', 'r') as file:
            for line in file:
                # print(line)
                partition.append(line)

        return kmeans_pb2.PartitionReqReply(partition_file_content=partition)


def euclidean_distance(point1, point2):
    return sum([(a - b) ** 2 for a, b in zip(point1, point2)]) ** 0.5
class Reducer(kmeans_pb2_grpc.ReducerServicer):
    def __init__(self, master):
        self.master = master
        self.partition_data = []
    
    def Reducer(self, request, context):
        print("Reducer called", request.reducer_id)
        self.mapper_addresses = request.mapper_addresses
        self.num_centroids = request.num_centroids
        self.reducer_id = request.reducer_id
        # request the reducer_id-th partition from all mappers
        for i, mapper_address in enumerate(self.mapper_addresses):
            channel = grpc.insecure_channel(mapper_address)
            stub = kmeans_pb2_grpc.MapperStub(channel)
            partition_id = self.reducer_id
            response = stub.PartitionReq(kmeans_pb2.PartitionReqArgs(reducer_id=self.reducer_id))
            # print(response.partition_file_content)
            self.partition_data.append(response.partition_file_content)

        # print(self.partition_data)
        self.grouped_partitions = self.shuffle_and_sort(self.partition_data)
        # print(self.grouped_partitions)
        new_centroids = self.reduce_operation(self.grouped_partitions)
        # print(new_centroids)
        response = kmeans_pb2.ReducerReply(status='Success')
        
        for key, value in new_centroids.items():
            # print(key, value)
            data = kmeans_pb2.computedCentroid(centroid_key=key+1, centroid=value)
            response.computed_centroids.append(data)
        return response


    def shuffle_and_sort(self, partitions):
        # Combine all the partitions into a single list of key-value pairs
        all_pairs = []
        for partition in partitions:
            for line in partition:
                key, *value = line.strip().split(',')
                key = int(key)
                value = [float(v) for v in value]
                all_pairs.append((key, value))

        # Sort the key-value pairs by key
        all_pairs.sort(key=lambda x: x[0])

        # Group the values by key
        grouped_pairs = {}
        for key, value in all_pairs:
            if key not in grouped_pairs:
                grouped_pairs[key] = []
            grouped_pairs[key].append(value)

        return grouped_pairs

    def reduce_operation(self, grouped_partitions):
        new_centroids = {}
        for key, values in grouped_partitions.items():
            new_centroid = [sum(x) / len(values) for x in zip(*values)]
            new_centroids[key] = new_centroid
        return new_centroids


if __name__ == '__main__':
    try:
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        # take input from user
        mappers = int(input("Enter number of mappers: "))
        reducers = int(input("Enter number of reducers: "))
        num_centroids = int(input("Enter number of centroids: "))
        num_iterations = int(input("Enter number of iterations: "))
        # create a master object
        master = Master(mappers, reducers, num_centroids, num_iterations)



        kmeans_pb2_grpc.add_MasterServicer_to_server(master, server)
        server.add_insecure_port('[::]:50050')
        server.start()
        server.wait_for_termination()

    except KeyboardInterrupt:
        print("Server stopped")
        server.stop(0)
    