import os
import time
import grpc
import shutil
import random
import kmeans_pb2
import kmeans_pb2_grpc
import multiprocessing as mp
from concurrent import futures
from concurrent.futures import ThreadPoolExecutor

EPSILON = 1e-4
THRESHOLD = 0.0


class Master(kmeans_pb2_grpc.MasterServicer):
    def __init__(self, mappers, reducers, num_centroids, num_iterations):

        self.mappers = mappers
        self.reducers = reducers
        self.num_centroids = num_centroids
        self.centroids = []
        self.all_points = self.read_points_file("points.txt")
        self.num_points = len(self.all_points)
        self.num_iterations = num_iterations
        self.epsilon = EPSILON
        self.start_indices = []
        self.end_indices = []
        self.mapper_addresses = []
        self.reducer_addresses = []
        self.mapper_pids = []
        self.reducer_pids = []
        self.mapper_responses = []
        self.reducer_responses = []

        self.init_centroids()
        self.create_file_structure()

        self.run()

    def dump(self, message):
        with open("master_dump.txt", "a+") as f:
            f.write(message)
            f.write("\n")
            f.flush()

    def create_file_structure(self):

        if os.path.exists("Mappers"):
            shutil.rmtree("Mappers")
        if os.path.exists("Reducers"):
            shutil.rmtree("Reducers")
        os.mkdir("Mappers")
        os.mkdir("Reducers")

        for i in range(self.mappers):
            os.mkdir(f"Mappers/M{i}")

    def read_points_file(self, filename):
        points = []
        with open(filename, "r") as file:
            for line in file:
                point = list(map(float, line.split(",")))
                points.append(point)
        return points

    # select random points as initial centroids
    def init_centroids(self):
        self.centroids = random.sample(self.all_points, self.num_centroids)

    # assign start and end indices to each mapper
    def assign_indices(self):
        self.start_indices = []
        self.end_indices = []
        points_per_mapper = self.num_points // self.alive_mappers
        for i in range(self.alive_mappers):
            self.start_indices.append(i * points_per_mapper)
            self.end_indices.append((i + 1) * points_per_mapper)
        self.end_indices[-1] = self.num_points

    # setup local host address for each mapper
    def assign_mapper_addresses(self):
        for i in range(self.mappers):
            self.mapper_addresses.append(f"localhost:5005{i+1}")

    def assign_reducer_addresses(self):
        for i in range(self.reducers):
            self.reducer_addresses.append(f"localhost:5006{i+1}")

    # launch mappers as grpc servers in parallel seperate processes
    def launch_mappers(self):
        self.dump("launching mappers")
        self.assign_mapper_addresses()
        for i in range(self.mappers):
            process_name = f"mapper{i+1}"
            p = mp.Process(
                target=self.launch_mapper,
                args=(self.mapper_addresses[i],),
                name=process_name,
            )
            p.start()
            self.mapper_pids.append(p.pid)

    def launch_mapper(self, address):
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        mapper = Mapper(self)
        kmeans_pb2_grpc.add_MapperServicer_to_server(mapper, server)
        server.add_insecure_port(address)
        server.start()
        server.wait_for_termination()

    def launch_reducers(self):
        self.dump("launching reducers")
        self.assign_reducer_addresses()
        for i in range(self.reducers):
            process_name = f"reducer{i+1}"
            p = mp.Process(
                target=self.launch_reducer,
                args=(self.reducer_addresses[i],),
                name=process_name,
            )
            p.start()
            self.reducer_pids.append(p.pid)

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
        completed_iterations = 0
        converged = False
        old_centroids = []
        while completed_iterations < self.num_iterations and not converged:

            self.dump(f"Iteration: {completed_iterations}")
            print(f"Iteration: {completed_iterations+1}")
            old_centroids = self.centroids
            self.alive_mappers = 0
            self.alive_mapper_addresses = []

            # if completed_iterations == 1:
            #     time.sleep(15)

            for i in range(self.mappers):
                try:
                    channel = grpc.insecure_channel(self.mapper_addresses[i])
                    stub = kmeans_pb2_grpc.MapperStub(channel)
                    args = kmeans_pb2.HeartBeatArgs(mapper_id=i)
                    response = stub.HeartBeat(args)
                    self.dump(f"Heartbeat from mapper: {i} {response.status}")
                    self.alive_mappers += 1
                    self.alive_mapper_addresses.append(self.mapper_addresses[i])
                except grpc.RpcError as e:
                    self.dump(f"mapper {i} is down")

            self.assign_indices()
            self.dump("Starting mappers in parallel")
            with ThreadPoolExecutor(max_workers=self.alive_mappers) as executor:
                futures = []
                for i in range(self.alive_mappers):
                    channel = grpc.insecure_channel(self.alive_mapper_addresses[i])
                    stub = kmeans_pb2_grpc.MapperStub(channel)
                    centroids = [
                        kmeans_pb2.Centroid(centroid=c) for c in self.centroids
                    ]
                    args = kmeans_pb2.MapperArgs(
                        mapper_id=i,
                        start_index=self.start_indices[i],
                        end_index=self.end_indices[i],
                        centroids=centroids,
                        num_reducers=self.reducers,
                    )

                    # Submit the request to the thread pool
                    future = executor.submit(stub.Mapper, args)
                    futures.append(future)

            self.mapper_responses = []
            failed_mappers = []
            for future_num in range(len(futures)):
                response = futures[future_num].result()
                if response.status == "FAIL":
                    failed_mappers.append(future_num)
                elif response.status == "Success":
                    self.mapper_responses.append(response)
            while True:
                self.dump(f"Failed mappers {failed_mappers}")
                if not failed_mappers:
                    break
                for failed_mapper in failed_mappers:
                    self.dump(f"Retrying mapper: {failed_mapper}")
                    channel = grpc.insecure_channel(
                        self.alive_mapper_addresses[failed_mapper]
                    )
                    stub = kmeans_pb2_grpc.MapperStub(channel)
                    centroids = [
                        kmeans_pb2.Centroid(centroid=c) for c in self.centroids
                    ]
                    args = kmeans_pb2.MapperArgs(
                        mapper_id=failed_mapper,
                        start_index=self.start_indices[failed_mapper],
                        end_index=self.end_indices[failed_mapper],
                        centroids=centroids,
                        num_reducers=self.reducers,
                    )
                    response = stub.Mapper(args)
                    if response.status == "FAIL":
                        pass
                    else:
                        failed_mappers.remove(failed_mapper)
                        self.mapper_responses.append(response)

            self.dump(f"Mapper responses: {self.mapper_responses}")
            if all([response.status == "Success" for response in self.mapper_responses]):

                self.alive_reducers = 0
                self.alive_reducer_addresses = []
                self.dump("Sending heartbeat to reducers")
                for i in range(self.reducers):
                    try:
                        channel = grpc.insecure_channel(self.reducer_addresses[i])
                        stub = kmeans_pb2_grpc.ReducerStub(channel)
                        args = kmeans_pb2.HeartBeatArgs(mapper_id=i)
                        response = stub.HeartBeat(args)
                        self.dump(f"Heartbeat from reducer: {i} {response.status}")
                        self.alive_reducers += 1
                        self.alive_reducer_addresses.append(self.reducer_addresses[i])
                    except grpc.RpcError as e:
                        self.dump(f"reducer {i} is down")

                self.dump("Starting reducers in parallel")

                reducer_partition_map = {}
                for i in range(self.alive_reducers):
                    reducer_partition_map[i] = []
                for i in range(self.reducers):
                    reducer_partition_map[i % self.alive_reducers].append(i)

                with ThreadPoolExecutor(
                    max_workers=self.alive_reducers
                ) as reducer_executor:
                    reducer_futures = []
                    for i in range(self.alive_reducers):
                        channel = grpc.insecure_channel(self.alive_reducer_addresses[i])
                        stub = kmeans_pb2_grpc.ReducerStub(channel)
                        reducer_id = i
                        args = kmeans_pb2.ReducerArgs(
                            reducer_id=reducer_id,
                            reducer_partition_map=reducer_partition_map[i],
                            mapper_addresses=self.alive_mapper_addresses,
                            num_centroids=self.num_centroids,
                        )
                        reducer_future = reducer_executor.submit(stub.Reducer, args)
                        reducer_futures.append(reducer_future)

                for future_num in range(len(reducer_futures)):
                    response = reducer_futures[future_num].result()
                    if response.status == "FAIL":
                        self.dump(f"Retrying reducer: {future_num}")
                    else:
                        self.reducer_responses.append(response)

                new_centroids = {}
                for reducer_response in self.reducer_responses:
                    for centroid in reducer_response.computed_centroids:
                        new_centroids[centroid.centroid_key - 1] = centroid.centroid

                formatted_new_centroids = [
                    new_centroids[i] for i in range(self.num_centroids)
                ]

                print("Old centroids: ", old_centroids)
                print("New centroids: ", formatted_new_centroids)
                self.centroids = formatted_new_centroids
                for i in range(self.num_centroids):
                    if (
                        abs(old_centroids[i][0] - formatted_new_centroids[i][0])
                        < self.epsilon
                        and abs(old_centroids[i][1] - formatted_new_centroids[i][1])
                        < self.epsilon
                    ):
                        converged = True
                    else:
                        converged = False
                        break
                completed_iterations += 1
                self.write_centroids_to_file(self.centroids)
                self.dump(f"centroids: {self.centroids}")
            else:
                print("Some mappers failed. Retrying iteration")
                completed_iterations += 1

        os.kill(os.getpid(), 2)

    def write_centroids_to_file(self, centroids):
        with open("centroids.txt", "w") as file:
            for centroid in centroids:
                file.write(",".join(map(str, centroid)) + "\n")


class Mapper(kmeans_pb2_grpc.MapperServicer):
    def __init__(self, master):
        self.master = master
        self.partitions = []

    def Mapper(self, request, context):
        self.master.dump(f"Mapper called {request.mapper_id}")
        self.mapper_id = request.mapper_id
        self.num_centroids = len(request.centroids)
        self.start_index = request.start_index
        self.end_index = request.end_index
        self.file_path = "points.txt"
        self.num_reducers = request.num_reducers
        self.points = self.read_points_file(self.file_path)
        self.threshold = THRESHOLD
        self.partitions = []
        self.point_assignments = []

        for point in self.points:
            min_distance = float("inf")
            closest_centroid = None
            for i, centroid in enumerate(request.centroids):
                distance = euclidean_distance(point, centroid.centroid)
                if distance < min_distance:
                    min_distance = distance
                    closest_centroid = i
            self.point_assignments.append({closest_centroid: point})
        self.partitions = self.create_partitions(
            self.point_assignments, self.num_reducers
        )
        self.master.dump(f"Mapper {self.mapper_id} partitions: {self.partitions}")
        self.write_partitions_to_file(self.partitions)

        # random chance to send FAIL status
        if self.mapper_id == 0:
            prob = random.random()
            if prob < self.threshold:
                return kmeans_pb2.MapperReply(status="FAIL")
            else:
                return kmeans_pb2.MapperReply(status="Success")
        else:
            return kmeans_pb2.MapperReply(status="Success")

    # read points from file from start_index to end_index
    def read_points_file(self, filename):
        points = []
        with open(filename, "r") as file:
            for i, line in enumerate(file):
                if i >= self.start_index and i < self.end_index:
                    point = list(map(float, line.split(",")))
                    points.append(point)
        return points

    # create num_reducers partitions of points such that each partition has all points assigned to a centroid and the centroid keys are equally distributed
    def create_partitions(self, centroid_point_assignments, num_reducers):

        partitions = [[] for _ in range(num_reducers)]
        for assignment in centroid_point_assignments:
            for centroid_key, point in assignment.items():
                partition_index = centroid_key % num_reducers
                partitions[partition_index].append((centroid_key, point))

        return partitions

    def write_partitions_to_file(self, partitions):
        self.master.dump(f"Writing partitions to file")
        for i, partition in enumerate(partitions):
            with open(f"Mappers/M{self.mapper_id}/partition_{i}.txt", "w") as file:
                for centroid_key, point in partition:
                    file.write(f'{centroid_key},{",".join(map(str, point))}\n')

    def PartitionReq(self, request, context):
        self.master.dump(
            f"Partition request from reducer {request.reducer_id} to mapper {self.mapper_id}"
        )
        partition_id = request.reducer_id
        partition = []
        with open(
            f"Mappers/M{self.mapper_id}/partition_{partition_id}.txt", "r"
        ) as file:
            for line in file:
                partition.append(line)

        return kmeans_pb2.PartitionReqReply(partition_file_content=partition)

    def HeartBeat(self, request, context):
        return kmeans_pb2.HeartBeatReply(status="Success")


def euclidean_distance(point1, point2):
    return sum([(a - b) ** 2 for a, b in zip(point1, point2)]) ** 0.5


class Reducer(kmeans_pb2_grpc.ReducerServicer):
    def __init__(self, master):
        self.master = master
        self.partition_data = []

    def Reducer(self, request, context):
        self.master.dump(f"Reducer called {request.reducer_id}")
        self.mapper_addresses = request.mapper_addresses
        self.num_centroids = request.num_centroids
        self.reducer_id = request.reducer_id
        self.partition_indexes = request.reducer_partition_map

        for _, mapper_address in enumerate(self.mapper_addresses):
            channel = grpc.insecure_channel(mapper_address)
            stub = kmeans_pb2_grpc.MapperStub(channel)
            for partition_id in self.partition_indexes:
                response = stub.PartitionReq(
                    kmeans_pb2.PartitionReqArgs(reducer_id=partition_id)
                )
                self.partition_data.append(response.partition_file_content)

        self.grouped_partitions = self.shuffle_and_sort(self.partition_data)
        new_centroids = self.reduce_operation(self.grouped_partitions)
        with open(f"Reducers/R{self.reducer_id}.txt", "w") as file:
            for key, value in new_centroids.items():
                file.write(f"{key},{','.join(map(str, value))}\n")

        response = kmeans_pb2.ReducerReply(status="Success")

        for key, value in new_centroids.items():
            data = kmeans_pb2.computedCentroid(centroid_key=key + 1, centroid=value)
            response.computed_centroids.append(data)
        return response

    def shuffle_and_sort(self, partitions):
        all_pairs = []
        for partition in partitions:
            for line in partition:
                key, *value = line.strip().split(",")
                key = int(key)
                value = [float(v) for v in value]
                all_pairs.append((key, value))

        all_pairs.sort(key=lambda x: x[0])

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

    def HeartBeat(self, request, context):
        return kmeans_pb2.HeartBeatReply(status="Success")


if __name__ == "__main__":
    try:
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        mappers = int(input("Enter number of mappers: "))
        reducers = int(input("Enter number of reducers: "))
        num_centroids = int(input("Enter number of centroids: "))
        num_iterations = int(input("Enter number of iterations: "))
        master = Master(mappers, reducers, num_centroids, num_iterations)
        kmeans_pb2_grpc.add_MasterServicer_to_server(master, server)
        server.add_insecure_port("[::]:50050")
        server.start()
        server.wait_for_termination()

    except KeyboardInterrupt:
        print("Server stopped")
        server.stop(0)