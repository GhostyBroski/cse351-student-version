"""
Course    : CSE 351
Assignment: 04
Student   : Ash Jones

Instructions:
    - review instructions in the course

In order to retrieve a weather record from the server, Use the URL:

f'{TOP_API_URL}/record/{name}/{recno}

where:

name: name of the city
recno: record number starting from 0

"""

import time
from queue import Queue
from common import *

from cse351 import *

THREADS = 250                # TODO - set for your program
WORKERS = 10
RECORDS_TO_RETRIEVE = 5000  # Don't change


# ---------------------------------------------------------------------------
def retrieve_weather_data(command_queue, data_queue):
    # TODO - fill out this thread function (and arguments)
    while True:
        item = command_queue.get()
        if item == "DONE":
            command_queue.task_done()
            break
        city, recno = item
        result = get_data_from_server(f"{TOP_API_URL}/record/{city}/{recno}")
        
        date = result["date"]
        temp = result["temp"]
        data_queue.put((city, date, temp))
        command_queue.task_done()


# ---------------------------------------------------------------------------
# TODO - Create Worker threaded class
class Worker(threading.Thread):
    def __init__(self, data_queue, noaa):
        super().__init__()
        self.data_queue = data_queue
        self.noaa = noaa

    def run(self):
        while True:
            item = self.data_queue.get()
            if item == "DONE":
                self.data_queue.task_done()
                break
            city, date, temp = item
            self.noaa.store(city, date, temp)
            self.data_queue.task_done()


# ---------------------------------------------------------------------------
# TODO - Complete this class
class NOAA:

    def __init__(self):
        self.data = {}
        self.lock = threading.Lock()

    def store(self, city, date, temp):
        with self.lock:
            if city not in self.data:
                self.data[city] = []
            self.data[city].append((date, temp))

    def get_temp_details(self, city):
        records = self.data.get(city, [])
        if not records:
            return 0.0
        total = sum(temp for date, temp in records)
        return round(total / len(records), 4)


# ---------------------------------------------------------------------------
def verify_noaa_results(noaa):

    answers = {
        'sandiego': 14.5004,
        'philadelphia': 14.865,
        'san_antonio': 14.638,
        'san_jose': 14.5756,
        'new_york': 14.6472,
        'houston': 14.591,
        'dallas': 14.835,
        'chicago': 14.6584,
        'los_angeles': 15.2346,
        'phoenix': 12.4404,
    }

    print()
    print('NOAA Results: Verifying Results')
    print('===================================')
    for name in CITIES:
        answer = answers[name]
        avg = noaa.get_temp_details(name)

        if abs(avg - answer) > 0.00001:
            msg = f'FAILED  Expected {answer}'
        else:
            msg = f'PASSED'
        print(f'{name:>15}: {avg:<10} {msg}')
    print('===================================')


# ---------------------------------------------------------------------------
def main():

    log = Log(show_terminal=True, filename_log='assignment.log')
    log.start_timer()

    noaa = NOAA()

    # Start server
    data = get_data_from_server(f'{TOP_API_URL}/start')

    # Get all cities number of records
    print('Retrieving city details')
    city_details = {}
    name = 'City'
    print(f'{name:>15}: Records')
    print('===================================')
    for name in CITIES:
        city_details[name] = get_data_from_server(f'{TOP_API_URL}/city/{name}')
        print(f'{name:>15}: Records = {city_details[name]['records']:,}')
    print('===================================')

    records = RECORDS_TO_RETRIEVE

    # TODO - Create any queues, pipes, locks, barriers you need

    print(" ")

    # Create queues
    command_queue = Queue(maxsize=10)
    data_queue = Queue(maxsize=10)

    # Start fetcher threads
    threads = []
    for _ in range(THREADS):
        t = threading.Thread(target=retrieve_weather_data, args=(command_queue, data_queue))
        t.start()
        threads.append(t)

    # Start worker threads
    workers = []
    for _ in range(WORKERS):
        w = Worker(data_queue, noaa)
        w.start()
        workers.append(w)

    # Fill the command queue with tasks
    for city in CITIES:
        for i in range(RECORDS_TO_RETRIEVE):
            command_queue.put((city, i))

    # Wait for queues to finish
    command_queue.join()
    data_queue.join()

    # Send stop signals to fetchers and workers
    for _ in range(THREADS):
        command_queue.put("DONE")
    for t in threads:
        t.join()

    for _ in range(WORKERS):
        data_queue.put("DONE")
    for w in workers:
        w.join()

    # End server - don't change below
    data = get_data_from_server(f'{TOP_API_URL}/end')
    print(data)

    verify_noaa_results(noaa)

    log.stop_timer('Run time: ')


if __name__ == '__main__':
    main()