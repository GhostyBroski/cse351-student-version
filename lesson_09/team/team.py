""" 
Course: CSE 351
Team  : 
File  : team.py
Author:  Ash Jones
"""

# Include CSE 351 common Python files. 
from cse351 import *
import time
import random
import multiprocessing as mp

# number of cleaning staff and hotel guests
CLEANING_STAFF = 2
HOTEL_GUESTS = 5

# Run program for this number of seconds
TIME = 60

STARTING_PARTY_MESSAGE =  'Turning on the lights for the party vvvvvvvvvvvvvv'
STOPPING_PARTY_MESSAGE  = 'Turning off the lights  ^^^^^^^^^^^^^^^^^^^^^^^^^^'

STARTING_CLEANING_MESSAGE =  'Starting to clean the room >>>>>>>>>>>>>>>>>>>>>>>>>>>>>'
STOPPING_CLEANING_MESSAGE  = 'Finish cleaning the room <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<'

def cleaner_waiting():
    time.sleep(random.uniform(0, 2))

def cleaner_cleaning(id):
    print(f'Cleaner: {id}')
    time.sleep(random.uniform(0, 2))

def guest_waiting():
    time.sleep(random.uniform(0, 2))

def guest_partying(id, count):
    print(f'Guest: {id}, count = {count}')
    time.sleep(random.uniform(0, 1))

def cleaner(id, start_time, cleaned_count, guest_count, cleaning, room_lock, room_access):
    """
    do the following for TIME seconds
        cleaner will wait to try to clean the room (cleaner_waiting())
        get access to the room
        display message STARTING_CLEANING_MESSAGE
        Take some time cleaning (cleaner_cleaning())
        display message STOPPING_CLEANING_MESSAGE
    """
    while time.time() - start_time < TIME:
        cleaner_waiting()

        with room_lock:
            if guest_count.value == 0 and not cleaning.value:
                room_access.acquire()
                cleaning.value = True
                print(STARTING_CLEANING_MESSAGE)

        if cleaning.value:
            cleaner_cleaning(id)
            with room_lock:
                print(STOPPING_CLEANING_MESSAGE)
                cleaned_count.value += 1
                cleaning.value = False
                room_access.release()

def guest(id, start_time, party_count, guest_count, cleaning, room_lock, room_access):
    """
    do the following for TIME seconds
        guest will wait to try to get access to the room (guest_waiting())
        get access to the room
        display message STARTING_PARTY_MESSAGE if this guest is the first one in the room
        Take some time partying (call guest_partying())
        display message STOPPING_PARTY_MESSAGE if the guest is the last one leaving in the room
    """
    while time.time() - start_time < TIME:
        guest_waiting()

        with room_lock:
            if cleaning.value:
                continue  # wait and try again later

            if guest_count.value == 0:
                room_access.acquire()
                print(STARTING_PARTY_MESSAGE)

            guest_count.value += 1
            local_count = guest_count.value

        guest_partying(id, local_count)

        with room_lock:
            guest_count.value -= 1
            if guest_count.value == 0:
                print(STOPPING_PARTY_MESSAGE)
                party_count.value += 1
                room_access.release()

def main():
    # Start time of the running of the program.
    start_time = time.time()

    # TODO - add any variables, data structures, processes you need
    # TODO - add any arguments to cleaner() and guest() that you need

    # Shared variables and synchronization primitives
    cleaned_count = mp.Value('i', 0)
    party_count   = mp.Value('i', 0)
    guest_count   = mp.Value('i', 0)
    cleaning      = mp.Value('b', False)

    room_lock     = mp.Lock()
    room_access   = mp.Semaphore(1)

    processes = []

    # Start cleaners
    for i in range(CLEANING_STAFF):
        p = mp.Process(target=cleaner, args=(i, start_time, cleaned_count, guest_count, cleaning, room_lock, room_access))
        processes.append(p)
        p.start()

    # Start guests
    for i in range(HOTEL_GUESTS):
        p = mp.Process(target=guest, args=(i, start_time, party_count, guest_count, cleaning, room_lock, room_access))
        processes.append(p)
        p.start()

    # Wait for all to finish
    for p in processes:
        p.join()
    
    # Results
    print(f'Room was cleaned {cleaned_count.value} times, there were {party_count.value} parties')


if __name__ == '__main__':
    main()