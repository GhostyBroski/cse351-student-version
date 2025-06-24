"""
Course: CSE 351, week 10
File: functions.py
Author: Ash Jones

Instructions:

Depth First Search
https://www.youtube.com/watch?v=9RHO6jU--GU

Breadth First Search
https://www.youtube.com/watch?v=86g8jAQug04


Requesting a family from the server:
family_id = 6128784944
data = get_data_from_server('{TOP_API_URL}/family/{family_id}')

Example JSON returned from the server
{
    'id': 6128784944, 
    'husband_id': 2367673859,        # use with the Person API
    'wife_id': 2373686152,           # use with the Person API
    'children': [2380738417, 2185423094, 2192483455]    # use with the Person API
}

Requesting an individual from the server:
person_id = 2373686152
data = get_data_from_server('{TOP_API_URL}/person/{person_id}')

Example JSON returned from the server
{
    'id': 2373686152, 
    'name': 'Stella', 
    'birth': '9-3-1846', 
    'parent_id': 5428641880,   # use with the Family API
    'family_id': 6128784944    # use with the Family API
}


--------------------------------------------------------------------------------------
You will lose 10% if you don't detail your part 1 and part 2 code below

Describe how to speed up part 1

<Add your comments here>


Describe how to speed up part 2

<Add your comments here>


Extra (Optional) 10% Bonus to speed up part 3

<Add your comments here>

"""
from common import *
import queue
import threading
from threading import Thread, Lock
from concurrent.futures import ThreadPoolExecutor, as_completed

# -----------------------------------------------------------------------------
def depth_fs_pedigree(family_id, tree):
    # KEEP this function even if you don't implement it
    # TODO - implement Depth first retrieval
    # TODO - Printing out people and families that are retrieved from the server will help debugging
    
    if family_id is None or tree.does_family_exist(family_id):
        return

    family_data = get_data_from_server(f'{TOP_API_URL}/family/{family_id}')
    if not family_data:
        return

    family = Family(family_data)
    tree.add_family(family)

    threads = []

    # Husband
    husband_id = family.get_husband()
    if husband_id and not tree.does_person_exist(husband_id):
        husband_data = get_data_from_server(f'{TOP_API_URL}/person/{husband_id}')
        if husband_data:
            husband = Person(husband_data)
            tree.add_person(husband)

            t = Thread(target=depth_fs_pedigree, args=(husband.get_parentid(), tree))
            threads.append(t)
            t.start()

    # Wife
    wife_id = family.get_wife()
    if wife_id and not tree.does_person_exist(wife_id):
        wife_data = get_data_from_server(f'{TOP_API_URL}/person/{wife_id}')
        if wife_data:
            wife = Person(wife_data)
            tree.add_person(wife)

            t = Thread(target=depth_fs_pedigree, args=(wife.get_parentid(), tree))
            threads.append(t)
            t.start()

    # Children (not recursive)
    for child_id in family.get_children():
        if child_id and not tree.does_person_exist(child_id):
            child_data = get_data_from_server(f'{TOP_API_URL}/person/{child_id}')
            if child_data:
                child = Person(child_data)
                tree.add_person(child)

    # Wait for all threads to finish
    for t in threads:
        t.join()

# -----------------------------------------------------------------------------
def breadth_fs_pedigree(family_id, tree):
    # KEEP this function even if you don't implement it
    # TODO - implement breadth first retrieval
    # TODO - Printing out people and families that are retrieved from the server will help debugging
    """Breadth-First Search using queue"""
    q = queue.Queue()
    q.put(family_id)

    lock = Lock()
    seen_families = set()
    seen_people = set()

    def fetch_person(person_id, add_parents=True):
        if person_id is None:
            return

        with lock:
            if person_id in seen_people:
                return
            seen_people.add(person_id)

        person_data = get_data_from_server(f'{TOP_API_URL}/person/{person_id}')
        if person_data:
            person = Person(person_data)
            with lock:
                tree.add_person(person)

            if add_parents:
                parent_fam_id = person.get_parentid()
                if parent_fam_id is not None:
                    with lock:
                        if parent_fam_id not in seen_families:
                            q.put(parent_fam_id)

    with ThreadPoolExecutor(max_workers=50) as executor:
        futures = []

        # The outer loop keeps going as long as there's work in the queue or futures
        while not q.empty() or futures:
            if not q.empty():
                fam_id = q.get()

                with lock:
                    if fam_id in seen_families:
                        continue
                    seen_families.add(fam_id)

                family_data = get_data_from_server(f'{TOP_API_URL}/family/{fam_id}')
                if not family_data:
                    continue

                family = Family(family_data)
                with lock:
                    tree.add_family(family)

                # Fetch family members in threads
                futures.append(executor.submit(fetch_person, family.get_husband(), True))
                futures.append(executor.submit(fetch_person, family.get_wife(), True))
                for child_id in family.get_children():
                    futures.append(executor.submit(fetch_person, child_id, False))

            # Clean up completed futures
            futures = [f for f in futures if not f.done()]

# -----------------------------------------------------------------------------
def breadth_fs_pedigree_limit5(family_id, tree):
    # KEEP this function even if you don't implement it
    # TODO - implement breadth first retrieval
    #      - Limit number of concurrent connections to the FS server to 5
    # TODO - Printing out people and families that are retrieved from the server will help debugging
    
    """BFS with 5 concurrent threads, using a thread-safe queue"""
    q = queue.Queue()
    lock = threading.Lock()
    q.put(family_id)

    def worker():
        while True:
            try:
                fam_id = q.get(timeout=1)
            except queue.Empty:
                return

            if fam_id is None or tree.does_family_exist(fam_id):
                q.task_done()
                continue

            family_data = get_data_from_server(f'{TOP_API_URL}/family/{fam_id}')
            if not family_data:
                q.task_done()
                continue

            family = Family(family_data)
            with lock:
                if not tree.does_family_exist(family.get_id()):
                    tree.add_family(family)

            # Husband
            husband_id = family.get_husband()
            if husband_id and not tree.does_person_exist(husband_id):
                husband_data = get_data_from_server(f'{TOP_API_URL}/person/{husband_id}')
                if husband_data:
                    husband = Person(husband_data)
                    with lock:
                        if not tree.does_person_exist(husband.get_id()):
                            tree.add_person(husband)
                    q.put(husband.get_parentid())

            # Wife
            wife_id = family.get_wife()
            if wife_id and not tree.does_person_exist(wife_id):
                wife_data = get_data_from_server(f'{TOP_API_URL}/person/{wife_id}')
                if wife_data:
                    wife = Person(wife_data)
                    with lock:
                        if not tree.does_person_exist(wife.get_id()):
                            tree.add_person(wife)
                    q.put(wife.get_parentid())

            # Children
            for child_id in family.get_children():
                if child_id and not tree.does_person_exist(child_id):
                    child_data = get_data_from_server(f'{TOP_API_URL}/person/{child_id}')
                    if child_data:
                        child = Person(child_data)
                        with lock:
                            if not tree.does_person_exist(child.get_id()):
                                tree.add_person(child)

            q.task_done()

    # Launch 5 threads
    threads = []
    for _ in range(5):
        t = threading.Thread(target=worker)
        t.start()
        threads.append(t)

    # Wait for queue to empty
    q.join()

    for t in threads:
        t.join()