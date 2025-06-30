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

I was able to speed up the depth-first search by adding multithreading to recursive implementation.
Then instead of traversing the familly tree one branch at a time, I used a new thread for each 
recursive call to a person's parent family. Doing this, I effectively allowed multiple generations of the tree 
to be built in parallel. After this, I joined the threads afterward to ensure that the tree was built 
correctly without skipping data.


Describe how to speed up part 2

I was able to speed up the breadth-first search by using a ThreadPoolExecutor that has a high number of 
worker threads (50). Then, by maintaining a thread-safe queue of family IDs and using concurrent 
threads to retrieve all family members (husband, wife, and children). To do this, parent family IDs 
for husband and wife were enqueued for later processing, allowing the queue to grow as 
the search progressed. This then enabled the program to fetch and process many people and 
families at the same time, significantly reducing total runtime compared to a 
single-threaded implementation.


Extra (Optional) 10% Bonus to speed up part 3

To limit the concurrency to 5 threads, I implemented a worker pool pattern with exactly 5 threads running 
concurrently. In doing this, each worker thread repeatedly dequeues a family ID from a shared queue and fetches the 
family and its members from the server. When a person has parent families, those family IDs are safely 
enqueued for further processing. Locks ensure that shared data structures like the family tree and sets 
tracking seen families and people are updated safely without race conditions. This approach maintains 
a steady pool of 5 active threads, efficiently processing the breadth-first traversal while respecting 
the thread limit and avoiding duplicates.

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
    import queue
    from concurrent.futures import ThreadPoolExecutor
    from threading import Lock

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
        active_futures = set()

        while not q.empty() or active_futures:
            while not q.empty():
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

                # Spawn threads to fetch people
                active_futures.add(executor.submit(fetch_person, family.get_husband(), True))
                active_futures.add(executor.submit(fetch_person, family.get_wife(), True))
                for child_id in family.get_children():
                    active_futures.add(executor.submit(fetch_person, child_id, False))

            # Remove completed futures
            active_futures = {f for f in active_futures if not f.done()}

# -----------------------------------------------------------------------------
def breadth_fs_pedigree_limit5(family_id, tree):
    # KEEP this function even if you don't implement it
    # TODO - implement breadth first retrieval
    #      - Limit number of concurrent connections to the FS server to 5
    # TODO - Printing out people and families that are retrieved from the server will help debugging
    
    """BFS with 5 concurrent threads, using a thread-safe queue"""
    q = queue.Queue()
    lock = Lock()
    seen_families = set()
    seen_people = set()

    q.put(family_id)

    def worker():
        while True:
            fam_id = q.get()

            if fam_id is None:
                q.task_done()
                break

            with lock:
                if fam_id in seen_families:
                    q.task_done()
                    continue
                seen_families.add(fam_id)

            family_data = get_data_from_server(f'{TOP_API_URL}/family/{fam_id}')
            if not family_data:
                q.task_done()
                continue

            family = Family(family_data)
            with lock:
                tree.add_family(family)

            # Fetch and add family members
            people_to_fetch = [
                (family.get_husband(), True),
                (family.get_wife(), True)
            ]
            for child_id in family.get_children():
                people_to_fetch.append((child_id, False))

            for person_id, add_parents in people_to_fetch:
                if person_id is None:
                    continue

                with lock:
                    if person_id in seen_people:
                        continue
                    seen_people.add(person_id)

                person_data = get_data_from_server(f'{TOP_API_URL}/person/{person_id}')
                if not person_data:
                    continue

                person = Person(person_data)
                with lock:
                    tree.add_person(person)

                if add_parents:
                    parent_fam_id = person.get_parentid()
                    if parent_fam_id:
                        with lock:
                            if parent_fam_id not in seen_families:
                                q.put(parent_fam_id)

            q.task_done()

    # Start exactly 5 persistent threads
    threads = []
    for _ in range(5):
        t = Thread(target=worker)
        t.start()
        threads.append(t)

    q.join()

    # Stop threads by sending sentinel values
    for _ in threads:
        q.put(None)
    for t in threads:
        t.join()
