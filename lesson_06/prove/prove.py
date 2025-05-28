"""
Course: CSE 351
Assignment: 06
Author: Ash Jones

Instructions:

- see instructions in the assignment description in Canvas

""" 

import multiprocessing as mp
import os
import cv2
import numpy as np

from cse351 import *

# Folders
INPUT_FOLDER = "faces"
STEP1_OUTPUT_FOLDER = "step1_smoothed"
STEP2_OUTPUT_FOLDER = "step2_grayscale"
STEP3_OUTPUT_FOLDER = "step3_edges"

# Parameters for image processing
GAUSSIAN_BLUR_KERNEL_SIZE = (5, 5)
CANNY_THRESHOLD1 = 75
CANNY_THRESHOLD2 = 155

# Allowed image extensions
ALLOWED_EXTENSIONS = ['.jpg']

NUM_PROCESSES_PER_STAGE = 10

# ---------------------------------------------------------------------------
def create_folder_if_not_exists(folder_path):
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)
        print(f"Created folder: {folder_path}")

# ---------------------------------------------------------------------------
def task_convert_to_grayscale(image):
    if len(image.shape) == 2 or (len(image.shape) == 3 and image.shape[2] == 1):
        return image # Already grayscale
    return cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)

# ---------------------------------------------------------------------------
def task_smooth_image(image, kernel_size):
    return cv2.GaussianBlur(image, kernel_size, 0)

# ---------------------------------------------------------------------------
def task_detect_edges(image, threshold1, threshold2):
    if len(image.shape) == 3 and image.shape[2] == 3:
        print("Warning: Applying Canny to a 3-channel image. Converting to grayscale first for Canny.")
        image = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
    elif len(image.shape) == 3 and image.shape[2] != 1 : # Should not happen with typical images
        print(f"Warning: Input image for Canny has an unexpected number of channels: {image.shape[2]}")
        return image # Or raise error
    return cv2.Canny(image, threshold1, threshold2)

# ---------------------------------------------------------------------------
def worker_smooth(q_in, q_out):
    while True:
        item = q_in.get()
        if item is None:
            q_out.put(None)
            break
        filename, image = item
        smoothed = task_smooth_image(image, GAUSSIAN_BLUR_KERNEL_SIZE)
        q_out.put((filename, smoothed))

# ---------------------------------------------------------------------------
def worker_grayscale(q_in, q_out):
    while True:
        item = q_in.get()
        if item is None:
            q_out.put(None)
            break
        filename, image = item
        gray = task_convert_to_grayscale(image)
        q_out.put((filename, gray))

# ---------------------------------------------------------------------------
def worker_edges(q_in, output_folder):
    while True:
        item = q_in.get()
        if item is None:
            break
        filename, image = item
        edges = task_detect_edges(image, CANNY_THRESHOLD1, CANNY_THRESHOLD2)
        output_path = os.path.join(output_folder, filename)
        cv2.imwrite(output_path, edges)

# ---------------------------------------------------------------------------
def run_image_processing_pipeline():
    print("Starting image processing pipeline...")

    create_folder_if_not_exists(STEP1_OUTPUT_FOLDER)
    create_folder_if_not_exists(STEP2_OUTPUT_FOLDER)
    create_folder_if_not_exists(STEP3_OUTPUT_FOLDER)

    # Queues
    que1 = mp.Queue()
    que2 = mp.Queue()
    que3 = mp.Queue()

    # --- Spawn smoothers ---
    smoothers = []
    for _ in range(NUM_PROCESSES_PER_STAGE):
        p = mp.Process(target=worker_smooth, args=(que1, que2))
        p.start()
        smoothers.append(p)

    # --- Spawn grayscale converters ---
    grays = []
    for _ in range(NUM_PROCESSES_PER_STAGE):
        p = mp.Process(target=worker_grayscale, args=(que2, que3))
        p.start()
        grays.append(p)

    # --- Spawn edge detectors ---
    edge_workers = []
    for _ in range(NUM_PROCESSES_PER_STAGE):
        p = mp.Process(target=worker_edges, args=(que3, STEP3_OUTPUT_FOLDER))
        p.start()
        edge_workers.append(p)

   # --- Main thread feeds filenames into queue 1 ---
    image_count = 0
    for filename in os.listdir(INPUT_FOLDER):
        ext = os.path.splitext(filename)[1].lower()
        if ext in ALLOWED_EXTENSIONS:
            path = os.path.join(INPUT_FOLDER, filename)
            img = cv2.imread(path)
            if img is not None:
                que1.put((filename, img))
                image_count += 1
            else:
                print(f"Could not read {filename}")

    # --- Send sentinel to smoothers and wait for barrier ---
    for _ in smoothers:
        que1.put(None)

    # --- Send sentinel to grays and wait for barrier ---
    for _ in grays:
        que2.put(None)

    # --- Send sentinel to edge detectors and wait for barrier ---
    for _ in edge_workers:
        que3.put(None)

    # Wait for all workers to fully finish
    for p in smoothers:
        p.join()
    for p in grays:
        p.join()
    for p in edge_workers:
        p.join()

    print("\nImage processing pipeline finished!")
    print(f"Original images are in: '{INPUT_FOLDER}'")
    print(f"Grayscale images are in: '{STEP1_OUTPUT_FOLDER}'")
    print(f"Smoothed images are in: '{STEP2_OUTPUT_FOLDER}'")
    print(f"Edge images are in: '{STEP3_OUTPUT_FOLDER}'")


# ---------------------------------------------------------------------------
if __name__ == "__main__":
    log = Log(show_terminal=True)
    log.start_timer('Processing Images')

    # check for input folder
    if not os.path.isdir(INPUT_FOLDER):
        print(f"Error: The input folder '{INPUT_FOLDER}' was not found.")
        print(f"Create it and place your face images inside it.")
    else:
        run_image_processing_pipeline()

    log.write()
    log.stop_timer('Total Time To complete')