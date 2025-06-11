"""
Course: CSE 351 
Assignment: 08 Prove Part 2
File:   prove_part_2.py
Author: <Add name here>

Purpose: Part 2 of assignment 8, finding the path to the end of a maze using recursion.

Instructions:
- Do not create classes for this assignment, just functions.
- Do not use any other Python modules other than the ones included.
- You MUST use recursive threading to find the end of the maze.
- Each thread MUST have a different color than the previous thread:
    - Use get_color() to get the color for each thread; you will eventually have duplicated colors.
    - Keep using the same color for each branch that a thread is exploring.
    - When you hit an intersection spin off new threads for each option and give them their own colors.

This code is not interested in tracking the path to the end position. Once you have completed this
program however, describe how you could alter the program to display the found path to the exit
position:

What would be your strategy?

My strategy would be to highlight the found path another color altogether, doing so if it is the end_path. This could be implemented by coloring for row,col in that path by using maze.move and coloring it before stopping that maze. This could be me using the path from the final thread back, highlighting as I go.

Why would it work?

This would work by being able to trace back my steps so that I can highlight a path back to the start, not spurring off to a side where a thread met an end.

"""

import math
import threading 
from screen import Screen
from maze import Maze
import sys
import cv2

# Include cse 351 files
from cse351 import *

SCREEN_SIZE = 700
COLOR = (0, 0, 255)
COLORS = (
    (0,0,255),
    (0,255,0),
    (255,0,0),
    (255,255,0),
    (0,255,255),
    (255,0,255),
    (128,0,0),
    (128,128,0),
    (0,128,0),
    (128,0,128),
    (0,128,128),
    (0,0,128),
    (72,61,139),
    (143,143,188),
    (226,138,43),
    (128,114,250)
)
SLOW_SPEED = 100
FAST_SPEED = 0

# Globals
current_color_index = 0
thread_count = 0
stop = False
speed = SLOW_SPEED
end_lock = threading.Lock()
end_path = None

def get_color():
    """ Returns a different color when called """
    global current_color_index
    if current_color_index >= len(COLORS):
        current_color_index = 0
    color = COLORS[current_color_index]
    current_color_index += 1
    return color


# TODO: Add any function(s) you need, if any, here.

def thread_dfs(maze, row, col, color, path):
    """ Recursive DFS: continues in the current thread until a fork. 
        At a fork, current thread takes one path and spawns threads for the rest. """
    global stop, thread_count, end_path

    while not stop and maze.can_move_here(row, col):
        maze.move(row, col, color)
        path.append((row, col))

        if maze.at_end(row, col):
            with end_lock:
                if not stop:
                    stop = True
                    end_path = list(path)
            return

        moves = maze.get_possible_moves(row, col)

        if len(moves) == 0:
            return  # Dead end

        elif len(moves) == 1:
            # Only one way to go, continue in same thread
            row, col = moves[0]
            continue

        else:
            # Fork: current thread takes the first path, spawn new threads for the rest
            threads = []
            for i, (next_row, next_col) in enumerate(moves):
                if i == 0:
                    row, col = next_row, next_col  # Continue in current thread
                else:
                    new_color = get_color()
                    new_path = list(path)
                    t = threading.Thread(target=thread_dfs, args=(maze, next_row, next_col, new_color, new_path))
                    t.start()
                    threads.append(t)
                    thread_count += 1
            # After forking, the current thread just continues (loop continues)

    return



def solve_find_end(maze):
    """ Finds the end position using threads. Nothing is returned. """
    global stop, end_path, thread_count
    stop = False
    end_path = None
    thread_count = 0

    start_row, start_col = maze.get_start_pos()
    color = get_color()
    main_thread = threading.Thread(target=thread_dfs, args=(maze, start_row, start_col, color, []))
    main_thread.start()
    thread_count += 1
    main_thread.join()





def find_end(log, filename, delay):
    """ Do not change this function """

    global thread_count
    global speed

    # create a Screen Object that will contain all of the drawing commands
    screen = Screen(SCREEN_SIZE, SCREEN_SIZE)
    screen.background((255, 255, 0))

    maze = Maze(screen, SCREEN_SIZE, SCREEN_SIZE, filename, delay=delay)

    solve_find_end(maze)

    log.write(f'Number of drawing commands = {screen.get_command_count()}')
    log.write(f'Number of threads created  = {thread_count}')

    done = False
    while not done:
        if screen.play_commands(speed): 
            key = cv2.waitKey(0)
            if key == ord('1'):
                speed = SLOW_SPEED
            elif key == ord('2'):
                speed = FAST_SPEED
            elif key == ord('q'):
                exit()
            elif key != ord('p'):
                done = True
        else:
            done = True


def find_ends(log):
    """ Do not change this function """

    files = (
        ('very-small.bmp', True),
        ('very-small-loops.bmp', True),
        ('small.bmp', True),
        ('small-loops.bmp', True),
        ('small-odd.bmp', True),
        ('small-open.bmp', False),
        ('large.bmp', False),
        ('large-loops.bmp', False),
        ('large-squares.bmp', False),
        ('large-open.bmp', False)
    )

    log.write('*' * 40)
    log.write('Part 2')
    for filename, delay in files:
        filename = f'./mazes/{filename}'
        log.write()
        log.write(f'File: {filename}')
        find_end(log, filename, delay)
    log.write('*' * 40)


def main():
    """ Do not change this function """
    sys.setrecursionlimit(5000)
    log = Log(show_terminal=True)
    find_ends(log)


if __name__ == "__main__":
    main()