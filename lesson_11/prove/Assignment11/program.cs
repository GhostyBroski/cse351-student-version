using System.Collections.Concurrent;
using System.Diagnostics;

namespace assignment11;

public class Assignment11
{
    private const long START_NUMBER = 10_000_000_000;
    private const int RANGE_COUNT = 1_000_000;
    private const int THREAD_COUNT = 10;

    private static readonly ConcurrentQueue<long> numberQueue = new();
    private static int primeCount = 0;
    private static int numbersProcessed = 0;

    private static readonly object consoleLock = new();

    private static bool IsPrime(long n)
    {
        if (n <= 3) return n > 1;
        if (n % 2 == 0 || n % 3 == 0) return false;

        for (long i = 5; i * i <= n; i += 6)
        {
            if (n % i == 0 || n % (i + 2) == 0)
                return false;
        }
        return true;
    }

    private static void Worker()
    {
        while (numberQueue.TryDequeue(out long number))
        {
            Interlocked.Increment(ref numbersProcessed);

            if (IsPrime(number))
            {
                Interlocked.Increment(ref primeCount);

                lock (consoleLock)
                {
                    Console.Write($"{number}, ");
                }
            }
        }
    }

    public static void Main()
    {
        Console.WriteLine("Prime numbers found:");

        var stopwatch = Stopwatch.StartNew();

        // Fill the queue with numbers
        for (long i = START_NUMBER; i < START_NUMBER + RANGE_COUNT; i++)
        {
            numberQueue.Enqueue(i);
        }

        // Start worker threads
        Thread[] workers = new Thread[THREAD_COUNT];
        for (int i = 0; i < THREAD_COUNT; i++)
        {
            workers[i] = new Thread(Worker);
            workers[i].Start();
        }

        // Wait for all threads to finish
        foreach (var thread in workers)
        {
            thread.Join();
        }

        stopwatch.Stop();

        Console.WriteLine(); // new line after primes
        Console.WriteLine();
        Console.WriteLine($"Numbers processed = {numbersProcessed}");
        Console.WriteLine($"Primes found      = {primeCount}");
        Console.WriteLine($"Total time        = {stopwatch.Elapsed}");
    }
}