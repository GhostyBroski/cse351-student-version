using System.Collections.Concurrent;
using Newtonsoft.Json.Linq;

namespace Assignment14;

public static class Solve
{

    static readonly SemaphoreSlim fetchLimiter = new(50); // limit to 50 parallel fetches
    static readonly object treeLock = new(); // protect tree from concurrent writes

    private static readonly HttpClient HttpClient = new()
    {
        Timeout = TimeSpan.FromSeconds(180)
    };
    public const string TopApiUrl = "http://127.0.0.1:8123";

    // This function retrieves JSON from the server
    public static async Task<JObject?> GetDataFromServerAsync(string url)
    {
        try
        {
            var jsonString = await HttpClient.GetStringAsync(url);
            return JObject.Parse(jsonString);
        }
        catch (HttpRequestException e)
        {
            Console.WriteLine($"Error fetching data from {url}: {e.Message}");
            return null;
        }
    }

    // This function takes in a person ID and retrieves a Person object
    // Hint: It can be used in a "new List<Task<Person?>>()" list
    private static async Task<Person?> FetchPersonAsync(long personId)
    {
        var personJson = await Solve.GetDataFromServerAsync($"{Solve.TopApiUrl}/person/{personId}");
        return personJson != null ? Person.FromJson(personJson.ToString()) : null;
    }

    // This function takes in a family ID and retrieves a Family object
    // Hint: It can be used in a "new List<Task<Family?>>()" list
    private static async Task<Family?> FetchFamilyAsync(long familyId)
    {
        var familyJson = await Solve.GetDataFromServerAsync($"{Solve.TopApiUrl}/family/{familyId}");
        return familyJson != null ? Family.FromJson(familyJson.ToString()) : null;
    }

    private static async Task<Person?> SafeFetchPerson(long id)
    {
        await fetchLimiter.WaitAsync();
        try
        {
            return await FetchPersonAsync(id);
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error fetching person {id}: {ex.Message}");
            return null;
        }
        finally
        {
            fetchLimiter.Release();
        }
    }

    private static async Task<Family?> SafeFetchFamily(long id)
    {
        await fetchLimiter.WaitAsync();
        try
        {
            return await FetchFamilyAsync(id);
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error fetching family {id}: {ex.Message}");
            return null;
        }
        finally
        {
            fetchLimiter.Release();
        }
    }


    // =======================================================================================================
    public static async Task<bool> DepthFS(long familyId, Tree tree)
    {
        // Note: invalid IDs are zero not null

        if (familyId == 0 || tree.GetFamily(familyId) != null)
            return true;

        var family = await SafeFetchFamily(familyId);
        if (family == null) return false;

        lock (treeLock)
        {
            tree.AddFamily(family);
        }

        var tasks = new List<Task>();

        async Task HandlePerson(long id, bool addParent)
        {
            if (id == 0 || tree.GetPerson(id) != null) return;
            var person = await SafeFetchPerson(id);
            if (person == null) return;

            lock (treeLock)
            {
                tree.AddPerson(person);
            }

            if (addParent)
                await DepthFS(person.ParentId, tree);
        }

        tasks.Add(HandlePerson(family.HusbandId, true));
        tasks.Add(HandlePerson(family.WifeId, true));

        foreach (var childId in family.Children)
        {
            tasks.Add(HandlePerson(childId, false));
        }

        await Task.WhenAll(tasks);
        return true;
    }


    // =======================================================================================================
    public static async Task<bool> BreathFS(long famid, Tree tree)
    {
        var seenFams = new ConcurrentDictionary<long, byte>();
        var seenPeople = new ConcurrentDictionary<long, byte>();
        var q = new ConcurrentQueue<long>();
        q.Enqueue(famid);

        var tasks = new List<Task>();
        var sem = new SemaphoreSlim(50);

        async Task ProcessFamily(long famId)
        {
            if (!seenFams.TryAdd(famId, 0)) return;

            var family = await SafeFetchFamily(famId);
            if (family == null) return;

            lock (treeLock)
            {
                tree.AddFamily(family);
            }

            async Task HandlePerson(long id, bool addParent)
            {
                if (id == 0 || !seenPeople.TryAdd(id, 0)) return;

                var person = await SafeFetchPerson(id);
                if (person == null) return;

                lock (treeLock)
                {
                    tree.AddPerson(person);
                }

                if (addParent && person.ParentId != 0)
                    q.Enqueue(person.ParentId);
            }

            tasks.Add(HandlePerson(family.HusbandId, true));
            tasks.Add(HandlePerson(family.WifeId, true));
            foreach (var childId in family.Children)
                tasks.Add(HandlePerson(childId, false));
        }

        while (!q.IsEmpty || tasks.Count > 0)
        {
            while (q.TryDequeue(out var famId))
            {
                await sem.WaitAsync();
                var task = ProcessFamily(famId).ContinueWith(t => sem.Release());
                tasks.Add(task);
            }

            tasks.RemoveAll(t => t.IsCompleted);
            await Task.Delay(10);
        }

        await Task.WhenAll(tasks);
        return true;
    }
}