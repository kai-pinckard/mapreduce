#include <iostream>
#include <thread>
#include <vector>
#include <queue>
#include <utility>
#include <algorithm>
#include <mutex>
#include "mapreduce.hh"
#include <cassert>

namespace MapReduce{
// C++ redefinition of mapreduce.h

using list_t = std::vector<std::pair<std::string, std::string>>;
using pair_t = std::pair<int, list_t>;
using results_t = std::vector<pair_t>;
using part_t = int;

// global variable.

// want a datastructure with n partitions where each partition can
// have a number of different values listed. all of the same key will be stored in the same
// partition. the datastructure needs to remember the last accessed address for each partition. 
results_t mapped_results;
std::queue<char*> files_queue;
std::mutex queue_mutex;

void Map(const char *file_name) {
    FILE *fp = fopen(file_name, "r");
    assert(fp != NULL);

    char *line = NULL;
    size_t size = 0;
    while (getline(&line, &size, fp) != -1) {
        char *token, *dummy = line;
        while ((token = strsep(&dummy, " \t\n\r")) != NULL) {
            MR_Emit(token, "1");
        }
    }
    free(line);
    fclose(fp);
}

void Reduce(const std::string& key, getter_t get_next, int partition_number) {
    int count = 0;
    std::string value;
    while ((value = get_next(key, partition_number)) != "")
        count++;
    std::cout << key << " " << count << "\n";
}

/*
This function needs to take key value pairs from many different 
mappers and store them in a way that later reducers can access them.
Designing and implementing this data structure is thus a central
challenge of the project. 

After the mappers are finished your library should have stored the key value pairs in such
a way that the reduce function can be called. Reduce is invoked once per key and is passed the 
key along with a function that enables iteration over all the values that produced that same key.
to iterate the code just calls get next repeatedly until a null value is returuned. Get next
returns a pointer to the value passed in by the mr emit function above or null when the key's
values have been processed. 

responsible for storing the key and value in shared datastructure
hash map to arrays.
*/
void MR_Emit(const std::string& key, const std::string& value)
{
    int index = MR_DefaultHashPartition(key, mapped_results.size());
    std::pair<std::string, std::string> pair(key, value);
    mapped_results[index].second.push_back(pair);
}


std::string getter(const std::string& key, int partition_number)
{
    //int index = MR_DefaultHashPartition(key, mapped_results.size());
    int index = partition_number;
    list_t partition = mapped_results[index].second;

    // Find the next key instance starting from previous key instance
    for(int i = mapped_results[index].first; i < partition.size(); i++)
    {
        if(key.compare(partition[i].first) == 0)
        {
            mapped_results[index].first = i;
            return partition[i].second;
        }
    }

    // If there are no more occurances of the key to find
    mapped_results[index].first += 1;
    return "";
}

/*
This function is used by the map reduce library to decide which partition
and hence which reducer thread gets a particular key/list of values to process. For some applications which
reducer thread processes a particular key is not important and thus
the default function above should be passed in to mr_run.


For each partition keys and the value list associated with said keys should be sorted in ascending key order,
thus when a particular reducer thread and its associated partition are working the reduce function should be called on each key in order for that 
partition. 
*/
unsigned long MR_DefaultHashPartition(const std::string& key, int num_partitions)
{
    unsigned long hash = 5381;
    int c;
    for(int i = 0; i < key.length(); i++)
    {
        hash = hash * 33 + c;
    }
    return hash % num_partitions;
}

void mapper_pool(mapper_t map)
{
    std::string file_name; // possible error
    while(true)
    {
        {
            std::lock_guard<std::mutex> lock(queue_mutex);
            if(files_queue.empty())
            {
                return;
            }
            file_name = files_queue.front();
            files_queue.pop();
        }
        map(file_name.c_str());
    }
}

void reducer_manager(reducer_t reduce, int part_number)
{
    while(mapped_results[part_number].first < mapped_results[part_number].second.size())
    {
        std::string key = mapped_results[part_number].second[mapped_results[part_number].first].first;
        reduce(key, getter, part_number);
    }
}

/*
    This function takes the command line arguments of a given program, a pointer to a
    map function, the number of mapper threads your library should create,
    a pointer to a reduce function, the number of reducers, and finally a pointer to a partition function

    Thus, when a user is writing a MapReduce computation with your library, they will implement a Map function,
    implement a Reduce function, possibly implement a Partition function, and then call MR_Run(). The infrastructure
    will then create threads as appropriate and run the computation.

    One basic assumption is that the library will create num_mappers threads (in a thread pool) that perform the map tasks. 
    Another is that your library will create num_reducers threads to perform the reduction tasks. Finally, your library will
    create some kind of internal data structure to pass keys and values from mappers to reducers; more on this below.
*/
/* nt Num_Threads = thread::hardware_concurrency();
    vector<thread> Pool;
    for(int ii = 0; ii < Num_Threads; ii++)
    {  Pool.push_back(thread(Infinite_loop_function));} 
    */

void MR_Run(int argc, char* argv[], mapper_t map, int num_mappers, reducer_t reduce, int num_reducers, partitioner_t partition)
{

    // Place the files into the queue
    for(int i = 0; i < argc; i++)
    {
        files_queue.push(argv[i]);
    }

    // Initialize one partition for each reducer
    for(int i = 0; i < num_reducers; i++)
    {
        list_t list;
        //list.push_back("hello there");
        pair_t pair(0, list);
        mapped_results.push_back(pair);
    }

    // start mappers
    std::vector<std::thread> mappers;
    for(int i = 0; i < num_mappers; i++)
    {
        mappers.push_back(std::thread(mapper_pool, map));
    }

    // wait for mappers to complete
    for(int i = 0; i < num_mappers; i++)
    {
        mappers[i].join();
    }

    // sort the partitions
    for(int i = 0; i < mapped_results.size(); i++)
    {
        std::sort(mapped_results[i].second.begin(), mapped_results[i].second.end());
    }

    // start reducers
    std::vector<std::thread> reducers;
    for(int i = 0; i < num_reducers; i++)
    {
        reducers.push_back(std::thread(reducer_manager, reduce, i));
    }

    // wait for reducers to complete
    for(int i = 0; i < num_reducers; i++)
    {
        reducers[i].join();
    }
}

int main(int argc, char* argv[])
{
    MR_Run(argc, argv, Map, 10, Reduce, 10, MR_DefaultHashPartition);
    return 0;
}

}