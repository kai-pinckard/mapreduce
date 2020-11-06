#include <iostream>
#include <thread>
#include <vector>
#include <queue>
#include <utility>

// C++ redefinition of mapreduce.h

using list_t = std::vector<std::string>;
using pair_t = std::pair<int, list_t>;
using results_t = std::vector<pair_t>;

// global variable.

// want a datastructure with n partitions where each partition can
// have a number of different values listed. all of the same key will be stored in the same
// partition. the datastructure needs to remember the last accessed address for each partition. 
results_t mapped_results;

/* map is handed a file name to process and is expected to handle it in its entirety.

*/

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
/* void MR_Emit(const std::string& key, const std::string& value)
{
    mapped_results[key].push_back(value);
}


void getter(const std::string& key, int partition_number)
{
    return;
}
 */
/*
This function is used by the map reduce library to decide which partition
and hence which reducer thread gets a particular key/list of values to process. For some applications which
reducer thread processes a particular key is not important and thus
the default function above should be passed in to mr_run.


For each partition keys and the value list associated with said keys should be sorted in ascending key order,
thus when a particular reducer thread and its associated partition are working the reduce function should be called on each key in order for that 
partition. 
*/
/* unsigned long MR_DefaultHashPartition(const std::string& key, int num_partitions)
{
    unsigned long hash = 5381;
    int c;
    while ((c = *key++) != '\0')
        hash = hash * 33 + c;
    return hash % num_partitions;
}
 */
/* // Need to update this to be thread safe.
void mapper_pool(mapper_t map)
{
    while(!queue.empty())
    {
        char* file_name = queue.front();
        queue.pop_front();
        map(file_name);
    }
}

void reducer_pool(reducer_t reduce)
{
    while(!queue.empty())
    {
        char* file_name = queue.front();
        queue.pop_front();
        map(file_name);
    }
}

 */
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
/* 
void MR_Run(int argc, char* argv[], mapper_t map, int num_mappers, reducer_t reduce, int num_reducers, partitioner_t partition)
{
    results_t results(num_partitions);
    

    std::queue<char*> files;
    for(int i = 0; i < argc; i++)
    {
        queue.push_back(argv[i]);
    }
    // start mappers
    std::vector<std::thread> mappers;
    for(int i = 0; i < num_mappers; i++)
    {
        mappers.push_back(std::thread(map));
    }


    // start reducers
    std::vector<std::thread> reducers;
    for(int i = 0; i < num_reducers; i++)
    {
        reducers.push_back(std::thread(map));
    }


    return;
} */

void tester()
{
    std::cout << mapped_results[0].second[0];
}

void creater()
{
  for( int i =0; i < 4; i++)
    {
        list_t list;
        list.push_back("hello there");
        pair_t pair(0, list);
        mapped_results.push_back(pair);
    }
}

int main(int argc, char* argv[])
{
    creater();
    tester();   
    return 0;
}