/*
Project 3: MapReduce
Completed by: Kai Pinckard
*/

#include <iostream>
#include <thread>
#include <vector>
#include <queue>
#include <utility>
#include <algorithm>
#include <mutex>
#include "mapreduce.hh"
#include <cassert>
#include <fstream>
#include <filesystem>
#include <memory>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>

namespace MapReduce
{
    // A list containing key value pairs.
    using list_t = std::vector<std::pair<std::string, std::string>>;
    // A utility pair containing an index into a list of key value pairs and a mutex ptr to lock that list
    // This index is used by get_next to keep track of which values have already been returned.
    using util_t = std::pair<long unsigned int, std::mutex*>;
    // A pair containing a utility pair and its corresponding list of key value pairs
    using pair_t = std::pair<util_t, list_t>;
    // The datatype for the central data structure: a list of key value pair lists each with its own utility pair.
    using results_t = std::vector<pair_t>;

    // The globally accessible central datastructure.
    results_t mapped_results;

    // A queue containing all input files names that need to be mapped
    std::queue<char*> files_queue;
    std::mutex queue_mutex;

    // Global partition function
    partitioner_t partition_func = MR_DefaultHashPartition;

    /*
        This function takes key value pairs and stores them
        in the central datastructure mapped_results in a way that later reducers can access them.
    */
    void MR_Emit(const std::string& key, const std::string& value)
    {
        int index = partition_func(key, mapped_results.size());
        std::pair<std::string, std::string> pair(key, value);
        {
            // Acquire the lock for the specific list that is being accessed.
            std::lock_guard<std::mutex> lock(*(mapped_results[index].first.second));
            mapped_results[index].second.push_back(pair);
        }
    }


    /*
        This function is used by reducer functions to get the next instance of 
        a key value pair with a particular key.
    */
    std::string getter(const std::string& key, int part_number)
    {
        // Access the list of key value pairs corresponding to part_number
        list_t partition = mapped_results[part_number].second;

        // Get the next index to process from inside the current list's utility pair
        long unsigned int i = mapped_results[part_number].first.first;

        // Check if there are more key value pairs to process
        if(i < partition.size())
        {
            // If the key matches increment the stored index and return the value
            if(key.compare(partition[i].first) == 0)
            {

                mapped_results[part_number].first.first= i + 1;
                return partition[i].second; 
            }
        }
        return "";
    }

    /*
        This function is used by the map reduce library by default to decide which partition
        and hence which reducer thread gets a particular key/list of values to process.
    */
    unsigned long MR_DefaultHashPartition(const std::string& key, int num_partitions)
    {
        unsigned long hash = 5381;
        for(long unsigned int i = 0; i < key.length(); i++)
        {
            hash = hash * 33 + key[i];
        }
        return hash % num_partitions;
    }

    /*
        Each mapper thread calls this function and will continuely grab work off of the global file_queue
        until all of the files have been processed.
    */
    void mapper_pool(mapper_t map)
    {
        std::string file_name;
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

    /*
        This function will keep calling reduce with different keys until all the keys in the specified partition
        have been processed. 
    */
    void reducer_manager(reducer_t reduce, int part_number)
    {
        while(mapped_results[part_number].first.first < mapped_results[part_number].second.size())
        {
            int next_key_index = mapped_results[part_number].first.first;
            std::string key = mapped_results[part_number].second[next_key_index].first;
            reduce(key, getter, part_number);
        }
    }

    /*
        This function takes the command line arguments of a given program (which should be input filenames), a pointer to a
        map function, the number of mapper threads your library should create,
        a pointer to a reduce function, the number of reducers, and finally a pointer to a partition function

        The user of this function will need to implement a Map function,
        a Reduce function, possibly implement a Partition function, and then call MR_Run(). This function
        will then create threads as appropriate and run the computation.
    */
    void MR_Run(int argc, char* argv[], mapper_t map, int num_mappers, reducer_t reduce, int num_reducers, partitioner_t partition=MR_DefaultHashPartition)
    {
        // Update the global partition function
        partition_func = partition;

        // Create a list of all the file names and their sizes.
        std::pair<long int, char*> file_info;
        std::vector<std::pair<long int, char*>> file_list;
        struct stat statbuffer;
        for (int i = 1; i < argc; i++)
        {
            int fd = open(argv[i], O_RDONLY, S_IRUSR | S_IWUSR);
            if(fd == -1)
            {
                perror("Unable to open file");
                exit(1);
            }

            if(fstat(fd, &statbuffer) == -1)
            {
                perror("Unable to get stat info for input file");
                exit(1);
            }

            file_info.first = statbuffer.st_size;
            file_info.second = argv[i];
            file_list.push_back(file_info);
        }

        // Sort the file_list in descending order
        // Since reduce phase does not begin until all mappers
        // finish it is good to get started on larger map jobs early.
        sort(file_list.begin(), file_list.end(), std::greater<std::pair<long int, char*>>()); 
  
        // Place the files into the queue in descending order by file length
        for(unsigned long int i = 0; i < file_list.size(); i++)
        {
            files_queue.push(file_list[i].second);
        }
        
        // Initialize central datastructure
        for(int i = 0; i < num_reducers; i++)
        {
            list_t list;
            std::mutex* mtx_ptr = new std::mutex;
            util_t util(0, mtx_ptr);
            pair_t pair(util, list);
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
        for(long unsigned int i = 0; i < mapped_results.size(); i++)
        {
            std::sort(mapped_results[i].second.begin(), mapped_results[i].second.end());
            delete mapped_results[i].first.second;
        }

        // start reducers
        // Since only one reducer accesses each partition no locking is needed
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
}


