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
#include <memory>

std::mutex output;

/*
    This function maps each line in an input file that contains "the" to a 
    key value pair where the key is the line and the value is 1.
*/
void map_grep(const char *file_name) 
{
    std::string pattern = "the";
    std::string line = "";
    std::ifstream input_file;
    input_file.open(file_name);
    if(input_file.is_open())
    {
        while(std::getline(input_file, line))
        {
            if(line.find(pattern) != std::string::npos)
            {
                MapReduce::MR_Emit(line, "1");
            }
        }
        input_file.close();
    }
    else
    {
        std::cout << "Unable to open file: " << file_name << std::endl;;
    }
}

/*
    This function simply outputs each occurance of a line that contains "the"
    that was previously emitted by map_grep.
*/
void reduce_grep(const std::string& key, MapReduce::getter_t get_next, int partition_number) 
{
    std::string value;
    while ((value = get_next(key, partition_number)) != "")
    {
        {
            std::lock_guard<std::mutex> lock(output);
            std::cout << key << std::endl;
        }
    }
}

int main(int argc, char* argv[])
{
    MapReduce::MR_Run(argc, argv, map_grep, 10, reduce_grep, 10, MapReduce::MR_DefaultHashPartition);
    return 0;
}