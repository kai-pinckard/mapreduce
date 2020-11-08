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
    This function maps each line in an input file to a key value pair
    where the key is the line and the value is 1.
*/
void map_line_count(const char* file_name)
{
    std::string line = "";
    std::ifstream input_file;
    input_file.open(file_name);
    if(input_file.is_open())
    {
        while(std::getline(input_file, line))
        {
            MapReduce::MR_Emit(line, "1");
        }
        input_file.close();
    }
    else
    {
        std::cout << "Unable to open file: " << file_name << std::endl;;
    }
}

/*
    This function counts the number of occurences of the specified key
    which is a line. The line and its number of occurances are then output into stdout.
*/
void reduce_line_count(const std::string& key, MapReduce::getter_t get_next, int partition_number) 
{
    std::string value;
    int sum = 0;
    while ((value = get_next(key, partition_number)) != "")
    {
        sum += 1;
    }
    {
        std::lock_guard<std::mutex> lock(output);
        std::cout << key << " " << sum << std::endl;
    }
}

int main(int argc, char* argv[])
{
    MapReduce::MR_Run(argc, argv, map_line_count, 10, reduce_line_count, 10, MapReduce::MR_DefaultHashPartition);
    return 0;
}