import os

"""
This file compiles and runs and tests two different programs using my mapreduce implementation.
"""


if __name__ == "__main__":
    print("Compiling map reduce grep test")
    os.system("g++ -Wall -Werror -Wextra -pedantic mapreduce.cc test_grep.cc -o mr_grep -lpthread")
    print("Running map reduce grep test")
    os.system("./mr_grep grep1.txt grep2.txt > grepout.txt")

    """
        test that map reduce grep is outputing all lines that contain "the" in
        the input files and no others.
    """

    with open("grep1.txt", "r") as f:
        input1 = f.readlines()
    with open("grep2.txt", "r") as f:
        input2 = f.readlines()

    # concat lists of input lines
    input_lines = input1 + input2
    with open("grepout.txt", "r") as f:
        output_lines = f.readlines()
    
    expected_lines = []
    for input_line in input_lines:
        if "the" in input_line:
            expected_lines.append(input_line)
    passed_test = True
    for expected_line in expected_lines:
        if not expected_line in output_lines:
            print("Grep Test failed: missing line", expected_line)
            passed_test = False
    for output_line in output_lines:
        if not "the" in output_line:
            print("Grep Test failed:", output_line, "does not contain \"the\"")
            passed_test = False

    if passed_test:
        print("Passed Grep Test")

    """
    test that map reduce line count is correctly counting the number of lines
    """

    print("Compiling map reduce line count test")
    os.system("g++ -Wall -Werror -Wextra -pedantic mapreduce.cc test_line_count.cc -o mr_line_count -lpthread")
    print("Running map reduce line count test")
    os.system("./mr_line_count line_count1.txt line_count2.txt > lineout.txt")

    with open("line_count1.txt", "r") as f:
        input1 = f.readlines()
    with open("line_count2.txt", "r") as f:
        input2 = f.readlines()
    # concat lists of input lines
    input_lines = input1 + input2

    line_count_dict = {}
    for input_line in input_lines:
        line_count_dict[input_line.strip()] = 0

    for input_line in input_lines:
        line_count_dict[input_line.strip()] += 1

    with open("lineout.txt", "r") as f:
        output_lines = f.readlines()

    output_dict = {}
    for output_line in output_lines:
        line, count = output_line.split()
        output_dict[line.strip()] = int(count)

    passed_test = True
    for key in output_dict.keys():
        if line_count_dict[key] != output_dict[key]:
            print(line_count_dict[key], output_dict[key])
            print("Line Count Test Failed:", "incorrect count for key", key)
            passed_test = False
    for key in line_count_dict.keys():
        if line_count_dict[key] != output_dict[key]:
            print("Line Count Test Failed:", "incorrect count for key", key)
            passed_test = False
    if passed_test:
        print("Passed Line Count Test")
        
        



