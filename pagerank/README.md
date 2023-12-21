This Pagerank implementation has 4 files:
- `pagerank.py`: the main file that contains the implementation of the algorithm
- `preprocessing.py`: contains the preprocessing functions
- `extract_graph.py`: contains the functions to extract the graph from the dataset
- `power_iteration.py`: contains the power iteration algorithm
- `leak_mr` : contains the map reduce implementation to calculate the leak (1 - sum of all ranks)

## How to run the code
To run the code, you need to call the pagerank.py file with no arguments
At the moment all the paths are hardcoded, so you need to change them in the code.
- call with python3 pagerank_script.py

## How the code is intendet to work.
The code is intended to work in the following way:
(At the moment with a small subset of the dataset - for local testing)
- The pagerank script will create the following directories:
- - `preprocess_path`: contains the dataset
- - `graph_path`: contains the graph
- - `vector_path`: contains the vector
- - `leak_path`: contains the leak
All these paths are used so we can run the code on a cluster. And don't have to merge all the output files.

- First the dataset is read by the preprocessing.py , this will yield the nuber of nodes and a list of all authors(Nodes)
- After this the dataset gets read by the extract_graph.py this will yield the Matrix like the lecture_6 slides (slide_23)
- After this the power_iteration.py will be called with the matrix and the number of nodes. This will yield the vector.
- - In the first iteration the vector will be initialized with 1/n

## Additional information
- If you want to extract the graph in revese order, you can use the `extract_graph_reverse.py` file by setting the `reverse` variable to `True` as a parameter in the `extract_graph` function.

## Additions to the report
- We added a map reduce implementation to calculate the leak (1 - sum of all ranks) which was needed in order to mitigate the dead ends in the dataset.
- In order to perform all calculation in a distributed manner we needed to change the way we handle the output files. In the first approach we merged all the parts
of the output files into one file which implied that the entire graph and vector needed to fit into main memory. Now we read the 
directories with all the files and use a combiner to properly match the values we need for our calculations.
- 