from preprocessing import Preprocessing
from extract_graph import ExtractGraph
from power_iteration import PowerIteration
from leak_mr import Leak
import time
import os
import ast


def preprocess(input_path, output_path):
    """
    calls the preprocessing job with the given input and output path
    :param input_path: path of the arxiv file
    :param output_path: path of the output folder
    :return:
    """
    start = time.time()
    print('Preprocessing...')
    mr_job = Preprocessing(args=[input_path, '--output', output_path])
    with mr_job.make_runner() as runner:
        runner.run()
    end = time.time()
    print('Preprocessing done.')
    print(f'Elapsed time: {end-start}')


def get_preprocess_results(filepath):
    """
    reads file of preprocessing job, clears the directory and returns the results
    :return: number of unique authors, list of all authors
    """
    with open(filepath, 'r') as f:
        for line in f.readlines():
            num_unique_authors, unique_authors_list = line.split('\t')
            unique_authors_list = ast.literal_eval(unique_authors_list)
            os.remove(filepath)
            return num_unique_authors, unique_authors_list


def extract_graph(input_path, output_path, reverse=False):
    """
    calls the extract_graph job with the given input and output path
    :param reverse: if true, the graph will be extracted in reverse order
    :param input_path: path of the arxiv file
    :param output_path: path of the output folder
    :return:
    """
    print('Extracting graph...')
    start = time.time()
    mr_job = ExtractGraph(args=[input_path, '--output', output_path])
    with mr_job.make_runner() as runner:
        runner.run()
    end = time.time()
    print('Extracting graph done.')
    print(f'Elapsed time: {end-start}')


def power_iteration(input_paths, output_path, number_of_authors, leak=0.0):
    """
    calls the power_iteration job with the given input and output path
    :param input_paths: paths of the graph and vector files
    :param output_path: path to safe the new vector
    :param number_of_authors: number of unique authors
    :param pagerank_vector: pagerank vector
    :param leak: leak value
    :return:
    """
    start = time.time()
    print('Power iteration...')
    mr_job = PowerIteration(args=[*input_paths,
                                  '--output', output_path,
                                  '--number_of_authors', str(number_of_authors),
                                  '--leak', str(leak)])
    with mr_job.make_runner() as runner:
        runner.run()
    end = time.time()
    print('Power iteration done.')
    print(f'Elapsed time: {end-start}')

def get_leak(input_path, output_path):
    """
    reads file of leak job, clears the directory and returns the results
    :return: leak value
    """
    # calculate leak
    mr_job = Leak(args=[input_path,
                        '--output', output_path])

    print('Calculating leak...')
    with mr_job.make_runner() as runner:
        runner.run()

    # read the file
    with open(os.path.join(output_path, 'part-00000'), 'r') as f:
        for line in f.readlines():
            leak = line.split('\t')[1]
            return leak


if __name__ == '__main__':
    # set input and output path
    preprocess_path = os.path.join(os.path.dirname(__file__), 'preprocess_path')
    graph_path = os.path.join(os.path.dirname(__file__), 'graph_path')
    vector_path = os.path.join(os.path.dirname(__file__), 'vector_path')
    arxiv_path = os.path.join(os.path.dirname(__file__), 'arxiv-metadata-oai-snapshot.json')
    test_path = os.path.join(os.path.dirname(__file__), 'data_10000.json')
    leak_path = os.path.join(os.path.dirname(__file__), 'leak_path')

    # setup environment
    os.makedirs(graph_path, exist_ok=True)
    os.makedirs(vector_path, exist_ok=True)
    os.makedirs(preprocess_path, exist_ok=True)
    os.makedirs(leak_path, exist_ok=True)

    # preprocess
    preprocess(test_path, preprocess_path)
    n, all_authors = get_preprocess_results(os.path.join(preprocess_path, 'part-00000'))
    print(f'Number of Authors: {n}')

    # generate graph
    extract_graph(test_path, graph_path)
    # generate graph in reverse order
    # extract_graph(test_path, graph_path, reverse=True)

    # power iteration start
    power_iteration([vector_path, graph_path], vector_path, n)

    # calculate pagerank
    for i in range(5):
        leak = get_leak(os.path.join(vector_path), leak_path)
        print(f'Leak: {leak}')
        power_iteration([vector_path, graph_path], vector_path, n, leak=float(leak))
        leak = 0

