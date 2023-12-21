from mrjob.job import MRJob
import json
import numpy as np


class Preprocessing(MRJob):
    """
    Preprocessing
    with this job we find the number of unique authors and list all of them by the end of the job
    """

    def mapper(self, _, line):
        """
        reads the arxiv line by line
        :param line:
        :return: yields constant key and the name of each author as value
        """
        author_list = json.loads(line)['authors_parsed']
        names = []

        # convert the name into a single string
        for author in author_list:
            names.append(str.strip(' '.join(author[0:-1])))

        # yield all names
        for name in names:
            yield 1, name

    def reducer(self, key, values):
        """
        :param key: constant key=1
        :param values: list of all the author's names
        :return: one key,value pair with: key=#of unique authors and value=list of all the author's names
        """
        values = list(values)
        unique_authors_list = np.unique(values)
        num_unique_authors = len(unique_authors_list)
        yield num_unique_authors, unique_authors_list.tolist()
