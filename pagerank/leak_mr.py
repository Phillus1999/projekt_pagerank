from mrjob import job
import json

class Leak(job.MRJob):
    """
    This will calculate the leak for the rank vector
    """

    def mapper(self, _, line):
        """
        :param line: reads the rank vector line by line
        :return: yields the author as key and the rank value as value
        """
        data, r_new_str = str.split(line, sep='\t')
        r_new = json.loads(r_new_str)['r_new']

        yield 1, float(r_new)

    def combiner(self, key, values):
        """
        reduce communication by summing up the rank values from each mapper
        :param key: constant key=1
        :param values: all rank values from one mapper
        :return: yields the sum of all rank values
        """
        values = list(values)
        r_new = 0
        for value in values:
            r_new += value
        yield 1, r_new

    def reducer(self, key, values):
        """
        :param key: constant key=1
        :param values: all rank values from each mapper
        :return: the leaked rank
        """
        values = list(values)
        r_new = 0
        for value in values:
            r_new += value

        # calculate the leak
        leak = 1 - r_new
        yield 'leak', leak

