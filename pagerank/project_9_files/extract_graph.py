from mrjob.job import MRJob
import json


class ExtractGraph(MRJob):
    """
    with this job we will extract the graph from the arxiv file and store it in a sparse representation
    """
    def mapper(self, _, line):
        """
        :param line: reads the arxiv line by line
        :return: yields all edges in the for of (author1, author2) = source, target
        """
        author_list = json.loads(line)['authors_parsed']
        names = []

        # convert the name into a single string
        for author in author_list:
            names.append(str.strip(' '.join(author[0:-1])))

        # yield all edges
        if len(names) > 1:
            for i in range(len(names)):
                for j in range(i+1, len(names)):
                    yield names[i], names[j]

    def reducer(self, key, values):
        """
        :param key: source author
        :param values: all destinations
        :return: sparse representation of the graph in the form of:
        key=dict(source=source, degree=degree, destinations=values), value='g'
        """
        values = list(values)
        degree = len(values)
        output = {'source': key, 'degree': degree, 'destinations': values}
        yield output, 'g'

