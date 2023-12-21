from mrjob.job import MRJob
import json
import ast


class PowerIteration(MRJob):
    """
    """

    def configure_args(self):
        """
        evaluate the arguments
        :return:
        """
        super(PowerIteration, self).configure_args()
        self.add_passthru_arg('--number_of_authors', type=str)
        self.add_passthru_arg('--leak', type=str)

    def mapper(self, _, line):
        """
        :param line: one line of the matrix or the vector, both in json format

        in case we read matrix files we get:
        {"source": "2 Collaboration", "degree": 6, "destinations": ["Bolmont J.", "Jacholkowska A.", "Atteia J. -L.", "Piron F.", "Bennett G. W.", "Batley J. R."]}	"g"
        and the "g" identifies the line as a graph line

        in case we read vector files we get:
        "Collaboration"	{"r_new": 0.00000000000000000000}
        collaboration is the author and the r_new is the rank value

        :return: yields the author as key and either the degree and the destinations or the old rank value as value
        so that all values for one author(source) are in one place
        """

        # read the file
        data = str.split(line, sep='\t')
        # convert the graph data
        if data[1] == '"g"':
            source = json.loads(data[0])['source']
            degree = json.loads(data[0])['degree']
            destinations = json.loads(data[0])['destinations']

            # yield in json format
            out_data = {'degree': degree, 'destinations': destinations}
            yield source, out_data

        # convert the vector data
        else:
            source, r_old_str = str.split(line, sep='\t')
            r_old = json.loads(r_old_str)['r_new']
            out_data = {'r_old': r_old}
            yield source, out_data

    def combiner(self, key, values):
        """
        :param key: name of the author
        :param values: either the degree and the destinations or the old rank value as a dict
        :return: yields all parts of the sum for the power iteration
        TODO: somewhere between line 60 and 70 r_old is lost (it does not get yielded properly)
        """
        source = key
        degree = 0
        destinations = []
        r_old = 1 / int(self.options.number_of_authors) # 1/n
        for value in list(values):
            if 'r_old' in value:
                r_old = value['r_old']
            else:
                degree = value['degree']
                destinations = value['destinations']

        for destination in destinations:
            r_new = (float(r_old) / degree) * 0.85
            yield destination, r_new

    def reducer(self, key, values):
        """
        :param key: author
        :param values: all results of the power iteration for this author (entry of the rank vector)
        :return: new rank vector entry
        """
        # calculate the new rank value for the author
        beta = 0.85
        r_new = 0
        for value in values:
            r_new += value
        r_new += (1 - beta) / int(self.options.number_of_authors)
        # add the leak/n
        r_new += float(self.options.leak) / int(self.options.number_of_authors)
        # yield the new rank value
        r_new_dict = {'r_new': r_new}
        yield key, r_new_dict
