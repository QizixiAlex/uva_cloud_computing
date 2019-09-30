from mrjob.job import MRJob
from mrjob.step import MRStep
import re

WORD_RE = re.compile(r"[\w']+")


class MR_Count_Freq(MRJob):

    def steps(self):
        return [
            MRStep(mapper_init=self.mapper_init_get_words,
                   mapper=self.mapper_get_words,
                   mapper_final=self.mapper_final_get_words,
                   combiner=self.combiner_count_words,
                   reducer=self.reducer_count_keys),
            MRStep(
                    mapper = self.mapper_sort_by_counts,
                    reducer=self.reducer_print_pairs)
        ]

    def mapper_init_get_words(self):
        self.words = {}

    def mapper_get_words(self, _, line):
        for word in WORD_RE.findall(line):
            word = re.sub(r'[^\w\s]','',word.lower())
            self.words.setdefault(word, 0)
            self.words[word] = self.words[word]+1

    def mapper_final_get_words(self):
        for word, val in self.words.items():
            yield word, val

    def combiner_count_words(self, word, counts):
        yield word, sum(counts)

    def reducer_count_keys(self, word, count):
        yield word, sum(count)

    def mapper_sort_by_counts(self, word, count):
        yield '%04d' % int(count), word

    def reducer_print_pairs(self, count, words):
        for word in words:
            yield count, word

if __name__ == '__main__':
    MR_Count_Freq.run()