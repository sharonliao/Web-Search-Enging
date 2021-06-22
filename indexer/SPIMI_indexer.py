import copy
import re
from nltk.stem.porter import *
import string
from nltk.corpus import reuters
from collections import OrderedDict
import math
import time
import nltk

class SPIMI_invert(object):
    def __init__(self):
        self._block_list = []
        self._cur_dictionanry = {}
        self.completed_dict = {}
        self._max_block_size = 1000000
        self.stemmer = PorterStemmer()
        self.stop_word = self.load_stopword()
        self.total_document_num = 0
        self.idf_dict = {}
        self.docLen_dict = {}
        self.Lave = 0

    def load_stopword(self):
        stopwords_list = []
        with open("stopwords.txt") as stopwords:
            lines = stopwords.readlines()
            for line in lines:
                stopword = line.strip()
                stopwords_list.append(stopword)
        return stopwords_list

    def build_term_tuples(self, token_stream):
        token_stream.sort()
        term_tuples = []
        cur_term = token_stream[0]
        tf = 1
        for token in token_stream[1:]:
            if token == cur_term:
                tf = tf +1
            else:
                term_tuples.append((cur_term,tf))
                cur_term = token
                tf = 1
        term_tuples.append((cur_term, tf))
        return term_tuples

    def clean_tokens(self, token_stream):
        #remove punctuation
        token_stream = [x for x in token_stream if x not in string.punctuation]
        # remove_cardinal
        token_stream = [x for x in token_stream if re.match('[0-9.,-]*$', x[0]) is None]
        # lowercase
        token_stream = [x.lower() for x in token_stream]
        # stemming
        token_stream = [self.stemmer.stem(x) for x in token_stream]
        # remove_stopwords
        token_stream = [x for x in token_stream if x not in self.stop_word]
        # token tuple (token, tf)
        term_tuples = self.build_term_tuples(token_stream)
        return term_tuples

    def clean_query_token(self,query_tokens):
        #remove punctuation
        token_stream = [x for x in query_tokens if x not in string.punctuation]
        # remove_cardinal
        token_stream = [x for x in token_stream if re.match('[0-9.,-]*$', x[0]) is None]
        # lowercase
        token_stream = [x.lower() for x in token_stream]
        # stemming
        token_stream = [self.stemmer.stem(x) for x in token_stream]
        # remove_stopwords
        token_stream = [x for x in token_stream if x not in self.stop_word]
        return token_stream

    def is_full_block(self):
        if len(self._cur_dictionanry) < self._max_block_size:
            return False
        else:
            return True

    def add_to_dicitonary(self, docID_tf_tuple, term):
        posting_list = [docID_tf_tuple]
        self._cur_dictionanry[term] = posting_list

    def add_to_postinglist(self, docID_tf_tuple, term):
        # print('add_to_postinglist tokens : {}'.format(term))
        self._cur_dictionanry.get(term).append(docID_tf_tuple)

    def get_input(self, docID, words):
        self.total_document_num = self.total_document_num + 1
        self.docLen_dict[docID] = len(words)
        self.Lave = sum(list(self.docLen_dict.values()))/self.total_document_num
        term_tf_tuples = self.clean_tokens(words)
        self.spimi_invert(docID, term_tf_tuples)


    def spimi_invert(self, docID, term_tf_tuples):
        for term,tf in term_tf_tuples:
            docID_tf_tuple = (docID,tf)
            if self.is_full_block() is False:
                if self._cur_dictionanry.get(term) is None:
                    self.add_to_dicitonary(docID_tf_tuple, term)
                else:
                    self.add_to_postinglist(docID_tf_tuple, term)
            else:
                self._cur_dictionanry = OrderedDict(sorted(self._cur_dictionanry.items()))
                self._block_list.append(copy.deepcopy(self._cur_dictionanry))
                self._cur_dictionanry = {}
                self.add_to_dicitonary(docID_tf_tuple, term)


    def end_input(self):
        self._cur_dictionanry = OrderedDict(sorted(self._cur_dictionanry.items()))
        self._block_list.append(copy.deepcopy(self._cur_dictionanry))
        self._cur_dictionanry = {}


    def get_min_term(self):
        head_terms = [list(block.keys())[0] for block in self._block_list]
        min_term = min(head_terms)
        # print('min value : {}'.format(min_term))
        block_indexs = [index for index, term in enumerate(head_terms) if term == min_term]

        posting = []
        for block_index in block_indexs:
            posting.extend(self._block_list[block_index].get(min_term))
            self._block_list[block_index].pop(min_term)

        # sorted by tf decreased
        posting.sort(key=lambda x: (x[1], x[0]),reverse=True)
        # print(posting)
        # limit length of posting list in 50
        posting = posting[:50]

        # 如果不改insert和union算法的话posting 必须 id 升序排序
        # posting.sort()

        # sorted by docID
        # posting.sort()

        del_count = 0
        for block_index in block_indexs:
            if len(self._block_list[block_index - del_count]) == 0:
                del self._block_list[block_index - del_count]
                del_count = del_count+1

        return min_term,posting

    def build_df_dict(self):
        N = self.total_document_num
        self.idf_dict = {term : math.log2(N / len(docIDs)) for term, docIDs in self.completed_dict.items()}

    def merge_blocks(self):
        print('length of self._block_list : {}'.format(len(self._block_list)))
        while len(self._block_list) > 1:
            min_value, posting = self.get_min_term()
            self.completed_dict[min_value] = posting

        if len(self._block_list) == 1:
            for id, posting in self._block_list[0].items():
                posting.sort(key=lambda x: (x[1], x[0]),reverse=True)
                # posting.sort()
                self.completed_dict[id] = posting

        # use completed_dict to build df dictionary
        self.build_df_dict()



    def create_inverted_index(self,content_dict):
        start = time.time()
        for id,content in content_dict.items():
            tokens = nltk.tokenize.word_tokenize(content)
            self.get_input(id, tokens)

        self.end_input()
        # example_block = self._block_list[1]
        # self.print_block(example_block)
        self.merge_blocks()
        # self.print_blocks_to_file()
        # self.print_block(self.completed_dict)


        end = time.time()
        building_time = end - start
        print(f"SPIMI index building time : {building_time}")



    def print_block(self,example_block):
        print(f"\nLength of this block : {len(example_block)}\n")
        for index,(term,posting) in enumerate(example_block.items()):
            if index>1500:
                print(f"{term} : {posting}")
            if index >1550:
                break

    def print_blocks_to_file(self):
        file_names = ['block/block-sample2.txt']
        blocks = [self._block_list[0]]
        for file_name,block in zip(file_names,blocks):
            f = open(file_name, "a")
            for cnt,(term,posting) in enumerate(block.items()):
                if cnt > 5000 and cnt < 5200:
                    f.write(f"{term} : {posting}\n")
                    # print(f"{term} : {posting}")
            f.close()











# indexer = SPIMI_invert()
# indexer.create_inverted_index()

