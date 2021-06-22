import re

from nltk.corpus import reuters

from indexer.SPIMI_indexer  import SPIMI_invert
from collections import OrderedDict
import json

class Probabilistic_search_engine(object):

    def __init__(self):
        self._indexer = SPIMI_invert()
        self.urls_ary, self._content_dict = self.load_jason_to_dict()
        self._indexer.create_inverted_index(self._content_dict)
        self.term_docID_dict = self._indexer.completed_dict
        self.df_dict = self._indexer.idf_dict
        self.docLen_dict = self._indexer.docLen_dict
        self.filename_dict = self.load_reuters_filenames()

        self.k1 = 2.5
        self.b = 0.5

    def load_jason_to_dict(self):
        # with open('content.jason') as f:
        with open('content_20000_p_h4.json') as f:
            data = json.load(f)
        content_dict = {}
        url_ary = []
        for i, (url, content) in enumerate(data.items()):
            url_ary.append(url)
            content_dict[i] = content
            # if i>1000:
            #     break
        return url_ary, content_dict

    def load_reuters_filenames(self):
        filename_dict = {}
        with open("reuters_filenames.txt") as reuters_filenames:
            lines = reuters_filenames.readlines()
            for line in lines:
                id_type = line.strip().split("/")
                filename_dict[id_type[1]] = id_type[0]
        return filename_dict


    def get_query_terms_posting(self,query_terms):
        clean_query_terms = self._indexer.clean_query_token(query_terms)
        all_term_docIDs = []

        for q_term in clean_query_terms:
            docIDs = self.term_docID_dict.get(q_term)
            if docIDs is not None:
                all_term_docIDs.append(docIDs)
        return clean_query_terms,all_term_docIDs


    def intersection_docIDs(self,iters_for_each_docIDsList):
        result_ids = []
        result_tfs = []
        cur_docID_tf_list = [next(iter, None) for iter in iters_for_each_docIDsList]
        while (len(iters_for_each_docIDsList) > 0):
            # print(cur_docID_tf_list)
            all_are_same = True
            min_docID = cur_docID_tf_list[0][0]
            min_index = 0
            for index, docID_tf in enumerate(cur_docID_tf_list):
                docID = docID_tf[0]
                if docID != min_docID:
                    all_are_same = False
                    if docID < min_docID:
                        min_docID = docID
                        min_index = index
            if all_are_same == True:
                result_ids.append(min_docID)
                tfs = [docID_tf[1] for docID_tf in cur_docID_tf_list]
                result_tfs.append(tfs)

                cur_docID_tf_list = []
                for iter in iters_for_each_docIDsList:
                    docID = next(iter, None)
                    if docID is None:
                        return result_ids, result_tfs
                    else:
                        cur_docID_tf_list.append(docID)
            else:
                cur_docID_tf_list[min_index] = next(iters_for_each_docIDsList[min_index], None)
                if cur_docID_tf_list[min_index] is None:
                    return result_ids, result_tfs


    def union_docIDs(self, iters_for_each_docIDsList):
        result_ids = []
        result_tfs = []
        cur_docID_tf_list = [next(iter, None) for iter in iters_for_each_docIDsList]
        while (len(iters_for_each_docIDsList) > 0):
            # print(cur_docID_tf_list)
            min_docID = None
            for docID_tf in cur_docID_tf_list:
                if docID_tf is not None:
                    min_docID = docID_tf[0]
            if min_docID == None:
                return result_ids, result_tfs

            for index, docID_tf in enumerate(cur_docID_tf_list):
                if docID_tf is not None:
                    docID = docID_tf[0]
                    if docID != min_docID:
                        if docID < min_docID:
                            min_docID = docID

            # find the docID equal to min docID
            tfs = [docID_tf[1] if docID_tf is not None and docID_tf[0] == min_docID else 0 for docID_tf in cur_docID_tf_list ]

            result_ids.append(min_docID)
            result_tfs.append(tfs)

            for index,tf, cur_docID,iter in zip(range(len(tfs)),tfs,cur_docID_tf_list,iters_for_each_docIDsList):
                if cur_docID is not None and tf > 0:
                        cur_docID_tf_list[index] = next(iter, None)



    def queryOR(self,query_terms):
        print("===== OR ======\n")
        query_terms,all_term_docIDs = self.get_query_terms_posting(query_terms)
        iters_for_each_docIDsList = [iter(docIDs) for docIDs in all_term_docIDs]
        docIDs, tfs = self.union_docIDs(iters_for_each_docIDsList)

        print("==== query_terms ====\n")
        print(','.join(query_terms))
        # print(f"\n==== {len(docIDs)} Unranked Results (docIDs-tfs) ====")
        # docIDs_tfs = [f"{docID} {repr(tf)}" for docID,tf in zip(docIDs,tfs)]
        # cnt_print_lines = 10
        # if len(docIDs_tfs)<cnt_print_lines:
        #     cnt_print_lines = len(docIDs_tfs)
        # print('\n'.join(docIDs_tfs[:cnt_print_lines]))
        # if len(docIDs_tfs) > cnt_print_lines:
        #     print(" ... continue ...")

        scores1 = self.get_score_BM25(query_terms, docIDs, tfs)
        scores2 = self.get_score_tf_idf(query_terms, docIDs, tfs)
        return scores1,scores2



    def queryAND(self,query_terms):
        print("===== AND ======\n")
        print("==== query_terms ====\n")
        print(','.join(query_terms))

        query_terms,all_term_docIDs = self.get_query_terms_posting(query_terms)

        # one situation： no this query term in the dictionary, how to do ? just return None
        if len(all_term_docIDs) < len(query_terms):
            return None,None
        # intersection docIDs
        iters_for_each_docIDsList = [iter(docIDs) for docIDs in all_term_docIDs]

        docIDs, tfs = self.intersection_docIDs(iters_for_each_docIDsList)

        # print(f"\n==== {len(docIDs)} Unranked Results (docIDs-tfs) ==== ")
        # docIDs_tfs = [f"{docID} {repr(tf)}" for docID,tf in zip(docIDs,tfs)]
        # cnt_print_lines = 10
        # if len(docIDs_tfs) < cnt_print_lines:
        #     cnt_print_lines = len(docIDs_tfs)
        # print('\n'.join(docIDs_tfs[:cnt_print_lines]))
        # if len(docIDs_tfs) > cnt_print_lines:
        #     print(" ... continue ...")

        scores1 = self.get_score_BM25(query_terms, docIDs, tfs)
        scores2 = self.get_score_tf_idf(query_terms, docIDs, tfs)
        return scores1,scores2


    def get_score_BM25(self, query_terms, docIDs, tfs):
        scores = {}
        idfs = self._indexer.idf_dict
        for docID,tf_doc in zip(docIDs,tfs):
            score = 0
            Ld = self.docLen_dict.get(docID)
            Lave = self._indexer.Lave
            for term,tf in zip(query_terms,tf_doc):
                if tf > 0 :
                    idf = idfs.get(term)
                    if idf:
                        score = score + idf * ((self.k1+1)*tf/(self.k1*((1-self.b)+self.b*(Ld/Lave))+tf))
            scores[docID] = round(score,2)
        scores_descending = OrderedDict(sorted(scores.items(), key=lambda kv: kv[1],reverse=True))
        # print(f"\n===={len(docIDs)} Ranked Result : (docID-score, k1={self.k1}, b={self.b} )==== ")
        # cnt_print_lines = 15
        # if len(docIDs)<cnt_print_lines:
        #     cnt_print_lines = len(docIDs)
        # print('\n'.join([f" {docID} : {score}" for docID,score in scores_descending.items()][:cnt_print_lines]))

        return scores_descending

    def get_score_tf_idf(self,query_terms, docIDs, tfs):
        scores = {}
        idfs = self._indexer.idf_dict
        for docID,tf_doc in zip(docIDs,tfs):
            score = 0
            for term,tf in zip(query_terms,tf_doc):
                if tf > 0 :
                    idf = idfs.get(term)
                    if idf:
                        score = score + tf*idf
            scores[docID] = round(score,2)
        scores_descending = OrderedDict(sorted(scores.items(), key=lambda kv: kv[1],reverse=True))
        # print(f"\n===={len(docIDs)} Ranked Result : (docID-score, k1={self.k1}, b={self.b} )==== ")
        # cnt_print_lines = 15
        # if len(docIDs)<cnt_print_lines:
        #     cnt_print_lines = len(docIDs)
        # print('\n'.join([f" {docID} : {score}" for docID,score in scores_descending.items()][:cnt_print_lines]))

        return scores_descending



    def get_file(self,scores1):
        for index,(fileid,score) in enumerate(scores1.items()):
            file_type = self.filename_dict.get(str(fileid))
            file_name = '%s/%d' % (file_type, fileid)
            print(f"=== {file_name}, score : {score} ====")
            print(reuters.raw(file_name))
            if index > 2:
                break

    def print_best_result(self, scores):
        # print(f"\n=== {len(scores.items())} results ===")
        for cnt,score_pair in enumerate(scores.items()):
            if cnt < 15:
                docID, score = score_pair
                url = self.urls_ary[docID]
                print(f" {url}, \nscore : {score} \n")


def query_url(urls_ary):
    pattern1 = re.compile('https://www.concordia.ca/academics/graduate/calendar/current/.*')
    for url in urls_ary:
        if pattern1.match(url):
            print(url)

def result_present(search_engine,scores_BM25,scores_tf_idf):
    if scores_BM25:
        print("\n==== BM25 Ranking (Top 15)  ====")
        search_engine.print_best_result(scores_BM25)
    if scores_tf_idf:
        print("\n==== tf/idf Ranking (Top 15)  ====")
        search_engine.print_best_result(scores_tf_idf)
    else:
        print("\n No result for this query")


def input_query_test():
    search_engine = Probabilistic_search_engine()

    while True:
        input_query = input("Query: ")
        if input_query == '0':
            break
        else:
            input_query = input_query.split()
            print("\n==== Query ====")
            print(' '.join(input_query))
            print()
            scores1_BM25, scores1_tf_idf = search_engine.queryAND(input_query)
            result_present(search_engine,scores1_BM25, scores1_tf_idf)

            scores2_BM25, scores2_tf_idf = search_engine.queryOR(input_query)
            result_present(search_engine,scores2_BM25, scores2_tf_idf)


def query_test():
    search_engine = Probabilistic_search_engine()

    # query_url(search_engine.urls_ary)
    query1 = " researchers COVID 19".split()
    query2 = "environmental issues , sustainability, energy and water conservation".split()
    query_list = [query1,query2]

    for query_terms in query_list:
        print("\n==== Query Terms  ====")
        print(' '.join(query_terms))
        scores1_BM25,scores1_tf_idf = search_engine.queryAND(query_terms)
        if scores1_BM25:
            search_engine.print_best_result(scores1_BM25)
        if scores1_tf_idf:
            search_engine.print_best_result(scores1_BM25)
        else:
            print("no return for this query")

        scores2_BM25,scores2_tf_idf = search_engine.queryOR(query_terms)
        if scores2_BM25:
            search_engine.print_best_result(scores1_BM25)
        if scores2_tf_idf:
            search_engine.print_best_result(scores1_BM25)
        else:
            print("no return for this query")




input_query_test()


# print(reuters.raw('test/14826'))



