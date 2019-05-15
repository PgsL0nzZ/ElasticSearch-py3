from elasticsearch import Elasticsearch
import pandas as pd

pd.set_option("display.max_columns", 500)

class SearchIndex(object):
    def __init__(self):
        self.es = Elasticsearch("localhost:9200")

    def search(self, index, field=None, query_string=None, size=20):
        '''
        数据查询函数，固定返回四个字段。在不提供field和query_string情况下，默认返回所有数据；
        在不提供field但提供query_string字段情况下，对所有字段进行匹配，在提供field和query_string
        的情况下，对该字段的内容进行匹配。
        :param index: 查询索引
        :param field: 查询字段
        :param query_string: 查询语句
        :return:
        '''
        res = None
        if field is None and query_string is None:
            body = {
                "query": {
                    "match_all": {}
                }
            }
            res = self.es.search(index=index, body=body, _source_include=['sys_name', 'sys_id', 'owner', 'tbl_id', 'tbl_name'],
                                 size=size)
        elif field is None and isinstance(query_string, str):
            body = {
                "query": {
                    "query_string": {
                        "fields": ['sys_name', 'sys_id', 'owner', 'tbl_id', 'tbl_name', 'col_names',
                                   'col_comments', 'sys_name_alias'],
                        "query": query_string
                    }
                }
            }
            res = self.es.search(index=index, body=body, _source_include=['sys_name', 'sys_id', 'owner', 'tbl_id', 'tbl_name'],
                                 size=size)
        elif isinstance(field, str) and isinstance(query_string, str):
            body = {
                "query": {
                    "query_string": {
                        "fields": [field],
                        "query": query_string
                    }
                }
            }
            res = self.es.search(index=index, body=body,  _source_include=['sys_name', 'sys_id', 'owner', 'tbl_id', 'tbl_name'],
                                 size=size)
        else:
            res = res

        return res

    def search_with_pattern(self, index, query_pat_str):
        body = {
            "query": {
                "query_string": {
                    "fields": ['texts', 'tbl_id'],
                    "query": query_pat_str,  # "7970C657B490A14AE050A8C0EBA*^2 OR "
                }
            }
        }
        res = self.es.search(index=index, body=body)
        return res

    def search_exect(self, index, field, query_string):
        body = {
            "query": {
                "term": {
                    field: query_string.lower()
                }
            }
        }
        res = self.es.search(index=index, body=body)
        return res


if __name__ == "__main__":
    searchindex = SearchIndex()
    res = searchindex.search("se", None, "营销分析与辅助决策")
    # res = searchindex.search("se", query_string="7970C657B490A14AE050A8C0EBA*^2")  # 增强查询
    print(res)
    for s in res['hits']['hits']:
        # print(pd.DataFrame([s['_source']]))
        print(s['_source'])
