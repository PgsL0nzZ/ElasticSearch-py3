from elasticsearch import Elasticsearch
from elasticsearch import ElasticsearchException
import cx_Oracle
from elasticsearch import helpers
import time


class CreateIndex(object):
    def __init__(self):
        self.db = cx_Oracle.connect("dmatdmp/D_Matdmp#2018@192.168.160.235:1521/dmat")
        self.cursor = self.db.cursor()
        self.es = Elasticsearch("localhost:9200")  # 本地测试

    def create_index(self, index, doc_type):
        '''
        设计索引结构es_body, 根据结构创建index, 并判断是否创建成功
        :param index: 创建的索引名称
        :param doc_type: 创建的文档类型
        :return:
        '''
        es_body = {
            "settings": {
                "number_of_shards": 5,
                "number_of_replicas": 0,
                "analysis": {
                    "analyzer": {
                        "english_standard_analyzer": {
                            "type": "standard",  # 标准分词器
                            "stopwords": "_english_",  # 去除英文停止词
                            "tokenizer": "standard",  # 使用标准分词器(以非字符、下划线的字符进行分割)
                            "filter": ["lowercase"]  # 小写，可以在后面增加任何token filter
                        },
                        "english_comma_pat_analyzer": {
                            "type": "pattern",  # 模式分词器
                            "pattern": ",",  # 使用逗号分隔模式(英文逗号)
                            "stopwords": "_english_",  # 去除英文停止词
                            "lowercase": "true"  # 小写
                        }
                    }
                }
            },
            "mappings": {
                doc_type: {
                    "properties": {
                        "tbl_id": {
                            "type": "keyword"
                        },
                        "sys_id": {
                            "type": "keyword"
                        },
                        "sys_name": {
                            "type": "text",
                            "analyzer": "ik_max_word",
                            "fields": {
                                "raw": {
                                    "type": "keyword"
                                }
                            }
                        },
                        "owner": {
                            "type": "keyword"
                        },
                        "tbl_name": {
                            "analyzer": "standard",  # TODO: 测试
                            "type": "text",
                            "fields": {
                                "raw": {
                                    "type": "keyword"
                                }   
                            }
                        },
                        "col_names": {
                            "analyzer": "english_comma_pat_analyzer",
                            "type": "text"
                        },
                        "col_comments": {
                            "analyzer": "ik_smart",
                            "type": "text"
                        },
                        "sys_name_alias": {
                            "analyzer": "ik_max_word",
                            "type": "text"
                        }
                    }
                }
            }
        }
        if not self.es.indices.exists(index=index):
            try:
                self.es.indices.create(index=index, body=es_body)
                print("索引创建成功...")
                return True
            except Exception as e:
                print(e, "索引创建失败！")
                return False
        else:
            return False

    def _gen_data(self, index, doc_type, batch_chunk_size):
        '''
        生成数据的生成器
        :param index: 要插入数据的index
        :param doc_type: 索引index的文档类型
        :param chunk_size: 批量插入数据量的大小
        :return:
        '''
        sql = """select * from tem_search_engine_1 """  # TODO: 提取sql语句作为参数
        self.cursor.execute(sql)
        col_name_list = [col[0].lower() for col in self.cursor.description]
        col_name_len = len(col_name_list)
        actions = []

        start = time.time()
        for row in self.cursor:
            source = {}
            tbl_id = ""
            for i in range(col_name_len):
                source.update({col_name_list[i]: str(row[i])})
                if col_name_list[i] == "tbl_id":
                    tbl_id = row[i]
            action = {
                "_index": index,
                "_type": doc_type,
                "_id": tbl_id,  # TODO:判空
                "_source": source
            }
            actions.append(action)
            if len(actions) == batch_chunk_size:
                print("actions增加数据用时：", time.time()-start)
                yield actions
                actions = []
        print("for总用时：", time.time()-start)
        yield actions

    def _gen_parallel_data(self, index, doc_type):
        sql = """select * from tem_search_engine_1"""  # TODO: 提取sql语句作为参数
        self.cursor.execute(sql)
        col_name_list = [col[0].lower() for col in self.cursor.description]
        col_name_len = len(col_name_list)
        for row in self.cursor:
            source = {}
            tbl_id = ""
            for i in range(col_name_len):
                source.update({col_name_list[i]: str(row[i])})
                if col_name_list[i] == "tbl_id":
                    tbl_id = row[i]
            action = {
                "_index": index,
                "_type": doc_type,
                "_id": tbl_id,  # TODO:判空
                "_source": source
            }
            yield action

    def bulk_data(self, index, doc_type, is_parallel=True, batch_chunk_size=5000, threads_counts=8):
        '''
        数据批量插入
        :param index: 要插入数据的index
        :param doc_type: index的文档类型
        :param chunk_size: 批量插入的大小，只用于非并行插入
        :param is_parallel: 是否要并行插入，默认为并行插入
        :param threads_counts: 线程数量，默认为4，只有在并行插入数据时该参数才有效
        :return:
        '''
        if is_parallel is None or is_parallel == True:
            gen_action = self._gen_parallel_data(index, doc_type)
            print("正在并行插入数据...")
            start = time.time()
            for success, info in helpers.parallel_bulk(client=self.es, actions=gen_action, thread_count=threads_counts, chunk_size=1000):
                if not success:
                    print("Insert failed: ", info)
            print("插入数据成功... ", time.time()-start)
        elif is_parallel == False:
            gen_action = self._gen_data(index, doc_type, batch_chunk_size)
            try:
                print("正在插入数据...")
                t3 = time.time()
                helpers.bulk(client=self.es, actions=gen_action, chunk_size=500)
                print("插入成功....", time.time() - t3)
            except  Exception as e:
                print(e, "插入失败！")
        else:
            raise ValueError("is_parallel应该为True或False")

    def exists_doc(self, index, doc_type, doc_id, source=False):
        '''
        确定索引中的一个文档是否存在
        :param index:
        :param doc_type:
        :param doc_id:
        :param source:
        :return:
        '''
        return self.es.exists(index=index, doc_type=doc_type, id=doc_id, _source=source)

    def get_doc(self, index, doc_type, id):
        '''

        :param index:
        :param doc_type:
        :param id:
        :return:
        '''
        return self.es.get(index=index, doc_type=doc_type, id=id)

    def get_docs(self, index, doc_type, body, source=False):
        '''

        ============================EXAMPLE===================================
        createindex = CreateIndex()
        body = {
             "docs": [
                {"_id": "7970C657B49BA14AE050A8C0EBA07C72"},
                {"_id": "7970C657B49EA14AE050A8C0EBA07C72"}
            ]
        }
        print(createindex.get_docs("example_index", "examplecase", body))

        body = {
        "ids": [ "7970C657B49BA14AE050A8C0EBA07C72" "7970C657B49EA14AE050A8C0EBA07C72"]
        }
        print(createindex.get_docs("example_index", "examplecase", body))
        ======================================================================
        :param index:
        :param doc_type:
        :param body: 根据"docs"或"ids"获取多条文档信息
        :param source: 是否返回展示原始数据，默认为False
        :return:
        '''
        return self.es.mget(index=index, doc_type=doc_type, body=body, _source=source)

    def update_doc(self, index, doc_type, id, body):
        '''

        :param index:
        :param doc_type:
        :param id:
        :param body:
        :return:
        '''
        self.es.update(index=index, doc_type=doc_type, id=id, body=body)

    def delete_index(self, index):
        '''

        :param index:
        :return:
        '''
        return self.es.indices.delete(index=index)

    def delete_docs(self, index, doc_type, doc_id):
        '''

        :param index:
        :param doc_type:
        :param doc_id:
        :return:
        '''
        return self.es.delete(index=index, doc_type=doc_type, id=doc_id)

    def delete_by_query(self, index, doc_type, body, source):
        '''
        使用删除语句对文档进行删除
        :param index:
        :param doc_type:
        :param body:
        :return:
        '''
        return self.es.delete_by_query(index=index, doc_type=doc_type, body=body, _source=source)

    def get_info(self, **kwargs):
        return self.es.info(**kwargs)


if __name__ == "__main__":
    createindex = CreateIndex()
#    createindex.delete_by_query(index="")
    createindex.create_index(index="se", doc_type="se_doc")
    createindex.bulk_data(index="se", doc_type="se_doc")
