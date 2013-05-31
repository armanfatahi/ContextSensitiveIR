from models import Collection,Query
from utilities import DB,GlobalVariables
import operator
import csv
class Display():
    @staticmethod
    def _print_in_file_from_database(file_name = "output"):
        output_file = open("D:\\Project\\output\\"+file_name+".csv",'w')
        db_query = """SELECT * FROM result order by query_id ASC, score DESC;"""
        result = DB._execute(db_query)
        for row in result:
            output_file.write("{0},{1},{2},{3}\n".format(row[0],row[1],row[2],row[3]))
        output_file.close()
    @staticmethod
    def _extract_percision_recall(table_name="output"):
        db_query = """
        CREATE TABLE IF NOT EXISTS `{0}_p_r` (
          `query_id` int(11) NOT NULL,
          `n` int(11) NOT NULL COMMENT 'Number of extracted results',
          `p` float NOT NULL COMMENT 'percision',
          `r` float NOT NULL COMMENT 'recall'
        ) ENGINE=MyISAM DEFAULT CHARSET=latin1;
        """.format(table_name)
        DB._query(db_query)
        db_query = """delete from {0}_p_r;""".format(table_name)
        DB._query(db_query)
        db_query = """SELECT count(*) FROM collection;"""
        result = DB._execute(db_query)
        total_document_number = result[0][0]
        for i in range(50):
            q = Query(100+i)
            for j in range(150):
                n = j+1
                percision = 0
                recall = 0
                db_query = """SELECT * FROM {2} where query_id = {0} and relevance = 1 limit {1};""".format(q.id,str(n),table_name)
                result = DB._execute(db_query)
                true_positive = len(result)
                true_positive *= 1.00
                db_query = """SELECT * FROM {2} where query_id = {0} limit {1};""".format(q.id,str(n),table_name)
                result = DB._execute(db_query)
                positive = len(result)               
                if positive > 0:
                    percision = true_positive/positive
                true = len(q.qrels)
                if true >0:
                    recall = true_positive/true
                print "N:",n,"Q:",q.id,"\trecall",recall,"\tpercision",percision,"\tpositive:", positive
                db_query = """Insert into {4}_p_r (query_id,n,p,r) values({0},{1},{2},{3});"""
                db_query = db_query.format(q.id,n,percision,recall,table_name)
                DB._query(db_query)
    @staticmethod
    def _combine_Percision_recal(table_name="output"):
        output_file = open("D:\\Project\\output\\{0}.csv".format(table_name),'w')
        db_query = """SELECT sum(p), sum(r) FROM {0}_p_r group by n;""".format(table_name)
        result = DB._execute(db_query)
        for row in result:
            output_file.write("{0},{1}\n".format(row[0],row[1]))
        output_file.close()
    @staticmethod
    def _plot(score,query):
        print"%%%%%%%%%\t Query number: ",query.id,"\t%%%%%%%%%%%%%%%%%%%%%%%%%"
        print "%%----------\t", len(query.qrels)," documents contain results"
        results = []
        for D in score.iterkeys():
            if score[D] > 45:
                results.append(D)
        true_count = 0
        for doc in results:
            for qrel in query.qrels:
                if qrel == doc:
                    true_count +=1  
        print "%%--------------\tPositive:\t",len(results),"\t-----------------"
        print "%%--------------\tTrue:    \t",true_count,"\t-----------------"
        false_count = len(results) - true_count
        print "%%--------------\tFalse:   \t", false_count,"\t-----------------"
        print"%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%"
    @staticmethod
    def _plot_dict(dic, limit = 0):
        print"%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%"
        if limit != 0:
            row_count = 0
            for item in dic.iterkeys():
                print "%%\t\t", dic[item], "\t",item
                row_count += 1
                if row_count > limit:
                    break
        elif limit == 0:
            for item in dic.iterkeys():
                print "%%\t\t", dic[item], "\t",item
    @staticmethod    
    def _plot_list(lst, limit = 0):
        print"%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%"
        if limit != 0:
            row_count = 0
            for item in lst:
                print "%%\t\t", item, "\t"
                row_count += 1
                if row_count > limit:
                    break
        elif limit == 0:
            for item in lst:
                print "%%\t\t", item, "\t"
    @staticmethod
    def _export_to_database(score_dict,query,table_name="output"):
        """
        file name: the name of output file,
        score_dict: dict("document_id":score)
        query is an instance of models.Query()
        """
        db_query = """
        CREATE TABLE IF NOT EXISTS `{0}` (
          `query_id` int(11) NOT NULL,
          `PMID` int(11) NOT NULL,
          `score` float NOT NULL,
          `relevance` int(11) NOT NULL
        ) ENGINE=InnoDB DEFAULT CHARSET=latin1;
        """.format(table_name)
        DB._query(db_query)
        db_query = "delete from {1} where query_id = {0};".format(query.id,table_name)
        DB._query(db_query)
        sorted_score = sorted(score_dict.iteritems(), key=operator.itemgetter(1))
        results = []
        for D in sorted_score:
            if sorted_score[1] > 0:
                results.append(D)
        row_number = len(results)
        for item in results:
            row_number -= 1
            db_query = "insert into {0}(query_id, PMID,score,relevance) values ".format(table_name)
            if item[0] in query.qrels:
                db_query +="({0},{1},{2},{3})".format(query.id,item[0],item[1],1)
            else:
                db_query +="({0},{1},{2},{3})".format(query.id,item[0],item[1],0)
            DB._query(db_query)
        
