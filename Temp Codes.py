'''
Checking to see how many of the documents are in the collection.
'''

# Read the file which contains extracted Ids!
'''
from Utility import DB,TextProcessor
db = DB()
tp = TextProcessor()
f = open(r"D:\Project\TREC\2005\adhoc2005narrative.txt")
i = 0
db.Query("delete from adhoc2005narrative;")
for line in f:
    query = line
    query = query.replace("\n", '')
    query = query.replace("'", '')
    query = query.replace("\r", '')
    query = "INSERT INTO adhoc2005narrative(QId, Title) VALUES ({0},'{1}');".format(i,query)
    print query
    db.Query(query)
    i = i+1
'''
# Add the answers to other table
'''
from Utility import DB,TextProcessor
db = DB()
tp = TextProcessor()
f = open(r"D:\Project\TREC\2005\genomics.qrels.txt")
db.Query("delete from genomics_qrels_small;")
counter = 0
rowId = 0
for line in f:
    row = line.split("\t")
    if row[0] != rowId:
        counter =0
    counter += 1
    rowId = row[0]
    if counter < 16:
        print row
        query = "INSERT INTO genomics_qrels_small(QId, docId) VALUES ({0},'{1}');".format(row[0],row[2].replace(' ',''))
        print query
        db.Query(query)
'''

'''
f = open(r"d:\temp\out.txt",'w')
doc_results = open(r'D:\Project\TREC\temp\doc_id_list.txt')
id_list = doc_results.readlines()
id_collection = ''
for line in id_list:
    id_collection += line + ' '
f.write(id_collection)
f.close()
'''
'''
file_name = r'f:\project\ontology\mesh.xml'
file = open(file_name)

f = open(r'f:\project\ontology\mesh_small.xml','w')

for line in file:
    if line.find("DescriptorRecord") != -1:
        f.write(line)
    if line.find("DescriptorRecordSet") != -1:
        f.write(line)
    if line.find("DescriptorUI") != -1:
        if line.find('     ') == -1:
            f.write(line)
    if line.find("DescriptorName") != -1:
        if line.find('     ') == -1:
            f.write(line)
    if line.find("String") != -1:
        if line.find('     ') == -1:
            f.write(line)

'''       
''' 
       for d in C:
            #Mono-terminology extraction
            for t in T:
                #List of candidate concepts
                R(d,t) = extract(d,t)
R(d,t) = Union(1,n,R(d,t))
            #Document Expansion
            d_prime = expand(d,R(d,t))
            #Document indexing
            i = add_index(d_prime)
            
   def extract(self,d,t):
        return 0
    def expand(self, d, r):
        return 0
    def add_index(self, d):
        return 0
'''

'''
class Collection():
    def __init__(self):
        print 'I am a collection'

class Terminology():
    def __init__(self):
        print 'I am a terminology'
'''


    def StoreDocumentsInDatabase(self):
        db = DB()
        properties = ('AD  -','FAU -','AU  -','LA  -','PT  -','PL  -','TA  -','JID -','RN  -','SB  -','MH  -','SO  -','DCOM-','LR  -','IS  -','VI  -','IP  -','DP  -','PG  -')
        f = open(r'F:\Project\TREC\2004\2004_TREC_ASCII_MEDLINE_1')
        doc = 'First Row'
        PMID = 'First Row'
        f.readline()
        while True:
            line = f.readline()
            if line == '':
                break

            if line.find('PMID-') == -1:
                doc += line
            else:
                IsTheLineProperty = False
                for prop in properties:
                    if line.find(prop) != -1:
                        IsTheLineProperty = True
                if IsTheLineProperty != True:
                    doc = doc.replace("'", ' ')
                    doc = doc.replace('"', ' ')
                    query = r"insert into doc(PMID,content) values('" + PMID + "','" + doc + "');"
                    #print query
                    db.Query(query)
                    PMID = line
                    doc = ''
        if db.con:
            db.close()
            
 
        '''
        T_go_mmRows = db.Execute('select cid,word,sig from go_mm limit 1000;')
        Terminology = {}
        for row in T_go_mmRows:
            Terminology[row[0]] = row[1]
        print 'Hello How are u?'
        for doc in C:
            print doc'''
        '''
        GO = ''
        for doc in C:
            doc_list = doc[0].split(' ')
            for concept in Terminology.iteritems():
                for term in doc_list:
                    if term == concept[1]:
                        GO += term + ','
            print GO'''    
                    #result = [concept[1] for concept[1] in Terminology if concept[1] == term]
                    #if result != []:
                    #    GO += result[0]+','+str(len(result))+';'
                    #    query = "Insert into doc (GO) values ('" + GO + "')"    
        #db.Execute('Insert into doc(GO) values (')
        '''
        C is a collection of Documents
        T is a collection of Terminologies
        '''
        # Conceptual Document Indexing