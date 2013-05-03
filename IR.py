from Ontology import GO,MeSH,ICD10,SNOMED,Ontology
from Utility import DB
from Utility import TextProcessor,Operation,Parameter
from sets import Set
from math import log10,log
from nltk.tokenize import word_tokenize
class CSIR():
    db = DB()
    def __init__(self):
        print "Good Luck with your IR!"

    def PrepareTerminologies(self):
        print "Preparing terminologies..."
        g = GO() # Gene Ontology Extraction
        g.ExtractTerms()
        g.MaxMatcher('go')
        print "Gene Ontology is hot to go!"
        g = MeSH() # Medical Subjects Headings Extraction
        g.ExtractTerms()
        g.MaxMatcher('mesh')
        print "Medical Subject Headings is hot to go!"
        g = ICD10() # International Statistical Classification of Diseases Extraction
        g.ExtractTerms()
        g.MaxMatcher('icd10')
        print "International Statistical Classification of Diseases is hot to go!"
        g = SNOMED() # Systematized Nomenclature of Medicine Extraction
        g.ExtractTerms()
        g.MaxMatcher("snomed")
        print "Systematized Nomenclature of Medicine is hot to go!"
        # Store all concepts into MaxMatcher Table.
        g = Ontology()
        g.CreateTerminologies()
        g.CreateMaxMatcher()
        print "Preparing terminologies and MaxMatcher Done."
    def PrepareDocuments(self):
        print "Storing Documents in Database Started..."
        db = DB()
        param = Parameter()
        db.Query('delete from doc;') # Make sure the table is empty
        tp = TextProcessor()
        f = open(r'd:\Project\TREC\2004\2004_TREC_ASCII_MEDLINE_1')
        doc = ''
        PMID = f.readline()
        AB = ''
        ABBegins = False # Abstract Begins
        QueryList = ''   # To make queries faster, First we collect a banch of them
        QueryCount = 0
        QueryDone = 0
        QueryLimit = 49
        N = param.GetDocNumber()
        DocCount = 0
        while True:
            # Every Abstract begins with "AB  -" this is how we find them
            line = f.readline()
            if line.find('PMID-') == -1:
                doc += line
                if line.find('AB  -') != -1:
                    ABBegins = True
                    line = line.replace("AB  -",'    ')                    
                if line.find('    ')!= -1:
                    if ABBegins:
                        while line.find('  ') != -1:
                            line = line.replace('  ',' ')
                        line = line.replace('\r\n',' ')
                        line = line.replace('\r',' ')
                        line = line.replace('\n',' ')
                        AB += line
            else:
                doc = tp.CleanDoc(doc)
                AB = tp.RemoveStopWords(AB) #We store not original data.
                AB = tp.CleanDoc(AB) # to remove ' and  " so that SQL query has no problem
                PMID = tp.CleanPMID(PMID)
                query = r"('" + PMID + "','" + doc+ "','" + AB + "'),"
                if AB != '':
                    QueryList += query
                    QueryCount += 1
                    DocCount +=1
                    if QueryCount > QueryLimit:
                        QueryList = QueryList[:-1]
                        QueryList = 'Insert into doc(PMID,content,AB) values ' + QueryList
                        db.Query(QueryList)
                        QueryCount = 0
                        QueryList = ''
                        QueryDone += QueryLimit+1
                        print QueryDone," documents stored."
                    PMID = line
                    doc = ''
                    AB = ''
                    ABBegins = False
                if DocCount > N:
                    break
        QueryList = QueryList[:-1]
        QueryList = 'Insert into doc(PMID,content,AB) values ' + QueryList 
        db.Query(QueryList)
        print "Preparing Documents is Done!"
    def ConceptExtraction(self):
        db = DB()        
        param = Parameter()
        # Extract Docs from DB
        N = param.GetDocNumber()
        collection = db.Execute('select PMID, AB  from doc limit {0};'.format(N))
        print "len(collection) ",len(collection)
        terminology_list = ["go","mesh","icd10","snomed"]
        for terminology in terminology_list:
            MaxMatcher = dict()
            for doc in collection:                          # For every single document do the Indexing
                print doc
                print "len(MaxMatcher) is ",len(MaxMatcher)
                document = doc[1]                           # document is the abstract text.
                concepts = self.ExtractConceptsFromDoc(document,db,terminology,MaxMatcher)
                if len(concepts)>0:
                    concept_list = ','.join(concepts)
                    print concept_list
                    if terminology == 'go':
                        self.AddGeneOntologyConceptPredecessors(doc[0],concepts)
                    query = "UPDATE doc SET "+ terminology +"='"+concept_list+"' WHERE PMID='"+doc[0]+"';"
                    db.Query(query)
    def QueryConceptExtraction(self,query):
        db = DB()        
        param = Parameter()
        # Extract Docs from DB
        N = param.GetDocNumber()
        collection = db.Execute('select PMID, AB  from doc limit {0};'.format(N))
        print "len(collection) ",len(collection)
        terminology_list = ["go","mesh","icd10","snomed"]
        conceptList = []
        for terminology in terminology_list:
            MaxMatcher = dict()
            for doc in collection:                          # For every single document do the Indexing
                print doc
                print "len(MaxMatcher) is ",len(MaxMatcher)                         # document is the abstract text.
                concepts = self.ExtractConceptsFromDoc(query,db,terminology,MaxMatcher)
                for concept in concepts:
                    conceptList.append(concept)
        return ' , '.join(conceptList)
    def AddGeneOntologyConceptPredecessors(self,doc,conceptList):
        predecessors = []
        for concept in conceptList:
            predecessors.append(GO.GetPredecessors(concept))
        query = "UPDATE doc SET GOChildNodes ='{0}' WHERE PMID='{1}';".format(' , '.join(predecessors),doc[0])
        CSIR.db.Query(query)
        
    
    def DocumentExpantion(self):
        '''
        BM25TermWeightingModel
        BM25 or Best Match algorithm, calculates the weight of 
        each word in each extracted concept for the document 
        '''
        print "Calculating weights is started..."
        wieght_threshold = 0.10
        tp = TextProcessor()
        param = Parameter()
        ontology = Ontology()
        db = DB()
        N = param.GetDocNumber()              #Number of documents in the collection
        collection = db.Execute("select PMID, AB, GO, MeSH, ICD10, SNOMED from doc limit {0}".format(N))
        #Terminologies are ('go','mesh','icd10','snomed') corresponding with columns 2,3,4,5
        T = ontology.GetDict('T')    #bring all ontologies into the memory to be faster!
        doc_avg_len = 122
        k1 = 1.2 
        b = 1.00
        doc_counter = 0                        # tuning parameters!
        f = open(r"D:\Project\temp\output.txt",'w')
        for doc in collection:
            doc_counter += 1
            f.write("\r\n" + str(doc) + "\r\n")
            doc_len = len(doc)
            weight = dict()
            for i in range(2,6):
                Concepts = []
                if doc[i]:
                    Concepts = doc[i].split(tp.Splitter()) #Getting the list of Concepts for terminology T
                tp.remove_values_from_list(Concepts, '')
                tp.remove_values_from_list(Concepts, ' ')
                for C in Concepts:
                    # Extract concept variants for C
                    var = ' '
                    for variant in T[C]:
                        var += ' {0} '.format(variant)
                    terms = set( var.split(tp.WordSplitter()))
                    tp.remove_values_from_list(terms,'')
                    l = len(terms)    
                    sumation = 0  
                    for term in terms:
                        term_weight = 0
                        #calculate the weight
                        tf = self.TermFrequency(doc[1],term)                     
                        #Here goes calculating the weight
                        n_k = self.InverseDocumentFrequency(collection,term)
                        tf = self.TermFrequency(doc[1], term)
                        term_weight = tf * (( log10((N-n_k+0.50)/(n_k+0.50)) )/(k1+((1-b)+b) * (doc_len/doc_avg_len)+(tf))) 
                        sumation += term_weight
                    if (sumation/l) > wieght_threshold:
                        weight[C] = (1.00/l) * sumation
                # Store concepts and weights in the database, concepts and their weights are semi-colon separated
            values = ''
            ConceptList = []
            for row in weight:
                for term in T[row]:
                    ConceptList.append(term)
                if values == '':
                    values = str(row) + ';' + str(weight[row])
                else:
                    values += ',' + str(row) + ';' + str(weight[row])
                f.write(str(weight[row]) + ' ' + row + '---> ' + ' , '.join(ConceptList) + "\n")
            self.ExpandDocument(doc[0], ConceptList)
            query = 'UPDATE doc SET Concepts = "{0}" where PMID = \'{1}\' '.format(values,doc[0])
            db.Query(query)
            f.write("\r\nPMID: " + str(doc[0]) + "\r\n")
        f.close()
        print "Calculating weights is Done!"
    def Indexing(self):
        '''
        IR Indexing Operations
            - Elimination of Stopwords
            - 
        '''
        print "Indexing is started..."
        tp = TextProcessor()
        param = Parameter()
        db = DB()
        N = param.GetDocNumber()              #Number of documents in the collection
        collection = db.Execute("select PMID, CONCAT(AB,' ',Tag) from doc limit {0}".format(N))
        for doc in collection:
            IndexList = tp.Tokenize(doc[1])
            IndexList = tp.EliminateStopWords(IndexList)
            IndexList = tp.Stem(IndexList)
            print IndexList
            self.AddDocumentIndex(doc[0],IndexList)
        print "Indexing is Done!"       

    def DocumentRetrieval(self):
        tp = TextProcessor()
        param = Parameter()
        db = DB()
        N = param.GetDocNumber()
        k1 = 1.2
        k3 = 8.00
        avg_dl = 122
        b = 1 # from 0.25 to 2.00 increase 0.25
        #Q = raw_input("Enter the query:")
        Q = tp.Tokenize('Describe the procedure or methods for how to "open up" a cell through a process called "electroporation."')
        Q = self.QueryConceptExtraction(Q)
        Q = tp.EliminateStopWords(Q)
        Q = tp.Stem(Q)
        Q = self.ExpandQuery(Q)
        print Q
        C = db.Execute('select PMID, Idx  from doc limit {0};'.format(N))
        score = dict()
        for D in C:
            summation = 0;
            dl = len(D) * 1.00
            for t in Q:
                Nt = self.InverseDocumentFrequency(C, t)
                tfn = self.TermFrequency(D[1], t)
                QQ = ' '.join(Q)
                qtf = self.TermFrequency(QQ, t)
                K = k1*((1-b)+b*(dl/avg_dl))
                w = log((N-Nt+0.5)/(Nt+0.5),2)
                if w<0:
                    #this makes the result a negative number
                    # if we break the result will be bigger than or equal to zero
                    break
                p1 = (((k1+1)*tfn)/(K+tfn))
                p2 = ((k3+1)*qtf/(k3+qtf))
                p3 = w
                summation += p1*p2*p3
            score[D] = summation
            #if score[D] > 0:
            #    print score[D], D
        print "Here we go"
        #for D in score.iterkeys():
        #    if score[D] >0:
        #        print D[0], "---->", score[D]
        # Find Top ranked Documents
        M = param.GetDocNumberForLocalContext()
        TopDocs = []
        TopNums = []
        for i in range(M):
            TopNums.append(0)
            TopDocs.append('')
        for D in score.iterkeys():
            for i in range(M):
                if score[D] > TopNums[i]:
                    for j in range(M-i-1):
                        TopDocs[M-j-1] = TopDocs[M-j-2]
                        TopNums[M-j-1] = TopNums[M-j-2]
                    TopDocs[i] = D
                    TopNums[i] = score[D]
                    break
        TopDocsTexts = ''
        for doc in TopDocs:
            TopDocsTexts += " "+ doc[1]
        TopDocsTexts = tp.Tokenize(TopDocsTexts)
        #----------------Calculate weight of each term which is a member of new query----------------------------
        for doc in TopDocs:
            print doc[0]
    def ExpandQuery(self,Q):
        tp = TextProcessor()
        param = Parameter()
        db = DB()
        N = param.GetDocNumber()
        k1 = 1.2
        k3 = 8.00
        avg_dl = 122
        b = 1 # from 0.25 to 2.00 increase 0.25
        #Q = raw_input("Enter the query:")
        print Q
        C = db.Execute('select PMID, Idx  from doc limit {0};'.format(N))
        score = dict()
        for D in C:
            summation = 0;
            dl = len(D) * 1.00
            for t in Q:
                Nt = self.InverseDocumentFrequency(C, t)
                tfn = self.TermFrequency(D[1], t)
                QQ = ' '.join(Q)
                qtf = self.TermFrequency(QQ, t)
                K = k1*((1-b)+b*(dl/avg_dl))
                w = log((N-Nt+0.5)/(Nt+0.5),2)
                if w<0:
                    #this makes the result a negative number
                    # if we break the result will be bigger than or equal to zero
                    break
                p1 = (((k1+1)*tfn)/(K+tfn))
                p2 = ((k3+1)*qtf/(k3+qtf))
                p3 = w
                summation += p1*p2*p3
            score[D] = summation
            #if score[D] > 0:
            #    print score[D], D
        print "Here we go"
        #for D in score.iterkeys():
        #    if score[D] >0:
        #        print D[0], "---->", score[D]
        # Find Top ranked Documents
        M = param.GetDocNumberForLocalContext()
        TopDocs = []
        TopNums = []
        for i in range(M):
            TopNums.append(0)
            TopDocs.append('')
        for D in score.iterkeys():
            for i in range(M):
                if score[D] > TopNums[i]:
                    for j in range(M-i-1):
                        TopDocs[M-j-1] = TopDocs[M-j-2]
                        TopNums[M-j-1] = TopNums[M-j-2]
                    TopDocs[i] = D
                    TopNums[i] = score[D]
                    break
        TopDocsTexts = ''
        for doc in TopDocs:
            TopDocsTexts += " "+ doc[1]
        TopDocsTexts = tp.Tokenize(TopDocsTexts)
        #----------------Calculate weight of each term which is a member of new query----------------------------
        N = N
        C = C
        K = TopDocsTexts
        Beta = 0.4
        weight = dict()
        MaxTFQ = 0
        for term in TopDocsTexts:
            tfq = self.TermFrequency(' '.join(Q), term)
            if tfq > MaxTFQ:
                MaxTFQ = tfq
        tfqN = 0
        MaxInfo = 0
        for term in TopDocsTexts:
            Lambda = self.TermFrequency(' '.join(K), term)
            Freq_t_k = self.TermFrequency(' '.join(K), term)
            log1 = log(1.00/(1.00+Lambda),2)
            log2 = log(Lambda/(1.00+Lambda),2)
            InfoBO1 = -log1 - Freq_t_k * log2
            if InfoBO1 > MaxInfo:
                MaxInfo = InfoBO1
        for term in TopDocsTexts:
            Lambda = self.TermFrequency(' '.join(K), term)
            Freq_t_k = self.TermFrequency(' '.join(K), term)
            log1 = log(1.00/(1.00+Lambda),2)
            log2 = log(Lambda/(1.00+Lambda),2)
            InfoBO1 = -log1 - Freq_t_k * log2
            tfq = self.TermFrequency(' '.join(Q), term)
            tfqN = (tfq +0.00) /MaxTFQ
            if MaxInfo >0 :
                weight[term] = tfqN + Beta*(InfoBO1/MaxInfo)
            else:
                weight[term] = 0
        QPrime = []
        for term in weight.iterkeys():
            if weight[term] > 0.25:
                QPrime.append(term)
                print term, "-->", weight[term]
        return  QPrime

    def ExtractConceptsFromDoc(self,document,db,terminology,MaxMatcher):
        # Set threshold 
        op = Operation()
        threshold = 0.95
        doc_token = [t for t in word_tokenize(document)]    #[word_tokenize(t) for t in sent_tokenize(document)]
        print "len(doc_token) " , len(doc_token)
        candidate_concepts = []
        
        #Prepare a dictionary for MaxMatcher result of tokens.
        for token_row in doc_token:
            if token_row not in MaxMatcher.keys():
                extracted_concepts = db.Execute("select cid, sig from "+ terminology +"_mm where word = '" + token_row + "'")
                MaxMatcher[token_row] = extracted_concepts
        for current_token_counter in range(len(doc_token)-3): #skip the last 3 token
            current_token = doc_token[current_token_counter]
            skip_counter = 0                                           # Number of skips
            skip_limit = 2                                        #Skip limit
            extracted_concepts = MaxMatcher[current_token]
            current_token_concepts = Set()
            current_token_score = dict()
            for c in extracted_concepts:                            # Create T_c
                current_token_concepts.add(c[0]) 
                current_token_score[c[0]] = c[1]
            next_token_counter = 1                                           # Next word counter
            next_token = doc_token[ current_token_counter + next_token_counter ]                     # t is the next word
            while (skip_counter < skip_limit):
                extracted_concepts = MaxMatcher[next_token]
                next_token_concepts = Set()
                next_token_score = dict()
                for c in extracted_concepts:
                    next_token_concepts.add(c[0])
                    next_token_score[c[0]] = c[1]
                mutual_concepts = next_token_concepts & current_token_concepts
                if len(mutual_concepts) == 0:
                    skip_counter = skip_counter + 1
                else:
                    current_token_concepts = mutual_concepts
                    for c in current_token_concepts:
                        current_token_score[c] += next_token_score[c]
                next_token_counter += 1
                if (current_token_counter + next_token_counter) < len (doc_token):
                    next_token = doc_token[ current_token_counter + next_token_counter ]
                else:
                    break
            candidate_concepts = op.union( candidate_concepts , [c for c in current_token_concepts if current_token_score[c]>threshold])
        print "-----------------------------------------------"
        print document
        print candidate_concepts
        print "-----------------------------------------------"
        return candidate_concepts
    def TermFrequency(self,doc,term):
        tp = TextProcessor() 
        doc_tokens = doc.split(tp.WordSplitter())
        result = doc_tokens.count(term)
        return result
    def InverseDocumentFrequency(self,docs,term):
        tp = TextProcessor()
        count = 0       
        for doc in docs:
            print doc
            doc_tokens = doc[1].split(tp.WordSplitter())
            if doc_tokens.count(term) > 0:
                count += 1
        return count
    def FindWordInDocument(self,document,word):
        doc_list = document.split(' ')
        for term in doc_list:
            if term == word:
                return True
        return False
    def ExpandDocument(self,PMID,ConceptList):
        db = DB()
        query = 'update doc set Tag = "{0}" where PMID = "{1}"'.format(' , '.join(ConceptList),PMID)
        db.Query(query)
    def AddDocumentIndex(self,PMID,IndexList):
        db = DB()
        query = 'update doc set Idx = "{0}" where PMID = "{1}"'.format(' '.join(IndexList),PMID)
        db.Query(query)