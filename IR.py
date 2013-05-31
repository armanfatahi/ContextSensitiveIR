import sys
from output import Display
from models import Document, Collection, Query, DB, GO, MeSH, ICD10, SNOMED, Ontology
from utilities import TextProcessor,Operation,Parameter, GlobalVariables
from sets import Set
from math import log10,log


class IR():
    title = "Informaion retrieval"
    def __init__(self):
        print "Good Luck with your ",self.title
    

class OntologyBasedIR(IR):
    title = "Context Sensitive"
    db = DB()
    go = GO()
    
    def _experiment(self,remove_stop_words,global_context_activated,ontology_based_IR_activated,add_concepts_multiple_times):
        GlobalVariables.remove_stop_words = remove_stop_words
        GlobalVariables.global_context_activated = global_context_activated
        GlobalVariables.ontology_based_IR_activated = ontology_based_IR_activated
        GlobalVariables.add_concepts_multiple_times = add_concepts_multiple_times
        GlobalVariables.adding_concept_times = 5
        GlobalVariables.result_title = "exp"
        if GlobalVariables.remove_stop_words:
            GlobalVariables.result_title += "_t"
        else:
            GlobalVariables.result_title += "_f"
        if GlobalVariables.global_context_activated:
            GlobalVariables.result_title += "_t"
        else:
            GlobalVariables.result_title += "_f"
        if GlobalVariables.ontology_based_IR_activated:
            GlobalVariables.result_title += "_t"
        else:
            GlobalVariables.result_title += "_f"
        if GlobalVariables.add_concepts_multiple_times:
            GlobalVariables.result_title += "_t"
        else:
            GlobalVariables.result_title += "_f"
        for i in range(50):
            print "Query: ",100+i
            #self.retrive_documents(100+i)
        
        Display._extract_percision_recall(GlobalVariables.result_title)
        Display._combine_Percision_recal(GlobalVariables.result_title)
    def __init__(self):
        print "Good Luck with your Context Sensitive IR!" 
    
    def extract_concepts(self):
        '''
        Concepts are extracted from each document.
        for the extracted concepts, Predecessors are extracted.
        We need to keep 'Already Extracted' List of 
        Go concepts so that we avoid extracting them again
        
        OR
        
        we use a try except when we are inserting the concept to avoid duplicate!
        we well pay the cost of duplicate extraction though!
        '''
        db = DB()
        Collection._load()
        terminology_list = ["go"]#,"mesh","icd10","snomed"]
        extracted_doc = 0
        for terminology in terminology_list:
            MaxMatcher = dict()
            for doc in Collection._documents:  
                extracted_doc += 1
                print "extracted_doc: ",extracted_doc , ' id:', doc.PMID
                document = doc.abstract                           # document is the abstract text.
                concepts = self._extract_concepts(document,terminology,MaxMatcher)
                if len(concepts)>0:
                    concept_id_list = ','.join(concepts)
                    if terminology == 'go':
                        self.AddGeneOntologyConceptPredecessors(doc.PMID,concepts)
                    query = "Insert into collection_go(PMID,go_id_list) values ('"+doc.PMID+"',' "+concept_id_list+"');"
                    try:
                        print query
                        db.Query(query)
                    except:
                        print ""#"Unexpected error:", sys.exc_info()[0]

    def AddGeneOntologyConceptPredecessors(self,doc,conceptList):
        """ 
        concept list is a list of GO ids
        """
        predecessors = []
        for concept in conceptList:
            predecessors.append(GO.GetPredecessors(concept))
        query = "Insert into collection_go_predecessors(PMID,Predecessors) values ('{0}','{1}')".format(doc,' , '.join(predecessors))
        try:
            OntologyBasedIR.db.Query(query)
        except:
            print "Unexpected error:", sys.exc_info()[0]

    def DocumentExpantion(self):
        '''
        db.Query("delete from collection_concepts;")!!!
        
        BM25TermWeightingModel
        BM25 or Best Match algorithm, calculates the weight of 
        each word in each extracted concept for the document 
        '''
        print "Calculating weights is started..."
        wieght_threshold = 0.10
        tp = TextProcessor()
        ontology = Ontology()
        db = DB()
        db.Query("delete from collection_concepts;")
        Collection._load()
        Collection._load_go()
        N = Collection._count
        #Terminologies are ('go','mesh','icd10','snomed') corresponding with columns 2,3,4,5
        T = ontology.GetDict('go')    #bring all ontologies into the memory to be faster!
        doc_avg_len = 122
        k1 = 1.2 
        b = 1.00
        doc_counter = 0   
        print Collection._count                     # tuning parameters!
        for d in Collection._documents:
            doc_counter += 1
            doc_len = d.length
            weight = dict()
            for C in d.go:
                C = C.replace(' ','')
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
                    tf = d.get_frequency(term)                     
                    #Here goes calculating the weight
                    n_k = Collection._get_frequency(term)
                    tf = d.get_frequency(term)
                    try:
                        term_weight = tf * (( log10((N-n_k+0.50)/(n_k+0.50)) )/(k1+((1-b)+b) * (doc_len/doc_avg_len)+(tf))) 
                    except:
                        pass
                        #print "One here!++++++++++++++++++++++++++++++++++"
                    sumation += term_weight
                if (sumation/l) > wieght_threshold:
                    weight[C] = (1.00/l) * sumation
            # Store concepts and weights in the database, concepts and their weights are semi-colon separated
            values = ''
            ConceptList = []
            for row in weight:
                row = row.replace(" ",'')
                for term in T[row]:
                    ConceptList.append(term)
                if values == '':
                    values = str(row) + ';' + str(weight[row])
                else:
                    values += ',' + str(row) + ';' + str(weight[row])
            d.set_tag(ConceptList)   #Adding tag tags to documents
            query = 'Insert into collection_concepts (PMID, Concepts) values({0}, "{1}")'.format(d.PMID,values)
            #print query
            db.Query(query)
        print "Calculating weights is Done! Concepts are added to Database"
    def Indexing(self):
        '''
        IR Indexing Operations
            - Elimination of Stopwords
            - 
        '''
        DB._execute("DELETE from collection_index")
        print "Indexing is started..."
        tp = TextProcessor() 
        Collection._load()
        Collection._load_tags() #loading document with PMID, tags and abstracts
        for doc in Collection._documents:
            index_list = []
            for term in doc.abstract:
                index_list.append(term)
            if GlobalVariables.global_context_activated:
                for term in doc.tag:
                    index_list.append(term)
            index_list = tp.EliminateStopWords(index_list)
            index_list = tp.Stem(index_list)
            doc.set_index(index_list)
        print "Indexing is Done!"       

    def retrive_documents(self,query_id):
        k1 = 1.2
        k3 = 8.00
        avg_dl = 122
        b = 1 # from 0.25 to 2.00 increase 0.25
        q = Query(query_id)
        #q.set_concepts(self.QueryConceptExtraction(q.text))
        self._expand_query(q)
        return
        print "Retrieving Documents for: ", q.text
        Collection._load()
        Collection._load_go()
        Collection._load_tags()
        Collection._load_indexes()      #Loads documents into _documents with PMID and Index
        score = dict()
        N = Collection._count
        Nt = dict()
        for term in q.text:
            Nt[term] = Collection._get_frequency(term)
        counter = 0
        for doc in Collection._documents:
            summation = 0;
            dl = doc.length * 1.00
            for t in q.text:
                tfn = doc.get_frequency(t)
                QQ = ' '.join(q.text)
                qtf = Document._term_frequency(QQ, t)
                K = k1*((1-b)+b*(dl/avg_dl))
                w = log((N-Nt[t]+0.5)/(Nt[t]+0.5),2)
                if w<0:
                    #this makes the result a negative number
                    # if we break the result will be bigger than or equal to zero
                    break
                p1 = (((k1+1)*tfn)/(K+tfn))
                p2 = ((k3+1)*qtf/(k3+qtf))
                p3 = w
                summation += p1*p2*p3
            score[doc.PMID] = summation
            counter += 1
            
        #Display.plot(score, q)
    def QueryConceptExtraction(self,query):
        terminology_list = ["go"]# ,"mesh","icd10","snomed"]
        conceptList = []
        for terminology in terminology_list:
            MaxMatcher = dict()
            #for doc in collection:                          # For every single document do the Indexing
            #print doc
            #print "len(MaxMatcher) is ",len(MaxMatcher)                         # document is the abstract text.
            concepts = OntologyBasedIR._extract_concepts(query,terminology,MaxMatcher)
            for concept in concepts:
                conceptList.append(concept)
    def _expand_query(self,q):
        #--STEP 1----------Extract TOP DOCUMENTS ----------------------------
        tp = TextProcessor()
        param = Parameter()
        k1      = 1.2
        k3      = 8.00
        avg_dl  = 122
        b       = 1                     # from 0.25 to 2.00 increase 0.25    
        Collection._load_indexes()      # Loads indexes into _documents
        N = len(Collection._documents)
        score = dict()
        for D in Collection._documents:
            summation = 0;
            dl = D.length * 1.00
            for t in q.text:
                Nt = Collection._get_frequency(t)
                tfn = D.get_frequency(t)
                qtf = q.get_frequency(t)
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
                
            score[D.PMID] = summation
        M = param.GetDocNumberForLocalContext()
        TopDocs = []
        TopNums = []
        new_score = dict()
        for item in score.iterkeys():
            if score[item] > 0:
                new_score[item] = score[item]
        
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
        Display._plot(new_score, q)
        Display._export_to_database(new_score,q,GlobalVariables.result_title)
        return
        TopDocsTexts = ''        
        TopDocsTexts = tp.Tokenize(TopDocsTexts)
        TopDocsTexts = TextProcessor._remove_stop_words(TopDocsTexts)
        #---STEP 2---------Calculate weight of each term which is a member of new query----------------------------
        K = TopDocsTexts
        Beta = 0.4
        weight = dict()
        MaxTFQ = 0.001
        for term in TopDocsTexts:
            tfq = q.get_frequency(term)
            if tfq > MaxTFQ:
                MaxTFQ = tfq
        tfqN = 0
        MaxInfo = 0
        for term in TopDocsTexts:
            Lambda = Document._term_frequency(' '.join(K), term)
            Freq_t_k = Document._term_frequency(' '.join(K), term)
            log1 = log(1.00/(1.00+Lambda),2)
            log2 = log(Lambda/(1.00+Lambda),2)
            InfoBO1 = -log1 - Freq_t_k * log2
            if InfoBO1 > MaxInfo:
                MaxInfo = InfoBO1
        for term in TopDocsTexts:
            Lambda = Document._term_frequency(' '.join(K), term)
            Freq_t_k = Document._term_frequency(' '.join(K), term)
            log1 = log(1.00/(1.00+Lambda),2)
            log2 = log(Lambda/(1.00+Lambda),2)
            InfoBO1 = -log1 - Freq_t_k * log2
            tfq = q.get_frequency(term)
            tfqN = (tfq +0.00) /MaxTFQ
            if MaxInfo >0 :
                weight[term] = tfqN + Beta*(InfoBO1/MaxInfo)
            else:
                weight[term] = 0
        QPrime = []
        for term in weight.iterkeys():
            if weight[term] > 0.25:
                QPrime.append(term)
        return  QPrime
    @staticmethod
    def _extract_concepts(document,terminology,MaxMatcher):
        """
        document:
        db:
        terminology:
        MaxMatcher:
        
        returns:
                Concept List
        """
        # Set threshold 
        op = Operation()
        threshold = 0.95
        doc_token = document
        #print "len(doc_token) " , len(doc_token)
        candidate_concepts = []
        #Prepare a dictionary for MaxMatcher result of tokens.
        for token_row in doc_token:
            if token_row not in MaxMatcher.keys():
                extracted_concepts = DB._execute("select cid, sig from "+ terminology +"_mm where word = '" + token_row + "'")
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
        return candidate_concepts
class ContextSensitiveIR(IR):
    title = "Context Sensitive"
    db = DB()
    go = GO()
    def __init__(self):
        print "Good Luck with your Context Sensitive IR!" 
    
    def extract_concepts(self):
        '''
        Concepts are extracted from each document.
        for the extracted concepts, Predecessors are extracted.
        We need to keep 'Already Extracted' List of 
        Go concepts so that we avoid extracting them again
        
        OR
        
        we use a try except when we are inserting the concept to avoid duplicate!
        we well pay the cost of duplicate extraction though!
        '''
        db = DB()
        Collection._load()
        terminology_list = ["go"]#,"mesh","icd10","snomed"]
        extracted_doc = 0
        for terminology in terminology_list:
            MaxMatcher = dict()
            for doc in Collection._documents:  
                extracted_doc += 1
                print "extracted_doc: ",extracted_doc , ' id:', doc.PMID
                document = doc.abstract                           # document is the abstract text.
                concepts = self._extract_concepts(document,terminology,MaxMatcher)
                if len(concepts)>0:
                    concept_id_list = ','.join(concepts)
                    if terminology == 'go':
                        self.AddGeneOntologyConceptPredecessors(doc.PMID,concepts)
                    query = "Insert into collection_go(PMID,go_id_list) values ('"+doc.PMID+"',' "+concept_id_list+"');"
                    try:
                        print query
                        db.Query(query)
                    except:
                        print ""#"Unexpected error:", sys.exc_info()[0]

    def AddGeneOntologyConceptPredecessors(self,doc,conceptList):
        """ 
        concept list is a list of GO ids
        """
        predecessors = []
        for concept in conceptList:
            predecessors.append(GO.GetPredecessors(concept))
        query = "Insert into collection_go_predecessors(PMID,Predecessors) values ('{0}','{1}')".format(doc,' , '.join(predecessors))
        try:
            ContextSensitiveIR.db.Query(query)
        except:
            print "Unexpected error:", sys.exc_info()[0]
        
        
    
    def DocumentExpantion(self):
        '''
        db.Query("delete from collection_concepts;")!!!
        
        BM25TermWeightingModel
        BM25 or Best Match algorithm, calculates the weight of 
        each word in each extracted concept for the document 
        '''
        print "Calculating weights is started..."
        wieght_threshold = 0.10
        tp = TextProcessor()
        ontology = Ontology()
        db = DB()
        db.Query("delete from collection_concepts;")
        Collection._load()
        Collection._load_go()
        N = Collection._count
        #Terminologies are ('go','mesh','icd10','snomed') corresponding with columns 2,3,4,5
        T = ontology.GetDict('go')    #bring all ontologies into the memory to be faster!
        doc_avg_len = 122
        k1 = 1.2 
        b = 1.00
        doc_counter = 0   
        print Collection._count                     # tuning parameters!
        for d in Collection._documents:
            doc_counter += 1
            doc_len = d.length
            weight = dict()
            for C in d.go:
                C = C.replace(' ','')
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
                    tf = d.get_frequency(term)                     
                    #Here goes calculating the weight
                    n_k = Collection._get_frequency(term)
                    tf = d.get_frequency(term)
                    try:
                        term_weight = tf * (( log10((N-n_k+0.50)/(n_k+0.50)) )/(k1+((1-b)+b) * (doc_len/doc_avg_len)+(tf))) 
                    except:
                        pass
                        #print "One here!++++++++++++++++++++++++++++++++++"
                    sumation += term_weight
                if (sumation/l) > wieght_threshold:
                    weight[C] = (1.00/l) * sumation
            # Store concepts and weights in the database, concepts and their weights are semi-colon separated
            values = ''
            ConceptList = []
            for row in weight:
                row = row.replace(" ",'')
                for term in T[row]:
                    ConceptList.append(term)
                if values == '':
                    values = str(row) + ';' + str(weight[row])
                else:
                    values += ',' + str(row) + ';' + str(weight[row])
            d.set_tag(ConceptList)   #Adding tag tags to documents
            query = 'Insert into collection_concepts (PMID, Concepts) values({0}, "{1}")'.format(d.PMID,values)
            #print query
            db.Query(query)
        print "Calculating weights is Done! Concepts are added to Database"
    def Indexing(self):
        '''
        IR Indexing Operations
            - Elimination of Stopwords
            - 
        '''
        DB._execute("DELETE from collection_index")
        print "Indexing is started..."
        tp = TextProcessor() 
        Collection._load()
        Collection._load_tags() #loading document with PMID, tags and abstracts
        for doc in Collection._documents:
            index_list = []
            for term in doc.abstract:
                index_list.append(term)
            if GlobalVariables.global_context_activated:
                for term in doc.tag:
                    index_list.append(term)
            index_list = tp.EliminateStopWords(index_list)
            index_list = tp.Stem(index_list)
            doc.set_index(index_list)
        print "Indexing is Done!"       

    def retrive_documents(self,query_id):
        k1 = 1.2
        k3 = 8.00
        avg_dl = 122
        b = 1 # from 0.25 to 2.00 increase 0.25
        q = Query(query_id)
        #q.set_concepts(self.QueryConceptExtraction(q.text))
        self._expand_query(q)
        return
        print "Retrieving Documents for: ", q.text
        Collection._load()
        Collection._load_go()
        Collection._load_tags()
        Collection._load_indexes()      #Loads documents into _documents with PMID and Index
        score = dict()
        N = Collection._count
        Nt = dict()
        for term in q.text:
            Nt[term] = Collection._get_frequency(term)
        counter = 0
        for doc in Collection._documents:
            summation = 0;
            dl = doc.length * 1.00
            for t in q.text:
                tfn = doc.get_frequency(t)
                QQ = ' '.join(q.text)
                qtf = Document._term_frequency(QQ, t)
                K = k1*((1-b)+b*(dl/avg_dl))
                w = log((N-Nt[t]+0.5)/(Nt[t]+0.5),2)
                if w<0:
                    #this makes the result a negative number
                    # if we break the result will be bigger than or equal to zero
                    break
                p1 = (((k1+1)*tfn)/(K+tfn))
                p2 = ((k3+1)*qtf/(k3+qtf))
                p3 = w
                summation += p1*p2*p3
            score[doc.PMID] = summation
            counter += 1
            
        #Display.plot(score, q)
    def QueryConceptExtraction(self,query):
        terminology_list = ["go"]# ,"mesh","icd10","snomed"]
        conceptList = []
        for terminology in terminology_list:
            MaxMatcher = dict()
            #for doc in collection:                          # For every single document do the Indexing
            #print doc
            #print "len(MaxMatcher) is ",len(MaxMatcher)                         # document is the abstract text.
            concepts = ContextSensitiveIR._extract_concepts(query,terminology,MaxMatcher)
            for concept in concepts:
                conceptList.append(concept)
    def _expand_query(self,q):
        #--STEP 1----------Extract TOP DOCUMENTS ----------------------------
        tp = TextProcessor()
        param = Parameter()
        k1      = 1.2
        k3      = 8.00
        avg_dl  = 122
        b       = 1                     # from 0.25 to 2.00 increase 0.25    
        Collection._load_indexes()      # Loads indexes into _documents
        N = len(Collection._documents)
        score = dict()
        for D in Collection._documents:
            summation = 0;
            dl = D.length * 1.00
            for t in q.text:
                Nt = Collection._get_frequency(t)
                tfn = D.get_frequency(t)
                qtf = q.get_frequency(t)
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
                
            score[D.PMID] = summation
        M = param.GetDocNumberForLocalContext()
        TopDocs = []
        TopNums = []
        new_score = dict()
        for item in score.iterkeys():
            if score[item] > 0:
                new_score[item] = score[item]
        
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
        Display._plot(new_score, q)
        TopDocsTexts = ''        
        TopDocsTexts = tp.Tokenize(TopDocsTexts)
        TopDocsTexts = TextProcessor._remove_stop_words(TopDocsTexts)
        #---STEP 2---------Calculate weight of each term which is a member of new query----------------------------
        K = TopDocsTexts
        Beta = 0.4
        weight = dict()
        MaxTFQ = 0.001
        for term in TopDocsTexts:
            tfq = q.get_frequency(term)
            if tfq > MaxTFQ:
                MaxTFQ = tfq
        tfqN = 0
        MaxInfo = 0
        for term in TopDocsTexts:
            Lambda = Document._term_frequency(' '.join(K), term)
            Freq_t_k = Document._term_frequency(' '.join(K), term)
            log1 = log(1.00/(1.00+Lambda),2)
            log2 = log(Lambda/(1.00+Lambda),2)
            InfoBO1 = -log1 - Freq_t_k * log2
            if InfoBO1 > MaxInfo:
                MaxInfo = InfoBO1
        for term in TopDocsTexts:
            Lambda = Document._term_frequency(' '.join(K), term)
            Freq_t_k = Document._term_frequency(' '.join(K), term)
            log1 = log(1.00/(1.00+Lambda),2)
            log2 = log(Lambda/(1.00+Lambda),2)
            InfoBO1 = -log1 - Freq_t_k * log2
            tfq = q.get_frequency(term)
            tfqN = (tfq +0.00) /MaxTFQ
            if MaxInfo >0 :
                weight[term] = tfqN + Beta*(InfoBO1/MaxInfo)
            else:
                weight[term] = 0
        QPrime = []
        for term in weight.iterkeys():
            if weight[term] > 0.25:
                QPrime.append(term)
        return  QPrime
    @staticmethod
    def _extract_concepts(document,terminology,MaxMatcher):
        """
        document:
        db:
        terminology:
        MaxMatcher:
        
        returns:
                Concept List
        """
        # Set threshold 
        op = Operation()
        threshold = 0.95
        doc_token = document
        #print "len(doc_token) " , len(doc_token)
        candidate_concepts = []
        
        #Prepare a dictionary for MaxMatcher result of tokens.
        for token_row in doc_token:
            if token_row not in MaxMatcher.keys():
                extracted_concepts = DB._execute("select cid, sig from "+ terminology +"_mm where word = '" + token_row + "'")
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
        #print "-----------------------------------------------"
        #print document
        #print candidate_concepts
        #print "-----------------------------------------------"
        return candidate_concepts

class TraditionalIR(IR):
    db = DB()
    def __init__(self):
        print "Good Luck with your Traditional IR!" 
    def Indexing(self):
        '''
        IR Indexing Operations
            - Elimination of Stopwords
            - 
        '''
        DB._execute("DELETE from collection_index")
        print "Indexing is started..."
        tp = TextProcessor() 
        Collection._load()
        Collection._load_tags() #loading document with PMID, tags and abstracts
        for doc in Collection._documents:
            index_list = []
            for term in doc.abstract:
                index_list.append(term)
            if GlobalVariables.global_context_activated:
                for term in doc.tag:
                    index_list.append(term)
            index_list = tp.EliminateStopWords(index_list)
            index_list = tp.Stem(index_list)
            doc.set_index(index_list)
        print "Indexing is Done!"       

    def retrive_documents(self,query_id):
        k1 = 1.2
        k3 = 8.00
        avg_dl = 122
        b = 1 # from 0.25 to 2.00 increase 0.25
        q = Query(query_id)
        #q.set_concepts(self.QueryConceptExtraction(q.text))
        self._expand_query(q)
        return
        print "Retrieving Documents for: ", q.text
        Collection._load()
        Collection._load_go()
        Collection._load_tags()
        Collection._load_indexes()      #Loads documents into _documents with PMID and Index
        score = dict()
        N = Collection._count
        Nt = dict()
        for term in q.text:
            Nt[term] = Collection._get_frequency(term)
        counter = 0
        for doc in Collection._documents:
            summation = 0;
            dl = doc.length * 1.00
            for t in q.text:
                tfn = doc.get_frequency(t)
                QQ = ' '.join(q.text)
                qtf = Document._term_frequency(QQ, t)
                K = k1*((1-b)+b*(dl/avg_dl))
                w = log((N-Nt[t]+0.5)/(Nt[t]+0.5),2)
                if w<0:
                    #this makes the result a negative number
                    # if we break the result will be bigger than or equal to zero
                    break
                p1 = (((k1+1)*tfn)/(K+tfn))
                p2 = ((k3+1)*qtf/(k3+qtf))
                p3 = w
                summation += p1*p2*p3
            score[doc.PMID] = summation
            counter += 1
