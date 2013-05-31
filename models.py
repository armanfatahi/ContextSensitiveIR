import sys
import xml.etree.ElementTree as ET
from utilities import DB,TextProcessor,GlobalVariables

class Document:
    PMID = ''           #PMID
    abstract = []       #Abstract a list of words
    go_predecessors = []       #GO Predecessors a list of words
    tag = []            #Tag
    go = []             #Gene Ontology concepts
    db = DB()           #Database
    length = 0
    index = ''
    term_frequency = dict() #term frequency
    def __init__(self,PMID,abstract):
        self.PMID = PMID
        self.length = len(abstract.split(TextProcessor.text_splitter))
        self.abstract = TextProcessor._tokenize(abstract)
        if GlobalVariables.remove_stop_words:
            self.abstract = TextProcessor._remove_stop_words(self.abstract)
        if GlobalVariables.stem:
            TextProcessor._stem(self.abstract)
        self.term_frequency = dict()
        for t in self.abstract:
            self.term_frequency[t] = self.abstract.count(t)
    def get_frequency(self,term):
        term = TextProcessor._normalize_word(term)
        if term in self.term_frequency.keys():
            return self.term_frequency[term]
        else:
            return 0
    def set_go(self,GO):
        Concepts = GO.split(TextProcessor.text_splitter) #Getting the list of Concepts for GO
        TextProcessor.CleanList(Concepts, '')
        TextProcessor.CleanList(Concepts, ' ')
        self.go = Concepts
    def set_tag(self,ConceptList):
        '''
        Input:
            ConceptList, a List of concepts to be tagged in the document.
        '''
        query = 'Insert into collection_tag(PMID,title) values ("{0}","{1}");'.format(self.PMID,' , '.join(ConceptList))
        self.db.Query(query)
    def set_index(self,ConceptList):
        '''
        Input:
            ConceptList, a List of concepts to be tagged in the document.
        '''
        query = 'Insert into collection_index(PMID,title) values ("{0}","{1}");'.format(self.PMID,' , '.join(ConceptList))
        self.db.Query(query)
    @staticmethod
    def _term_frequency(doc,term):
        doc_tokens = doc.split(TextProcessor._word_splitter)
        result = doc_tokens.count(term)
        return result
class Collection():
    _status = 0         #Status of the collection, 
                        #Load is called --> 1
                        #Go loaded --> 2
                        #Tags loaded --> 3
                        #Indexes loaded --> 4
    _documents = []
    _count = 0
    _inverse_term_frequency = dict()
    @staticmethod
    def _extract_from_file(self,n):
        sys.stdout.write("Storing Documents in Database Started...") 
        db = DB()
        #db.Query('delete from doc;') # Make sure the table is empty
        #db.Query('delete from collection;') # Make sure the table is empty
        tp = TextProcessor()
        f = open(r'd:\Project\TREC\2004\2004_TREC_ASCII_MEDLINE_'+n)
        
        #Extract Document Ids that we want to extract
        # id_collection is a space separated format string to keep ids
        db_results = db.Execute("select docId from genomics_qrels_small;")
        id_collection = ''
        for row in db_results:
            id_collection += row[0] + ' '
        print "Number of documents to extract is: ", len(db_results)
        
        
        abst = ''
        PMID = ''

        QueryList = ''   # To make queries faster, First we collect a banch of them
        QueryCount = 0
        QueryDone = 0
        QueryLimit = 0
        
        DocCount = 0
        line = f.readline()
        while (True):
            if line.find('PMID-') != -1:  # if the line contains PMID, new document begins
                PMID = tp.CleanPMID(line)
                print DocCount, ' . ', PMID
                DocCount += 1
                if id_collection.find(PMID)== -1:
                    line = f.readline()
                    continue
                while (True):
                    line = f.readline()
                    if line.find('AB  -') != -1: # Here the abstract begins
                        abst = line
                        while (True):
                            line = f.readline()
                            if line.find('    ')!= -1:
                                abst += line
                            else:
                                break
                    else:
                        if line.find('PMID-') != -1:
                            break
                if abst != '':
                    while abst.find('  ') != -1:
                        abst = abst.replace('  ',' ')
                    PMID = tp.CleanPMID(PMID)
                    abst = tp.RemoveStopWords(abst) #We store not original data.
                    abst = tp.CleanDoc(abst) # to remove ' and  " so that SQL query has no problem
                    QueryList += r"('" + PMID + "','" + abst + "'),"
                    PMID = abst = ''
                    QueryCount += 1
                    if QueryCount > QueryLimit:
                        QueryList = QueryList[:-1]
                        QueryList = 'Insert into collection(PMID,AB) values ' + QueryList
                        try:
                            db.Query(QueryList)
                        except:
                            print "Unexpected error:"#, sys.exc_info()[0]
                        QueryList = ''
                        QueryDone += QueryLimit + 1
                        QueryCount = 0
                        #sys.stdout.write(str(QueryDone)+" . ")   
            else:
                line = f.readline()
        sys.stdout.write("Done") 
    @staticmethod
    def _load():
        """Loads documents into _document with PMID,
            and Abstract.
        """
        if Collection._status > 0:
            return
        sys.stdout.write("Loading Collection...")
        Collection._documents = []
        if(GlobalVariables.ontology_based_IR_activated):
            query = "SELECT collection.PMID, "
            query += "collection.AB, collection_go_predecessors.title "
            query += "FROM collection, collection_go_predecessors "
            query += "WHERE collection.PMID = collection_go_predecessors.PMID " 
            collection = DB._execute(query)
            Collection._documents = []
            for doc in collection:
                concepts  = doc[2]
                if GlobalVariables.add_concepts_multiple_times:
                    for i in range(GlobalVariables.adding_concept_times):
                        concepts += " , ",doc[2]
                d = Document(doc[0],doc[1] + ' ' + concepts)
                Collection._documents.append(d)
        else:
            query = "SELECT collection.PMID, "
            query += "collection.AB "
            query += "FROM collection " 
            collection = DB._execute(query)
            Collection._documents = []
            for doc in collection:
                d = Document(doc[0],doc[1])
                Collection._documents.append(d)
        Collection._count = len(Collection._documents)
        sys.stdout.write(" ...Done!\n")
        Collection._status = 1   
    @staticmethod
    def _load_go():
        """Loads go into _document list
        """
        if Collection._status > 1:
            return
        if Collection._status < 1:
            Collection._load()
        sys.stdout.write( "Loading GO...")
        for doc in Collection._documents:
            query = "SELECT collection_go.title "
            query += "FROM collection_go " 
            query += " WHERE collection_go.PMID = {0}".format(doc.PMID)
            result = DB._execute(query)
            doc.set_go(result[0][0])
        sys.stdout.write(" ...Done!\n")
        Collection._status = 2
    @staticmethod
    def _load_tags():
        """Loads tags into _document list
        """
        if Collection._status > 2:
            return
        if Collection._status < 2:
            Collection._load_go()
        sys.stdout.write( "Loading Tags...")
        for doc in Collection._documents:
            query = "SELECT collection_tag.title "
            query += "FROM collection_tag " 
            query += " WHERE collection_tag.PMID = {0}".format(doc.PMID)
            result = DB._execute(query)
            doc.tag = TextProcessor._tokenize(result[0][0]) 
        sys.stdout.write(" ...Done!\n")
        Collection._status = 3
    @staticmethod
    def _load_indexes():
        """Loads indexes into _document list
        """
        if Collection._status > 3:
            return
        if Collection._status < 3:
            Collection._load_tags()
        sys.stdout.write( "Loading Indexes...")
        for doc in Collection._documents:
            query = "SELECT collection_index.title "
            query += "FROM collection_index " 
            query += " WHERE (collection_index.PMID = {0})".format(doc.PMID)
            result = DB._execute(query)
            doc.index = result[0][0] 
        sys.stdout.write(" ...Done!\n")
        Collection._status = 4
    @staticmethod
    def _get_frequency(term):
        count = 0       
        for doc in Collection._documents:
            doc_tokens = doc.index.split(TextProcessor._word_splitter)
            if doc_tokens.count(term) > 0:
                count += 1
        return count
class Query:
    text = []
    concepts = ''
    term_frequency = dict()
    qrels = []
    id = 0
    def __init__(self,query_id):
        self.id = query_id
        result = DB._execute("select title from adhoc2005narrative where id = {0}".format(query_id))
        result_qrels = DB._execute("select document_id from genomics_qrels_small where query_id = {0}".format(query_id))
        self.text = TextProcessor._tokenize(result[0][0])
        self.qrels = []
        for row in result_qrels:
            self.qrels.append(row[0])
        if GlobalVariables.remove_stop_words:
            self.remove_stop_words()
        if GlobalVariables.stem:
            self.stem()
        for t in self.text:
            self.term_frequency[t] = self.text.count(t)
    def set_concepts(self,concepts):
        self.concepts = concepts
    def remove_stop_words(self):
        self.text = TextProcessor._remove_stop_words(self.text)
    def stem(self):
        self.text = TextProcessor._stem(self.text)
    def get_frequency(self,term):
        term = TextProcessor._normalize_word(term)
        if term in self.term_frequency.keys():
            return self.term_frequency[term]
        else:
            return 0
class Ontology:
    @staticmethod
    def _initialize():
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
    def MaxMatcher(self,source):
        ''' 
        MaxMatcher is to train the significance score of each word to biological concepts containing that word.
        source is a table which schema is: (id,title,syn,alt_id)
        dist is a table which schema is: ( cid, word, sig)
        This method uses source table content and will calculates the significance of each word in each variant
        to each concept.
        '''
        print "Creating MaxMatcher for " + source +" Started..."
        con = DB()
        con.Query('delete from {0}_mm ;'.format(source))
        # GO Databse scheme is ( id, variant)
        con.cur.execute("SELECT * from {0};".format(source))
        rows = con.cur.fetchall()
        #concept_dict contains concept unique identifier and its variants
        concept_dict = {}
        i = 0
        for row in rows:
            i += 1
            syn_list =  row[1].split(',')
            # Making a dictionary that contains every Concept
            concept_dict[row[0]] = syn_list
        # N(w) is the the number of concepts whose variant names contain word w,
        # Add every possible word into a Dictionary as a Key
        # and add N(w) as the value
        # Step one is to calculate the N(w) in a temporary list
        temp_word_list = {}
        for concept in concept_dict.itervalues():
            for variant in concept:
                var_word_list = variant.split()
                for word in var_word_list:
                    if word in temp_word_list:
                        if temp_word_list[word][0] != variant:
                            temp_word_list[word] = ( variant, temp_word_list[word][1] + 1)
                    else:
                        temp_word_list[word] = ( variant , 1)
        # Now Create a dictionary that contains N(w)
        word_dic = {}
        for word in temp_word_list:
            word_dic[word] = temp_word_list[word][1]
        
        # Next Step is to calculte the I(w,Sj) where Sn is the 
        # nth variant of a concept
        Sig_word_to_variant = {}
        Sig_word_to_concept = {}

        for concept in concept_dict.iteritems():
            concept_word_list = []
            for variant in concept[1]:
                # Create a list of words in this variant
                word_list = variant.split()
                concept_word_list.extend(word_list)
                summ = 0
                for word in word_list:
                    summ += 1.00/word_dic[word]
                for word in word_list:
                    Sig_word_to_variant[(word,variant)] = (1.00/word_dic[word])/summ                 
            for word in concept_word_list:
                temp_value = 0
                # find the Max of significance among all variants of a concept
                for variant in concept[1]:
                    word_list = variant.split()
                    if word in word_list:
                        if temp_value < Sig_word_to_variant[(word,variant)]:
                            temp_value = Sig_word_to_variant[(word,variant)]
                Sig_word_to_concept[(word,concept[0])] = temp_value
        # Calculateing the significance of each word to each concept
        # to do this we can make a dictionary with a tuple of (concept, word)
        # as key and the significance as the value
        # Store the result in Database go_mm (GO MaxMatcher) table
        query_values = ''
        values_limit = 10000
        values = 0
        for row in Sig_word_to_concept.iteritems():
            concept = row[0][1]
            word = row[0][0]
            significance = row[1]
            if(query_values != ''):
                query_values += ",('"+concept+"','"+word+"',"+str(significance)+")" 
            else:
                query_values =  "('"+concept+"','"+word+"',"+str(significance)+")" 
            values += 1
            if values >= values_limit:
                query = "insert into {0}_mm (cid,word,sig) values {1};".format(source,query_values)
                con.Query(query)
                query_values = ''
                values = 0
        query = "insert into {0}_mm (cid,word,sig) values {1};".format(source,query_values)
        con.Query(query)
        print "Creating MaxMatcher for " + source +" Done!"
    def CreateMaxMatcher(self):
        print "Creating MaxMatcher started..."
        db = DB()
        db.Query("delete from MaxMatcher")
        ontologies = {'go','mesh','icd10','snomed'}
        query_values = ''
        query_values_count = 0
        query_values_limit = 1000
        query_header = "Insert into MaxMatcher (cid,word,sig) values "
        for ontology in ontologies:
            print "adding {0} terminology".format(ontology)
            result = db.Execute("select * from "+ontology+"_mm")
            print ontology, " has ",len(result)," rows!"
            for row in result:
                if query_values == '':
                    query_values = "('{0}','{1}',{2})".format(row[0],row[1],row[2])
                    query_values_count += 1
                else:
                    query_values += ",('{0}','{1}',{2})".format(row[0],row[1],row[2])
                    query_values_count += 1
                if query_values_count > query_values_limit:
                    db.Query(query_header + query_values)
                    query_values_count = 0;
                    query_values = ''
        print "Creating MaxMatcher Done!"
    def CreateTerminologies(self):
        print "Creating T table(collection of all comcepts) started..."
        db = DB()
        db.Query("delete from T")
        ontologies = {'go','mesh','icd10','snomed'}
        query_values = ''
        query_values_count = 0
        query_values_limit = 1000
        query_header = "Insert into T (id,variant) values "
        for ontology in ontologies:
            print "adding {0} terminology".format(ontology)
            result = db.Execute("select * from "+ontology)
            print ontology, " has ",len(result)," rows!"
            for row in result:
                if query_values == '':
                    query_values = "('{0}','{1}')".format(row[0],row[1])
                    query_values_count += 1
                else:
                    query_values += ",('{0}','{1}')".format(row[0],row[1])
                    query_values_count += 1
                if query_values_count > query_values_limit:
                    db.Query(query_header + query_values)
                    query_values_count = 0;
                    query_values = ''
        print "Creating T Done!"
    def GetDict(self,Terminology):
        '''
        returns a Dictionary containing each concept and it's variants
        '''
        db = DB()
        tp = TextProcessor()
        result = db.Execute('Select id, variant from {0}'.format(Terminology))
        terminology_dict = dict()
        for row in result:
            variants = row[1].split(tp.Splitter())
            variants = [c for c in variants if c != '']
            terminology_dict[row[0]] = variants
        return terminology_dict
        
class GO(Ontology):
    variantDict = dict()
    childrenDict = dict()
    '''Gene Ontology Handler'''
    def __init__(self):
        self.file_name = r'd:\project\ontology\go.xml'
        self.go_file = open(self.file_name)
        GO.variantDict = self.GetVariantDict()
        GO.childrenDict = self.GetChildrenDict()
        f = open(r'd:\project\temp\output.txt','w')
        f.write(str(GO.variantDict))
    def ExtractTerms(self):
        print "Extracting Go Terms started..."
        tp = TextProcessor()
        con = DB()
        con.Query('delete from go;')
        tree = ET.parse(self.file_name)
        root = tree.getroot()
        query_values = ''
        values_limit = 1000
        values = 0
        for term in root.iter(tag='term'):
            accession = term.find('accession').text
            variant = term.find('name').text
            children = ''
            for child in term.iter(tag='is_a'):
                if str(child.attrib['resource']).find('GO:') != -1:
                    if len(children) > 0:
                        children += ','    
                    children += str(child.attrib['resource'])[31:41]
            for synonym in term.iter(tag='synonym'):
                temp_syn = synonym.text;
                if temp_syn.find('GO:') == -1:
                    variant += ' , ' + temp_syn
            if(query_values != ''):
                query_values += ",('{0}','{1}','{2}')".format(accession,tp.Clean(variant),children) 
            else:
                query_values =  "('{0}','{1}','{2}')".format(accession,tp.Clean(variant),children) 
            values += 1
            if values >= values_limit:
                query = "insert into go(id,variant,children) values " + query_values +";"
                con.Query(query)
                query_values = ''
                values = 0
        query = "insert into go(id,variant,children) values " +query_values+";"
        con.Query(query)
        con.Close()
        print 'Extracting GO terms Done!'
    @staticmethod
    def GetPredecessors(conceptId):
        variantDict = GO.variantDict
        childrenDict = GO.childrenDict
        result = []
        result.append(variantDict[conceptId])
        childrenId = childrenDict[conceptId]
        try:
            for child in childrenId:
                result.append(GO.GetChildren(child))
        except:
            pass
        return ' '.join(result)
    def GetVariantDict(self):
        '''
        returns a Dictionary containing each concept and it's variants
        '''
        db = DB()
        tp = TextProcessor()
        result = db.Execute('Select id, variant from go')
        terminology_dict = dict()
        for row in result:
            variants = row[1].split(tp.Splitter())
            variants = [c for c in variants if c != '']
            terminology_dict[row[0]] = ' , '.join(variants)
        db.Close()
        return terminology_dict
    def GetChildrenDict(self):
        '''
        returns a Dictionary containing each concept and it's children
        '''
        db = DB()
        tp = TextProcessor()
        result = db.Execute('Select id, children from go')
        terminology_dict = dict()
        for row in result:
            children = row[1].split(tp.Splitter())
            children = [c for c in children if c != '']
            terminology_dict[row[0]] = children
        db.Close()
        return terminology_dict
class MeSH(Ontology):
    """MeSH Ontology Handler."""
    #Gene ontology folder location    
    def __init__(self):
        self.file_name = r'd:\project\ontology\mesh_small.xml'
        self.file = open(self.file_name)
    
    def ExtractTerms(self):
        print "Extracting MsSH Terms started..."
        tp = TextProcessor()
        con = DB()
        con.Query('delete from mesh;')
        tree = ET.parse(self.file_name)
        root = tree.getroot()
        query_values = ''
        values_limit = 100
        values = 0
        for DescriptorRecord in root.findall('DescriptorRecord'):
            DescriptorUI = DescriptorRecord.find('DescriptorUI').text
            DescriptorName = DescriptorRecord.find('DescriptorName').find('String') .text
            DescriptorName = tp.RemoveStopWords(DescriptorName.replace(","," , "))
            if(query_values != ''):
                query_values += ",('"+DescriptorUI+"','"+tp.Clean(DescriptorName) +"')" 
            else:
                query_values =  "('"+DescriptorUI+"','"+tp.Clean(DescriptorName) +"')" 
            values += 1
            if values >= values_limit:
                query = "insert into mesh(id,variant) values " + query_values +";"
                con.Query(query)
                query_values = ''
                values = 0
        query = "insert into mesh(id,variant) values " + query_values +";"
        con.Query(query)
        con.Close()
        print "Extracting MsSH Terms Done!"
class SNOMED(Ontology):
    def __init__(self):
        self.file_name = r'd:\project\ontology\snomed.xml'
        self.file = open(self.file_name)
        
    def CreateTempSNOMEDCoreTable(self):
        print "Creating Temp SNOMED Core Table started..."
        db = DB()
        db.Execute("delete from snomed_core;")
        print 'snomed_temp is Empty!'
        tp = TextProcessor()
        
        value = ''
        values_list = ''
        values_count = 0
        query_header =  "Insert into snomed_core(CONCEPTID,CONCEPTSTATUS,FULLYSPECIFIEDNAME,SNOMEDID) values "
        for line in self.file:
            line_fields = line.split('\t')
            if str(line_fields[1]) == '0':
                value = "( '" + str(line_fields[0]) + "',"
                value += str(line_fields[1])+",'"+tp.Clean(str(line_fields[2]))+"','"
                value += tp.Clean(str(line_fields[4]))+"'),"
                values_list += value
                values_count += 1
                if values_count >100:
                    print "1000 + "
                    db.Insert(query_header + values_list[0:-1])
                    values_count = 0
                    values_list = ''
        db.Insert(query_header + values_list)
        print "Creating Temp SNOMED Core Table Done!"
    def CreateTempSNOMEDRelationshipsTable(self):
        print "Creating Temp SNOMED Core Table Started..."
        rel_file = open(r"D:\Project\Ontologies\SNOMED\SnomedCT_Release_INT_20120731\RF1Release\OtherResources\StatedRelationships\res1_StatedRelationships_Core_INT_20120731.txt")
        db = DB()
        db.Execute("delete from snomed_rel;")
        values_list = ''
        values_count = 0
        query_header =  "Insert into snomed_rel(CONCEPTID1,CONCEPTID2) values "
        print rel_file.readline()
        for line in rel_file:
            line_fields = line.split('\t')
            if str(line_fields[2]).find('116680003') != -1:
                values_list += "( '" + str(line_fields[1])+"','"+str(line_fields[3])+"'),"
                values_count += 1
                if values_count >100:
                    print "1000 + "
                    db.Insert(query_header + values_list[0:-1])
                    values_count = 0
                    values_list = ''
        db.Insert(query_header + values_list)
        print "Creating Temp SNOMED Core Table Done!"
    def GetCoreDict(self):
        db = DB()
        core = db.Exec('select CONCEPTID,FULLYSPECIFIEDNAME from snomed_core')
        print 'snomed_core is ready!'
        print len(core)
        core_dict = dict()
        print "preparing Core Dictionary!"
        for row in core:
            core_dict[row[0]] = row[1]
        print "Core Dictionary is Ready To GO!"
        return core_dict
    def GetRelDict(self):
        ''' =========Preoare the synonym Dictionary ========================'''
        db = DB()
        core_dict = self.GetCoreDict()
        query = 'select CONCEPTID1,CONCEPTID2 from snomed_rel;'
        syn_id_list = db.Exec(query) #370126003 this id should be removed!
        core_syn_dict = dict()
        for cid in syn_id_list:
            core_syn_dict[cid[0]] = ''
        for cid in syn_id_list:
            try:
                core_syn_dict[cid[0]] += core_dict[cid[1]] + ' , '
            except:
                print "one got away!"
        print "Relationship Dictionary Created!"
        return core_syn_dict
        
    def ExtractTerms(self):
        db = DB()
        '''
        Using 
        snomed_core (CONCEPTID,CONCEPTSTATUS,FULLYSPECIFIEDNAME,CTV3ID,SNOMEDID)
        and
        snomed_rel (CONCEPTID1,CONCEPTID2)
        to create snomed (id,variant)
        table.
        '''
        print "Extracting SNOMED Terms started..."
        db.Exec("delete from snomed;")
        core = db.Exec('select CONCEPTID,FULLYSPECIFIEDNAME from snomed_core')
        rel_dict = self.GetRelDict()
        value_list = ''
        value = ''
        value_count = 0
        query_header = "Insert into snomed(id , variant) values"
        total_done = 0
        for row in core:
            cid = row[0]
            title = row[1]
            syn_list = ''
            try:
                syn_list = str(rel_dict[cid])
            except:
                pass
            if len(syn_list) >0:
                title += " , " + syn_list 
            value = "({0},'{1}'),".format(cid,title)
            value_list += value;
            value_count += 1
            if value_count > 1000:
                query = query_header + value_list;
                db.Query(query[0:-1])
                total_done += 1000
                #print str(total_done) + " Done!"
                value_count = 0
                value_list = ''
        if len(value_list)>10:
            query = query_header + value_list;
            db.Query(query[0:-1])
        print "Extracting SNOMED Terms Done!"    
class ICD10(Ontology):
    """ICD10 Handler."""
    #SNOMED folder location
    def __init__(self):
        self.file_name = r'd:\project\ontology\icd102010en.xml'
        self.file = open(self.file_name)
        
    def ExtractTerms(self):
        '''
        snomed_file: It's a file that contains SNOMED
        simple_file: It's a text file that can be filled with SNOMED terms
        simple_file will have a term per line.      
        '''
        print "Extracting ICD10 Terms started..."
        tp = TextProcessor()
        con = DB()
        con.Query('delete from icd10;')
        tree = ET.parse(self.file_name)
        root = tree.getroot()
        query_values = ''
        values_limit = 100
        values = 0
        
        for Class in root.iter(tag='Class'):
            accession = tp.RemoveStopWords(str( Class.attrib['code']))
            variant = ''
            for Rubric in Class.iter(tag='Rubric'):
                for Label in Rubric.iter(tag='Label'):
                    if variant != '':
                        try:
                            variant += ' , ' + tp.RemoveStopWords(str(Label.text))
                        except:
                            print "This is the problem--> ",variant
                    else:
                        try:
                            variant = tp.RemoveStopWords(str(Label.text))
                        except:
                            print "This is the problem--> ",variant
            if(query_values != ''):
                query_values += ",('"+accession+"','"+tp.Clean(variant) +"')" 
            else:
                query_values =  "('"+accession+"','"+tp.Clean(variant) +"')" 
            values += 1
            if values >= values_limit:
                query = "insert into icd10(id,variant) values " + query_values +";"
                try:
                    con.Query(query)
                except:
                    print query
                query_values = ''
                values = 0
        query = "insert into icd10(id,variant) values " + query_values +";"
        con.Query(query)
        con.Close()
        print "Extracting ICD10 Terms Done!"            
    
    
    
    
    
    
    
    
    
    
    
    
    