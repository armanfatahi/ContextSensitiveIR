import xml.etree.ElementTree as ET
from Utility import DB
from Utility import TextProcessor

class Ontology:
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
    '''Gene Ontology Handler'''
    def __init__(self):
        self.file_name = r'd:\project\ontology\go.xml'
        self.go_file = open(self.file_name)
        
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
            for synonym in term.iter(tag='synonym'):
                temp_syn = synonym.text;
                if temp_syn.find('GO:') == -1:
                    variant += ' , ' + temp_syn
            if(query_values != ''):
                query_values += ",('"+accession+"','"+tp.Clean(variant) +"')" 
            else:
                query_values =  "('"+accession+"','"+tp.Clean(variant) +"')" 
            values += 1
            if values >= values_limit:
                query = "insert into go(id,variant) values " + query_values +";"
                con.Query(query)
                query_values = ''
                values = 0
        query = "insert into go(id,variant) values " +query_values+";"
        con.Query(query)
        con.Close()
        print 'Extracting GO terms Done!'
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
                        variant += ' , ' + tp.RemoveStopWords(str(Label.text))
                    else:
                        variant += tp.RemoveStopWords(str(Label.text))
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