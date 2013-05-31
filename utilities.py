import _mysql
import MySQLdb as mdb
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
from nltk.stem.porter import PorterStemmer
import re
from MySQLdb.constants.ER import NEW_ABORTING_CONNECTION
class DB:
    _conn = mdb.connect('localhost', 'root', '', 'ir')
    _con = _mysql.connect('localhost', 'root', '', 'ir')
    _cur = _conn.cursor()
    def __init__(self):
        try:
            self.conn = mdb.connect('localhost', 'root', '', 'ir')
            self.con = None
            self.con = _mysql.connect('localhost', 'root', '', 'ir')
            self.cur = self.conn.cursor()
        except:
            print "Cannot reach the Database..."
            raise SystemExit
    @staticmethod
    def _execute(query):
        DB._cur.execute(query)
        rows = DB._cur.fetchall()
        return rows
    def Query(self,query):
        return self.con.query(query)
    @staticmethod
    def _query(query):
        return DB._con.query(query)
    def Execute(self,query):
        self.cur.execute(query)
        rows = self.cur.fetchall()
        return rows
    def Insert(self,query):
        x = self.conn.cursor()
        try:
            x.execute(query)
            self.conn.commit()
        except:
            self.conn.rollback()
    def Update(self,query):
        x = self.conn.cursor()
        try:
            x.execute(query)
            self.conn.commit()
        except:
            self.conn.rollback()
    def Exec(self,query):
        x = self.conn.cursor()
        rows = ['Error']
        try:
            x.execute(query)
            self.conn.commit()
            rows = x.fetchall()
        except:
            self.conn.rollback()
        finally:
            return rows
    def Close(self):
        if self.con:
            self.con.close()
class TextProcessor:
    text_splitter = ','
    _word_splitter = ' '
    _text_splitter = ','
    def Splitter(self):
        return ','
    def WordSplitter(self):
        return ' '
    def Clean(self,line):
        line = line.replace("\n", '')
        line = line.replace("\r", '')
        line = line.replace("'", ' ')
        line = line.replace('"', ' ')
        line = line.replace('\t', ' ')
        #line = line.replace(',', ' ')
        line = line.replace('(', ' ')
        line = line.replace(')', ' ')
        line = line.replace(' 1 ', ' ')
        line = line.replace(' 2 ', ' ')
        line = line.replace(' 3 ', ' ')
        line = line.replace(' 4 ', ' ')
        line = line.replace(' 5 ', ' ')
        line = line.replace(' 6 ', ' ')
        line = line.replace(' 7 ', ' ')
        line = line.replace(' 8 ', ' ')
        line = line.replace(' 9 ', ' ')
        line = line.replace(' 0 ', ' ')
        line = line.replace('=', ' ')
        line = line.replace('[', ' ')
        line = line.replace(']', ' ')
        line = line.replace(':', ' ')
        line = line.replace('+', ' ')
        line = line.replace(' -', ' ')
        line = line.replace('- ', ' ')
        return line
    def CleanDoc(self,line):
        line = line.replace("'", ' ')
        line = line.replace('"', ' ')
        #line = line.replace('(', ' ')
        #line = line.replace(')', ' ')
        return line
    def CleanText(self,text):
        bag = ["'",'"','(',')','%','/',':',';','.',',','[',']','}','{',' - ',' -','- ', ' x ' , ' x ']
        for i in range(1000):
            bag.append(str(i))
        for item in bag:
            text = text.replace(item, ' ')
        text = self.CleanSpace(text)
        return text
    @staticmethod
    def _clean_text(word_list):
        bag = ['',' ','  ',"'",'"','(',')','%','/',':',';','.',',','[',']','}','{',' - ',' -','- ', ' x ' , ' x ']
        for i in range(1000):
            bag.append(str(i))
        for item in bag:
            if item in word_list:
                word_list.remove(item)
        return word_list
    @staticmethod
    def _normalize_word(word):
        """ to lower
        """
        word = word.lower()
        bag = ['',' ','  ',"'",'"','(',')','%','/',':',';','.',',','[',']','}','{',' - ',' -','- ', ' x ']
        for item in bag:
            word = word.replace(item,'')
        return word
    def CleanPMID(self,PMID):
        PMID = PMID.replace('\n','')
        PMID = PMID.replace('\r','')
        PMID = PMID.replace(' ','')
        PMID = PMID.replace("PMID-",'')
        return PMID
    def CleanSpace(self,text):
        while text.find('  ') != -1:
            text = text.replace("  "," ")
        return text
    def RemoveStopWords(self, text):
        '''
        To remove Stopwords from a given document
        '''
        text = text.lower()
        text = self.CleanText(text)
        return text
        stopwords = ['a','b','c','d','e','f','g','h','i','j','k','l','m','n','o','p','q','r','s','t','u','v','w','x','y','z','0','1','2','3','4','5','6','7','8','9','i','me','my','myself','we','our','ours','ourselves','you','your','yours','yourself','yourselves','he','him','his','himself','she','her','hers','herself','it','its','itself','they','them','their','theirs','themselves','what','which','who','whom','this','that','these','those','am','is','are','was','were','be','been','being','have','has','had','having','do','does','did','doing','a','an','the','and','but','if','or','because','as','until','while','of','at','by','for','with','about','against','between','into','through','during','before','after','above','below','to','from','up','down','in','out','on','off','over','under','again','further','then','once','here','there','when','where','why','how','all','any','both','each','few','more','most','other','some','such','no','nor','not','only','own','same','so','than','too','very','s','t','can','will','just','don','should','now']
        u_list= text.split(self.WordSplitter())
        u_list = [word for word in u_list if word not in stopwords]
        return ' '.join(u_list)
    @staticmethod
    def _remove_stop_words(word_list):
        '''
        word_list is self explanatory
        To remove Stopwords from a given document
        '''
        for word in word_list:
            word = word.lower()
        word_list = TextProcessor._clean_text(word_list)
        stopwords = ['-','+','a','b','c','d','e','f','g','h','i','j','k','l','m','n','o','p','q','r','s','t','u','v','w','x','y','z','0','1','2','3','4','5','6','7','8','9','i','me','my','myself','we','our','ours','ourselves','you','your','yours','yourself','yourselves','he','him','his','himself','she','her','hers','herself','it','its','itself','they','them','their','theirs','themselves','what','which','who','whom','this','that','these','those','am','is','are','was','were','be','been','being','have','has','had','having','do','does','did','doing','a','an','the','and','but','if','or','because','as','until','while','of','at','by','for','with','about','against','between','into','through','during','before','after','above','below','to','from','up','down','in','out','on','off','over','under','again','further','then','once','here','there','when','where','why','how','all','any','both','each','few','more','most','other','some','such','no','nor','not','only','own','same','so','than','too','very','s','t','can','will','just','don','should','now']
        u_list = [word for word in word_list if word not in stopwords]
        punctuation = re.compile(r'[-+.?!,":;()|0-9]')
        u_list = [punctuation.sub("", word) for word in u_list]    
        return u_list
    def Tokenize(self,text):
        return [t for t in word_tokenize(text)]
    @staticmethod
    def _tokenize(text):
        results = [TextProcessor._normalize_word(t) for t in word_tokenize(text)]
        new_result = []
        for term in results:
            if term != '':
                new_result.append(term)
        return new_result
    def EliminateStopWords(self, wordList):
        '''
        Input: Tokenized word list
        
        from nltk.tokenize import word_tokenize
        word_list = [t for t in word_tokenize(doc)]
        '''
        filtered_words = [w for w in wordList if not w in stopwords.words('english')]
        punctuation = re.compile(r'[-+.?!,":;()|0-9]')
        filtered_words = [punctuation.sub("", word) for word in filtered_words]    
        return filtered_words
    def Stem(self,wordList):
        #LStem = LancasterStemmer()
        PStem = PorterStemmer()
        wordSet = []
        for word in wordList:
            temp = PStem.stem(word)
            wordSet.append(temp)
        return wordSet
    @staticmethod
    def _stem(wordList):
        #LStem = LancasterStemmer()
        PStem = PorterStemmer()
        wordSet = []
        for word in wordList:
            temp = PStem.stem(word)
            wordSet.append(temp)
        return wordSet
    @staticmethod
    def CleanList(the_list, val):
        while val in the_list:
            the_list.remove(val)
    def remove_values_from_list(self,the_list, val):
        while val in the_list:
            the_list.remove(val)
class Operation:
    def unique(self,a):
        """ return the list with duplicate elements removed """
        return list(set(a))
    
    def intersect(self,a, b):
        """ return the intersection of two lists """
        return list(set(a) & set(b))
    
    def union(self,a, b):
        """ return the union of two lists """
        return list(set(a) | set(b))
    
    @staticmethod
    def _union(a, b):
        """ return the union of two lists """
        return list(set(a) | set(b))
class Parameter:
    predecessors_depth = 4
    def GetDocNumber(self):
        return 500
    def GetDocNumberForLocalContext(self):
        return 5
class GlobalVariables():
    remove_stop_words = True
    stem = False
    global_context_activated = False
    ontology_based_IR_activated = False
    add_concepts_multiple_times = False
    adding_concept_times = 5
    result_title = "Exp_T_F_F_F"
    
    
    