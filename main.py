from IR import OntologyBasedIR
from models import Collection,Ontology,GlobalVariables
from output import Display
from models import Query
from utilities import DB

csir = OntologyBasedIR()
#Collection._extract_from_file(1)
#Collection._extract_from_file(2)
#Ontology._initialize()
#csir.extract_concepts()
#csir.DocumentExpantion()
#csir.Indexing()
#result_ontology_based

csir._experiment(True,True,False,False)
