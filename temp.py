from IR import OntologyBasedIR
from models import Collection,Ontology,GlobalVariables
from output import Display
from models import Query
from utilities import DB

csir = OntologyBasedIR()
#csir.Indexing()
#result_ontology_based

csir._experiment(True,True,False,False)
