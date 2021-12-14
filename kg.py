# -*- coding: utf-8 -*-
"""
Created on Mon Dec  6 10:06:57 2021

@author: ezgtt
"""

from tqdm import tqdm
from rdflib import Graph
from glob import glob


DATASETS = ['litcovid', 'cord19']

DRANGE = ['abstract', 'full']

ANNOTATORS = ['rlimsp', 'efip', 'mirtex']

COMBINATIONS = [(dataset, annotator) for dataset in DATASETS
                                        for annotator in ANNOTATORS]
GRAPH_DICT = {
    'iTextMine':{},
    'RemSep':{}
    }


class KnowledgeGraph():
    
    def __init__(self, graph_type , dataset, folder = 'kg/', drange = None, annotator = None):
        
        self.folder = folder
        self.graph_type = graph_type
        self.dataset = dataset
        if drange == 'abstract':
            self.drange = 'medline'
        elif drange == 'full':
            self.drange = 'pmc'
        else:
            self.drange = drange
        self.annotator = annotator
        
    def __get_prefix(self):        
        
        if self.graph_type == 'RemSep':
            prefix = self.dataset + ': <https://semrep.nlm.nih.gov/covid19/'+ self.dataset +'#>'
        elif self.graph_type == 'iTextMine':
            array = [self.dataset, self.drange, self.annotator]
            prefix = self.annotator +': <https://research.bioinformatics.udel.edu/itextmine/'+ '/'.join(array) +'#>'

        return prefix
    
    def __get_filenames(self):        
        
        if self.graph_type == 'RemSep':
            filenames = glob(self.folder + self.dataset + '_semrep/*.ttl')
        elif self.graph_type == 'iTextMine':
            array = [self.dataset, self.drange, self.annotator]
            ttl = self.folder + '_'.join(array) + '.ttl'
            filenames = ttl.split()
        return filenames
    
    def graph(self):
        
        filenames = self.__get_filenames()
        g = Graph()
        for filename in tqdm(filenames):
            g.parse(filename, format = 'ttl')
        return g
    
    def get_relation_query(self, entity1, entity2):
        
        prefix = self.__get_prefix()
        
        if self.graph_type == 'RemSep':
            
            query = '''          
            PREFIX '''+ prefix +'''
            PREFIX semrep: <https://semrep.nlm.nih.gov/>
            PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
            
            SELECT DISTINCT  ?subject_text  ?subject_semantic_type ?subject_semantic_type_name  ?predicate ?object_text ?object_semantic_type ?object_semantic_type_name (count(concat(?subject_text, ?subject_semantic_type_name,  ?predicate, ?object_text, ?object_semantic_type_name)) as ?count)
            FROM <https://semrep.nlm.nih.gov/covid19/litcovid>
            WHERE
            {
              [
                    '''+ self.dataset +''':relation
            		[
            			a semrep:Relation ;
            			'''+ self.dataset +''':subject_semantic_type_for_relation ?Subject_semantic_type ;
            			'''+ self.dataset +''':subject_text "'''+ entity1 +'''" ;
            			semrep:predicate ?predicate ;
            			'''+ self.dataset +''':object_semantic_type_for_relation ?Object_semantic_type ;
            			'''+ self.dataset +''':object_text "'''+ entity2 +'''"" ;
            	    ]
                ] .
                ?Subject_semantic_type a semrep:SemanticType .
                ?Subject_semantic_type rdfs:label ?subject_semantic_type.
                ?Subject_semantic_type rdfs:comment ?subject_semantic_type_name.
                ?Object_semantic_type a semrep:SemanticType .
                ?Object_semantic_type rdfs:label ?object_semantic_type.
                ?Object_semantic_type rdfs:comment ?object_semantic_type_name.
            }
            GROUP BY ?subject_text ?subject_semantic_type ?subject_semantic_type_name ?predicate ?object_text ?object_semantic_type ?object_semantic_type_name
            ORDER BY DESC(?count)
            LIMIT 50
            '''
        elif self.graph_type == 'iTextMine':
            
            annotator = self.annotator
            query= '''
            PREFIX '''+ prefix +'''

            SELECT DISTINCT (?relation_duid as ?relation) ?relationType (?entity_duid1 as ?entity1) ?entity_text1 ?role1 (?entity_duid2 as ?entity2) ?entity_text2 ?role2
            
            WHERE {
            [ '''+ annotator +''':entity
                      [ ?entity1
                                [ '''+ annotator +''':duid ?entity_duid1 ;
                                  '''+ annotator +''':entityText "'''+ entity1 +'''";
                  	]
                  ];
              '''+ annotator +''':entity
                      [ ?entity2
                                [ '''+ annotator +''':duid ?entity_duid2 ;
                                  '''+ annotator +''':entityText "'''+ entity2 +'''";
                  	]
                  ];
              '''+ annotator +''':relation
                      [ ?relation
                                [ '''+ annotator +''':argument
                                          [ '''+ annotator +''':entity_duid ?entity_duid1 ;
                                            '''+ annotator +''':role ?role1
                                          ] ;
                                  '''+ annotator +''':argument
                                          [ '''+ annotator +''':entity_duid ?entity_duid2 ;
                                            '''+ annotator +''':role ?role2
                                          ] ;
                                  '''+ annotator +''':duid ?relation_duid ;
                                  '''+ annotator +''':relationType ?relationType ;
                                  '''+ annotator +''':source ?source
                                ] 
            
                      ]
            ]
            
            }
            LIMIT 10
            '''           
            
        return query
        
    
    def get_entity_query(self, entity, limit = 1):
        
        prefix = self.__get_prefix()        
        
        if self.graph_type == 'RemSep':
            
            query = '''
            PREFIX ''' + prefix +'''
            PREFIX semrep: <https://semrep.nlm.nih.gov/>
            PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
            
            SELECT DISTINCT  ?entity_text ?preferred_name ?semantic_type ?semantic_type_name  (COUNT(concat(?entity_text,?preferred_name, ?semantic_type, ?semantic_type_name )) AS ?count)
            
            WHERE
            {
                [
            	    '''+ self.dataset +''':_id ?doc_id ;
                    '''+ self.dataset +''':annotations
                    [
                         litcovid:entity
                        [
                            a semrep:Entity ;
                            '''+ self.dataset +''':semantic_type ?Semantic_type ;
                            '''+ self.dataset +''':entity_text  ?entity_text ;
                            '''+ self.dataset +''':preferred_name  ?preferred_name ;
                        ]
                    ]
            	] .
                ?Semantic_type a semrep:SemanticType .
                ?Semantic_type rdfs:label ?semantic_type.
                ?Semantic_type rdfs:comment ?semantic_type_name.
            }
            GROUP BY ?entity_text ?semantic_type  ?semantic_type_name ?preferred_name
            ORDER BY DESC(?count)
            LIMIT 50
            '''
        elif self.graph_type == 'iTextMine':
            annotator = self.annotator
            query= '''
            PREFIX '''+ prefix +'''
            
            SELECT DISTINCT ?source ?duid ?entityText ?entityType
            
            WHERE {
            [ 
              '''+ annotator +''':entity
                      [ ?entity 
                                [ '''+ annotator +''':duid ?duid ;
                                  '''+ annotator +''':entityText "'''+ entity +'''";
                                  '''+ annotator +''':entityType ?entityType ;
                                  '''+ annotator +''':source ?source
                                ] 
            		]
            ]
            }
            
            LIMIT ''' + str(limit)
        return query
    
    
class iTextMine():   

    def __init__(self, datasource):
        
        self.dataset = datasource[0]
        self.drange = datasource[1]
        self.annotator = datasource[2]      


    def __get_filename(self):
        
        array = [self.dataset, self.drange, self.annotator]
        ttl = FOLDER + '_'.join(array) + '.ttl'
        
        return ttl
    
    def __get_prefix(self):
        
        array = [self.dataset, self.drange, self.annotator]
        link = self.annotator +': <https://research.bioinformatics.udel.edu/itextmine/'+ '/'.join(array) +'#>'
        
        return link
 
    def graph(self):
        
        filename = self.__get_filename()
        g = Graph()
        g.parse(filename, format = 'ttl')
        
        return g  

    def get_entity_query(self, entity, limit):
        
        prefix = self.__get_prefix()
        annotator = self.annotator
        query= '''
        PREFIX '''+ prefix +'''
        
        SELECT DISTINCT ?source ?duid ?entityText ?entityType
        
        WHERE {
        [ 
          '''+ annotator +''':entity
                  [ ?entity 
                            [ '''+ annotator +''':duid ?duid ;
                              '''+ annotator +''':entityText "'''+ entity +'''";
                              '''+ annotator +''':entityType ?entityType ;
                              '''+ annotator +''':source ?source
                            ] 
        		]
        ]
        }
        
        LIMIT ''' + str(limit)
        return query
    
    def get_relation_query(self, entity1, entity2):
        
        prefix = self.__get_prefix()
        annotator = self.annotator
        query= '''
        PREFIX '''+ prefix +'''

        SELECT DISTINCT (?relation_duid as ?relation) ?relationType (?entity_duid1 as ?entity1) ?entity_text1 ?role1 (?entity_duid2 as ?entity2) ?entity_text2 ?role2
        
        WHERE {
        [ '''+ annotator +''':entity
                  [ ?entity1
                            [ '''+ annotator +''':duid ?entity_duid1 ;
                              '''+ annotator +''':entityText "'''+ entity1 +'''";
              	]
              ];
          '''+ annotator +''':entity
                  [ ?entity2
                            [ '''+ annotator +''':duid ?entity_duid2 ;
                              '''+ annotator +''':entityText "'''+ entity2 +'''";
              	]
              ];
          '''+ annotator +''':relation
                  [ ?relation
                            [ '''+ annotator +''':argument
                                      [ '''+ annotator +''':entity_duid ?entity_duid1 ;
                                        '''+ annotator +''':role ?role1
                                      ] ;
                              '''+ annotator +''':argument
                                      [ '''+ annotator +''':entity_duid ?entity_duid2 ;
                                        '''+ annotator +''':role ?role2
                                      ] ;
                              '''+ annotator +''':duid ?relation_duid ;
                              '''+ annotator +''':relationType ?relationType ;
                              '''+ annotator +''':source ?source
                            ] 
        
                  ]
        ]
        
        }
        LIMIT 10
        '''
        return query
   


class SemRep():
    
    def __int__(self, dataset):
        self.dataset = dataset
        
    def graph(self):
        file_names = glob('kg/' + self.dataset + '_semrep/*.ttl')
        g = Graph()
        for file_name in file_names:
            g.parse(file_name)            
        return g
    
    def __get_prefix(self):
        prefix = self.dataset + ': <https://semrep.nlm.nih.gov/covid19/'+ self.dataset +'#>'
        return prefix
        
    def get_relation_query(self, entity1, entity2):
        
        prefix = self.__get_prefix()
        query = '''
        PREFIX '''+ prefix +'''
        PREFIX semrep: <https://semrep.nlm.nih.gov/>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        
        SELECT DISTINCT  ?subject_text  ?subject_semantic_type ?subject_semantic_type_name  ?predicate ?object_text ?object_semantic_type ?object_semantic_type_name (count(concat(?subject_text, ?subject_semantic_type_name,  ?predicate, ?object_text, ?object_semantic_type_name)) as ?count)
        FROM <https://semrep.nlm.nih.gov/covid19/litcovid>
        WHERE
        {
          [
                '''+ self.dataset +''':relation
        		[
        			a semrep:Relation ;
        			'''+ self.dataset +''':subject_semantic_type_for_relation ?Subject_semantic_type ;
        			'''+ self.dataset +''':subject_text "'''+ entity1 +'''" ;
        			semrep:predicate ?predicate ;
        			'''+ self.dataset +''':object_semantic_type_for_relation ?Object_semantic_type ;
        			'''+ self.dataset +''':object_text "'''+ entity2 +'''"" ;
        	    ]
            ] .
            ?Subject_semantic_type a semrep:SemanticType .
            ?Subject_semantic_type rdfs:label ?subject_semantic_type.
            ?Subject_semantic_type rdfs:comment ?subject_semantic_type_name.
            ?Object_semantic_type a semrep:SemanticType .
            ?Object_semantic_type rdfs:label ?object_semantic_type.
            ?Object_semantic_type rdfs:comment ?object_semantic_type_name.
        }
        GROUP BY ?subject_text ?subject_semantic_type ?subject_semantic_type_name ?predicate ?object_text ?object_semantic_type ?object_semantic_type_name
        ORDER BY DESC(?count)
        LIMIT 50
        '''
        return query



     
    
def get_graph(graph_type, dataset, drange = None, annotator = None):
    
    if graph_type == 'iTextMine':
        datasource = [dataset, drange, annotator]
        key = '-'.join(datasource)
    elif graph_type == 'RemSep':
        key = dataset 
    
    if key not in GRAPH_DICT[graph_type]:
        
        kg = KnowledgeGraph(graph_type, dataset, drange = drange, annotator = annotator)
        graph = kg.graph()
        
        GRAPH_DICT[graph_type].update({key: [kg, graph]})
        
    return GRAPH_DICT[graph_type][key][0], GRAPH_DICT[graph_type][key][1]
        

def get_entity(entity, limit = 1):
    
    entities = []
    for datasource in tqdm(DATASOURCES[:6]):
        
        kg, graph = get_graph(datasource)
        query = kg.get_entity_query(entity, limit)
        results = graph.query(query)
        
        if results is None: continue        
        
        for result in results:            

            entity_duid = str(result[1])
            entity_type = str(result[3])
            
            dict_to_add = {
                'entity_text': entity,
                'entity_duid': entity_duid,
                'entity_type': entity_type,
                'datasource': datasource
                }
            
            entities.append(dict_to_add)
    
    return entities

def get_relation(graph_type, entity1, entity2, dranges = ['abstract']):
    
    relations = []
    
    if graph_type == 'iTextMine':
        
        for combination in COMBINATIONS:
            
            dataset = combination[0]
            annotator = combination[1]
            
            for drange in dranges:               
                
                kg, graph = get_graph(graph_type, dataset, drange = drange, annotator = annotator)
                query = kg.get_relation_query(entity1, entity2)
                results = graph.query(query)
                
                for result in results:
                    dict_to_add = {
                        'dataset': dataset,
                        'annotator': annotator,
                        'drange': drange,
                        'relation': {
                            'id': str(result[0]),
                            'type': str(result[1]),
                            },
                        'entity1': {
                            'id' : str(result[2]),
                            'text': entity1,
                            'role': str(result[4]),
                            },
                        'entity2': {
                            'id' : str(result[5]),
                            'text': entity1,
                            'role': str(result[7]),
                            }
                        }
                    relations.append(dict_to_add)
                    
    elif graph_type == 'RemSep':        

        for dataset in DATASETS:
            kg, graph = get_graph(graph_type, dataset)
            query = kg.get_relation_query(entity1, entity2)
            print(query)
            result = graph.query(query)
            relations.append(result)
            
    return relations






results = get_relation('RemSep','Hsa-miR-217', 'sirtuin 1')
for result in results:
    print(result)
