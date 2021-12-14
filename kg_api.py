# -*- coding: utf-8 -*-
"""
Created on Fri Dec 10 17:02:10 2021

@author: ezgtt
"""


import requests

class KnowledgeGraph():
    
    def __init__(self, dataset):
        self.dataset = dataset
        
    def __get_entity_regex(self, entity):
        
        array = [entity['entity'], entity['entity'].lower()]
        if 'lemma' in entity and entity['entity'].casefold() != entity['lemma'].casefold():
            array.append(entity['lemma'])
            array.append(entity['lemma'].lower())
        return '^'+'|'.join(list(set(array)))+'$'
        
        
    def __get_query(self, entity1, entity2 = None, entity_type = None): 

        
        dataset = self.dataset
        prefix = dataset + ': <https://semrep.nlm.nih.gov/covid19/'+dataset+'#>'
        
        if entity2 is not None: # query for relations        
                    
            entity1_regex = self.__get_entity_regex(entity1)
            entity2_regex = self.__get_entity_regex(entity2)
            query = '''
            PREFIX '''+ prefix +''' 
            PREFIX semrep: <https://semrep.nlm.nih.gov/> 
            PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> 
            
            SELECT DISTINCT  ?subject_text  ?subject_semantic_type ?subject_semantic_type_name  ?predicate ?object_text ?object_semantic_type ?object_semantic_type_name (count(concat(?subject_text, ?subject_semantic_type_name,  ?predicate, ?object_text, ?object_semantic_type_name)) as ?count)
            FROM <https://semrep.nlm.nih.gov/covid19/'''+dataset+'''>
            WHERE
            {
              [
                    '''+dataset+''':relation
            		[
            			a semrep:Relation ;
            			'''+dataset+''':subject_semantic_type_for_relation ?Subject_semantic_type ;
            			'''+dataset+''':subject_text ?subject_text ;
            			semrep:predicate ?predicate ;
            			'''+dataset+''':object_semantic_type_for_relation ?Object_semantic_type ;
            			'''+dataset+''':object_text ?object_text ;
            	    ]
                ] .
                ?Subject_semantic_type a semrep:SemanticType .
                ?Subject_semantic_type rdfs:label ?subject_semantic_type.
                ?Subject_semantic_type rdfs:comment ?subject_semantic_type_name.
                ?Object_semantic_type a semrep:SemanticType .
                ?Object_semantic_type rdfs:label ?object_semantic_type.
                ?Object_semantic_type rdfs:comment ?object_semantic_type_name.
                
              FILTER regex(?subject_text, "'''+ entity1_regex +'''", 'i').
              FILTER regex(?object_text, "'''+ entity2_regex +'''", 'i').
            }
            GROUP BY ?subject_text ?subject_semantic_type ?subject_semantic_type_name ?predicate ?object_text ?object_semantic_type ?object_semantic_type_name
            ORDER BY DESC(?count)
            LIMIT 1
            '''
        else: # query for entities
            entity1_regex = self.__get_entity_regex(entity1)
            semantic_type = '?semantic_type' if entity_type is None else '"'+ entity_type +'"'
            query = '''
            PREFIX '''+ prefix +'''
            PREFIX semrep: <https://semrep.nlm.nih.gov/>
            PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
            
            SELECT DISTINCT  ?entity_text ?preferred_name ?semantic_type ?semantic_type_name  (COUNT(concat(?entity_text,?preferred_name, ?semantic_type, ?semantic_type_name )) AS ?count)
            FROM <https://semrep.nlm.nih.gov/covid19/'''+dataset+'''>
            WHERE
              {
                  [
              	    '''+dataset+''':_id ?doc_id ;
                      '''+dataset+''':annotations
                      [
                           '''+dataset+''':entity
                          [
                              a semrep:Entity ;
                              '''+dataset+''':semantic_type ?Semantic_type ;
                              '''+dataset+''':entity_text  ?entity_text ;
                              '''+dataset+''':preferred_name  ?preferred_name ;
                          ]
                      ]
              	] .
                  ?Semantic_type a semrep:SemanticType .
                  ?Semantic_type rdfs:label ''' + semantic_type +'''.
                  ?Semantic_type rdfs:comment ?semantic_type_name.
                FILTER regex(?entity_text, "'''+ entity1_regex +'''", 'i').
              }
              GROUP BY ?entity_text ?semantic_type  ?semantic_type_name ?preferred_name
              ORDER BY DESC(?count)
              LIMIT 1
            '''
        return query
    
    def get_response(self, entity1, entity2 = None, entity_type = None):    
    
        headers = {
            'Accept': 'application/sparql-results+json',
        }       
        
        query = self.__get_query(entity1, entity2 = entity2, entity_type = entity_type)
        data = {'query': query}
        
        response = requests.post('https://sparql.proconsortium.org/virtuoso4/sparql', headers=headers, data=data)
        
        result = None
        if response.status_code == 200 and 'results' in response.json():
            result = response.json()['results']['bindings'][0] if len(response.json()['results']['bindings']) > 0 else None
        
        return result
    

'''
    
def get_relation_result(g, entity1, entity2, response):
    
    entity1_type = response['subject_semantic_type']['value']
    entity2_type = response['object_semantic_type']['value']    
    
    response_subj_type = g.get_response(entity1, entity1_type)
    response_obj_type = g.get_response(entity2, entity2_type)        
        
    subject_preferred_name = response_subj_type['subject_semantic_type_name']['value'] if response_subj_type is not None else ''
    object_preferred_name = response_obj_type['object_semantic_type_name']['value'] if response_obj_type is not None else ''    
    
    result = {
        'relation_type': response['predicate']['value'],
        'subject':{
            'corpus_text': entity1['entity'],
            'entity_text': response['subject_text']['value'],
            'preferred_name': subject_preferred_name,
            'type': response['subject_semantic_type_name']['value'],
            },
        'object':{
            'corpus_text': entity2['entity'],
            'entity_text': response['object_text']['value'],
            'preferred_name': object_preferred_name,
            'type': response['object_semantic_type_name']['value'],
            }
        }   

    
    response = g.get_response(entity2, entity2 = entity1)
    
    if response is None: return [result]
    
    subj_type = response['subject_semantic_type']['value']
    obj_type = response['object_semantic_type']['value']
    
    response_subj_type = g.get_response(entity2, subj_type)
    response_obj_type = g.get_response(entity1, obj_type) 
        
    subject_preferred_name = response_subj_type['subject_semantic_type_name']['value'] if response_subj_type is not None else ''
    object_preferred_name = response_obj_type['object_semantic_type_name']['value'] if response_obj_type is not None else ''  
    
    result_reversed = {
        'relation_type': response['predicate']['value'],
        'subject':{
            'corpus_text': entity2['entity'],
            'entity_text': response['subject_text']['value'],
            'preferred_name': subject_preferred_name,
            'type': response['subject_semantic_type_name']['value'],
            },
        'object':{
            'corpus_text': entity1['entity'],
            'entity_text': response['object_text']['value'],
            'preferred_name': object_preferred_name,
            'type': response['object_semantic_type_name']['value'],
            }
        }
    
    return [result,result_reversed]
    
'''
def get_preferred_name(g, subj_entity, obj_entity, response):
    
    subj_type = response['subject_semantic_type']['value']
    obj_type = response['object_semantic_type']['value']   
    
    response_subj = g.get_response(subj_entity, entity_type = subj_type)
    response_obj = g.get_response(obj_entity, entity_type = obj_type)    

    subj_name = response_subj['preferred_name']['value'] if response_subj is not None else ''
    obj_name = response_obj['preferred_name']['value'] if response_obj is not None else ''
    
    return subj_name, obj_name
    
    

def get_response(entity1, entity2 = None):
    
    datasets = ['litcovid', 'cord19']
    
    result = None
    for dataset in datasets:
        
        g = KnowledgeGraph(dataset)
        
        if entity2 is not None: # relation
        
            response_1 = g.get_response(entity1, entity2 = entity2)
            response_2 = g.get_response(entity2, entity2 = entity1)
            
            if response_1 is None and response_2 is None: continue            
            
            result = []
            if response_1 is not None:
                res = response_1
                subj, obj = entity1['entity'], entity2['entity']
                sub_preferred_name, obj_preferred_name = get_preferred_name(g, entity1, entity2, response_1)
                result_to_add = {
                    'relation_type': res['predicate']['value'],
                    'subject':{
                        'corpus_text': subj,
                        'entity_text': res['subject_text']['value'],
                        'preferred_name': sub_preferred_name,
                        'type': res['subject_semantic_type_name']['value'],
                        },
                    'object':{
                        'corpus_text': obj,
                        'entity_text': res['object_text']['value'],
                        'preferred_name': obj_preferred_name,
                        'type': res['object_semantic_type_name']['value'],
                        }
                    }
                result.append(result_to_add)
                
            if response_2 is not None:
                res = response_2
                subj, obj = entity2['entity'], entity1['entity']
                sub_preferred_name, obj_preferred_name = get_preferred_name(g, entity2, entity1, response_2)
                result_to_add = {
                    'relation_type': res['predicate']['value'],
                    'subject':{
                        'corpus_text': subj,
                        'entity_text': res['subject_text']['value'],
                        'preferred_name': sub_preferred_name,
                        'type': res['subject_semantic_type_name']['value'],
                        },
                    'object':{
                        'corpus_text': obj,
                        'entity_text': res['object_text']['value'],
                        'preferred_name': obj_preferred_name,
                        'type': res['object_semantic_type_name']['value'],
                        }
                    }
                result.append(result_to_add)
                
            return result

        else: # entity
            response = g.get_response(entity1)
            
            if response is None: continue
        
            result = {
                'corpus_text': entity1['entity'],
                'entity_text': response['entity_text']['value'],
                'preferred_name': response['preferred_name']['value'],
                'entity_type': response['semantic_type_name']['value'],
                }
            return result
            
    return result


