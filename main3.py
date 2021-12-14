# -*- coding: utf-8 -*-
"""
Created on Fri Dec 10 18:35:39 2021

@author: ezgtt
"""

import pandas as pd
import os
from tqdm import tqdm
import csv
import spacy
from itertools import combinations
import json

import kg_api as kg
from preprocess import clean_corpus
import noisy

POS_TAGS = ['NOUN','PROPN','X']
#NOT_NOISY_POS_TAGS = ['ADJ', 'ADJ', 'NOUN', 'PROPN', 'VERB', 'X']
#NOISY_POS_TAGS = ['DET','PART','CCONJ','NUM', 'PRON', 'SYM', 'SPACE', 'PUNCT'] #?

NOISY = noisy.NOISY
VERSION = 9

nlp = spacy.load('en_core_web_sm') 
#nlp.add_pipe('merge_noun_chunks')
#nlp.add_pipe('merge_noun_chunks')
#nlp_token = spacy.load('en_core_web_sm')
        
## get corpus
def get_corpus(filename):
    folder = 'data/'
    pkl_path = folder + filename + '.pkl'
    if os.path.isfile(pkl_path):
        df = pd.read_pickle(pkl_path)
    else:
        df = pd.read_json(folder + filename + '.json')
        df.to_pickle(pkl_path)
    return df
df = get_corpus('10k_aylien_covid_news_data')

def write_sent_record(index, sent_index, sent, responses, ent1, ent2):
    
    path = 'result/v' + str(VERSION) + '_sents.csv'
    
    if not os.path.isfile(path):
        header = ['corpus_index', 'sent_index', 'original_sent', 'labeled_sent', 'relation_type', 'first_entity', 'second_entity', 'first_text','second_text','json']
        with open(path, 'w', newline='') as file:
            writer = csv.writer(file, delimiter=';')
            writer.writerow(header)
    
    for response in responses:
        
        new_sent = sent.replace(response['subject']['corpus_text'], '@' + response['subject']['type'] + '$')
        new_sent = new_sent.replace(response['object']['corpus_text'], '@' + response['object']['type'] + '$')
        

        row_sent = [index, sent_index, sent, new_sent, response['relation_type']]
        row_to_add = row_sent        

        
        if ent1['left_index'] > ent2['left_index'] and response['subject']['corpus_text'] == ent1['entity']:
            row_to_add.append('Subject')
            row_to_add.append('Object')
        elif ent2['left_index'] > ent1['left_index'] and response['subject']['corpus_text'] == ent2['entity']:
            row_to_add.append('Subject')
            row_to_add.append('Object')
        else:
            row_to_add.append('Object')
            row_to_add.append('Subject')
        
        row_to_add.append(response)
        
        with open(path, 'a', newline='') as file:
            writer = csv.writer(file, delimiter=';')
            writer.writerow(row_to_add)

    
def write_token_record(tokens, labels, jsons):
    
    path = 'result/v' + str(VERSION) + '_tokens.csv'
    
    if not os.path.isfile(path):
        header = ['token','label','json']
        with open(path, 'w', newline='') as file:
            writer = csv.writer(file, delimiter=';')
            writer.writerow(header)
    
    rows_to_add = []
    for token, label, json_cell in zip(tokens,labels, jsons):
        rows_to_add.append([token, label, json_cell])
    
    with open(path, 'a', newline='') as file:
        writer = csv.writer(file, delimiter=';')
        writer.writerows(rows_to_add)
        writer.writerow([])
    
def change_label(labels, jsons, ent, response):
    
    #if response is list: # from relation, need to label two entities

    comb = ent
    res = response[0]
    
    if comb[0] == res['subject']['corpus_text']:
        
        left_index = comb[0]['left_index']
        right_index = comb[0]['right_index']
        labels[left_index:right_index + 1] = [res['subject']['type']] * ( right_index + 1 - left_index )
        jsons[left_index:right_index + 1] = [json.dumps(res)] * ( right_index + 1 - left_index )
        
        left_index = comb[1]['left_index']
        right_index = comb[1]['right_index']
        labels[left_index:right_index + 1] = [res['object']['type']] * ( right_index + 1 - left_index )
        jsons[left_index:right_index + 1] = [json.dumps(res)] * ( right_index + 1 - left_index )
        
    else:
    
        left_index = comb[0]['left_index']
        right_index = comb[0]['right_index']
        labels[left_index:right_index + 1] = [res['object']['type']] * ( right_index + 1 - left_index )
        jsons[left_index:right_index + 1] = [json.dumps(res)] * ( right_index + 1 - left_index )
        
        left_index = comb[1]['left_index']
        right_index = comb[1]['right_index']
        labels[left_index:right_index + 1] = [res['subject']['type']] * ( right_index + 1 - left_index )
        jsons[left_index:right_index + 1] = [json.dumps(res)] * ( right_index + 1 - left_index )
    '''
    else:
        
        left_index = ent['left_index']
        right_index = ent['right_index']
        labels[left_index:right_index+1] = response['entity_type'] * ( right_index + 1 - left_index )
        jsons[left_index:right_index+1] = json.dumps(response) * ( right_index + 1 - left_index )
    '''
    
    return labels, jsons
    

for index, row in tqdm(df.iterrows(), total=df.shape[0]):
#for index, row in tqdm(df.iloc[5:6].iterrows(), total=df.iloc[5:6].shape[0]):
    
    corpus = row['body']    
    corpus = clean_corpus(corpus)
    
    
    try:
        sent_index = -1
        for sent in nlp(corpus).sents:
            
            try:
            
                sent_index += 1        
                ents = []
        
                for phrase in nlp(str(sent)).noun_chunks:          
        
                    if len(phrase) == 1:        
        
                        if phrase.text.casefold() in NOISY or phrase.lemma_.casefold() in NOISY or len(phrase.text) < 2: continue
                        
                        ents.append({
                            'entity': phrase.text,
                            'lemma': phrase.lemma_,
                            'left_index': phrase[0].left_edge.i,
                            'right_index': phrase[0].right_edge.i,
                            })
                        
                    else:                
                        i = 0
                        first_index = None
                        for token in phrase:
                            if i == 0: first_index = token.i
                            i += 1
                            if token.pos_ not in POS_TAGS: continue   
                            left_index = token.i
                            right_index =  first_index + len(phrase) - 1
                            new_phrase = sent[left_index : right_index + 1]
                            break
                        if str(new_phrase).casefold() in NOISY or new_phrase.lemma_.casefold() in NOISY or len(str(new_phrase)) < 2: continue
                        ents.append({
                           'entity': str(new_phrase),
                           'lemma': new_phrase.lemma_,
                           'left_index': left_index,
                           'right_index': right_index,
                           })
                
                for token in nlp(str(sent)):
                    tokens = [token.text for token in sent]
        
                ents_count = len(ents)
                token_labels = ['X'] * len(tokens)
                token_jsons = [{}] * len(tokens)
                
                #if ents_count == 0: continue
                if ents_count < 2: continue 
                elif ents_count < 10:
                    
                    combs = list(combinations(ents,2))
        
                    for comb in tqdm(combs): 
                        
                        responses = kg.get_response(comb[0], comb[1])
                        
                        if responses is None: 
                            continue
                            '''
                            response_0 = kg.get_response(comb[0])
                            response_1 = kg.get_response(comb[1])
                            
                            if response_0 is not None:
                                token_labels, token_jsons = change_label(token_labels, token_jsons, comb[0], response_0)
                            if response_1 is not None:
                                token_labels, token_jsons = change_label(token_labels, token_jsons, comb[1], response_1)                       
                            '''
                        else:
                            token_labels, token_jsons = change_label(token_labels, token_jsons, comb, responses)
                            write_sent_record(index, sent_index, str(sent), responses, comb[0], comb[1])
                else:
                    row_to_add = [index, sent_index, 'too many entities']
                    with open('result/v' + str(VERSION) + '_error.csv', 'a', newline='') as file:
                        writer = csv.writer(file, delimiter=';')
                        writer.writerow(row_to_add)
                        
                        
                '''
                else: # only one entity
                
                    response = kg.get_response(ents[0])
                    if response is None: continue
                    token_labels, token_jsons = change_label(token_labels, token_jsons, ents[0], response)
                '''
                if not token_labels == ['X'] * len(tokens):
                    write_token_record(tokens, token_labels, token_jsons)
            except:
                row_to_add = [index, sent_index, '']
                with open('result/v' + str(VERSION) + '_error.csv', 'a', newline='') as file:
                    writer = csv.writer(file, delimiter=';')
                    writer.writerow(row_to_add)
                continue
                
    except:
        row_to_add = [index, '', '']
        with open('result/v' + str(VERSION) + '_error.csv', 'a', newline='') as file:
            writer = csv.writer(file, delimiter=';')
            writer.writerow(row_to_add)
        continue