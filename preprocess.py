# -*- coding: utf-8 -*-
"""
Created on Wed Dec  8 02:38:04 2021

@author: ezgtt
"""
import re
import preprocessor as p
from nltk import sent_tokenize
from nltk.tokenize import word_tokenize
from nltk.stem import WordNetLemmatizer
import spacy

lemmatizer = WordNetLemmatizer()

pattern = re.compile("\xa0|\n|\t")
pattern_space = re.compile(" {2,}")
pattern_end = re.compile("([0-9a-z])\.([A-Z])")
p.set_options(p.OPT.URL, p.OPT.EMOJI, p.OPT.MENTION, p.OPT.HASHTAG) # tweet-preprocessor

def lemmatize(word):
    word = word.lower()
    lemmatizer = WordNetLemmatizer()    
    return lemmatizer.lemmatize(word)

def clean_corpus(corpus):
    corpus = pattern.sub(" ",corpus)
    corpus = pattern_space.sub(" ",corpus)
    corpus = p.clean(corpus)
    corpus = pattern_end.sub(r"\1. \2",corpus)
    return corpus

def get_sents(corpus):
    return sent_tokenize(corpus)

def get_tokens(texts):
    return word_tokenize(texts)


        
        