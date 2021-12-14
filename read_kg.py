# -*- coding: utf-8 -*-
"""
Created on Thu Dec  9 02:21:06 2021

@author: ezgtt
"""
import tarfile
'''
file = tarfile.open('kg/cord19_semrep.rdf.tar.gz', 'r:gz')
file.extractall('kg')
file.close()



file = tarfile.open('kg/litcovid_semrep.rdf.tar.gz', 'r:gz')
file.extractall('kg')
file.close()
'''
file = tarfile.open('kg/cord19.rdf.tar.gz', 'r:gz')
file.extractall('kg')
file.close()

file = tarfile.open('kg/litcovid.rdf.tar.gz', 'r:gz')
file.extractall('kg')
file.close()

