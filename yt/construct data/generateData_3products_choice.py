import pandas as pd
import random as rd
import math

numProducts = 3
optPerProduct = 2
retail = 700

    
    
nodes = pd.read_csv('yt_nodes_20220123.csv')

prodSum = {}
for prod in range(1,numProducts+1):
    prodSum[prod] = 0

option = 0
optionColumn = {}
optionArray = []
for prod in range(1,numProducts+1):
    for opp in range(optPerProduct):
        option += 1
        optionArray += [option]
        optionColumn[option] = []


for u in nodes['Node']:
    option = 0
    for prod in range(1,numProducts+1):
        preferences = []        
        [value_u_prod] = nodes.loc[nodes['Node']==u,'Value%s'%prod]
        for opp in range(optPerProduct):
            option += 1
            preferences += [rd.uniform(0,1) * math.exp(- value_u_prod / retail)]
        preferences = sorted(preferences,reverse = True)
        prodSum[prod] += sum(preferences)
        optionColumn[option-1] += [preferences[0]]
        optionColumn[option] += [preferences[1]]
        
for option in optionArray:
    nodes['Option%s'%option] = optionColumn[option]
        
nodes.to_csv(r'yt_nodes_choice.csv', index = False)#Check








