import pandas as pd
import random as rd
import math


for rep in [1,2,3,4,5,6,7,8,9]:
    numProducts = 3
    optPerProduct = 2
    retail = 700
    
    
    nodes = pd.read_csv('nodes_Gowalla.csv')
    
    
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
    
    
    value_prod = {}
    for prod in range(1,numProducts+1):
        value_prod[prod] = []
    
    for u in nodes['Node']:
        valueArray = [] 
        for prod in range(1,numProducts+1):
            valueArray += [rd.uniform(0,retail)]        
        valueArray = sorted(valueArray)
        for prod in range(1,numProducts+1):
            value_prod[prod] += [valueArray[prod-1]]
            
    for prod in range(1,numProducts+1):
        nodes['Value%s'%prod] = value_prod[prod]
        
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
    
            
    nodes.to_csv(r'nodes_3products_choice/nodes_Gowalla_choice_%s.csv'%(rep), index = False)#Check
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    

