import pandas as pd
import networkx as nx  
import copy  
from itertools import combinations
import math
import socket
import glob
import os
import time
import datetime
import multiprocessing as mp


def doubleLS(doubleQ1,doubleQ2,profiling,logSum,iteration,machineName,networkID,rep,tic):
    nodeList, optionList, lineList, forbidList, price, pw, confG = profiling      
    
    totalRevenue, revenue, is_offered, optionsOffered, tpw, optionsCount, stable = initialDouble(doubleQ1,doubleQ2,profiling)    

    toc = time.time()
    initialTime = toc - tic
    initialOptions = optionsCount
    initialTotalRevenue = totalRevenue
    initialPackage = initialTime, initialOptions, initialTotalRevenue
    
    print()
    print('initial total revenue =',totalRevenue)
    print(doubleQ1,doubleQ2,stable,totalRevenue,optionsCount)
    print(datetime.datetime.now())
        
    iterColumn = [iteration]
    machineColumn = [machineName]    
    netColumn = [networkID]
    repColumn = [rep]
    # metColumn = ['Initial']
    stepColumn = ['Initial']
    revColumn = [totalRevenue]
    logColumn = [logSum]
    toc = time.time()
    timeColumn = [toc - tic]
    #bestOptions = initialOptions
    offeredColumn = [optionsCount]
    # processColumn = [0] # addition
    addColumn = [0]
    # numGrandImproveColumn = [0] # deletion
    delColumn = [0]
    
    columnPackage = iterColumn,machineColumn,netColumn,repColumn,stepColumn,addColumn,delColumn,logColumn,revColumn,offeredColumn,timeColumn
    inputPackage = doubleQ1, doubleQ2, tic, optionsCount, networkID, rep, machineName, iteration
        
    finalSolution = LSD(totalRevenue,revenue,is_offered,optionsOffered,tpw,confG,pw,logSum,columnPackage,inputPackage,profiling)    
        
    bestTotalRevenue,bestRevenue,bestIs_offered,bestOptionsOffered,bestTpw,resultPackage = finalSolution    
        
    finalRevenue = 0.0
    finalOptions = 0
    for u in nodeList:
        finalOptions += len(bestOptionsOffered[u])
        for q in bestOptionsOffered[u]:
            choiceProbability = pow(bestTpw[u],logSum) / (1 + pow(bestTpw[u],logSum)) * pow(pw[u,q],1/logSum) / bestTpw[u]
            revenue_u_q = choiceProbability * price[q]
            finalRevenue += revenue_u_q

    iterColumn,machineColumn,netColumn,repColumn,stepColumn,addColumn,delColumn,logColumn,revColumn,offeredColumn,timeColumn = resultPackage
      
    iterColumn += [iteration]
    machineColumn += [machineName]    
    netColumn += [networkID]
    repColumn += [rep]
    stepColumn += ['Final']            
    revColumn += [finalRevenue]
    logColumn += [logSum]
    toc = time.time()
    timeColumn += [toc - tic]
    offeredColumn += [finalOptions]    
    addColumn += [0]
    delColumn += [0]
    
    listZip = list(zip(iterColumn,machineColumn,netColumn,repColumn,stepColumn,addColumn,delColumn,logColumn,revColumn,offeredColumn,timeColumn))
    colName = ['Iteration','Machine','networkID','rep','step','addition','deletion','logSum','LOPT','Offered Options','Time']
    result = pd.DataFrame(listZip, columns = colName)
    result.to_csv(r'parallel_double/result_LS/result_LS_logSum%s_Network%s_Rep%s_doubleQ1%s_doubleQ2%s.csv'%(int(logSum*100),networkID,rep,doubleQ1,doubleQ2), index = False)#Check
    
    return finalSolution, finalRevenue, finalOptions, inputPackage, initialPackage


def doubleLS2(arg):
    doubleQ1,doubleQ2,profiling,logSum,iteration,machineName,networkID,rep,tic = arg
    return doubleLS(doubleQ1,doubleQ2,profiling,logSum,iteration,machineName,networkID,rep,tic)


def doubleOption(doubleQ1,doubleQ2,profiling):
    nodeList, optionList, lineList, forbidList, price, pw, confG = profiling

    is_offered = {}
    optionsOffered = {} 
    for u in nodeList:
        for q in optionList:
            is_offered[u,q] = 0
        is_offered[u,doubleQ1] = 1
        is_offered[u,doubleQ2] = 1
        optionsOffered[u] = [doubleQ1,doubleQ2]
        
    tpw = {}
    for u in nodeList:
        tpw[u] = 0.0
        for q in optionsOffered[u]:
            tpw[u] += pow(pw[u,q],1/logSum)
            
    totalRevenue = 0.0
    revenue = {}
    optionsCount = 0
    for u in nodeList:
        revenue[u] = 0.0
        for q in optionsOffered[u]:
            choiceProbability = pow(tpw[u],logSum) / (1 + pow(tpw[u],logSum)) * pow(pw[u,q],1/logSum) / tpw[u]
            revenue_u_q = choiceProbability * price[q]
            totalRevenue += revenue_u_q
            revenue[u] += revenue_u_q
            optionsCount += 1

    return totalRevenue, revenue, is_offered, optionsOffered, tpw, optionsCount


def frontMatter(lines,nodes,options,forbidden):
    nodeList = list(nodes['Node'])
    
    optionList = list(options['Option'])
    
    lineList = []
    for l in lines['Line']:
        [source_l] = lines.loc[lines['Line']==l,'Source']
        [target_l] = lines.loc[lines['Line']==l,'Target']
        lineList += [(source_l,target_l)]
    
    forbidList = []
    for pair in forbidden['Pair']:
        [source_pair] = forbidden.loc[forbidden['Pair']==pair,'Source']
        [target_pair] = forbidden.loc[forbidden['Pair']==pair,'Target']
        forbidList += [(source_pair,target_pair)]   
    
    price = {}
    for p in options['Option']:
        [price_p] = options.loc[options['Option']==p,'Price']
        price[p] = price_p
    
    pw = {}
    confG = nx.Graph()
    for u in nodeList:
        for p in optionList:
            [preference_u_p] = nodes.loc[nodes['Node']==u,'Option%s'%p]
            pw[u,p] = preference_u_p
            confG.add_node((u,p))
    
    for (p,q) in forbidList:
        for u in nodeList:
            confG.add_edge((u,p),(u,q))
        for (u,v) in lineList:
            confG.add_edge((u,p),(v,q))
            confG.add_edge((u,q),(v,p))

    return nodeList, optionList, lineList, forbidList, price, pw, confG


def initialDouble(doubleQ1,doubleQ2,profiling):
    nodeList, optionList, lineList, forbidList, price, pw, confG = profiling
    
    if (doubleQ1,doubleQ2) in forbidList or (doubleQ2,doubleQ1) in forbidList:    
        totalRevenue, revenue, is_offered, optionsOffered, tpw, optionsCount = stableOption(doubleQ1,doubleQ2,profiling)
        stable = True
    else:
        totalRevenue, revenue, is_offered, optionsOffered, tpw, optionsCount = doubleOption(doubleQ1,doubleQ2,profiling)
        stable = False

    return totalRevenue, revenue, is_offered, optionsOffered, tpw, optionsCount, stable


def LSD(bestTotalRevenue,bestRevenue,bestIs_offered,bestOptionsOffered,bestTpw,confG,pw,logSum,columnPackage,inputPackage,profiling):
    nodeList, optionList, lineList, forbidList, price, pw, confG = profiling
    doubleQ1, doubleQ2, tic, bestOptions, networkID, rep, machineName, iteration = inputPackage
    iterColumn,machineColumn,netColumn,repColumn,stepColumn,addColumn,delColumn,logColumn,revColumn,offeredColumn,timeColumn = columnPackage
    listZip = list(zip(iterColumn,machineColumn,netColumn,repColumn,stepColumn,addColumn,delColumn,logColumn,revColumn,offeredColumn,timeColumn))
    colName = ['Iteration','Machine','networkID','rep','step','addition','deletion','logSum','LOPT','Offered Options','Time']
    result = pd.DataFrame(listZip, columns = colName)
    result.to_csv(r'parallel_double/result_LS/result_LS_logSum%s_Network%s_Rep%s_doubleQ1%s_doubleQ2%s.csv'%(int(logSum*100),networkID,rep,doubleQ1,doubleQ2), index = False)#Check

    improve = True
    while improve == True:
        improve = False
        notProcessed = list(confG.nodes())
        addition = 0
        deletion = 0
        while len(notProcessed) > 0:
            processed = []
            for (u,q) in notProcessed:
                processed += [(u,q)]
                if bestIs_offered[u,q] == 0: # addition
                    tempOptions = 1
                    nodesInvolved = [u]
                    for (v,p) in confG.neighbors((u,q)):
                        if bestIs_offered[v,p] == 1:
                            nodesInvolved += [v]
                    nodesInvolved = list(set(nodesInvolved))
                    
                    tempTpw = {}
                    tempOptionsOffered = {}
                    for v in nodesInvolved:
                        tempTpw[v] = bestTpw[v]
                        tempOptionsOffered[v] = copy.deepcopy(bestOptionsOffered[v])
            
                    tempTpw[u] += pow(pw[u,q],1/logSum)
                    tempOptionsOffered[u].append(q)
                    for (v,p) in confG.neighbors((u,q)):
                        if bestIs_offered[v,p] == 1:
                            tempOptions = tempOptions - 1
                            tempTpw[v] = tempTpw[v] - pow(pw[v,p],1/logSum)
                            tempOptionsOffered[v].remove(p)
                    
                    bestRevenueInvolved = 0.0
                    for v in nodesInvolved:
                        bestRevenueInvolved += bestRevenue[v]
                        
                    tempRevenueInvolved = 0.0
                    tempRevenue = {}
                    tempIs_offered = {}
                    for v in nodesInvolved:
                        tempRevenue[v] = 0.0
                        for p in optionList:
                            tempIs_offered[v,p] = 0
                        for p in tempOptionsOffered[v]:
                            choiceProbability_v_p = pow(tempTpw[v],logSum) / (1 + pow(tempTpw[v],logSum)) * pow(pw[v,p],1/logSum) / tempTpw[v]
                            tempRevenueInvolved += price[p] * choiceProbability_v_p
                            tempRevenue[v] += price[p] * choiceProbability_v_p
                            tempIs_offered[v,p] = 1
                            
                    if bestRevenueInvolved < tempRevenueInvolved:                      
                        improve = True
                        bestTotalRevenue = bestTotalRevenue - bestRevenueInvolved + tempRevenueInvolved
                        for v in nodesInvolved:
                            bestRevenue[v] = tempRevenue[v]
                            bestOptionsOffered[v] = copy.deepcopy(tempOptionsOffered[v])
                            bestTpw[v] = tempTpw[v] 
                            for p in optionList:
                                bestIs_offered[v,p] = tempIs_offered[v,p]
                                
                        # print('addition; bestTotalRevenue =',bestTotalRevenue)
                        addition += 1         
                        bestOptions = bestOptions + tempOptions
                        break
        
                if bestIs_offered[u,q] == 1: # deletion
                    tempOptions = -1
                    tempTpw_u = bestTpw[u] - pow(pw[u,q],1/logSum)
                    tempRevenue_u = 0.0
                    for p in bestOptionsOffered[u]:
                        if p != q:
                            tempRevenue_u += price[p] * pow(tempTpw_u,logSum) / (1 + pow(tempTpw_u,logSum)) * pow(pw[u,p],1/logSum) / tempTpw_u
                            
                    if bestRevenue[u] < tempRevenue_u:
                        improve = True
                        bestTotalRevenue = bestTotalRevenue - bestRevenue[u] + tempRevenue_u
                        bestRevenue[u] = tempRevenue_u                
                        bestOptionsOffered[u].remove(q)                
                        bestTpw[u] = tempTpw_u 
                        bestIs_offered[u,q] = 0
                                
                        # print('deletion; bestTotalRevenue =',bestTotalRevenue)
                        deletion += 1        
                        bestOptions = bestOptions + tempOptions
                        break
                    
            for (u,q) in processed:   
                notProcessed.remove((u,q))                     

        print()
        print('addition =',addition)
        print('deletion =',deletion) 
        print('bestTotalRevenue =',bestTotalRevenue)  
        print(datetime.datetime.now())                      

        if addition + deletion > 0:
            iterColumn += [iteration]
            machineColumn += [machineName]    
            netColumn += [networkID]
            repColumn += [rep]
            stepColumn += ['Intermediate']            
            revColumn += [bestTotalRevenue]
            logColumn += [logSum]
            toc = time.time()
            timeColumn += [toc - tic]
            offeredColumn += [bestOptions]    
            addColumn += [addition]
            delColumn += [deletion]
            
            listZip = list(zip(iterColumn,machineColumn,netColumn,repColumn,stepColumn,addColumn,delColumn,logColumn,revColumn,offeredColumn,timeColumn))
            colName = ['Iteration','Machine','networkID','rep','step','addition','deletion','logSum','LOPT','Offered Options','Time']
            result = pd.DataFrame(listZip, columns = colName)
            result.to_csv(r'parallel_double/result_LS/result_LS_logSum%s_Network%s_Rep%s_doubleQ1%s_doubleQ2%s.csv'%(int(logSum*100),networkID,rep,doubleQ1,doubleQ2), index = False)#Check

    resultPackage = iterColumn,machineColumn,netColumn,repColumn,stepColumn,addColumn,delColumn,logColumn,revColumn,offeredColumn,timeColumn
        
    return bestTotalRevenue,bestRevenue,bestIs_offered,bestOptionsOffered,bestTpw,resultPackage                


def stableOption(doubleQ1,doubleQ2,profiling):

    nodeList, optionList, lineList, forbidList, price, pw, confG = profiling
    
    nodeWeight = {}
    for u in nodeList:
        for p in optionList:
            nodeWeight[u,p] = price[p] * pw[u,p] / (1 + pw[u,p])
            
    subConfGNodeList = []
    for u in nodeList:
        subConfGNodeList += [(u,doubleQ1),(u,doubleQ2)]
    subConfG = nx.Graph(confG.subgraph(subConfGNodeList))
    
    # source_Flow = (-1,-1)
    # target_Flow = (-2,-2)
    
    for u in nodeList:
        subConfG.add_edge((-1,-1), (u,doubleQ1), capacity = nodeWeight[u,doubleQ1])
    
    for u in nodeList:
        subConfG.add_edge((u,doubleQ2), (-2,-2), capacity = nodeWeight[u,doubleQ2])
    
    # tic = time.time()
    cut_value, partition = nx.minimum_cut(subConfG, (-1,-1), (-2,-2), capacity='capacity', flow_func=None)
    # toc = time.time()
    
    reachable, non_reachable = partition
    
    # read the optimal solution        
    is_offered = {}
    optionsOffered = {} 
    for u in nodeList:
        optionsOffered[u] = []
        for q in optionList:
            is_offered[u,q] = 0
    
    for (u,q) in reachable:
        if q == doubleQ1:
            is_offered[u,q] = 1
            optionsOffered[u].append(q)
    
    for (u,q) in non_reachable:
        if q == doubleQ2:
            is_offered[u,q] = 1
            optionsOffered[u].append(q)
    
    totalRevenue = 0
    revenue = {}
    tpw = {}
    optionsCount = 0
    for u in nodeList:
        tpw[u] = 0.0
        revenue[u] = 0.0
        for q in optionsOffered[u]:        
            prob_u_q = pw[u,q] / (1 + pw[u,q])
            totalRevenue += price[q] * prob_u_q
            revenue[u] += price[q] * prob_u_q
            tpw[u] += pow(pw[u,q],1/logSum)
            optionsCount += 1
                
    return totalRevenue, revenue, is_offered, optionsOffered, tpw, optionsCount



# Code Starts Here
## Identify Machine Name
machineName = socket.gethostname()

## Parameters 
logSum = 0.75

numProducts = 3
## Common instance
options = pd.read_csv('options_%sproducts.csv'%numProducts)
forbidden = pd.read_csv('forbiddenPairs_%sproducts_choice.csv'%numProducts)
doubleOptionList = list(combinations(list(options['Option']),2))
#doubleOptionList = [(1,2),(3,4),(5,6),(2,3),(4,5)]


## Instances
for (networkID,repNum) in [('yt',10)]:
    
    #lines = pd.read_csv('lines/lines_%s.csv'%networkID)
    lines = pd.read_csv('yt_lines.csv')

    grandMachineColumn = []     
    grandNetColumn = []
    grandRepColumn = []
    grandMetColumn = []        
    #grandSingleQColumn = []
    grandDoubleQ1Column = []
    grandDoubleQ2Column = []
    grandInitialTimeColumn = []
    grandTimeColumn = []
    grandInitialTotalRevenueColumn = [] 
    grandBestTotalRevenueColumn = []
    grandInitialOfferColumn = []
    grandBestOptionsColumn = []

    for rep in range(repNum):            

        print()
        print('### START')
        print(datetime.datetime.now())
        print('networkID =',networkID)
        print('rep =',rep)
        # nodes = pd.read_csv('nodes_%sproducts_choice/nodes_%s_choice_%s.csv'%(numProducts,networkID,rep))
        nodes = pd.read_csv('yt_nodes_choice.csv')

        profiling = frontMatter(lines,nodes,options,forbidden)
        nodeList, optionList, lineList, forbidList, price, pw, confG = profiling


        if __name__ == '__main__':
            numCores = len(doubleOptionList)
            p = mp.Pool(numCores)
        
            tic = time.time()
            multiArgs = []  
            for coreID in range(numCores):
                (doubleQ1,doubleQ2) = doubleOptionList[coreID]
                iteration = coreID
        
                multiArgs += [(doubleQ1,doubleQ2,profiling,logSum,iteration,machineName,networkID,rep,tic)]
        
            multiResults = p.map(doubleLS2, multiArgs)
            entireToc = time.time()
            
            bestFinalTotalRevenue = 0.0
            for finalSolution, finalTotalRevenue, finalOptions, inputPackage, initialPackage in multiResults:
                if bestFinalTotalRevenue < finalTotalRevenue:
                    bestFinalTotalRevenue = finalTotalRevenue
                    bestDoubleQ1, bestDoubleQ2, tic, initialOptionCount, networkID, rep, machineName, bestIteration = inputPackage
                    # doubleQ1, doubleQ2, tic, optionsCount, networkID, rep, machineName, iteration
                    bestFinalSolution = copy.deepcopy(finalSolution)  
                    bestInitialPackage = copy.deepcopy(initialPackage)
                    bestFinalOptions = finalOptions

            print(bestFinalTotalRevenue,bestDoubleQ1,bestDoubleQ2,bestFinalOptions,bestIteration)
            
            finalTotalRevenue,finalRevenue,finalIs_offered,finalOptionsOffered,finalTpw,finalPackage = bestFinalSolution
            
            uColumn = []
            qColumn = []
            pColumn = []
            for u in nodeList:
                for q in finalOptionsOffered[u]:
                    uColumn += [u]
                    qColumn += [q]
                    pColumn += [math.pow(finalTpw[u],logSum) / (1 + math.pow(finalTpw[u],logSum)) * math.pow(pw[u,q],1/logSum) / finalTpw[u]]
        
            loptSolution = pd.DataFrame(list(zip(uColumn,qColumn,pColumn)),columns =['Node', 'Option', 'Probability'])
            loptSolution.to_csv(r'parallel_double/lopt/lopt_LS_doubleQ1%s_doubleQ2%s_Net%s_Rep%s_%s_logSum%s.csv'%(bestDoubleQ1,bestDoubleQ2,networkID,rep,machineName,int(logSum*100)), index = False)#Check
        

            bestInitialTime, bestInitialOptions, bestInitialTotalRevenue = bestInitialPackage

            grandMachineColumn += [machineName]    
            grandNetColumn += [networkID]
            grandRepColumn += [rep]
            grandMetColumn += ['Double+LS']        
            #grandSingleQColumn += [bestSingleQ]
            grandDoubleQ1Column += [bestDoubleQ1]
            grandDoubleQ2Column += [bestDoubleQ2]
            grandInitialTimeColumn += [bestInitialTime]
            grandTimeColumn += [entireToc - tic]
            grandInitialTotalRevenueColumn += [bestInitialTotalRevenue] 
            grandBestTotalRevenueColumn += [bestFinalTotalRevenue]
            grandInitialOfferColumn += [bestInitialOptions]
            grandBestOptionsColumn += [bestFinalOptions]            
            
            listZip = list(zip(grandMachineColumn,grandNetColumn,grandRepColumn,grandMetColumn,grandDoubleQ1Column,grandDoubleQ2Column,grandInitialTimeColumn,grandTimeColumn,grandInitialTotalRevenueColumn,grandBestTotalRevenueColumn,grandInitialOfferColumn,grandBestOptionsColumn))
            colName = ['Machine','NetworkID','Rep','Method','doubleQ1','doubleQ2','Initial Time','Final Time','Initial Revenue','Final Revenue','Initial Options','Final Options']
            grandTable = pd.DataFrame(listZip,columns = colName)
            grandTable.to_csv(r'parallel_double/summary/Summary_LS_Double_%s_%s_logSum%s.csv'%(networkID,machineName,int(logSum*100)), index = False)#Check
        
        
        
        
        
        
        
        
        
        













