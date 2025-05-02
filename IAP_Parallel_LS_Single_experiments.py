# conflict graph model with general number of options solved by GUROBI

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

def singleLocalSearch2(args):
    singleQ,iteration,profiling,tic = args
    return singleLocalSearch(singleQ,iteration,profiling,tic)

def singleLocalSearch(singleQ,iteration,profiling,tic):
    nodeList, optionList, lineList, forbidList, price, pw, confG = profiling
    singleOptionList = singleOption(singleQ,iteration,pw,price,nodeList,optionList,logSum)       
    bestTotalRevenue, bestOffered, bestNotOffered, bestRevenue, bestChoice, bestTpw = singleOptionList    
    toc = time.time()
    initialTime = toc - tic
                    
            
    # local search
    print('###local search starts')
    print('datetime.datetime.now() =',datetime.datetime.now())
    print('networkID =',networkID)
    print('rep =',rep)
    print('Initial Revenue =',bestTotalRevenue)
    print('Initial Number of Offered Options =',len(bestOffered))
    #tic = time.time()
    # initializing local search
    initialTotalRevenue = bestTotalRevenue
    initialOffer = len(bestOffered)
    
    localSearchResult = localSearchSingle(singleQ,iteration, singleOptionList, price, confG, pw, logSum, machineName, networkID, rep, tic)
    
    return localSearchResult, initialTotalRevenue, initialOffer, initialTime, singleQ


def singleOption(singleQ,iteration,pw,price,nodeList,optionList,logSum):
    q = singleQ 
    totalRevenue_q = 0
    choiceP = {}
    choice = {}
    offered = []
    notOffered = []
    revenue = {}
    tpw = {}
    for u in nodeList:
        choice[u] = [q]
        choiceP[u,q] = pw[u,q] / (1 + pw[u,q])
        revenue[u] = price[q] * choiceP[u,q]
        totalRevenue_q += revenue[u]
        offered += [(u,q)]
        tpw[u] = math.pow(pw[u,q],1/logSum)
        for p in optionList:
            if p != q:
                notOffered += [(u,p)]
    return totalRevenue_q, offered, notOffered, revenue, choice, tpw


def localSearchSingle(singleQ,iteration,singleOptionList, price, confG, pw, logSum, machineName, networkID, rep, tic):
    bestTotalRevenue, bestOffered, bestNotOffered, bestRevenue, bestChoice, bestTpw = singleOptionList
    bestOptions = len(bestOffered)

    iterColumn = [iteration]
    machineColumn = [machineName]    
    netColumn = [networkID]
    repColumn = [rep]
    metColumn = ['Initial']
    revColumn = [bestTotalRevenue]
    logColumn = [logSum]
    toc = time.time()
    timeColumn = [toc - tic]
    offeredColumn = [bestOptions]
    processColumn = [0]
    numGrandImproveColumn = [0]
    
    listZip = list(zip(iterColumn,machineColumn,netColumn,repColumn,metColumn,processColumn,numGrandImproveColumn,logColumn,revColumn,offeredColumn,timeColumn))
    colName = ['Iteration','Machine','networkID','rep','method','improves','total improves','logSum','LOPT','Offered Options','Time']
    result = pd.DataFrame(listZip, columns = colName)
    result.to_csv(r'parallel_single/result_LS/result_LS_logSum%s_Network%s_Rep%s_SingleQ%s.csv'%(int(logSum*100),networkID,rep,singleQ), index = False)#Check
    
    numGrandImprove = 0
    grandImprove = True
    while grandImprove == True:
        grandImprove = False
    
        print()
        print('## deletion starts')

        numImprove = 0    
        improve = True
        while improve == True:
            improve = False
            for (u,q) in bestOffered:
                if len(bestChoice[u]) > 1:
                    tempTpw_u = bestTpw[u] - math.pow(pw[u,q], 1/logSum)
                    tempChoice_u = copy.deepcopy(bestChoice[u])
                    tempChoice_u.remove(q)
                    tempRevenue_u = 0
                    for p in tempChoice_u:
                        tempRevenue_u = tempRevenue_u + price[p] * math.pow(tempTpw_u,logSum) / (1 + math.pow(tempTpw_u,logSum)) * math.pow(pw[u,p],1/logSum) / tempTpw_u
        
                    if bestRevenue[u] < tempRevenue_u:    
                        improve = True
                        grandImprove = True                                                           
                        numImprove += 1    
                        break
                    
            if improve == True:
                bestTotalRevenue = bestTotalRevenue - bestRevenue[u] + tempRevenue_u
                bestRevenue[u] = tempRevenue_u
                bestOffered.remove((u,q))
                bestOptions = bestOptions - 1
                bestNotOffered.append((u,q))                
                bestTpw[u] = tempTpw_u
                bestChoice[u].remove(q)

        if numImprove > 0:

            iterColumn += [iteration]
            machineColumn += [machineName]    
            netColumn += [networkID]
            repColumn += [rep]
            metColumn += ['Deletion']
            revColumn += [bestTotalRevenue]
            logColumn += [logSum]
            toc = time.time()
            timeColumn += [toc - tic]
            offeredColumn += [bestOptions]    
            processColumn += [numImprove]
            numGrandImprove += numImprove
            numGrandImproveColumn += [numGrandImprove]
            
            listZip = list(zip(iterColumn,machineColumn,netColumn,repColumn,metColumn,processColumn,numGrandImproveColumn,logColumn,revColumn,offeredColumn,timeColumn))
            colName = ['Iteration','Machine','networkID','rep','method','improves','total improves','logSum','LOPT','Offered Options','Time']
            result = pd.DataFrame(listZip, columns = colName)
            result.to_csv(r'parallel_single/result_LS/result_LS_logSum%s_Network%s_Rep%s_SingleQ%s.csv'%(int(logSum*100),networkID,rep,singleQ), index = False)#Check

                    
        print('number of deletion improve =',numImprove)
        print('grandImprove =',grandImprove)
        print('bestTotalRevenue =',bestTotalRevenue)
        print('len(bestOffered) =',len(bestOffered),bestOptions)
    
    
        print()
        print('## addition starts')
        numImprove = 0    
        improve = True
        while improve == True:
            improve = False
            for (u,q) in bestNotOffered:
                neighbor_u_q = list(set(confG.neighbors((u,q))) & set(bestOffered)) ### might delay computational time
        
                tempNodeSet = [u]        
                for (v,p) in neighbor_u_q:
                    tempNodeSet += [v]                    
                tempNodeSet = list(set(tempNodeSet))
                
                tempTpw = {}
                tempChoice = {}
                for v in tempNodeSet:
                    tempTpw[v] = bestTpw[v]
                    tempChoice[v] = copy.deepcopy(bestChoice[v])

                tempTpw[u] += math.pow(pw[u,q],1/logSum)
                tempChoice[u].append(q)
                for (v,p) in neighbor_u_q:
                    tempTpw[v] = tempTpw[v] - math.pow(pw[v,p],1/logSum)
                    tempChoice[v].remove(p)

                                
                originalPartialRevenue = 0
                tempPartialRevenue = 0
                tempRevenue = {}
                for v in tempNodeSet:
                    originalPartialRevenue += bestRevenue[v]
                    tempRevenue[v] = 0
                    if len(tempChoice[v]) > 0:
                        for p in tempChoice[v]:
                            tempPartialRevenue += price[p] * math.pow(tempTpw[v],logSum) / (1 + math.pow(tempTpw[v],logSum)) * math.pow(pw[v,p],1/logSum) / tempTpw[v]
                            tempRevenue[v] += price[p] * math.pow(tempTpw[v],logSum) / (1 + math.pow(tempTpw[v],logSum)) * math.pow(pw[v,p],1/logSum) / tempTpw[v]
             
                if originalPartialRevenue < tempPartialRevenue:
                    bestTotalRevenue = bestTotalRevenue - originalPartialRevenue + tempPartialRevenue
                    for v in tempNodeSet:                    
                        bestRevenue[v] = tempRevenue[v]
                        bestTpw[v] = tempTpw[v]
                        bestChoice[v] = copy.deepcopy(tempChoice[v])
                    bestOffered.append((u,q))
                    bestNotOffered.remove((u,q))                
                    for (v,p) in neighbor_u_q:
                        bestNotOffered.append((v,p))                
                        bestOffered.remove((v,p))
                    bestOptions = bestOptions + 1 - len(neighbor_u_q)
                    # bestTpw = copy.deepcopy(tempTpw)
                    # bestChoice = copy.deepcopy(tempChoice)
                    improve = True
                    grandImprove = True
                    numImprove += 1    
        
                    break

        if numImprove > 0:

            iterColumn += [iteration]
            machineColumn += [machineName]    
            netColumn += [networkID]
            repColumn += [rep]
            metColumn += ['Addition']
            revColumn += [bestTotalRevenue]
            logColumn += [logSum]
            toc = time.time()
            timeColumn += [toc - tic]
            offeredColumn += [bestOptions]    
            processColumn += [numImprove]
            numGrandImprove += numImprove
            numGrandImproveColumn += [numGrandImprove]
            
            listZip = list(zip(iterColumn,machineColumn,netColumn,repColumn,metColumn,processColumn,numGrandImproveColumn,logColumn,revColumn,offeredColumn,timeColumn))
            colName = ['Iteration','Machine','networkID','rep','method','improves','total improves','logSum','LOPT','Offered Options','Time']
            result = pd.DataFrame(listZip, columns = colName)
            result.to_csv(r'parallel_single/result_LS/result_LS_logSum%s_Network%s_Rep%s_SingleQ%s.csv'%(int(logSum*100),networkID,rep,singleQ), index = False)#Check
                            
                
        print('number of addition improve =',numImprove)
        print('grandImprove =',grandImprove)
        print('bestTotalRevenue =',bestTotalRevenue)
        print('len(bestOffered) =',len(bestOffered),bestOptions)
        print()

    iterColumn += [iteration]
    machineColumn += [machineName]    
    netColumn += [networkID]
    repColumn += [rep]
    metColumn += ['Final']
    revColumn += [bestTotalRevenue]
    logColumn += [logSum]
    toc = time.time()
    timeColumn += [toc - tic]
    offeredColumn += [bestOptions]
    processColumn += [numImprove]
    numGrandImproveColumn += [numGrandImprove]
    
    listZip = list(zip(iterColumn,machineColumn,netColumn,repColumn,metColumn,processColumn,numGrandImproveColumn,logColumn,revColumn,offeredColumn,timeColumn))
    colName = ['Iteration','Machine','networkID','rep','method','improves','total improves','logSum','LOPT','Offered Options','Time']
    result = pd.DataFrame(listZip, columns = colName)
    result.to_csv(r'parallel_single/result_LS/result_LS_logSum%s_Network%s_Rep%s_SingleQ%s.csv'%(int(logSum*100),networkID,rep,singleQ), index = False)#Check
                        
    return bestTotalRevenue, bestOffered, bestNotOffered, bestRevenue, bestChoice, bestTpw, bestOptions, toc


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


# Code Starts Here
## Identify Machine Name
machineName = socket.gethostname()

## Parameters 
logSum = 0.75

numProducts = 3
options = pd.read_csv('options_%sproducts.csv'%numProducts)
forbidden = pd.read_csv('forbiddenPairs_%sproducts_choice.csv'%numProducts)


## Instances
for (networkID,repNum) in [(9,50),(5,50),(3,50),(4,50),(2,50),(0,10),(8,10),(7,10),(6,10),(1,10)]:

    
    lines = pd.read_csv('lines/lines_%s.csv'%networkID)

    grandMachineColumn = []     
    grandNetColumn = []
    grandRepColumn = []
    grandMetColumn = []        
    grandSingleQColumn = []
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
        nodes = pd.read_csv('nodes_%sproducts_choice/nodes_%s_%s.csv'%(numProducts,networkID,rep))
        
        profiling = frontMatter(lines,nodes,options,forbidden)
        nodeList, optionList, lineList, forbidList, price, pw, confG = profiling
        
        if __name__ == '__main__':
            numCores = len(options['Option'])
            p = mp.Pool(numCores)
        
            tic = time.time()
            multiArgs = []  
            for coreID in range(numCores):
                multiArgs += [(list(options['Option'])[coreID],coreID,profiling,tic)]  
        
            multiResults = p.map(singleLocalSearch2, multiArgs)
            entireToc = time.time()
        
            machineColumn = []    
            netColumn = []
            repColumn = []
            metColumn = []    
            initialTimeColumn = []
            initialOfferColumn = []
            initialTotalRevenueColumn = []
            revColumn = []
            logColumn = []
            timeColumn = []
            offeredColumn = []
            entireTimeColumn = []
        
            grandBestTotalRevenue = 0
            for localSearchResult, initialTotalRevenue, initialOffer, initialTime, singleQ in multiResults:
                bestTotalRevenue, bestOffered, bestNotOffered, bestRevenue, bestChoice, bestTpw, bestOptions, toc = localSearchResult
                print('singleQ =',singleQ,'bestTotalRevenue =',bestTotalRevenue)
                
                machineColumn += [machineName]    
                netColumn += [networkID]
                repColumn += [rep]
                metColumn += ['Single+LS']        
                initialTimeColumn += [initialTime]
                initialOfferColumn += [initialOffer]
                initialTotalRevenueColumn += [initialTotalRevenue]        
                revColumn += [bestTotalRevenue]
                logColumn += [logSum]
                timeColumn += [toc - tic]
                offeredColumn += [bestOptions]
                entireTimeColumn += [entireToc - tic]
        
                if grandBestTotalRevenue < bestTotalRevenue:
                    grandBestTotalRevenue = bestTotalRevenue
                    grandSingleQ = singleQ
                    grandInitialTotalRevenue = initialTotalRevenue 
                    grandInitialOffer = initialOffer
                    grandInitialTime = initialTime
                    grandBestOffered = copy.deepcopy(bestOffered)
                    grandTime = toc - tic
                    grandBestOptions = bestOptions
                    grandTpw = copy.deepcopy(bestTpw)
        
                
            listZip = list(zip(machineColumn,netColumn,repColumn,metColumn,logColumn,initialTotalRevenueColumn,revColumn,initialOfferColumn,offeredColumn,initialTimeColumn,timeColumn,entireTimeColumn))
            colName = ['Machine','networkID','rep','method','logSum','Initial','Final','Initial Options','Final Options','Initial Time','Final Time','Entire Time']
            summaryResult = pd.DataFrame(listZip, columns = colName)
            summaryResult.to_csv(r'parallel_single/summary/parallel_Single-LS_logSum%s_Network%s_Rep%s.csv'%(int(logSum*100),networkID,rep), index = False)#Check
            
            uColumn = []
            qColumn = []
            pColumn = []
            for (u,q) in grandBestOffered:
                uColumn += [u]
                qColumn += [q]
                pColumn += [math.pow(grandTpw[u],logSum) / (1 + math.pow(grandTpw[u],logSum)) * math.pow(pw[u,q],1/logSum) / grandTpw[u]]
        
            loptSolution = pd.DataFrame(list(zip(uColumn,qColumn,pColumn)),columns =['Node', 'Option', 'Probability'])
            loptSolution.to_csv(r'parallel_single/lopt/lopt_LS_Single%s_Net%s_Rep%s_%s_logSum%s.csv'%(grandSingleQ,networkID,rep,machineName,int(logSum*100)), index = False)#Check
        

            grandMachineColumn += [machineName]    
            grandNetColumn += [networkID]
            grandRepColumn += [rep]
            grandMetColumn += ['Single+LS']        
            grandSingleQColumn += [grandSingleQ]
            grandInitialTimeColumn += [grandInitialTime]
            grandTimeColumn += [grandTime]
            grandInitialTotalRevenueColumn += [grandInitialTotalRevenue] 
            grandBestTotalRevenueColumn += [grandBestTotalRevenue]
            grandInitialOfferColumn += [grandInitialOffer]
            grandBestOptionsColumn += [grandBestOptions]            
            
            listZip = list(zip(grandMachineColumn,grandNetColumn,grandRepColumn,grandMetColumn,grandSingleQColumn,grandInitialTimeColumn,grandTimeColumn,grandInitialTotalRevenueColumn,grandBestTotalRevenueColumn,grandInitialOfferColumn,grandBestOptionsColumn))
            colName = ['Machine','NetworkID','Rep','Method','SingleQ','Initial Time','Final Time','Initial Revenue','Final Revenue','Initial Options','Final Options']
            grandTable = pd.DataFrame(listZip,columns = colName)
            grandTable.to_csv(r'parallel_single/Summary_LS_Single_%s_%s_logSum%s.csv'%(networkID,machineName,int(logSum*100)), index = False)#Check
        
                    
        
        
        
        
        
        
        
        
        
        
                
                
                
            
            
            
            
            
            
        
        
        
        
        
        
        
