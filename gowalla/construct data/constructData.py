import pandas as pd
import networkx as nx

table = pd.read_csv('Gowalla_edges.txt', sep = '\t')
G = nx.Graph()

for index, row in table.iterrows():
    source = int(row[0])
    target = int(row[1])
    G.add_edge(source,target)
    
print('len(G.nodes())=',len(G.nodes()))
print('len(G.edges())=',len(G.edges()))

nodeArray = sorted(list(G.nodes()))
nodeColumn = []    
for u in nodeArray:
    nodeColumn += [u]
    
listZip = list(zip(nodeColumn))
colName = ['Node']
nodes = pd.DataFrame(listZip, columns = colName)
nodes.to_csv(r'nodes_Gowalla.csv', index = False)#Check

lineColumn = []
sourceColumn = []
targetColumn = []
lineNumber = 0
for (source,target) in G.edges():
    lineNumber += 1
    lineColumn += [lineNumber]
    [source,target] = sorted([source,target])
    sourceColumn += [source]
    targetColumn += [target]
    
listZip = list(zip(lineColumn, sourceColumn, targetColumn))
colName = ['Line','Source','Target']
lines = pd.DataFrame(listZip, columns = colName)
lines.to_csv(r'lines_Gowalla.csv', index = False)#Check
    

    
