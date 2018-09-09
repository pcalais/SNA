import pandas as pd
import numpy as np
import networkx as nx
import scipy.stats as ss
from collections import defaultdict
from igraph import *
from sklearn import preprocessing

# Create the Scaler object
scaler = preprocessing.StandardScaler()

g_user_file = '../grafos/grafo_ELEICOES_BR.txt'
g_user_file = '../grafos/grafo_bipartite_ncol.txt'


print('reading graph...')
g_user = nx.read_edgelist(g_user_file, delimiter="\t", create_using=nx.Graph(), data=True)
print('number of nodes', g_user.number_of_nodes())

igraph = Graph.Read_Ncol(g_user_file, directed=True)

'''
print('computing katz centrality...')
k_user = nx.katz_centrality(g_user)
k_user_rank = {key: rank for rank, key in enumerate(sorted(k_user, key=k_user.get, reverse=True)) }
'''

'''
print('computing closeness centrality...')
c_user = dict()
for node in g_user.nodes():
  c = Graph.closeness(igraph, vertices = node)
  c_user[node] = c
c_user_rank = {key: rank for rank, key in enumerate(sorted(c_user, key=c_user.get, reverse=True)) }
'''

print('computing degree centrality...')
d_user = nx.degree_centrality(g_user)
d_user_rank = {key: rank for rank, key in enumerate(sorted(d_user, key=d_user.get, reverse=True)) }

#print('computing in-degree centrality...')
#id_user = nx.in_degree_centrality(g_user)
#id_user_rank = {key: rank for rank, key in enumerate(sorted(id_user, key=id_user.get, reverse=True)) }

#print('computing out-degree centrality...')
#od_user = nx.out_degree_centrality(g_user)
#od_user_rank = {key: rank for rank, key in enumerate(sorted(od_user, key=od_user.get, reverse=True)) }

print('computing betweeness centrality...')
b_user = nx.betweenness_centrality(g_user, k = 800)
b_user_rank = {key: rank for rank, key in enumerate(sorted(b_user, key=b_user.get, reverse=True)) }

print('computing eigenvector centrality...')
e_user =  nx.eigenvector_centrality(g_user, tol=1.0e-7, max_iter = 600)
e_user_rank = {key: rank for rank, key in enumerate(sorted(e_user, key=e_user.get, reverse=True)) }

user_list = list()
for user in d_user:
    dic = dict()

    dic['user'] = user
    dic['degree'] = d_user[user]
    dic['std_degree'] = d_user[user]
    dic['degree_rank'] = d_user_rank[user]


    dic['betweeness'] = b_user[user]
    dic['std_betweeness'] = b_user[user]
    dic['betweeness_rank'] =  b_user_rank[user]

    dic['eigenvector'] = e_user[user]
    dic['std_eigenvector'] = e_user[user]
    dic['eigenvector_rank'] =  e_user_rank[user]

    #dic['katz'] = k_user[user]
    #dic['std_katz'] = k_user[user]
    #dic['katz_rank'] =  k_user_rank[user]

    #dic['indegree'] = id_user[user]
    #dic['std_indegree'] = id_user[user]
    #dic['indegree_rank'] = id_user_rank[user]

    #dic['outdegree'] = od_user[user]
    #dic['std_outdegree'] = od_user[user]
    #dic['outdegree_rank'] = od_user_rank[user]

    '''
    dic['5_closeness_centrality_user'] = c_user[user]
    dic['closeness_centrality_user_rank'] =  c_user_rank[user]

    dic['6_betweeness_centrality_user'] = b_user[user]
    dic['betweeness_centrality_user_rank'] =  b_user_rank[user]

    ranks = [d_user_rank[user], k_user_rank[user], c_user_rank[user], b_user_rank[user], e_user_rank[user]]
    dic['std_ranks'] = np.std(ranks)
    '''

    user_list.append(dic)

df = pd.DataFrame(user_list)
df[['std_degree']] = scaler.fit_transform(df[['std_degree']])
#df[['std_indegree']] = scaler.fit_transform(df[['std_indegree']])
#df[['std_outdegree']] = scaler.fit_transform(df[['std_outdegree']])
df[['std_betweeness']] = scaler.fit_transform(df[['std_betweeness']])
df[['std_eigenvector']] = scaler.fit_transform(df[['std_eigenvector']])
#df[['std_katz']] = scaler.fit_transform(df[['std_katz']])



df.sort_values(by=['degree'], ascending = False).to_csv('ranks.csv', sep= '\t')