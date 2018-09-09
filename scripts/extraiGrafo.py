from itertools import groupby
import sys
import gzip
import json as simplejson
import unicodedata
from datetime import datetime, timedelta
from email.utils import parsedate_tz
import re
import os
import traceback
import glob
import networkx as nx
from collections import defaultdict

inputFileName = "../tweets_processados/tweets_processados_cruzeiro_flamengo.csv.gz"
inputFileName = "../tweets_processados/tweets_processados_palmeiras_corinthians.csv.gz"
inputFileName = "../tweets_processados/tweets_processados.txt.gz"
inputFileName = "../tweets_processados/tweets_processados_eleicoes_2018.txt.gz"
inputFileName = "../tweets_processados/tweets_processados_debate_band.txt.gz"
outputDir = "../grafos"
maxUsers = 5 * 1000 * 1000 
maxCount = 100 * 1000 * 1000


###########################
def dict_int():
        return defaultdict(int)
###########################
def extracting_counts(maxCount):
 size = 0
 print('computing # of edges on the whole ' + inputFileName + "...")
 dic_authors = defaultdict(dict_int)
 dic_tweets = defaultdict(dict_int)
 dic_edges = defaultdict(int)
 dic_threshold_topic = dict()
 count = 0
 with gzip.open(inputFileName, 'r') as fin:
   for line in fin:
      size += 1
      count += 1

      if count % (1 * 1000 * 1000) == 0:
        print("extracting_count " + str(count))
      if count > maxCount or maxCount == -1:
        break		
	
      #if size == 1000000:
      #	  	break	
      tweet = simplejson.loads(line.decode('utf-8'))
      is_retweet_button = tweet['is_retweet_button']
      topics = tweet['topics']		
	   
      for topicWithEntity in topics:	
        topic = topicWithEntity[0]	
        if is_retweet_button:	
          retweeted_msg_id = tweet['retweeted_msg_id']	  
          author = tweet['author']
          dic_authors[topic][author] += 1
          dic_tweets[topic][retweeted_msg_id] += 1		
          dic_edges[topic] += 1 

 print('# of edges : ' + str(size))
 print("\n")
 for topic in dic_tweets:
  print('# of authors: ' + topic + " "  + str(len(dic_authors[topic])))
  print('# of tweet ids: ' + topic + " " + str(len(dic_tweets[topic])))
  print('# of edges: ' + topic + " " + str(dic_edges[topic]))

  if len(dic_authors[topic]) > maxUsers or maxUsers == -1:
    userFreqThreshold = sorted(dic_authors[topic].values(), reverse=True)[maxUsers]
  else:
    userFreqThreshold = 0 
  dic_threshold_topic[topic] = userFreqThreshold
  print("user freq threshold: " + str(userFreqThreshold))
  print("\n")
 
 return dic_authors, dic_threshold_topic

######################
def create_graph(maxCount, dic_authors, dic_threshold_topic, is_bipartite):
 dic_G = dict()
 with gzip.open(inputFileName, 'r') as fin:
     count = 0	
     for line in fin:
         tweet = simplejson.loads(line.decode('utf-8'))
	 #print tweet	

         if count > maxCount or maxCount == -1:
           break		
	
         is_retweet_button = tweet['is_retweet_button']
         is_reply_button = tweet['is_reply_button']

         if is_retweet_button or is_reply_button:	


            if is_retweet_button:
               edge_type = 'RT'
            else:
               continue
               edge_type = 'REPLY'

            topics = tweet['topics']		
            if is_retweet_button:	
                    retweeted_msg_id = tweet['retweeted_msg_id']	  
                    retweeted_user = tweet['retweeted_user']	
                    retweet_reaction_time_sec = tweet['retweet_reaction_time_sec']		
 
                    interacted_user = retweeted_user
            else:
                    replied_user = tweet['replied_user']
                    interacted_user = replied_user 
	
            author = tweet['author']
            content = tweet['text']	
            for topicWithEntity in topics:	
               topic = topicWithEntity[0]	


               if topic != 'ELEICOES_BR':
                 continue	


               if len(topicWithEntity[1]) > 0:	
                 mentioned_entity = topicWithEntity[1][0] # TODO there can be more than one entity	
               else:
                 mentioned_entity = "NULL" 
               if dic_authors[topic][author] >= dic_threshold_topic[topic]:		
                 if topic not in dic_G:
                   #dic_G[topic] = nx.Graph()
                   dic_G[topic] = nx.DiGraph()
	           # TODO should be entity, not entidade
                 if is_bipartite:		
                    dic_G[topic].add_edge(author, retweeted_msg_id, type=edge_type)
	            #dic_G[topic].add_edge(author, retweeted_msg_id, type='RT', reaction_time_sec=retweet_reaction_time_sec, entidade=mentioned_entity)

                 else:
	            #dic_G[topic].add_edge(author, retweeted_user, type='RT', reaction_time_sec=retweet_reaction_time_sec, entidade=mentioned_entity)
		
                    weight = 1
                    dic_G[topic].add_node(retweeted_user, value="pt")
                    dic_G[topic].add_node(author, value="psdb")
                    dic_G[topic].add_edge(author, interacted_user) # type=edge_type, weight=weight)
	
         if count % (1 * 1000 * 1000) == 0:
           print("creating_graph " + str(count))
         count += 1    	

 print("Finished creating graph. Going to the last step...")
 print("Topics in dic_G are:")
 for topic in dic_G:
    print("topic " + topic)
 assert(len(dic_G) > 0)
 for topic in dic_G:

    #print("removing degree=1 edges from graph " + topic)
    #dic_G[topic].remove_nodes_from((n for n,d in dic_G[topic].degree_iter() if d==1))
    print("finding connected components for topic " + topic)
    #G_list = nx.connected_component_subgraphs(dic_G[topic])	
    #G_list = nx.strongly_connected_component_subgraphs(dic_G[topic])	
    G_list = nx.weakly_connected_component_subgraphs(dic_G[topic])	
    G_list = sorted(G_list, key = len, reverse = True)
    #G_list = [dic_G[topic]]
    if is_bipartite:
      print("storing " + outputDir + "/grafo_bipartite_" + topic +".txt/gml")
      nx.write_edgelist(G_list[0], outputDir + "/grafo_bipartite_" + topic +".txt", delimiter="\t", data=False) 
      nx.write_gml(G_list[0], outputDir + "/grafo_bipartite_" + topic +".gml") 
    else: 
      print("storing " + outputDir + "/grafo_" + topic +".txt/gml")
      nx.write_edgelist(G_list[0], outputDir + "/grafo_" + topic +".txt", delimiter="\t") 
      nx.write_gml(G_list[0], outputDir + "/grafo_" + topic +".gml") 
#
####################################

####################################
print("extracting counts:")
dic_authors, dic_threshold_topic = extracting_counts(maxCount)
#print "generating bipartite graphs:"
#create_graph(dic_authors, dic_threshold_topic, True)
print("creating graphs:")
create_graph(maxCount, dic_authors, dic_threshold_topic, False)
