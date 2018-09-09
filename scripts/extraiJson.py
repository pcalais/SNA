import os
import sys
import glob
import traceback
import gzip
import json as simplejson
import re
import unicodedata
from datetime import datetime, timedelta
from email.utils import parsedate_tz
from itertools import groupby


############################################
def loadTermTopicMapping(term_topic_file):
    fh = open(term_topic_file, "r")
    dic_term_topic = dict()
    for line in fh.readlines():
        (topic, termString) = line.rstrip().split("\t")

        termsWithEntity = termString.split(",")
        for termWithEntity in termsWithEntity:
            if ":" in termWithEntity:
                (entity, term) = termWithEntity.split(":")
                dic_term_topic[term.lower()] = (topic, entity)
                print(term)
            else:
                term = termWithEntity
                dic_term_topic[term.lower()] = (topic, "NULL")
                print(term)

    return dic_term_topic


############################################
def extractFields(jsonInputFile, outputFile, dic_term_topic):
    print("input file: " + jsonInputFile + "...")
    ok = 0
    errors = 0
    otherLanguage = 0
    with gzip.open(jsonInputFile, 'rt') as fin:
        for line in fin:
            try:
                tweet = simplejson.loads(str(line))
            except:
                print(line)
                exc_type, exc_obj, exc_tb = sys.exc_info()
                fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                print(exc_type, fname, exc_tb.tb_lineno)
                traceback.print_exc()

                errors += 1
                continue

            try:
                filtered_tweet = filterFields(tweet)
            except:
                 #print(tweet['text'])
                 #exc_type, exc_obj, exc_tb = sys.exc_info()
                 #fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                 #print(exc_type, fname, exc_tb.tb_lineno)
                 #traceback.print_exc()

                 # exit
                 errors += 1
                 continue

            # if 'lang' in tweet and (tweet['lang'] != 'pt' or tweet['lang'] != 'us'):
            #	otherLanguage += 1
            #	continue

            text = filtered_tweet['text']
            topicEntityList = getTopics(text, dic_term_topic)

            json_filtered_tweet = simplejson.dumps(filtered_tweet, sort_keys=True)
            outputFile.write(json_filtered_tweet)
            outputFile.write("\n")
            ok += 1

    print("ok: " + str(ok))
    print("errors: " + str(errors) + " - " + str(errors*100/float(ok)) + "%")
    print("other language: " + str(otherLanguage))


############################################
def filterFields(tweet):
    id = tweet['id']
    text = strip_accents(tweet['text'])
    retweeted = tweet['retweeted']
    datetime = tweet['created_at']

    user_json = tweet['user']
    author_screen_name = user_json['screen_name'].lower()

    is_retweet_button = 'retweeted_status' in tweet
    is_manual_retweet = is_retweet_button == False and text.startswith("RT @")
    is_quote_retweet = is_manual_retweet == False and is_retweet_button == False and "RT @" in text

    is_reply_button = tweet['in_reply_to_screen_name'] != None
    is_manual_reply = is_retweet_button == False and is_manual_retweet == False and is_reply_button == False and text.startswith(
        "@")
    # TODO gerar um replyPattern e usa-lo aqui http://stackoverflow.com/questions/12595051/python-check-if-string-matches-pattern
    # TODO criar testes para garantir que vc nao esta misturando RTs com replies
    is_mentioned_reply = is_retweet_button == False and is_manual_retweet == False and is_reply_button == False and is_manual_reply == False and "@" in text

    filtered_tweet = {
        'msg_id': id,
        'text': text,
        'author': author_screen_name,
        'is_reply_button': is_reply_button,
        'is_manual_reply': is_manual_reply,
        'is_retweet_button': is_retweet_button,
        'is_manual_retweet': is_manual_retweet,
        'is_quote_retweet': is_quote_retweet,
        'datetime': datetime}

    if is_retweet_button:
        retweeted_user = tweet['retweeted_status']['user']['screen_name']
        retweeted_user_followers_count = tweet['retweeted_status']['user']['followers_count']
        is_retweeted_user_verified = tweet['retweeted_status']['user']['verified']
        retweeted_msg_id = str(tweet['retweeted_status']['id'])

        # TODO guardar ids dos RTs comentados tambem.
        filtered_tweet['retweeted_user'] = retweeted_user
        filtered_tweet['retweeted_user_followers_count'] = retweeted_user_followers_count
        filtered_tweet['retweeted_msg_id'] = "msg_" + retweeted_user + "_" + retweeted_msg_id
        filtered_tweet['is_retweeted_user_verified'] = is_retweeted_user_verified

        original_msg_datetime = tweet['retweeted_status']['created_at']
        delta_time = getDeltaTime(original_msg_datetime, datetime)
        filtered_tweet['retweet_reaction_time_sec'] = delta_time
    if is_manual_retweet or is_quote_retweet:
        rt_patterns = re.compile(r".*RT @(\w+)")
        retweeted_user = [retweeted_user for retweeted_user in rt_patterns.findall(text)][0]
        filtered_tweet['retweeted_user'] = retweeted_user

    if is_reply_button:
        replied_user = tweet['in_reply_to_screen_name']
        filtered_tweet['replied_user'] = replied_user

    if is_manual_reply or is_mentioned_reply:
        mention_patterns = re.compile(r"@(\w+)")
        mentioned_user = [mentioned_user for mentioned_user in mention_patterns.findall(text)][0]
        filtered_tweet['replied_user'] = mentioned_user

    topics = getTopics(text, dic_term_topic)

    filtered_tweet['topics'] = topics

    return filtered_tweet


############################################
def strip_accents(s):
    return ''.join(c for c in unicodedata.normalize('NFD', s)
                   if unicodedata.category(c) != 'Mn')


############################################
def getDeltaTime(datetime1, datetime2):
    # Sun Sep 21 21:09:18 +0000 2014
    # print datetime1 + "|" + datetime2
    time_tuple = parsedate_tz(datetime1.strip())
    dt = datetime(*time_tuple[:6])
    datetimeobject1 = dt - timedelta(seconds=time_tuple[-1])
    time_tuple = parsedate_tz(datetime2.strip())
    dt = datetime(*time_tuple[:6])
    datetimeobject2 = dt - timedelta(seconds=time_tuple[-1])

    delta_sec = int((datetimeobject2 - datetimeobject1).total_seconds())

    return delta_sec


############################################
def getTopics(text, dic_term_topic):
    topicalTerms = dic_term_topic.keys()
    topicList = list()
    tweetTerms = re.findall(r"[\w']+", text.lower())

    for topicalTerm in topicalTerms:
        if not " " in topicalTerm:
            if topicalTerm in tweetTerms:
                topicList.append(dic_term_topic[topicalTerm])
            elif len(topicalTerm) > 4:
                for tweetTerm in tweetTerms:
                    if len(tweetTerm) > 4 and topicalTerm in tweetTerm:
                      topicList.append(dic_term_topic[topicalTerm])
        elif topicalTerm.lower() in text.lower():
            topicList.append(dic_term_topic[topicalTerm])

    tuples = list()
    for topic, entities in groupby(topicList, lambda x: x[0]):
        entities = list(set([entity[1] for entity in entities]))
        if 'NULL' in entities:
            entities.remove('NULL')
        tuples.append((topic, entities))
    if len(tuples) == 0:
        tuples.append(('NO_TOPIC', []))

    return tuples


############################################
termTopicFilename = "termos_topico.txt"
# outputFilename = "/media/pcalais/My Passport/dados_processados/dados_processados_coleta180.txt.gz"
outputFilename = "/home/pedro/PESQUISA/tweets_processados/tweets_processados_cruzeiro_flamengo.csv.gz"
outputFilename = "/home/pedro/PESQUISA/tweets_processados/tweets_processados_palmeiras_corinthians.csv.gz"
outputFilename = "/home/pedro/PESQUISA/tweets_processados/tweets_processados_vasco_cruzeiro.txt.gz"
outputFilename = "/home/pedro/PESQUISA/tweets_processados/tweets_processados_corinthians_palmeiras_brasileiro_turno1.txt.gz"
outputFilename = "/home/pedro/PESQUISA/tweets_processados/tweets_processados_classicosCAM_CRU_VAS_FLA_brasileiro_turno1.txt.gz"
outputFilename = "/home/pedro/PESQUISA/tweets_processados/tweets_processados_vasco_fluminense_20180719.txt.gz"
outputFilename = "/home/pedro/PESQUISA/tweets_processados/tweets_processados_eleicoes_2018.txt.gz"
outputFilename = "/home/pedro/PESQUISA/tweets_processados/tweets_processados_debate_band.txt.gz"



assert (outputFilename.endswith(".gz"))
outputFile = gzip.open(outputFilename, "wt")
print("outputfile " + outputFilename)


dic_term_topic = loadTermTopicMapping(termTopicFilename)

#inputFiles = glob.glob("/media/pedro/My Passport/coletaTwitter/coleta2017*")
# inputFiles = glob.glob("/media/pedro/FreeAgent Drive/coletaTwitter/coleta2017*")
# inputFiles = glob.glob("/home/pedro/PESQUISA/tweets_processados/coleta201709-Cruzeiro-Flamengo-Copa-BR.txt.gz")
#inputFiles = glob.glob("/home/pedro/PESQUISA/tweets_processados/coletaCorinthiansPalmeirasBrasileirao2018Turno1.txt.gz")
#inputFiles = glob.glob("/home/pedro/PESQUISA/tweets_processados/coletaClassicosCAM_CRU_VAS_FLA_brasileiro_turno1.txt.gz")
#inputFiles = glob.glob("/home/pedro/PESQUISA/tweets_processados/coleta20180719.txt.gz")
inputFiles = glob.glob("/home/pedro/PESQUISA/tweets_processados/coletaEleicoes2018.txt.gz")
inputFiles = glob.glob("/home/pedro/PESQUISA/tweets_processados/coleta20180809.txt.gz")



for file in inputFiles:
    print(file)
    extractFields(file, outputFile, dic_term_topic)

print('FINISHED.')

print(getTopics("This is fake news.", dic_term_topic))
print(getTopics("O Luxa, do Palmeiras, nao tinha dito que o Flamengo e o Palmeiras iam ganhar? E a Dilma?",
                dic_term_topic))
print(getTopics("olha o GOL!", dic_term_topic))
print(getTopics("olha o golpista!", dic_term_topic))
print(getTopics("gol", dic_term_topic))
print(getTopics("O Luxa nao tinha dito que o flamengo ia ganhar?", dic_term_topic))
print(getTopics("Dilma e golpista", dic_term_topic))
print(getTopics("Dilma e o gol, o que dizer?", dic_term_topic))
print(getTopics("E quando tem dentro da tag? #VaiCorinthians", dic_term_topic))
