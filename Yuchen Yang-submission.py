## import modules here

########## Question 1 ##########
'''
========= Version one According to the Psedudo code to complete the program =================================================

def c2lsh(data_hashes, query_hashes, alpha_m, beta_n):
    offset = -1
    while True:
        offset += 1
        RDD = data_hashes.filter(lambda x : Match(x[1], query_hashes, alpha_m, offset)) # select RDD
        RDD2 = RDD.map(lambda x : x[0])    # change RDD to new RDD
        nb_candidate = RDD2.count()
        if nb_candidate >= beta_n:
            return RDD2

def Match(data_list, query_list, alpha_m, offset):
    count = 0
    for i in range(len(data_list)):
        if (abs(data_list[i] - query_list[i])) <= offset:
            count += 1
    if count >=alpha_m:
        return 1
    else:
        return 0

========= Version two  change map and filter these two transformation function to one flatMap transformation function ======

def c2lsh(data_hashes, query_hashes, alpha_m, beta_n):
    offset = -1
    while True:
        offset += 1
        RDD = data_hashes.flatMap(lambda x : [x[0]] if Match(x[1], query_hashes, alpha_m, offset) else [])
        nb_candidate = RDD.count()
        if nb_candidate >= beta_n:
            return RDD

def Match(data_list, query_list, alpha_m, offset):
    count = 0
    for i in range(len(data_list)):
        if (abs(data_list[i] - query_list[i])) <= offset:
            count += 1
    if count >=alpha_m:
        return 1
    else:
        return 0

======================   Version three  —— small change in Match function   ================================================

def c2lsh(data_hashes, query_hashes, alpha_m, beta_n):
    offset = -1
    while True:
        offset += 1
        RDD = data_hashes.flatMap(lambda x : [x[0]] if Match(x[1], query_hashes, alpha_m, offset) else [])
        nb_candidate = RDD.count()
        if nb_candidate >= beta_n:
            return RDD

def Match(data_list, query_list, alpha_m, offset):
    count = 0
    for i in range(len(data_list)):
        if (abs(data_list[i] - query_list[i])) <= offset:
            count += 1
        if count >=alpha_m:
            return 1
    return 0

=====================   Version four  ——  Ignore these elements which have been tested with matched  =======================


def c2lsh(data_hashes, query_hashes, alpha_m, beta_n):
    offset = 0
    data_hashes = data_hashes.map(lambda x : [0, x[0], x[1]])
    while True:
        RDD = data_hashes.map(lambda x : x[1] if Match(x, query_hashes, alpha_m, offset) else [])
        RDD = RDD.filter(lambda x : x != [])
        nb_candidate = RDD.count()
        if nb_candidate >= beta_n:
            return RDD
        else:
            offset += 1
def Match(Data_Hash, query_list, alpha_m, offset):
    if Data_Hash[0] == 1:
        return 1
    count = 0
    for i in range(len(Data_Hash[2])):
        if (abs(Data_Hash[2][i] - query_list[i])) <= offset:
            count += 1
        if count >= alpha_m:
            Data_Hash[0] = 1
            return 1
    return 0

=====================   Version five  ——  Map ---> Mappartitions  ===========================================================
'''

def c2lsh(data_hashes, query_hashes, alpha_m, beta_n):
    def iteration(iter):
        for x in iter:
            yield ([0, x[0], x[1]])
    data_hashes = data_hashes.mapPartitions(lambda x : iteration(x))

    def Match(Data_Hash, query_list, alpha_m, offset):
        if Data_Hash[0] == 1:
            return 1
        count = 0
        for i in range(len(Data_Hash[2])):
            if (abs(Data_Hash[2][i] - query_list[i])) <= offset:
                count += 1
            if count >= alpha_m:
                Data_Hash[0] = 1
                return 1
        return 0

    offset = 0
    while True:
        RDD = data_hashes.map(lambda x : x[1] if Match(x, query_hashes, alpha_m, offset) else [])
        RDD = RDD.filter(lambda x : x != [])
        nb_candidate = RDD.count()
        if nb_candidate >= beta_n:
            return RDD
        else:
            offset += 1











