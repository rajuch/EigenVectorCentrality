'''
Created on 14-Aug-2013

@author: raju
'''
import MySQLdb as mdb
import networkx as nx
from Queue import *
from sets import Set
from time import time
import matplotlib.pyplot as plt

def executeSQL(con,sql):
    cursor = con.cursor()
    cursor.execute(sql)    
    return cursor.fetchall()

def loadUsers(con):
    sql = 'select distinct actor from FollowEvents'
    global userMap
    res=executeSQL(conn,sql)
    count=0
    for row in res:
        user = row[0]
        if user not in userMap:
            userMap[user] = count
            print user, count
            count+=1
    sql1 = 'select distinct followedUser_login from FollowEvents'
    res1=executeSQL(conn,sql1)
    for row in res1:
        user = row[0]
        if user not in userMap:
            userMap[user] = count
            print user, count
            count+=1
            
def getFollowers(user):
    """
    retrieves the followers of a given user from FollowEvents table
    Parameters:
    user - user name
    """
    sql = 'select actor from FollowEvents where followedUser_login="'+user+'"';
    print sql
    res=executeSQL(conn,sql)
    followersList = []
    for row in res:
        followersList.append(row[0])
    return followersList
    

def getFollowing(user):
    """
    retrieves the the users followed by given user
    parameters:
    user - user name
    """
    sql = 'select followedUser_login from FollowEvents where actor="'+user+'"';
    #print sql
    res=executeSQL(conn,sql)
    followingList = []
    for row in res:
        followingList.append(row[0])
    return followingList

#
def addIncomingEdges(user, followersList, userConnectedGraph):
    """
    adds the incoming edges to the graph. for ex: user1 has followers user2, user3 then the edges (user2, user1)
   (user2, user1) will be added to the graph
    parameters:
    user - user name
    followersList - list of users
    userConnectedGraph - graph
    """
    for followUser in followersList:
        userConnectedGraph.add_edge(followUser,user)


def addOutGoingEdges(user, followingUsersList, userConnectedGraph):
    """
    adds the outgoing edges to the graph for ex: user1 follows user2, user3 then the edges (user1, user2)
    (user1, user3) will be added to the graph
    parameters:
    user - user name
    followingUsersList - list of users
    userConnectedGraph - graph
    """
    for followingUser in followingUsersList:
        userConnectedGraph.add_edge(user,followingUser)

def getUsers(repos):
    """
    retrieve the owners of repositories
    parameters:
    repos - comma separated repository ids
    """
    sql = 'select author from repository where id in ('+repos+')'
    print sql
    res = executeSQL(conn1, sql)
    usersSet =Set()
    for row in res:
        usersSet.add(row[0])
    return usersSet

def createUserConnectedWholeGraph(userConnectedGraph,unExploredUserQueue, ExploredUserMap):
    """
    traverse the unExploredUserQueue, for every user get the following users and followed users and add these to the graph
    and to the queue
    parameters:
    userConnectedGraph - graph
    unExploredUserQueue - queue contains the users
    ExploredUserMap - map contains the explored users
    """
    while unExploredUserQueue.empty()== False:
        user=unExploredUserQueue.get()
        if user not in ExploredUserMap:
            ExploredUserMap[user]=1
        
            followersList=getFollowers(user, conn)
            followingUsersList=getFollowing(user)
        
            userConnectedGraph.add_nodes_from(followersList)
            userConnectedGraph.add_nodes_from(followingUsersList)
        
            addIncomingEdges(user,followersList, userConnectedGraph)
            addOutGoingEdges(user, followingUsersList, userConnectedGraph)
            print len(followersList), len(followingUsersList)
            for followUser in followersList:
                unExploredUserQueue.put(followUser)
            for followingUser in followingUsersList:
                unExploredUserQueue.put(followingUser)
            print 'queueSize::', unExploredUserQueue.qsize()

def writeCentralityOutput(centralityDict, fileName):
    """
    writes the centrality output to the file
    parameters:
    centralityDict - centrality dictionary contains nodes and values
    fileName - writes the output to this file 
    """
    centralityList = []
    for node in centralityDict:
        class centrality():
            pass
        obj = centrality()
        obj.node = node
        obj.val = centralityDict[node]
        centralityList.append(obj)

    #sort the the results based on the values     
    centralityList.sort(key = lambda a: a.val, reverse=True)

    f = open(fileName,'w')
    for centralityObj in centralityList:
        f.write(str(centralityObj.node)+" "+str(centralityObj.val))
        f.write('\n')
        f.flush()
    f.close() 

def plotgraph(conn, filePath, fileName):
    userList = []
    centralityValList = []
    followersCountList = []
    followingCountList =[]
    counter=0
    try:
        f=open(filePath+fileName,'r')
        for line in f:
            if counter < 20:
                vals = line.split(' ')
                user = vals[0]
                userList.append(user)
                centralityValList.append(vals[1])
                sql = 'select count(*) from FollowEvents where actor="' + user + '"'
            
                res = executeSQL(conn, sql)
                for row in res:
                    count = row[0]
                    followersCountList.append(count)
#                 sql1 = 'select count(*) from FollowEvents where followedUser_login="' + user + '"'
#                 res = executeSQL(conn, sql1)
#                 for row in res:
#                     count1 = row[0]
#                     followingCountList.append(count1)
                counter += 1
            # print vals[1], count, count1
        p1,=plt.plot(centralityValList, followersCountList, marker='o')
        #p2,=plt.plot(centralityValList, followingCountList,marker='o')
        #plt.legend([p2, p1], ["following", "followers"])
    
        plt.xlabel('centralityValue', fontsize=10)
        plt.ylabel('following',fontsize=10)
    
        plt.savefig(filePath+'graph/'+fileName+'.png')
        plt.close()
    except Exception as e:
        print e

def plotgraphForCentralities(filePath, filesList):
    """
    plots the graphs for all the centralities
    parameters:
    filePath - file path
    filesList - list of file names, from these files it reads the outputs of centralities
    """
    userList = []
    centralityList = []
    followersList = []
    
    for centralityFile in filesList:
        f=open(filePath+centralityFile,'r')
        counter=0
        centralityValList = []
        followersCountList = []
        for line in f:
            if counter <20:
                vals=line.split(' ')
                user = vals[0]
                userList.append(user)
                centralityValList.append(vals[1])
                sql = 'select count(*) from FollowEvents where actor="'+user+'"'
            
                res=executeSQL(conn, sql)
                for row in res:
                    count=row[0]
                    followersCountList.append(count)
                counter +=1
        centralityList.append(centralityValList)
        followersList.append(followersCountList)
        f.close()
        
    p1,=plt.plot(centralityList[0], followersList[0], marker='o')
    p2,=plt.plot(centralityList[1], followersList[1],marker='o')
    #p3,=plt.plot(centralityList[2], followersList[2],marker='o')
    #plt.legend([p3, p2, p1], ["closeness","Degree", "Eigenvector"])
    plt.legend([p2, p1], ["Degree", "Eigenvector"])
    
    plt.xlabel('centralityValue', fontsize=10)
    plt.ylabel('followers',fontsize=10)
    
    #plt.show()
    plt.savefig(filePath+'graph/'+'eigen'+filesList[1]+'.png')
    plt.close()

def calculateEigenCentrality(userConnectedGraph, counter):
    """
    calculates the eigenVector Centrality for given graph and writes the output to file
    parameters:
    userConnectedGraph - graph
    counter - int value for maintining unique file names
    """
    eigenCentrality=nx.eigenvector_centrality(userConnectedGraph)
    writeCentralityOutput(eigenCentrality,path+'eigenCentrality'+str(counter))
    plotgraph(conn, path, 'eigenCentrality'+str(counter))

def calculateDegreeCentrality(userConnectedGraph, counter):
    """ calculates the degree Centrality for given graph and writes the output to file
    parameters:
    userConnectedGraph - graph
    counter - int value for maintining unique file names """
    degreeCentrality = nx.out_degree_centrality(userConnectedGraph)
    writeCentralityOutput(degreeCentrality,path+'degreeCentrality'+str(counter))
    plotgraph(conn, path, 'degreeCentrality'+str(counter))
    
def calculateClosenessCentrality(userConnectedGraph, counter):
    """ calculates the closeness Centrality for given graph and writes the output to file
    parameters:
    userConnectedGraph - graph
    counter - int value for maintining unique file names """
    closenessCentrality = nx.closeness_centrality(userConnectedGraph)
    writeCentralityOutput(closenessCentrality,path+'closenessCentrality'+str(counter))
    plotgraph(conn, path, 'closenessCentrality'+str(counter))
 
def createUserConnectedGraph(user1,userConnectedGraph,usersSet):
    """
    creates the user connected graph, for an user it finds the following users and add the the outgoing edges to the graph
    parameters:
    user1 - user
    userConnectedGraph - graph
    usersSet - set of users
    """
    unExploredUserQueue = Queue() #queue for maintaining the unexplored users
    ExploredUserMap = {} #map for maintaining the explored users
    unExploredUserQueue.put(user1)
    addedUserCount=0
    userSetSize = len(usersSet) -1
    edgeWeight =1
    addedUserMap ={}
    while unExploredUserQueue.empty()== False and addedUserCount < userSetSize:
        user=unExploredUserQueue.get()
        #nullss is a marker to identify the levels
        if user == 'nullss':
            edgeWeight +=1
        if user != 'nullss' and user not in ExploredUserMap:
            ExploredUserMap[user]=1
        
            followingMap=getFollowing(user)
            
            for otherUser in usersSet:
                if user1!=otherUser:
                    if otherUser in followingMap:
                        if otherUser in addedUserMap:
                            pass
                        else:
                            addedUserMap[otherUser]=1
                            addedUserCount +=1
                            userConnectedGraph.add_edge(user1,otherUser,weight=edgeWeight)
                            print 'found'
                            pass
                
            for followUser in followingMap:
                unExploredUserQueue.put(followUser)
            unExploredUserQueue.put('nullss')
            #print 'queueSize::', unExploredUserQueue.qsize()
            
def start(repos, counter):
    """
    creates the user connected graph with the given repository owners as nodes
    parameters:
    repos - comma separated repository ids
    counter - int value
    """
    usersSet = getUsers(repos, conn1)
    print len(usersSet)
    userConnectedGraph =nx.DiGraph()
    try:
        for user in usersSet:
            createUserConnectedGraph(user,userConnectedGraph,usersSet)
            
        calculateEigenCentrality(userConnectedGraph, counter)
        calculateDegreeCentrality(userConnectedGraph, counter)
        calculateClosenessCentrality(userConnectedGraph, counter)
    except Exception as e:
        try:
            calculateEigenCentrality(userConnectedGraph, counter)
            calculateDegreeCentrality(userConnectedGraph, counter)
            calculateClosenessCentrality(userConnectedGraph, counter)    
        except Exception as e:
            print e
            pass
           
def startForCreatingWholeGraph(repos, counter):
    """
    creates the whole user connected graph
    parameters:
    repos - comma separated repository ids
    counter - int value
    """
    unExploredUserQueue = Queue() #queue for maintaining the unexplored users
    ExploredUserMap = {} #map for maintaining the explored users

    usersSet = getUsers(repos)
    print len(usersSet)
    
    userConnectedGraph =nx.DiGraph()
    try:
    #add the users to the unExploredQueue
        for user in usersSet:
            unExploredUserQueue.put(user)
            createUserConnectedWholeGraph(userConnectedGraph,unExploredUserQueue, ExploredUserMap)
            
        calculateEigenCentrality(userConnectedGraph, counter)
        calculateDegreeCentrality(userConnectedGraph, counter)
        #calculateClosenessCentrality(userConnectedGraph, counter)
        
        filesList=[]
        filesList.append('eigenCentrality'+str(counter))
        filesList.append('degreeCentrality'+str(counter))
    #filesList.append('closenessCentrality'+str(counter))
        plotgraphForCentralities(path, filesList)
    except Exception as e:
        print e
    
   
    
    #calculate the eigen vector centrality for userconnected graph
    
    
    #calculate the betweeness centrality for userconnected graph
#     try:
#         betweenessCentrality = nx.betweenness_centrality(userConnectedGraph)
#         writeCentralityOutput(betweenessCentrality,path+'betweenessCentrality'+str(counter))
#         plotgraph(conn, path, 'betweenessCentrality'+str(counter))
#     except Exception as e:
#         print e

    #t3=time()
#     print 'between centrality completed===>',(t3-t2)
#     try:
#         closenessCentrality = nx.closeness_centrality(userConnectedGraph)
#         writeCentralityOutput(closenessCentrality,path+'closenessCentrality'+str(counter))
#         plotgraph(conn, path, 'closenessCentrality'+str(counter))
#     except Exception as e:
#         print e

    # nx.draw(userConnectedGraph)
    # plt.savefig("/home/raju/Work/centrality/userConnectedGraph.png")
            
try:
    conn =mdb.connect(host="localhost",user="root",passwd="root",db="github")
    conn1 = mdb.connect(host="localhost",user="root",passwd="root",db="github_cluster")
    path='/home/raju/Work/centrality/test/'
    f = open('/home/raju/Work/newoutput_100','r')
    counter=1
    for line in f:
        line=line.replace('\n','')
        start(line, counter)
        counter +=1
except Exception as e:
    raise e            
            
