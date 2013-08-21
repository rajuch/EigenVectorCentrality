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
            
#retrieves the followers of a given user from FollowEvents table
def getFollowers(user, conn):
    sql = 'select actor from FollowEvents where followedUser_login="'+user+'"';
    print sql
    res=executeSQL(conn,sql)
    followersList = []
    for row in res:
        followersList.append(row[0])
    return followersList
    

#retrieves the the users followed by given user
def getFollowing(user):
    sql = 'select followedUser_login from FollowEvents where actor="'+user+'"';
    #print sql
    res=executeSQL(conn,sql)
    followingList = []
    for row in res:
        followingList.append(row[0])
    return followingList

#adds the incoming edges to the graph. for ex: user1 has followers user2, user3 then the edges (user2, user1)
# (user2, user1) will be added to the graph
def addIncomingEdges(user, followersList, userConnectedGraph):
    for followUser in followersList:
        userConnectedGraph.add_edge(followUser,user)

#adds the outgoing edges to the graph for ex: user1 follows user2, user3 then the edges (user1, user2)
# (user1, user3) will be added to the graph
def addOutGoingEdges(user, followingUsersList, userConnectedGraph):
    for followingUser in followingUsersList:
        userConnectedGraph.add_edge(user,followingUser)

#retrieve the owners of repositories
def getUsers(repos, conn):
    sql = 'select author from repository where id in ('+repos+')'
    print sql
    res = executeSQL(conn, sql)
    usersSet =Set()
    for row in res:
        usersSet.add(row[0])
    return usersSet

def addNodes(userList, userConnectedGraph):
    for user in userList:
        userConnectedGraph.add_node(user)

# traverse the unExploredUserQueue, for every user get the following users and followed users and add these to the graph
# and to the queue
def createUserConnectedGraph(userConnectedGraph,unExploredUserQueue, ExploredUserMap):
    while unExploredUserQueue.empty()== False:
        user=unExploredUserQueue.get()
        if user not in ExploredUserMap:
            ExploredUserMap[user]=1
        
            followersList=getFollowers(user, conn)
            followingUsersList=getFollowing(user)
        
#             userConnectedGraph.add_nodes_from(followersList)
#             userConnectedGraph.add_nodes_from(followingUsersList)
            addNodes(followersList, userConnectedGraph)
            addNodes(followingUsersList, userConnectedGraph)
        
            addIncomingEdges(user,followersList, userConnectedGraph)
            addOutGoingEdges(user, followingUsersList, userConnectedGraph)
            print len(followersList), len(followingUsersList)
            for followUser in followersList:
                unExploredUserQueue.put(followUser)
            for followingUser in followingUsersList:
                unExploredUserQueue.put(followingUser)
            print 'queueSize::', unExploredUserQueue.qsize()

def writeCentralityOutput(centralityDict, fileName):
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
    f=open(filePath+fileName,'r')
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
            sql1='select count(*) from FollowEvents where followedUser_login="'+user+'"'
            res=executeSQL(conn, sql1)
            for row in res:
                count1=row[0]
                followingCountList.append(count1)
            counter +=1
            #print vals[1], count, count1
    p1,=plt.plot(centralityValList, followersCountList, marker='o')
    p2,=plt.plot(centralityValList, followingCountList,marker='o')
    plt.legend([p2, p1], ["following", "followers"])
    
    plt.xlabel('centralityValue', fontsize=10)
    plt.ylabel('followers/following',fontsize=10)
    
    #plt.show()
    plt.savefig(filePath+'graph/'+fileName+'.png')
    plt.close()

            
def start(repos, counter):
    unExploredUserQueue = Queue() #queue for maintaining the unexplored users
    ExploredUserMap = {} #map for maintaining the explored users

    usersSet = getUsers(repos, conn1)
    print len(usersSet)
    
    userConnectedGraph =nx.Graph()
    try:
    #add the users to the unExploredQueue
        for user in usersSet:
            unExploredUserQueue.put(user)
            createUserConnectedGraph(userConnectedGraph,unExploredUserQueue, ExploredUserMap)
    except Exception as e:
        print e
    
    t0 = time()
    path='/home/raju/Work/centrality/'
    #calculate the eigen vector centrality for userconnected graph
    try:
        eigenCentrality=nx.eigenvector_centrality(userConnectedGraph)
        writeCentralityOutput(eigenCentrality,path+'eigenCentrality'+str(counter))
        plotgraph(conn, path, 'eigenCentrality'+str(counter))
        #plotgraph(conn, path, 'eigenCentrality'+str(counter), 0)
    except Exception as e:
        print e

    t1=time()
    print 'eigen vector centrality completed', (t1-t0)
    #calculate the degree centrality for userconnected graph
    try:
        degreeCentrality = nx.degree_centrality(userConnectedGraph)
        writeCentralityOutput(degreeCentrality,path+'degreeCentrality'+str(counter))
        plotgraph(conn, path, 'degreeCentrality'+str(counter))
        #plotgraph(conn, path, 'degreeCentrality'+str(counter), 0)
    except Exception as e:
        print e

    t2=time()
    print 'degree centrality completed===>',(t2-t1)
    #calculate the betweeness centrality for userconnected graph
    try:
        betweenessCentrality = nx.betweenness_centrality(userConnectedGraph)
        writeCentralityOutput(betweenessCentrality,path+'betweenessCentrality'+str(counter))
        plotgraph(conn, path, 'betweenessCentrality'+str(counter))
    except Exception as e:
        print e

    t3=time()
    print 'between centrality completed===>',(t3-t2)
    try:
        closenessCentrality = nx.closeness_centrality(userConnectedGraph)
        writeCentralityOutput(closenessCentrality,path+'closenessCentrality'+str(counter))
        plotgraph(conn, path, 'closenessCentrality'+str(counter))
    except Exception as e:
        print e
    t4=time()
    print 'closeness centrality completed===>',(t4-t3)

    # nx.draw(userConnectedGraph)
    # plt.savefig("/home/raju/Work/centrality/userConnectedGraph.png")
            
try:
    conn =mdb.connect(host="localhost",user="root",passwd="root",db="github")
    conn1 = mdb.connect(host="localhost",user="root",passwd="root",db="github_cluster")
    f = open('/home/raju/Work/newoutput_100','r')
    counter=1
    for line in f:
        line=line.replace('\n','')
        start(line, counter)
        counter +=1
except Exception as e:
    raise e            
            