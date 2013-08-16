'''
Created on 14-Aug-2013

@author: raju
'''
import MySQLdb as mdb
import networkx as nx
from Queue import *
from sets import Set

def executeSQL(con,sql):
    cursor = con.cursor()
    cursor.execute(sql)    
    return cursor.fetchall()

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
    print sql
    res=executeSQL(conn,sql)
    followingList = []
    for row in res:
        followingList.append(row[0])
    return followingList

#adds the incoming edges to the graph. for ex: user1 has followers user2, user3 then the edges (user2, user1)
# (user2, user1) will be added to the graph
def addIncomingEdges(user, followersList):
    global userConnectedGraph
    for followUser in followersList:
        userConnectedGraph.add_edge(followUser,user)
    pass

#adds the outgoing edges to the graph for ex: user1 follows user2, user3 then the edges (user1, user2)
# (user1, user3) will be added to the graph
def addOutGoingEdges(user, followingUsersList):
    global userConnectedGraph
    for followingUser in followingUsersList:
        userConnectedGraph.add_edge(user,followingUser)
    pass

#retrieve the owners of repositories
def getUsers(repos, conn):
    sql = 'select author from repository where id in ('+repos+')'
    print sql
    res = executeSQL(conn, sql)
    usersSet =Set()
    for row in res:
        usersSet.add(row[0])
    return usersSet

# traverse the unExploredUserQueue, for every user get the following users and followed users and add these to the graph
# and to the queue
def createUserConnectedGraph():
    global userConnectedGraph
    global unExploredUserQueue
    print 'queuesize::', unExploredUserQueue.qsize()
    while unExploredUserQueue.empty()== False:
        user=unExploredUserQueue.get()
        if user not in ExploredUserMap:
            ExploredUserMap[user]=1
        
            followersList=getFollowers(user, conn)
            followingUsersList=getFollowing(user)
        
            userConnectedGraph.add_nodes_from(followersList)
            userConnectedGraph.add_nodes_from(followingUsersList)
        
            addIncomingEdges(user,followersList)
            addOutGoingEdges(user, followingUsersList)
            print len(followersList), len(followingUsersList)
            for followUser in followersList:
                unExploredUserQueue.put(followUser)
            for followingUser in followingUsersList:
                unExploredUserQueue.put(followingUser)
            print 'queueSize::', unExploredUserQueue.qsize()
            
conn =mdb.connect(host="localhost",user="root",passwd="root",db="github")
conn1 = mdb.connect(host="localhost",user="root",passwd="root",db="github_cluster")

#read the repos cluster from file
repos =''
try:
    f = open('/home/raju/Work/repoCluster.txt','r')
    repos = f.read()
except Exception as e:
    raise e

unExploredUserQueue = Queue() #queue for maintaining the unexplored users
ExploredUserMap = {} #map for maintaining the explored users

usersSet = getUsers(repos, conn1)
print len(usersSet)

#add the users to the unExploredQueue
for user in usersSet:
    unExploredUserQueue.put(user)

userConnectedGraph=nx.DiGraph()
createUserConnectedGraph()

#calculate the eigen vector centrality for userconnected graph
centrality=nx.eigenvector_centrality(userConnectedGraph)

centralityList = []
for node in centrality:
    class eigencentrality():
        pass
    obj = eigencentrality()
    obj.node = node
    obj.val = centrality[node]
    centralityList.append(obj)

#sort the the results based on the values     
centralityList.sort(key = lambda a: a.val)

f = open('/home/raju/Work/sortedcentralityOutput1','w')
for centralityObj in centralityList:
    f.write(str(centralityObj.node)+" "+str(centralityObj.val))
    f.write('\n')
    f.flush()
f.close()            
            
            
            