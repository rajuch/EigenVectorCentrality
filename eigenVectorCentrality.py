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

# traverse the UnExploredUserQueue, for every user get the following users and followed users and add these to the graph
# and to the queue
def createUserConnectedGraph():
    global userConnectedGraph
    global UnExploredUserQueue
    print 'queuesize::', UnExploredUserQueue.qsize()
    while UnExploredUserQueue.empty()== False:
        user=UnExploredUserQueue.get()
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
                UnExploredUserQueue.put(followUser)
            for followingUser in followingUsersList:
                UnExploredUserQueue.put(followingUser)
            print 'queueSize::', UnExploredUserQueue.qsize()
            
conn =mdb.connect(host="localhost",user="root",passwd="root",db="github")
conn1 = mdb.connect(host="localhost",user="root",passwd="root",db="github_cluster")
#repos ='1300,6276,6801,10512,10633,11394,24024,24222,24873,26019,28190,31738,38812,40323,41737,44790,51428,63074,69689,74865,79981,80013'
repos1='1488,1920,2516,5813,6466,6654,7114,10091,10203,10282,13297,15168,15655,17886,18127,19627,19633,21987,23595,24005,24380,24809,24811,24978,25222,26243,27405,27463,28484,29244,30439,31726,36148,36325,37332,37405,37592,38513,38691,41011,41930,44032,46514,46553,47471,47982,49165,50359,50412,50445,51282,52331,53381,53404,54483,55113,55219,55633,56031,56169,56708,57625,59542,62021,62999,63017,63342,64012,64125,64230,64920,65008,65023,65790,66878,66926,67794,68574,68827,69546,71918,74633,77219,78674,81124'

UnExploredUserQueue = Queue() #queue for maintaining the unexplored users
ExploredUserMap = {} #map for maintaining the explored users

usersSet = getUsers(repos1, conn1)
print len(usersSet)

for user in usersSet:
    UnExploredUserQueue.put(user)

userConnectedGraph=nx.DiGraph()
createUserConnectedGraph()

centrality=nx.eigenvector_centrality(userConnectedGraph)
#print(['%s %0.2f'%(node,centrality[node]) for node in centrality])
centralityList = []
for node in centrality:
    class eigencentrality():
        pass
    obj = eigencentrality()
    obj.node = node
    obj.val = centrality[node]
    centralityList.append(obj)
    
centralityList.sort(key = lambda a: a.val)
f = open('/home/raju/Work/sortedcentralityOutput1','w')
for centralityObj in centralityList:
    f.write(str(centralityObj.node)+" "+str(centralityObj.val))
    f.write('\n')
    f.flush()
f.close()            
            
            
            