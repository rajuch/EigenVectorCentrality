EigenVectorCentrility
=====================
Problem Statement:
 Finding the important actors in the github using the eigenvector centrality.
 
 Approach.
 1. Take the cluster of similar repositories, find the owners of it.
 2. Add these owners to the unExploredQueue.
 3. loop unExploredQueue until empty
 4. add the user to the explored map if the user is not explored
 5. find the following, followers for the user
 6. add these nodes to the graph
 7. create incoming edges for user, followers. ex: if user1 has followers user2, user3 then the edges (user2, user1)
 (user2, user1) will be added to the graph
 8. create outgoing edges for user, following. ex: user1 follows user2, user3 then the edges (user1, user2)
 (user1, user3) will be added to the graph
 9. loop ends when the unExploredQueue is empty
 9. find the eigenvalue vector of the created user connected graph using python networkx library
