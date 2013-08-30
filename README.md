EigenVectorCentrality
=====================
Problem Statement:
 Finding the important actors in the github using the eigenvector centrality.
 
 Procedure for creating whole user connected graph
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

Procedure for creating user connected graph for single cluster

Take the cluster of similar repositories (read from file), find the owners of it and add to the userSet

   For every user,
   
	add the user to  unExploredqueue
	
	until unExploredqueue is empty
	
		find the users followed by the user, add to the followingList
		
		add these users to the unExploredqueue
		
		check the other users in the userSet are present in the followingList
		
		if present then add the outgoing edge to the graph(user, otheruser)

One important feature of networks is the relative centrality of individuals in them. Centrality is a structural characteristic of individuals in the network, meaning a centrality score tells you something about how that individual fits within the network overall. Individuals with high centrality scores are often more likely to be leaders, key conduits of information, and be more likely to be early adopters of anything that spreads in a network.

Eigenvector centrality:
It is a measure of the influence of a node in a network. It assigns relative scores to all nodes in the network based on the concept that connections to high-scoring nodes contribute more to the score of the node in question than equal connections to low-scoring nodes. 

Eigenvector centrality is calculated by assessing how well connected an individual is to the parts of the network with the greatest connectivity. Individuals with high eigenvector scores have many connections, and their connections have many connections, and their connections have many connections … out to the end of the network.

High eigenvector centrality individuals are leaders of the network. They are often important members with many connections to other high-profile individuals. Thus, they often play roles of key opinion leaders.

python networkx library:
eigenvector_centrality:
Uses the power method to find the eigenvector for the largest eigenvalue of the adjacency matrix of G.
