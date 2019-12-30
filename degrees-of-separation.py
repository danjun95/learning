#Boilerplate stuff:
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("DegreesOfSeparation")
sc = SparkContext(conf = conf)

# The characters we wish to find the degree of separation between:
startCharacterID = 5306 #SpiderMan                                              #hard coded start
targetCharacterID = 14  #ADAM 3,031 (who?)                                      #hard coded end

# Our accumulator, used to signal when we find the target character during
# our BFS traversal.
hitCounter = sc.accumulator(0)

def convertToBFS(line):
    #converts lines in Marvel-Graph.txt into BFS nodes
    fields = line.split()                                                       #split each item in line into array indexes
    heroID = int(fields[0])                                                     #takes first ID as the Hero. Rest are associates
    connections = []
    for connection in fields[1:]:
        connections.append(int(connection))                                     #inserts hero's associates into a new array

    color = 'WHITE'
    distance = 9999

    if (heroID == startCharacterID):                                            #change the node for the hard-coded starting character
        color = 'GRAY'
        distance = 0

    return (heroID, (connections, distance, color))                             #returns tuple for each line that associates hero to its associates, a color, and a distance


def createStartingRdd():                                                        #creates rdd using Marvel-Graph.txt and turns each line into a desired node
    inputFile = sc.textFile("C:/Users/Dan/Documents/SparkCourse/Marvel-Graph.txt")
    return inputFile.map(convertToBFS)

def bfsMap(node):                                                               #used in flatMap() operation to return rdd of size M
    characterID = node[0]                                                       #<New_Node> : Unknown associates, distance = distance + 1, Color = Gray (yet to process)
    data = node[1]                                                              #<Old_Node(s)>: color changed to BLACK to show it has been processed
    connections = data[0]
    distance = data[1]
    color = data[2]

    results = []

    #If this node needs to be expanded...
    if (color == 'GRAY'):                                                       #only processes the gray nodes
        for connection in connections:
            newCharacterID = connection
            newDistance = distance + 1
            newColor = 'GRAY'
            if (targetCharacterID == connection):                               #if the targetCharacterID is part of the associates, the accumulator counter is increased. This can happen in each node in the rdd
                hitCounter.add(1)

            newEntry = (newCharacterID, ([], newDistance, newColor))
            results.append(newEntry)

        #We've processed this node, so color it black
        color = 'BLACK'

    #Emit the input node so we don't lose it.
    results.append( (characterID, (connections, distance, color)) )             #the array of nodes being returned results in new lines in the rdd, thereby increasing the length
    return results

def bfsReduce(data1, data2):                                                    #used in a .reduceByKey, so no need to consider the actual CharacterIDs
    edges1 = data1[0]                                                           #takes data from the new Character and original Character
    edges2 = data2[0]                                                           #the original node is largely going to remain the same
    distance1 = data1[1]                                                        #the new nodes made of affiliates to the original node(s) will be combined with their "WHITE" colored node to get the associates
    distance2 = data2[1]
    color1 = data1[2]
    color2 = data2[2]

    distance = 9999
    color = color1                                                              #GRAY
    edges = []

    # See if one is the original node with its connections.
    # If so preserve them.
    if (len(edges1) > 0):
        edges.extend(edges1)
    if (len(edges2) > 0):
        edges.extend(edges2)

    # Preserve minimum distance
    if (distance1 < distance):
        distance = distance1

    if (distance2 < distance):
        distance = distance2

    # Preserve darkest color
    if (color1 == 'WHITE' and (color2 == 'GRAY' or color2 == 'BLACK')):
        color = color2

    if (color1 == 'GRAY' and color2 == 'BLACK'):
        color = color2

    if (color2 == 'WHITE' and (color1 == 'GRAY' or color1 == 'BLACK')):
        color = color1

    if (color2 == 'GRAY' and color1 == 'BLACK'):
        color = color1

    return (edges, distance, color)


#Main program here:
iterationRdd = createStartingRdd()                                              #creates the rdd broken up into nodes

for iteration in range(0, 10):                                                  #for loop to check if
    print("Running BFS iteration# " + str(iteration+1))

    # Create new vertices as needed to darken or reduce distances in the
    # reduce stage. If we encounter the node we're looking for as a GRAY
    # node, increment our accumulator to signal that we're done.
    mapped = iterationRdd.flatMap(bfsMap)                                       #maps each node to the nodes of associates

    # Note that mapped.count() action here forces the RDD to be evaluated, and
    # that's the only reason our accumulator is actually updated.
    print("Processing " + str(mapped.count()) + " values.")                     #this is what makes the previous flatMap(bfsMap) occur. also tells you how much the rdd length increases

    if (hitCounter.value > 0):                                                  #checks if in this iteration, there was a match to the start and end character. The number of iterations needed is the degrees of separation
        print("Hit the target character! From " + str(hitCounter.value) \       #The "hitCounter" value tells us how many ways it reached that degree of separation
            + " different direction(s).")
        break

    # Reducer combines data for each character ID, preserving the darkest
    # color and shortest path.
    iterationRdd = mapped.reduceByKey(bfsReduce)                                #recombines the nodes into a single rdd
