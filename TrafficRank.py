from pyspark import SparkContext
sc = SparkContext()

from operator import add

## import the data
rx = sc.textFile("./chicago-taxi-rides.csv")

def compute_contribs(neighbors, rank):
    """
    this function compute the rank that a given area contribute to its neighbors
    Args:
        neighbors: neighbors of a given area
        rank: the current rank of the given area

    Returns:
        ngr[0]: each area (neighbor)
        rank_new: the new rank of each neighbor
    """
    sum_traffic = 0
    for ngr in neighbors:
        sum_traffic += ngr[1]
    for ngr in neighbors:
        rank_new = rank*ngr[1]/sum_traffic
        yield (ngr[0], rank_new)


## remove the head to get the clean RDDs
clean_rx = rx.filter(lambda x: x.split(',')[0].isdigit())

## create a map of pickup and dropoff and traffic for each trip
trips = clean_rx.map(lambda x: (int(x.split(',')[0]), (int(x.split(',')[1]), int(x.split(',')[2]))))

# create a pair RDD (area, [(neighbor_area, neighbor_traffic)])
links = trips.groupByKey().distinct().mapValues(lambda x: list(x)).cache()

# Start each area at a rank of 1 [(14, 1), (29, 1), (54, 1), ...]
ranks = trips.map(lambda x : (x[0], 1)).distinct()



for i in range(20):
    # (url, ()[(neighbors, traffic)], rank)) join neighbor_areas and rank
    # have current area contribute its rank to all of its neighbors
    contribs = links.join(ranks).flatMap(lambda x: compute_contribs(x[1][0], x[1][1]))
    # add the rank for each area after getting the contributions
    ranks = contribs.reduceByKey(add).mapValues(lambda rank : rank * 0.85 + 0.15)

for (link, rank) in ranks.collect():
    print("%s has rank %s." % (link, rank))
