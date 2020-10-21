import pymongo

client = pymongo.MongoClient('mongodb://localhost:27017/')
client.start_session()
collection = client['pset2']['reddit_posts']

test = collection.find(
    {'subreddit': "politics"}
)

for i in test:
    print(i)



