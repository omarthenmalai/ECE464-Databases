import sys
import praw
import pymongo
from praw.models.comment_forest import CommentForest
import api_keys


def main(argv, argc):
    if argc < 3:
        print("Usage: python scrape_posts.py <subreddits (comma-separated)> <number_of_posts>")
        exit(1)

    client = pymongo.MongoClient('mongodb://localhost:27017/')
    client.start_session()
    collection = client['pset2']['reddit_posts']

    # Delete collection so that it can be repopulated with the latest posts
    collection.delete_many({})

    # Scrape the desired number of posts
    scrape_posts(collection, argv[1].split(','), int(argv[2]))

    exit(0)


def scrape_posts(collection, subreddits, num_posts):
    reddit = praw.Reddit(client_id=api_keys.client_id, client_secret=api_keys.client_secret,
                         user_agent=api_keys.user_agent)

    print('test')

    depth = 5

    for subreddit_name in subreddits:
        subreddit = reddit.subreddit(subreddit_name)
        hot_posts = subreddit.hot(limit=num_posts)

        # loop through the hot posts for the given subreddit
        for post in hot_posts:
            # Add post information to the document
            document = get_post_info(subreddit_name, post)

            # Recursively add comments to the specified depth
            document['comments'] = parse_comments(post.comments, depth, {})

            collection.insert_one(document)
            # with open('test.json', 'w')  as f:
            # json.dump(document, f, indent=4)
    return


def parse_comments(comments, limit, comments_dict):
    for i in range(0, limit):
        # Check if there are any replies to that comment

        if isinstance(comments, CommentForest):
            comments.replace_more(limit=0)
            try:
                comments_dict[('comment_' + str(i))] = comments[i].body
                comments_dict[('replies_' + str(i))] = parse_comments(comments[i].replies, limit, {})
            except IndexError as e:
                continue
        else:
            comments_dict[('comment_' + str(i))] = comments[i].body
    return comments_dict


def get_post_info(subreddit_name, post):
    post_dict = {}
    post_dict['subreddit'] = subreddit_name
    post_dict['title'] = post.title
    post_dict['author'] = post.author.name
    post_dict['score'] = post.score
    post_dict['id'] = post.id
    post_dict['url'] = post.url
    post_dict['selftext'] = post.selftext
    post_dict['num_commments'] = post.num_comments
    post_dict['over_18'] = post.over_18
    return post_dict


if __name__ == "__main__":
    main(sys.argv, len(sys.argv))
