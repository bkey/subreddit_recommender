import praw
import re
from neo4jrestclient import client, constants
from neo4jrestclient.client import GraphDatabase
from collections import defaultdict

r_regex = re.compile('/r/([A-Za-z0-9\-\_]+)')

def get_random_nodes(num):
    q = "MATCH (n) WITH n WHERE rand() < 0.05 RETURN n LIMIT %d" % (num)
    result = gdb.query(q=q, returns=(client.Node))
    n = [r[0] for r in result[0:num]]
    return n

def parse_for_subreddits(text,link_dict):
    if text == None:
        return

    text = text.encode('ascii', 'ignore')
    for link in r_regex.finditer(text):
        try:
            n2_name = str(link.group(1)).lower()
        except:
            continue
                
        link_dict[n2_name] += 1

def scrape_subreddit(wrapper,name,limit):
    
    name = str(name).lower()
    
    #temp, don't go too far down the rabbit hole
    if limit > 50:
        return None
    
    #already visited subreddit
    q = "MATCH (n {r_name:\"%s\" }) RETURN n" % (name)
    result = gdb.query(q=q, returns=(client.Node))
    
    if len(result) == 1:
        return result[0][0]
    
    print 'subreddit ' +  name
    
    sr = wrapper.get_subreddit(name)
    
    try:
        t = sr.subreddit_type
    except:
        #some other weird error
        return None
    
    if t == 'public':
        
        reddit_node = gdb.nodes.create(r_name=name, desc=sr.description, num_subscribers=sr.subscribers)
        subreddits.add(reddit_node)
        
        desc = sr.description
        #header = sr.get_stylesheet()
        
        #create dict to remove dupes
        link_dict = defaultdict(int)
        
        parse_for_subreddits(desc,link_dict)
        #parse_for_subreddits(header,link_dict)

        for k,v in link_dict.iteritems():   
            if k == name:
                continue
                
            n2 = scrape_subreddit(wrapper,k,limit+1)
            if n2 != None: 
                try:
                    reddit_node.relationships.create("links_to", n2)
                except:
                    #most likely a node linking to itself
                    pass
        
        return reddit_node
    else:
        return None

def get_public_history(wrap,username):
    user_reddits = defaultdict(int)
    
    try:
        user = wrap.get_redditor(username)
    except:
        return user_reddits

    #get last 50 submissons
    submitted = user.get_submitted(limit=50)
    for sub in submitted:
        user_reddits[sub.subreddit.display_name] += sub.score
    
    #get last 50 comments
    commented = user.get_comments(limit=50)
    for comment in commented:
        user_reddits[comment.subreddit.display_name] += comment.score

    return user_reddits

def get_user_subs(wrap,username):

    username = username.lower()
    q = "MATCH (n {username:\"%s\" }) RETURN n" % (username)
    result = gdb.query(q=q, returns=(client.Node))

    if len(result) <= 0:
        user = gdb.nodes.create(username=username)
        user.labels.add("redditor")
    else:
        return
    
    print 'user ' + username

    d = get_public_history(wrap, username)

    q = "MATCH (n {r_name:\"%s\" }) RETURN n";
    for k,v in d.iteritems():
        k = k.encode('ascii', 'ignore')
        query = q % (k)
        results = gdb.query(query, returns=(client.Node))
        if len(results) <= 0:
            n1 = scrape_subreddit(wrap,k,0)
        else:
            n1 = results[0][0]
    
        if n1 <> None:
            user.relationships.create("subscribes_to", n1, karma=v)

def get_list_of_users(wrap,num):
    usernames = defaultdict(int)

    for i in range(num):
    	res = wrap.get_random_subreddit()
    	r = str(res).lower()
        try:
    	    submissions = wrap.get_subreddit(r).get_top(limit=20)
        except:
            #can't get data
            return usernames

        #gets a random commenter from comments
    	for sub in submissions:
	    try:
                usernames[sub.author.name] += 1
            except:
                return usernames
            comments = sub.comments
            for c in comments: 
                try:
            	    author = c.author 
                    usernames[author.name] += 1
                except:
                    continue
    return usernames

if __name__ == "__main__": 

    gdb = GraphDatabase("http://localhost:7474/db/data/")

    subreddits = gdb.labels.create("subreddits")
    redditors = gdb.labels.create("redditors")

    gdb.relationships.indexes.create("links_to")
    gdb.relationships.indexes.create("subscribes_to")

    wrap = praw.Reddit('subredditor suggestor by bkey23')

    usernames = get_list_of_users(wrap,100)

    print "number of users to add %d" % (len(usernames))

    for user,value in usernames.iteritems():
        user = user.lower()
    	get_user_subs(wrap,user)


