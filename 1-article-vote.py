import time
import redis

ONE_WEEK_IN_SECONDS = 7 * 86400
VOTE_SCORE = 432
ARTICLES_PER_PAGE = 25
SCORE_KEY = "score:"
ARTICLE_KEY = "article:"
VOTE_KEY= "voted:"
GROUP_KEY="group:"

def article_vote(conn, user, article):
    cut_off = time.Time() - ONE_WEEK_IN_SECONDS
    if conn.zscore('time',article) < cut_off:
        return
    
    article_id = article.partition(':')[-1]
    if conn.sadd(VOTE_KEY + article_id, user):
        conn.zincrby(SCORE_KEY, article, VOTE_SCORE)
        conn.hincrby(article,'votes', 1)
        

def post_article(conn, user, title, link):
    article_id = str(conn.incr(ARTICLE_KEY))
    voted = VOTE_KEY + article_id
    conn.sadd(voted, user)
    conn.expire(voted, ONE_WEEK_IN_SECONDS)
    
    now = time.time()
    article = ARTICLE_KEY+article_id
    conn.hmset(article,{
        'title':title,
        'link':link,
        'poster':user,
        'time':now,
        'votes': 1
    })        
    
    conn.zadd(ARTICLE_KEY, article, now + VOTE_SCORE)
    conn.zadd('time:', article, now)
    
    return article_id


def get_articles(conn, page, order='score:'):
    start = (page-1) * ARTICLES_PER_PAGE
    end = start + ARTICLES_PER_PAGE -1
    
    ids = conn.zrevrange(order, start, end)
    articles = []
    for id in ids:
        article_data = conn.hgetall(id)
        article_data['id'] = id
        articles.append(article_data)
    
    return articles


def add_remove_groups(conn, artcile_id, to_add=[], to_remove=[]):
    article = ARTICLE_KEY+artcile_id
    for group in to_add:
        conn.sadd(GROUP_KEY+group, article)
    for group in to_remove:
        conn.srem(GROUP_KEY+group,article)  
        

def get_group_articles(conn, group, page, order='score:'):
    key = order + group
    if not conn.exists(key):
        conn.zinterstore(key,[GROUP_KEY+group,order],aggregate='max')
        conn.expire(key, 60)
    
    return get_articles(conn, page, key) 


conn = redis.Redis()                