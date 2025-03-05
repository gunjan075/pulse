import tweepy
from typing import Dict, List, Optional, Any
import time
from ..base_collector import BaseCollector

class TwitterCollector(BaseCollector):
    """
    Collector for Twitter/X data using the Twitter API.
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the Twitter collector.
        
        Args:
            config: Dictionary containing Twitter API credentials and settings
        """
        super().__init__(config)
        self.api_key = config.get('api_key')
        self.api_secret = config.get('api_secret')
        self.access_token = config.get('access_token')
        self.access_token_secret = config.get('access_token_secret')
        self.bearer_token = config.get('bearer_token')
        self.client = None
        
    def connect(self) -> bool:
        """
        Connect to the Twitter API.
        
        Returns:
            bool: True if connection is successful, False otherwise
        """
        try:
            # Set up authentication
            if self.bearer_token:
                self.client = tweepy.Client(bearer_token=self.bearer_token)
            else:
                auth = tweepy.OAuth1UserHandler(
                    self.api_key, self.api_secret,
                    self.access_token, self.access_token_secret
                )
                self.client = tweepy.API(auth)
            
            # Test connection
            self.client.get_me()
            self.logger.info("Successfully connected to Twitter API")
            return True
        except Exception as e:
            self.logger.error(f"Failed to connect to Twitter API: {str(e)}")
            return False
    
    def collect(self, keyword: str, limit: Optional[int] = 100) -> List[Dict[str, Any]]:
        """
        Collect tweets containing the specified keyword.
        
        Args:
            keyword: The keyword to search for
            limit: Maximum number of tweets to collect (default: 100)
            
        Returns:
            List of collected tweets as dictionaries
        """
        if not self.client:
            self.connect()
            
        try:
            tweets = []
            # Use Twitter API v2 search
            response = self.client.search_recent_tweets(
                query=keyword,
                max_results=min(limit, 100),
                tweet_fields=['created_at', 'author_id', 'public_metrics']
            )
            
            if response.data:
                for tweet in response.data:
                    tweets.append({
                        'id': tweet.id,
                        'content': tweet.text,
                        'timestamp': tweet.created_at.isoformat(),
                        'author': tweet.author_id,
                        'url': f"https://twitter.com/user/status/{tweet.id}",
                        'metadata': {
                            'retweets': tweet.public_metrics.get('retweet_count', 0),
                            'likes': tweet.public_metrics.get('like_count', 0),
                            'replies': tweet.public_metrics.get('reply_count', 0)
                        }
                    })
            
            return self.filter_by_keywords(tweets)
        except Exception as e:
            self.logger.error(f"Error collecting tweets for keyword '{keyword}': {str(e)}")
            return []
    
    def stream(self, keywords: List[str]) -> Any:
        """
        Create a streaming connection for real-time tweet collection.
        
        Args:
            keywords: List of keywords to track
            
        Returns:
            Stream object
        """
        class TweetListener(tweepy.StreamingClient):
            def __init__(self, bearer_token, parent):
                super().__init__(bearer_token)
                self.parent = parent
                
            def on_tweet(self, tweet):
                processed_tweet = {
                    'id': tweet.id,
                    'content': tweet.text,
                    'timestamp': tweet.created_at.isoformat(),
                    'author': tweet.author_id,
                    'url': f"https://twitter.com/user/status/{tweet.id}",
                    'metadata': {}
                }
                # Process the tweet (e.g., send to Kafka)
                self.parent.logger.info(f"Received tweet: {tweet.id}")
                return True
                
            def on_error(self, status):
                self.parent.logger.error(f"Stream error: {status}")
                if status == 420:  # Rate limit
                    return False  # Stop the stream
        
        try:
            stream = TweetListener(self.bearer_token, self)
            
            # Clear existing rules
            rules = stream.get_rules()
            if rules.data:
                rule_ids = [rule.id for rule in rules.data]
                stream.delete_rules(rule_ids)
            
            # Add new rules based on keywords
            for keyword in keywords:
                stream.add_rules(tweepy.StreamRule(keyword))
            
            # Start streaming
            stream.filter(tweet_fields=['created_at', 'author_id'])
            return stream
        except Exception as e:
            self.logger.error(f"Error setting up Twitter stream: {str(e)}")
            return None 