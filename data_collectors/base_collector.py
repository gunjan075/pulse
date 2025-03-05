from abc import ABC, abstractmethod
import logging
from typing import Dict, List, Optional, Any

class BaseCollector(ABC):
    """
    Abstract base class for all data collectors.
    Defines the common interface for collecting data from different sources.
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the collector with configuration.
        
        Args:
            config: Dictionary containing configuration parameters
        """
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__)
        self.keywords = config.get('keywords', [])
        self.rate_limit = config.get('rate_limit', 60)  # Default: 60 requests per minute
        
    @abstractmethod
    def connect(self) -> bool:
        """
        Establish connection to the data source API.
        
        Returns:
            bool: True if connection is successful, False otherwise
        """
        pass
    
    @abstractmethod
    def collect(self, keyword: str, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        Collect data for a specific keyword.
        
        Args:
            keyword: The keyword to search for
            limit: Maximum number of items to collect (optional)
            
        Returns:
            List of collected data items as dictionaries
        """
        pass
    
    @abstractmethod
    def stream(self, keywords: List[str]) -> Any:
        """
        Create a streaming connection for real-time data collection.
        
        Args:
            keywords: List of keywords to track
            
        Returns:
            Stream object or generator
        """
        pass
    
    def preprocess(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Preprocess collected data before sending to Kafka.
        
        Args:
            data: Raw data item
            
        Returns:
            Preprocessed data item
        """
        # Default implementation - can be overridden by subclasses
        return {
            'source': self.__class__.__name__,
            'timestamp': data.get('timestamp', None),
            'content': data.get('content', ''),
            'author': data.get('author', ''),
            'url': data.get('url', ''),
            'metadata': data.get('metadata', {})
        }
    
    def filter_by_keywords(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Filter data items by keywords.
        
        Args:
            data: List of data items
            
        Returns:
            Filtered list of data items
        """
        if not self.keywords:
            return data
            
        filtered_data = []
        for item in data:
            content = item.get('content', '').lower()
            if any(keyword.lower() in content for keyword in self.keywords):
                filtered_data.append(item)
                
        return filtered_data 