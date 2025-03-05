from abc import ABC, abstractmethod
from typing import Dict, Any, List, Tuple
import logging

class SentimentAnalyzer(ABC):
    """
    Abstract base class for sentiment analysis.
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the sentiment analyzer.
        
        Args:
            config: Configuration parameters
        """
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__)
    
    @abstractmethod
    def analyze(self, text: str) -> Dict[str, Any]:
        """
        Analyze the sentiment of a text.
        
        Args:
            text: Text to analyze
            
        Returns:
            Dictionary containing sentiment analysis results
        """
        pass
    
    @abstractmethod
    def batch_analyze(self, texts: List[str]) -> List[Dict[str, Any]]:
        """
        Analyze the sentiment of multiple texts.
        
        Args:
            texts: List of texts to analyze
            
        Returns:
            List of dictionaries containing sentiment analysis results
        """
        pass
    
    def extract_keywords(self, text: str, max_keywords: int = 5) -> List[str]:
        """
        Extract key phrases from text.
        
        Args:
            text: Text to analyze
            max_keywords: Maximum number of keywords to extract
            
        Returns:
            List of extracted keywords
        """
        # Default implementation - should be overridden by subclasses
        return []


class TransformerSentimentAnalyzer(SentimentAnalyzer):
    """
    Sentiment analyzer using Hugging Face Transformers.
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the transformer-based sentiment analyzer.
        
        Args:
            config: Configuration parameters including model name
        """
        super().__init__(config)
        self.model_name = config.get('model_name', 'distilbert-base-uncased-finetuned-sst-2-english')
        self.tokenizer = None
        self.model = None
        self.load_model()
    
    def load_model(self):
        """
        Load the transformer model and tokenizer.
        """
        try:
            from transformers import AutoModelForSequenceClassification, AutoTokenizer
            
            self.tokenizer = AutoTokenizer.from_pretrained(self.model_name)
            self.model = AutoModelForSequenceClassification.from_pretrained(self.model_name)
            self.logger.info(f"Loaded sentiment analysis model: {self.model_name}")
        except Exception as e:
            self.logger.error(f"Failed to load sentiment analysis model: {str(e)}")
    
    def analyze(self, text: str) -> Dict[str, Any]:
        """
        Analyze the sentiment of a text using the transformer model.
        
        Args:
            text: Text to analyze
            
        Returns:
            Dictionary containing sentiment analysis results
        """
        if not self.model or not self.tokenizer:
            self.logger.error("Model not loaded")
            return {'sentiment': 'neutral', 'score': 0.5}
            
        try:
            import torch
            
            # Tokenize and predict
            inputs = self.tokenizer(text, return_tensors="pt", truncation=True, max_length=512)
            with torch.no_grad():
                outputs = self.model(**inputs)
                
            # Get probabilities
            probs = torch.nn.functional.softmax(outputs.logits, dim=-1)
            
            # For binary sentiment (positive/negative)
            if probs.shape[1] == 2:
                negative_prob = probs[0][0].item()
                positive_prob = probs[0][1].item()
                
                if positive_prob > negative_prob:
                    sentiment = 'positive'
                    score = positive_prob
                else:
                    sentiment = 'negative'
                    score = negative_prob
            # For 3-class sentiment (positive/neutral/negative)
            elif probs.shape[1] == 3:
                negative_prob = probs[0][0].item()
                neutral_prob = probs[0][1].item()
                positive_prob = probs[0][2].item()
                
                if positive_prob > neutral_prob and positive_prob > negative_prob:
                    sentiment = 'positive'
                    score = positive_prob
                elif negative_prob > neutral_prob and negative_prob > positive_prob:
                    sentiment = 'negative'
                    score = negative_prob
                else:
                    sentiment = 'neutral'
                    score = neutral_prob
            else:
                sentiment = 'unknown'
                score = 0.0
                
            # Extract keywords
            keywords = self.extract_keywords(text)
                
            return {
                'sentiment': sentiment,
                'score': score,
                'keywords': keywords
            }
        except Exception as e:
            self.logger.error(f"Error analyzing sentiment: {str(e)}")
            return {'sentiment': 'neutral', 'score': 0.5, 'keywords': []}
    
    def batch_analyze(self, texts: List[str]) -> List[Dict[str, Any]]:
        """
        Analyze the sentiment of multiple texts.
        
        Args:
            texts: List of texts to analyze
            
        Returns:
            List of dictionaries containing sentiment analysis results
        """
        return [self.analyze(text) for text in texts]
    
    def extract_keywords(self, text: str, max_keywords: int = 5) -> List[str]:
        """
        Extract key phrases from text using a keyword extraction model.
        
        Args:
            text: Text to analyze
            max_keywords: Maximum number of keywords to extract
            
        Returns:
            List of extracted keywords
        """
        try:
            from keybert import KeyBERT
            
            # Initialize KeyBERT model if not already done
            if not hasattr(self, 'keyword_model'):
                self.keyword_model = KeyBERT()
                
            # Extract keywords
            keywords = self.keyword_model.extract_keywords(text, keyphrase_ngram_range=(1, 2), stop_words='english', top_n=max_keywords)
            
            # Return just the keywords (not the scores)
            return [keyword for keyword, _ in keywords]
        except Exception as e:
            self.logger.error(f"Error extracting keywords: {str(e)}")
            return [] 