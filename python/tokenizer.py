import logging
import os
from typing import Dict, Optional
from transformers import AutoTokenizer

logger = logging.getLogger(__name__)


class DocumentTokenizer:
    
    def __init__(self, tokenizer_name: Optional[str] = None):
        if tokenizer_name is None:
            tokenizer_name = os.getenv("TOKENIZER_NAME", "bert-base-uncased")
        
        self.tokenizer_name = tokenizer_name
        logger.info(f"Loading tokenizer: {tokenizer_name}")
        
        try:
            self.tokenizer = AutoTokenizer.from_pretrained(tokenizer_name)
            logger.info(f"Successfully loaded tokenizer: {tokenizer_name}")
        except Exception as e:
            logger.error(f"Failed to load tokenizer {tokenizer_name}: {e}")
            raise
    
    def tokenize(self, text: str) -> Dict:
        if not text:
            logger.warning("Empty text provided for tokenization")
            return {"tokens": [], "attention_mask": [], "token_count": 0, "tokenizer_name": self.tokenizer_name}
        
        try:
            encoding = self.tokenizer(
                text,
                return_tensors=None,
                add_special_tokens=True,
                max_length=int(os.getenv("MAX_TOKENS", "512")),
                truncation=True,
                padding=False
            )
            
            return {
                "tokens": encoding.get("input_ids", []),
                "attention_mask": encoding.get("attention_mask", []),
                "token_count": len(encoding.get("input_ids", [])),
                "tokenizer_name": self.tokenizer_name,
                "tokenized": True
            }
        except Exception as e:
            logger.error(f"Error tokenizing text: {e}")
            return {"tokenized": False}
    
    def is_available(self) -> bool:
        return hasattr(self, 'tokenizer') and self.tokenizer is not None


def get_tokenizer(tokenizer_name: Optional[str] = None) -> Optional[DocumentTokenizer]:
    try:
        return DocumentTokenizer(tokenizer_name)
    except Exception as e:
        logger.warning(f"Could not initialize tokenizer: {e}")
        return None

