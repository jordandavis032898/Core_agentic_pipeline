"""
Compatibility patch for OpenAI SDK and llama-index-llms-openai version mismatch.

This patch fixes the import error where llama-index-llms-openai 0.3.38
tries to import ResponseTextAnnotationDeltaEvent which doesn't exist in
OpenAI SDK 1.109.1. We create an alias to ResponseOutputTextAnnotationAddedEvent.
"""
import sys

# Patch openai.types.responses to add the missing ResponseTextAnnotationDeltaEvent
# This must be done before llama-index-llms-openai tries to import it
try:
    import openai.types.responses as responses_module
    
    # Check if the old event type is missing
    if not hasattr(responses_module, 'ResponseTextAnnotationDeltaEvent'):
        # Try to use the new event type as an alias
        try:
            from openai.types.responses import ResponseOutputTextAnnotationAddedEvent
            # Create an alias
            responses_module.ResponseTextAnnotationDeltaEvent = ResponseOutputTextAnnotationAddedEvent
        except ImportError:
            # If the new event type doesn't exist either, create a dummy class
            # that matches the expected interface
            class ResponseTextAnnotationDeltaEvent:
                """Compatibility alias for ResponseOutputTextAnnotationAddedEvent"""
                def __init__(self, *args, **kwargs):
                    # Try to initialize with the new event type if available
                    try:
                        from openai.types.responses import ResponseOutputTextAnnotationAddedEvent
                        self._event = ResponseOutputTextAnnotationAddedEvent(*args, **kwargs)
                        # Copy attributes
                        for attr in dir(self._event):
                            if not attr.startswith('_'):
                                setattr(self, attr, getattr(self._event, attr))
                    except (ImportError, TypeError):
                        # Fallback: create minimal dummy
                        self.annotation = kwargs.get('annotation', {})
            
            responses_module.ResponseTextAnnotationDeltaEvent = ResponseTextAnnotationDeltaEvent
except ImportError:
    # openai.types.responses not available, skip patching
    pass
