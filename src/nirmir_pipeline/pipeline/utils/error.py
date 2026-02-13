
class PipelineError(Exception):
    "Base class for all pipeline-raised errors"

class ConfigError(PipelineError, ValueError):
    "Raised when the congif is missing or failed to load"

class ValidationError(PipelineError, ValueError):
    "Raised when validation is failed"