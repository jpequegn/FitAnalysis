import os
import json
from typing import Dict, Any, Optional, List
from dataclasses import dataclass, asdict
from pathlib import Path
import logging

try:
    import yaml
    HAS_YAML = True
except ImportError:
    HAS_YAML = False

logger = logging.getLogger(__name__)


@dataclass
class DatabaseConfig:
    """Database configuration settings."""
    path: str = 'fit_metadata.db'
    read_only: bool = False


@dataclass
class GarminConfig:
    """Garmin Connect API configuration."""
    email: Optional[str] = None
    password: Optional[str] = None
    rate_limit_delay: float = 1.0
    max_retries: int = 3


@dataclass
class WebConfig:
    """Web API configuration."""
    host: str = '127.0.0.1'
    port: int = 8000
    max_file_size: int = 100 * 1024 * 1024  # 100MB
    allowed_extensions: Optional[List[str]] = None
    temp_dir: Optional[str] = None

    def __post_init__(self):
        if self.allowed_extensions is None:
            self.allowed_extensions = ['.fit']


@dataclass
class LoggingConfig:
    """Logging configuration."""
    level: str = 'INFO'
    format: str = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    file_path: Optional[str] = None


@dataclass
class FitAnalysisConfig:
    """Main configuration class."""
    database: DatabaseConfig
    garmin: GarminConfig
    web: WebConfig
    logging: LoggingConfig

    @classmethod
    def from_dict(cls, config_dict: Dict[str, Any]) -> 'FitAnalysisConfig':
        """Create config from a dictionary."""
        return cls(
            database=DatabaseConfig(**config_dict.get('database', {})),
            garmin=GarminConfig(**config_dict.get('garmin', {})),
            web=WebConfig(**config_dict.get('web', {})),
            logging=LoggingConfig(**config_dict.get('logging', {}))
        )

    @classmethod
    def from_file(cls, config_path: str) -> 'FitAnalysisConfig':
        """Load configuration from a file."""
        config_path = Path(config_path)
        
        if not config_path.exists():
            logger.warning(f"Config file {config_path} not found. Using default configuration.")
            return cls.default()
        
        try:
            with open(config_path, 'r') as f:
                if config_path.suffix.lower() in ['.yaml', '.yml']:
                    if not HAS_YAML:
                        raise ValueError("PyYAML is required to load YAML config files. Install with: pip install pyyaml")
                    config_dict = yaml.safe_load(f)
                elif config_path.suffix.lower() == '.json':
                    config_dict = json.load(f)
                else:
                    raise ValueError(f"Unsupported config file format: {config_path.suffix}")
            
            return cls.from_dict(config_dict)
        
        except Exception as e:
            logger.error(f"Error loading config file {config_path}: {e}")
            logger.info("Using default configuration.")
            return cls.default()

    @classmethod
    def from_env(cls) -> 'FitAnalysisConfig':
        """Load configuration from environment variables."""
        config_dict = {
            'database': {
                'path': os.getenv('FITANALYSIS_DB_PATH', 'fit_metadata.db'),
                'read_only': os.getenv('FITANALYSIS_DB_READ_ONLY', 'false').lower() == 'true'
            },
            'garmin': {
                'email': os.getenv('GARMIN_EMAIL'),
                'password': os.getenv('GARMIN_PASSWORD'),
                'rate_limit_delay': float(os.getenv('GARMIN_RATE_LIMIT_DELAY', '1.0')),
                'max_retries': int(os.getenv('GARMIN_MAX_RETRIES', '3'))
            },
            'web': {
                'host': os.getenv('FITANALYSIS_WEB_HOST', '127.0.0.1'),
                'port': int(os.getenv('FITANALYSIS_WEB_PORT', '8000')),
                'max_file_size': int(os.getenv('FITANALYSIS_MAX_FILE_SIZE', str(100 * 1024 * 1024))),
                'temp_dir': os.getenv('FITANALYSIS_TEMP_DIR'),
                'allowed_extensions': os.getenv('FITANALYSIS_ALLOWED_EXTENSIONS', '.fit').split(',')
            },
            'logging': {
                'level': os.getenv('FITANALYSIS_LOG_LEVEL', 'INFO'),
                'format': os.getenv('FITANALYSIS_LOG_FORMAT', '%(asctime)s - %(name)s - %(levelname)s - %(message)s'),
                'file_path': os.getenv('FITANALYSIS_LOG_FILE')
            }
        }
        
        return cls.from_dict(config_dict)

    @classmethod
    def default(cls) -> 'FitAnalysisConfig':
        """Create a default configuration."""
        return cls(
            database=DatabaseConfig(),
            garmin=GarminConfig(),
            web=WebConfig(),
            logging=LoggingConfig()
        )

    def to_dict(self) -> Dict[str, Any]:
        """Convert the configuration to a dictionary."""
        return asdict(self)

    def save_to_file(self, config_path: str) -> None:
        """Save the configuration to a file."""
        config_path = Path(config_path)
        config_dict = self.to_dict()
        
        try:
            with open(config_path, 'w') as f:
                if config_path.suffix.lower() in ['.yaml', '.yml']:
                    if not HAS_YAML:
                        raise ValueError("PyYAML is required to save YAML config files. Install with: pip install pyyaml")
                    yaml.dump(config_dict, f, default_flow_style=False)
                elif config_path.suffix.lower() == '.json':
                    json.dump(config_dict, f, indent=2)
                else:
                    raise ValueError(f"Unsupported config file format: {config_path.suffix}")
            
            logger.info(f"Configuration saved to {config_path}")
        
        except Exception as e:
            logger.error(f"Error saving config file {config_path}: {e}")
            raise


class ConfigManager:
    """Configuration manager for the application."""
    
    def __init__(self, config_path: Optional[str] = None):
        self.config_path = config_path
        self._config: Optional[FitAnalysisConfig] = None
    
    def load(self) -> FitAnalysisConfig:
        """Load configuration from file or environment."""
        if self._config is not None:
            return self._config
        
        if self.config_path:
            self._config = FitAnalysisConfig.from_file(self.config_path)
        else:
            possible_paths = [
                'fitanalysis.yaml',
                'fitanalysis.yml',
                'fitanalysis.json',
                'config/fitanalysis.yaml',
                'config/fitanalysis.yml',
                'config/fitanalysis.json',
                os.path.expanduser('~/.fitanalysis.yaml'),
                os.path.expanduser('~/.fitanalysis.yml'),
                os.path.expanduser('~/.fitanalysis.json')
            ]
            
            for path in possible_paths:
                if os.path.exists(path):
                    logger.info(f"Found config file at {path}, loading.")
                    self._config = FitAnalysisConfig.from_file(path)
                    break
            else:
                logger.info("No config file found. Loading from environment variables.")
                self._config = FitAnalysisConfig.from_env()
        
        return self._config
    
    def get(self) -> FitAnalysisConfig:
        """Get the current configuration."""
        if self._config is None:
            self._config = self.load()
        return self._config
    
    def reload(self) -> FitAnalysisConfig:
        """Reload the configuration."""
        self._config = None
        return self.load()


# Global config manager instance
config_manager = ConfigManager()


def get_config() -> FitAnalysisConfig:
    """Get the current configuration."""
    return config_manager.get()


def setup_logging(config: Optional[FitAnalysisConfig] = None) -> None:
    """Set up logging based on the configuration."""
    if config is None:
        config = get_config()
    
    log_level = getattr(logging, config.logging.level.upper(), logging.INFO)
    
    logging.basicConfig(
        level=log_level,
        format=config.logging.format,
        filename=config.logging.file_path,
        filemode='a' if config.logging.file_path else None
    )
    
    logger.info(f"Logging configured with level: {config.logging.level}")



