# main.py
import asyncio
import logging
import sys
from pathlib import Path

from core.engine import HybridEngine


def setup_logging(config_path: str = "config/settings.yaml"):
    """Настройка логирования"""
    import yaml
    
    try:
        with open(config_path) as f:
            config = yaml.safe_load(f)
        log_config = config.get('logging', {})
    except FileNotFoundError:
        log_config = {}
    
    level = getattr(
        logging, log_config.get('level', 'INFO')
    )
    fmt = log_config.get(
        'format',
        '%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    )
    
    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(level)
    console_handler.setFormatter(logging.Formatter(fmt))
    
    # File handler
    log_file = log_config.get('file', 'logs/bot.log')
    log_dir = Path(log_file).parent
    log_dir.mkdir(parents=True, exist_ok=True)
    
    from logging.handlers import RotatingFileHandler
    file_handler = RotatingFileHandler(
        log_file,
        maxBytes=log_config.get('max_size_mb', 50) * 1024 * 1024,
        backupCount=log_config.get('backup_count', 5),
    )
    file_handler.setLevel(level)
    file_handler.setFormatter(logging.Formatter(fmt))
    
    # Root logger
    root = logging.getLogger()
    root.setLevel(level)
    root.addHandler(console_handler)
    root.addHandler(file_handler)
    
    # Подавляем шумные логгеры
    logging.getLogger('websocket').setLevel(logging.WARNING)
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    logging.getLogger('httpx').setLevel(logging.WARNING)


def main():
    config_path = "config/settings.yaml"
    
    if len(sys.argv) > 1:
        config_path = sys.argv[1]
    
    setup_logging(config_path)
    
    logger = logging.getLogger(__name__)
    logger.info("Starting Hybrid Trading Bot...")
    
    engine = HybridEngine(config_path)
    
    try:
        asyncio.run(engine.run())
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
    except Exception as e:
        logger.critical(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == '__main__':
    main()
