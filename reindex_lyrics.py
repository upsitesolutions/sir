import logging
import sys
from sqlalchemy import select
# Ensure PYTHONPATH is included if needed, but assuming run from sir root as per existign scripts

try:
    from sir import config
    from sir.indexing import live_index
    from sir.schema import SCHEMA
    from sir.schema.modelext import LocalRecordingLyrics
    from sir.util import db_session
except ImportError:
    print("Error: Could not import 'sir'. Make sure you run this script from the 'sir' repository root and set PYTHONPATH=.")
    sys.exit(1)

logger = logging.getLogger("sir")
logging.basicConfig(level=logging.INFO)

def main():
    # Load configuration
    try:
        config.read_config()
    except Exception as e:
        logger.error(f"Error reading configuration: {e}")
        sys.exit(1)

    # Get the recording model
    try:
        Recording = SCHEMA["recording"].model
    except KeyError:
        logger.error("Recording schema not found.")
        sys.exit(1)

    logger.info("Fetching recordings with lyrics overrides...")

    session_factory = db_session()
    with session_factory() as session:
        # Query to find Recording IDs that have corresponding lyrics in LocalRecordingLyrics
        # We assume LocalRecordingLyrics.lyrics_original is not null/empty implies it has lyrics
        # or just existence of the row if that's the criteria. 
        # The request said "reindex all recordings that have lyrics".
        
        try:
            stmt = select(Recording.id).\
                join(LocalRecordingLyrics, Recording.gid == LocalRecordingLyrics.recording_gid).\
                where(LocalRecordingLyrics.lyrics_original != None)
            
            result = session.execute(stmt).scalars().all()
            recording_ids = set(result)
        except Exception as e:
            logger.error(f"Database query failed: {e}")
            sys.exit(1)

    if not recording_ids:
        logger.info("No recordings found with lyrics overrides.")
        return

    logger.info(f"Found {len(recording_ids)} recordings to reindex.")
    
    # Trigger live index
    try:
        logger.info("Starting live reindex...")
        live_index({"recording": recording_ids})
        logger.info("Reindexing complete.")
    except Exception as e:
        logger.error(f"Reindexing failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
