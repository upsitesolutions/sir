import logging
import sys
from sqlalchemy import select
# Ensure PYTHONPATH is included if needed, but assuming run from sir root as per existign scripts

try:
    from sir import config
    from sir.indexing import live_index
    from sir.schema import SCHEMA
    from sir.schema.modelext import LocalRecordingLyrics, LocalRecordingPreferredKey
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

    logger.info("Fetching recordings with lyrics overrides or preferred keys...")

    session_factory = db_session()
    with session_factory() as session:
        # Query to find Recording IDs that have corresponding lyrics in LocalRecordingLyrics
        try:
            # Fetch recordings with lyrics
            stmt_lyrics = select(Recording.id).\
                join(LocalRecordingLyrics, Recording.gid == LocalRecordingLyrics.recording_gid).\
                where(LocalRecordingLyrics.lyrics_original != None)
            
            result_lyrics = session.execute(stmt_lyrics).scalars().all()
            recording_ids = set(result_lyrics)
            logger.info(f"Found {len(recording_ids)} recordings with lyrics.")

            # Fetch recordings with preferred keys
            # We assume existence in this table means it has a preferred key
            stmt_keys = select(Recording.id).\
                join(LocalRecordingPreferredKey, Recording.gid == LocalRecordingPreferredKey.recording_gid)
            
            result_keys = session.execute(stmt_keys).scalars().all()
            keys_count = len(result_keys)
            logger.info(f"Found {keys_count} recordings with preferred keys.")
            
            recording_ids.update(result_keys)

        except Exception as e:
            logger.error(f"Database query failed: {e}")
            sys.exit(1)

    if not recording_ids:
        logger.info("No recordings found with overrides.")
        return

    logger.info(f"Found {len(recording_ids)} total unique recordings to reindex.")
    
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
