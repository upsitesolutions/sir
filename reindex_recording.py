import argparse
import sys
import uuid
import logging
from sqlalchemy import select

# This assumes the script is run from the sir root directory
# Ensure PYTHONPATH is set to include the current directory, e.g. PYTHONPATH=.
try:
    from sir import config
    from sir.indexing import live_index
    from sir.schema import SCHEMA
    from sir.util import db_session
except ImportError:
    print("Error: Could not import 'sir'. Make sure you run this script from the 'sir' repository root and set PYTHONPATH=.")
    sys.exit(1)

logger = logging.getLogger("sir")
logging.basicConfig(level=logging.INFO)

def main():
    parser = argparse.ArgumentParser(description="Reindex a single recording by GID.")
    parser.add_argument("gid", type=str, help="Recording GID (UUID)")
    args = parser.parse_args()

    # Load configuration
    try:
        config.read_config()
    except Exception as e:
        logger.error(f"Error reading configuration: {e}")
        sys.exit(1)

    try:
        recording_gid = uuid.UUID(args.gid)
    except ValueError:
        logger.error(f"Invalid UUID: {args.gid}")
        sys.exit(1)

    # Get the recording model
    try:
        recording_model = SCHEMA["recording"].model
    except KeyError:
        logger.error("Recording schema not found.")
        sys.exit(1)

    # Resolve GID to ID
    session_factory = db_session()
    with session_factory() as session:
        # Assuming recording_model has 'id' and 'gid' attributes
        # We need to construct the query correctly. 
        # SCHEMA['recording'].model should be the mapped class.
        
        # Using filter_by is often simpler if attributes match
        # or just model.gid == recording_gid
        
        try:
            stmt = select(recording_model.id).where(recording_model.gid == recording_gid)
            result = session.execute(stmt).scalar_one_or_none()
        except Exception as e:
             logger.error(f"Database query failed: {e}")
             sys.exit(1)
        
        if result is None:
            logger.error(f"Recording with GID {args.gid} not found.")
            sys.exit(1)
        
        recording_id = result
        logger.info(f"Found recording ID: {recording_id} for GID: {recording_gid}")

    logger.info(f"Starting live reindex for recording {recording_id}...")
    
    # Trigger live index
    # Note: live_index handles Solr connection and commit
    try:
        live_index({"recording": {recording_id}})
        logger.info("Reindexing complete.")
    except Exception as e:
        logger.error(f"Reindexing failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
